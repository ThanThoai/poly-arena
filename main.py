import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from database import SessionLocal
from models import BalanceHistory, BinaryOption, Bot, BOResult
from routers import achievements as achievements_router, admin as admin_router, auth, binary_options, bots, dashboard, ws as ws_router, ws_polymarket, futures as futures_router
from services.orderbook_broadcaster import broadcaster
from services.redis_client import get_async_redis, close_async_redis
from config.fees import maker_rebate_from_levels
from services.order_trace import make_trace, append_trace
from services.user_balance import record_user_balance
from ws_feed_service.config import (
    STREAM_BRACKET_EXITS,
    STREAM_ORDER_CANCELS,
    STREAM_ORDER_FILLS,
    STREAM_MARKET_RESOLVED,
)

_log_level = (
    logging.DEBUG
    if os.getenv("DEBUG", "").strip() in ("1", "true", "yes")
    else logging.INFO
)

logging.basicConfig(
    level=_log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)

log = logging.getLogger(__name__)


def _check_achievements(bo, db):
    """Run achievement checkers — failure must never block settlement."""
    try:
        from services.achievements import on_trade_resolved
        on_trade_resolved(bo, db)
    except Exception as exc:
        log.debug("Achievement check failed for BO #%d: %s", bo.id, exc)


def _get_token_id(symbol: str, timeframe: str, pm_status: str):
    """Resolve token_id from Redis price cache (same as router helper)."""
    try:
        from services.redis_client import get_sync_redis
        from ws_feed_service.config import PRICE_KEY_PREFIX
        sr = get_sync_redis()
        key = f"{PRICE_KEY_PREFIX}:{symbol}:{timeframe}:{pm_status}"
        return sr.hget(key, "token_id")
    except Exception:
        return None


def _publish_trace_sync(bo) -> None:
    """Publish the latest trace from a BO to Redis for real-time UI."""
    if not bo.traces:
        return
    try:
        from services.redis_client import get_sync_redis
        from services.order_trace import publish_trace_to_redis
        sr = get_sync_redis()
        publish_trace_to_redis(sr, bo.id, bo.traces[-1])
    except Exception as exc:
        log.debug("Failed to publish trace to Redis for BO #%d: %s", bo.id, exc)


async def _handle_bracket_exit(r, stream, group, msg_id, data) -> None:
    """
    Process a single bracket exit message.

    Full exit (exit_filled >= num_shares):
        Settle immediately — result = WIN/LOSS, balance updated, moved to history.

    Partial exit (exit_filled < num_shares):
        Write exit data only. Scheduler will settle using shadow profit for the
        exited portion + binary formula for the remaining shares at candle close.
    """
    try:
        bo_id = int(data["bo_id"])
        trigger = data["trigger"]
        exit_price = float(data["exit_price"])
        exit_filled = float(data["exit_filled"])
        order_id = data.get("order_id", "")
        exit_at_str = data.get("exit_at", "")

        db = SessionLocal()
        try:
            bo = db.get(BinaryOption, bo_id)
            if bo is None:
                log.warning("Bracket exit: BO #%d not found", bo_id)
            elif bo.exit_trigger is None and bo.result == BOResult.PENDING:
                bo.exit_trigger = trigger
                bo.exit_price = exit_price
                bo.exit_filled = exit_filled
                append_trace(bo, make_trace(
                    "MONITORING", "BRACKET_EXIT",
                    f"Active Monitoring: Best Bid hit {trigger} threshold. "
                    f"Exit Price: ${exit_price:.4f}, Qty: {exit_filled:.4f}.",
                    {"trigger": trigger, "exit_price": exit_price,
                     "exit_filled": exit_filled},
                ))
                bo.exit_at = (
                    datetime.fromisoformat(exit_at_str)
                    if exit_at_str
                    else datetime.now(timezone.utc)
                )
                if order_id and not bo.me_order_id:
                    bo.me_order_id = order_id
                bo.me_order_status = "FILLED"

                # Persist exit walk prices
                wp_str = data.get("walk_prices", "")
                if wp_str:
                    try:
                        exit_levels = json.loads(wp_str)
                        wp = bo.walk_prices or {}
                        wp["exit"] = wp.get("exit", []) + exit_levels
                        bo.walk_prices = wp
                    except (json.JSONDecodeError, TypeError):
                        pass

                num_shares = bo.num_shares or 0.0
                is_full_exit = exit_filled >= num_shares and num_shares > 0

                # Record exit data only — profit is ALWAYS calculated at
                # session end by the scheduler using _settle_single_trade().
                # This ensures consistent settlement timing for all orders.
                if is_full_exit:
                    append_trace(bo, make_trace(
                        "MONITORING", "BRACKET_EXIT_FULL",
                        f"Full bracket exit: {trigger} triggered. "
                        f"Entry: ${bo.avg_price:.4f} → Exit: ${exit_price:.4f}. "
                        f"Shares: {exit_filled:.4f}. Pending session-end settlement.",
                        {"trigger": trigger, "entry_price": bo.avg_price,
                         "exit_price": exit_price, "exit_filled": exit_filled},
                    ))
                else:
                    append_trace(bo, make_trace(
                        "MONITORING", "PARTIAL_EXIT",
                        f"Partial exit: {exit_filled:.4f} / {num_shares:.4f} shares "
                        f"exited via {trigger}. Pending session-end settlement.",
                        {"trigger": trigger, "exit_filled": exit_filled,
                         "num_shares": num_shares},
                    ))

                db.commit()
                _publish_trace_sync(bo)
                log.info(
                    "Bracket exit recorded (deferred settlement): BO #%d trigger=%s "
                    "exit_price=%.6f exit_filled=%.4f / num_shares=%.4f",
                    bo_id, trigger, exit_price, exit_filled, num_shares,
                )
        finally:
            db.close()

        # Step 2: ACK only after successful DB commit
        # If ACK fails, message stays in PEL → _drain_pending() replays on restart
        await r.xack(stream, group, msg_id)
    except Exception as exc:
        # Do NOT ack — message remains in PEL for reprocessing
        log.error("Error processing bracket exit %s: %s", msg_id, exc)


async def _handle_order_cancel(r, stream, group, msg_id, data) -> None:
    """Process a single order cancel message."""
    try:
        bo_id = int(data["bo_id"])
        order_id = data.get("order_id", "")
        reason = data.get("reason", "TTL_EXPIRED")
        filled = float(data.get("filled", 0))
        avg_entry = float(data.get("avg_entry_price", 0))

        db = SessionLocal()
        try:
            bo = db.get(BinaryOption, bo_id)
            if bo is not None and bo.result == BOResult.PENDING:
                if order_id and not bo.me_order_id:
                    bo.me_order_id = order_id

                if filled > 0 and avg_entry > 0:
                    # Preserve original budget before adjustment
                    if bo.original_amount is None:
                        bo.original_amount = bo.amount

                    # Tính phần chưa fill để hoàn lại
                    actual_cost = round(filled * avg_entry, 8)
                    unfilled_refund = round(max(0, bo.amount - actual_cost), 8)

                    # Compute fill breakdown for trace
                    original_budget = bo.original_amount or bo.amount
                    limit_price = bo.limit_price or avg_entry
                    requested_qty = round(original_budget / limit_price, 8) if limit_price > 0 else 0
                    unfilled_qty = round(max(0, requested_qty - filled), 8)

                    # Cập nhật lệnh: chỉ giữ lại phần đã fill
                    bo.avg_price = avg_entry
                    bo.num_shares = filled
                    bo.amount = actual_cost
                    bo.me_order_status = "CANCELED"

                    append_trace(bo, make_trace(
                        "MATCHING", "PARTIAL_FILL_EXPIRY",
                        f"Order expired with partial fill. "
                        f"Filled: {filled:.4f} / {requested_qty:.4f} shares @ ${avg_entry:.4f}. "
                        f"Unfilled: {unfilled_qty:.4f} shares. "
                        f"Unfilled refund: ${unfilled_refund:.4f}.",
                        {"filled": filled, "avg_entry_price": avg_entry,
                         "actual_cost": actual_cost, "unfilled_refund": unfilled_refund,
                         "requested_quantity": requested_qty, "unfilled_quantity": unfilled_qty,
                         "original_amount": original_budget,
                         "reason": reason},
                    ))

                    # Hoàn trả phần chưa fill về balance ngay lập tức
                    if unfilled_refund > 0:
                        bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
                        if bot:
                            bot.balance = round(bot.balance + unfilled_refund, 8)
                            db.add(
                                BalanceHistory(
                                    bot_name=bo.bot_name,
                                    balance=bot.balance,
                                    trade_id=bo.id,
                                )
                            )

                    db.commit()
                    _publish_trace_sync(bo)
                    log.info(
                        "Partial fill expiry: BO #%d filled=%.4f avg=%.6f "
                        "actual_cost=%.2f unfilled_refund=%.2f — kept PENDING for settlement",
                        bo_id,
                        filled,
                        avg_entry,
                        actual_cost,
                        unfilled_refund,
                    )
                else:
                    bo.result = BOResult.CANCELLED
                    bo.profit = 0.0
                    bo.me_order_status = "CANCELED"

                    append_trace(bo, make_trace(
                        "MATCHING", "ORDER_CANCELLED",
                        f"Order cancelled ({reason}). "
                        f"No fills. Refund: ${bo.amount:.4f}.",
                        {"reason": reason, "refund": bo.amount},
                    ))

                    bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
                    if bot:
                        bot.balance = round(bot.balance + bo.amount, 8)
                        db.add(
                            BalanceHistory(
                                bot_name=bo.bot_name,
                                balance=bot.balance,
                                trade_id=bo.id,
                            )
                        )
                    record_user_balance(db, bo.bot_name, trade_id=bo.id, pnl_amount=0.0)

                    db.commit()
                    _publish_trace_sync(bo)
                    log.info(
                        "Order cancelled (zero fill): BO #%d reason=%s refund=%.2f",
                        bo_id,
                        reason,
                        bo.amount,
                    )
            elif bo is None:
                log.warning("Order cancel: BO #%d not found", bo_id)
        finally:
            db.close()

        # Step 2: ACK only after successful DB commit
        # If ACK fails, message stays in PEL → _drain_pending() replays on restart
        await r.xack(stream, group, msg_id)
    except Exception as exc:
        # Do NOT ack — message remains in PEL for reprocessing
        log.error("Error processing order cancel %s: %s", msg_id, exc)


async def _handle_order_fill(r, stream, group, msg_id, data) -> None:
    """
    Process a single order fill message.

    Normal case (candle still open):
        Update avg_price / num_shares / me_order_status. Scheduler settles later.

    Late fill (fill arrives after settlement_at has passed):
        Update fill data then immediately settle using Binance candle, so the
        trade moves out of open positions without waiting for the next scheduler run.
    """
    try:
        bo_id = int(data["bo_id"])
        filled = float(data["filled"])
        avg_entry = float(data["avg_entry_price"])
        status = data.get("status", "")
        order_id = data.get("order_id", "")

        db = SessionLocal()
        try:
            bo = db.get(BinaryOption, bo_id)
            if bo is not None and bo.result == BOResult.PENDING:
                bo.avg_price = avg_entry
                bo.num_shares = filled
                if status in ("PARTIAL", "FILLED"):
                    bo.me_order_status = status
                if order_id and not bo.me_order_id:
                    bo.me_order_id = order_id

                # Persist entry walk prices (append for partial fills)
                wp_str = data.get("walk_prices", "")
                if wp_str:
                    try:
                        new_levels = json.loads(wp_str)
                        wp = bo.walk_prices or {}
                        wp["entry"] = wp.get("entry", []) + new_levels
                        bo.walk_prices = wp
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Fee handling: maker rebate for ALL LIMIT fills from ME.
                # MARKET orders are filled at API level (taker fee deducted there).
                # All ME fills for LIMIT orders are maker fills (order rests on
                # book until matched), including aggressive LIMIT remainders
                # that already have entry_fee > 0 from the REST taker portion.
                fee_applied = 0.0
                is_maker_fill = bo.limit_price is not None
                if wp_str and is_maker_fill:
                    try:
                        fill_levels = json.loads(wp_str)
                        rebate = maker_rebate_from_levels(fill_levels)
                        if rebate > 0:
                            bot = db.query(Bot).filter(
                                Bot.bot_name == bo.bot_name,
                            ).first()
                            if bot:
                                bot.balance = round(bot.balance + rebate, 8)
                            bo.entry_fee = round(
                                (bo.entry_fee or 0) - rebate, 8,
                            )
                        fee_applied = rebate
                    except (json.JSONDecodeError, TypeError, KeyError):
                        pass

                # MATCHING trace
                nominal = round(fee_applied / 0.20, 8) if fee_applied > 0 else 0.0
                if status == "FILLED":
                    append_trace(bo, make_trace(
                        "MATCHING", "ORDER_FILLED",
                        f"Order fully filled. Avg Entry Price: ${avg_entry:.4f}. "
                        f"Shares: {filled:.4f}. "
                        f"Rebate: ${fee_applied:.4f} (MAKER).",
                        {"avg_entry_price": avg_entry, "filled": filled,
                         "status": status, "nominal_fee": nominal,
                         "role": "MAKER", "actual_fee_deducted": 0.0,
                         "rebate_earned": fee_applied},
                    ))
                elif status == "PARTIAL":
                    # Compute requested quantity for trace
                    _orig_budget = bo.original_amount or bo.amount
                    _lp = bo.limit_price or avg_entry
                    _req_qty = round(_orig_budget / _lp, 8) if _lp > 0 else 0
                    append_trace(bo, make_trace(
                        "MATCHING", "PARTIAL_FILL",
                        f"Partial fill: {filled:.4f} / {_req_qty:.4f} shares @ ${avg_entry:.4f}. "
                        f"Rebate: ${fee_applied:.4f} (MAKER).",
                        {"avg_entry_price": avg_entry, "filled": filled,
                         "requested_quantity": _req_qty,
                         "status": status, "nominal_fee": nominal,
                         "role": "MAKER", "actual_fee_deducted": 0.0,
                         "rebate_earned": fee_applied},
                    ))

                db.commit()

                # Publish trace to Redis for real-time UI
                _publish_trace_sync(bo)

                role = "MAKER" if is_maker_fill else "TAKER"
                log.info(
                    "Fill update: BO #%d filled=%.4f avg=%.6f status=%s role=%s fee=%.6f",
                    bo_id,
                    filled,
                    avg_entry,
                    status,
                    role,
                    fee_applied,
                )

                # ── Immediate settlement khi fill đến muộn ────────────────
                # Nếu lệnh đã FILLED và settlement_at đã qua, không cần chờ
                # scheduler — settle ngay để lệnh rời khỏi open positions.
                # Skip if already settled by auto-exit above.
                if (
                    status == "FILLED"
                    and bo.result == BOResult.PENDING
                    and bo.settlement_at is not None
                ):
                    now = datetime.now(timezone.utc)
                    settle_at = bo.settlement_at
                    if settle_at.tzinfo is None:
                        settle_at = settle_at.replace(tzinfo=timezone.utc)
                    if settle_at <= now:
                        from services.settlement import (
                            fetch_binance_candle,
                            _settle_single_trade,
                        )

                        candle = fetch_binance_candle(
                            bo.symbol,
                            bo.timeframe,
                            bo.settlement_at,
                        )
                        if candle is not None:
                            open_price, close_price = candle
                            _settle_single_trade(
                                bo, open_price, close_price, db, tag="LATE_FILL"
                            )
                            # Reconcile balance from DB truth
                            from services.settlement import reconcile_bot_balances
                            reconcile_bot_balances(
                                db,
                                {bo.bot_name},
                                {bo.bot_name: [bo.id]},
                            )
                            db.commit()
                            _check_achievements(bo, db)
                        else:
                            log.warning(
                                "Late fill BO #%d — no candle data, will be caught by sweeper",
                                bo_id,
                            )
        finally:
            db.close()

        # Step 2: ACK only after successful DB commit
        # If ACK fails, message stays in PEL → _drain_pending() replays on restart
        await r.xack(stream, group, msg_id)
    except Exception as exc:
        # Do NOT ack — message remains in PEL for reprocessing
        log.error("Error processing order fill %s: %s", msg_id, exc)


async def _handle_market_resolved(r, stream, group, msg_id, data) -> None:
    """
    Process a market resolution event.

    The matching engine fires this when Polymarket resolves a market.
    However, the ME does NOT know the winning outcome (winning_outcome=""),
    so we CANNOT determine WIN/LOSS here.

    Instead, we record a trace and clear bracket monitoring (TP/SL) so the
    order proceeds to scheduler settlement using Binance candle data — the
    canonical source of truth for this platform.
    """
    try:
        asset_id = data.get("asset_id", "")
        bo_ids_str = data.get("bo_ids", "")

        # Parse bo_ids from stream data
        target_bo_ids: list[int] = []
        if bo_ids_str:
            target_bo_ids = [int(x) for x in bo_ids_str.split(",") if x.strip()]

        if not target_bo_ids:
            log.warning(
                "Market resolved event has no bo_ids — skipping (asset_id=%s)",
                asset_id[:16] if asset_id else "?",
            )
            await r.xack(stream, group, msg_id)
            return

        db = SessionLocal()
        try:
            pending_orders = (
                db.query(BinaryOption)
                .filter(
                    BinaryOption.id.in_(target_bo_ids),
                    BinaryOption.result == BOResult.PENDING,
                    BinaryOption.position_closed.isnot(True),
                )
                .all()
            )

            for bo in pending_orders:
                # Clear bracket TP/SL so ME stops monitoring — the market
                # has resolved, no more price updates will arrive.
                bo.tp_price = None
                bo.sl_price = None
                bo.position_closed = True
                bo.me_order_status = "RESOLVED"

                append_trace(bo, make_trace(
                    "MONITORING", "MARKET_RESOLVED",
                    f"Polymarket Event: market resolved for asset "
                    f"{asset_id[:16]}. Bracket monitoring stopped. "
                    f"Pending session-end settlement via Binance candle.",
                    {"asset_id": asset_id},
                ))

                log.info(
                    "Market resolved recorded (deferred): BO #%d asset=%s",
                    bo.id, asset_id[:16],
                )

            db.commit()
        finally:
            db.close()

        await r.xack(stream, group, msg_id)
    except Exception as exc:
        log.error("Error processing market resolved %s: %s", msg_id, exc)


async def _drain_pending(r, stream: str, group: str, consumer: str, handler) -> int:
    """
    Process any unACKed messages (PEL) left over from a previous crash/restart.
    Returns the number of messages reprocessed.
    """
    count = 0
    while True:
        results = await r.xreadgroup(
            group,
            consumer,
            {stream: "0"},  # "0" reads pending (unACKed) messages
            count=10,
        )
        if not results:
            break
        has_messages = False
        for _stream, messages in results:
            if not messages:
                continue
            has_messages = True
            for msg_id, data in messages:
                await handler(r, stream, group, msg_id, data)
                count += 1
        if not has_messages:
            break
    if count:
        log.info("Drained %d pending message(s) from %s", count, stream)
    return count


async def _consume_bracket_exits() -> None:
    """
    XREADGROUP consumer that reads bracket exit events from Redis Stream
    and updates the corresponding BinaryOption rows in the DB.

    Uses consumer group 'api-workers' with consumer name 'api-{pid}'.
    Idempotent: only writes if bo.exit_trigger is None.
    """
    r = get_async_redis()
    group = "api-workers"
    consumer = f"api-{os.getpid()}"

    # Create consumer group (idempotent)
    try:
        await r.xgroup_create(STREAM_BRACKET_EXITS, group, id="0", mkstream=True)
        log.info(
            "Created consumer group '%s' on stream '%s'", group, STREAM_BRACKET_EXITS
        )
    except Exception:
        pass  # group already exists

    log.info("Bracket exit consumer started: group=%s consumer=%s", group, consumer)

    # Drain any unACKed messages from previous run
    await _drain_pending(r, STREAM_BRACKET_EXITS, group, consumer, _handle_bracket_exit)

    while True:
        try:
            results = await r.xreadgroup(
                group,
                consumer,
                {STREAM_BRACKET_EXITS: ">"},
                count=10,
                block=5000,
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    await _handle_bracket_exit(
                        r, STREAM_BRACKET_EXITS, group, msg_id, data
                    )

        except asyncio.CancelledError:
            log.info("Bracket exit consumer shutting down")
            return
        except Exception as exc:
            log.error("Bracket exit consumer error: %s", exc)
            await asyncio.sleep(1)


async def _consume_order_cancels() -> None:
    """
    XREADGROUP consumer that reads order cancel events from Redis Stream
    and updates the corresponding BinaryOption rows.

    Handles two cases:
      1. Zero fill (filled=0): result → CANCELLED, profit=0
      2. Partial fill (filled>0): update num_shares to actual filled qty,
         keep result PENDING so settlement can resolve it via candle
    """
    r = get_async_redis()
    group = "api-workers"
    consumer = f"api-cancel-{os.getpid()}"

    try:
        await r.xgroup_create(STREAM_ORDER_CANCELS, group, id="0", mkstream=True)
        log.info(
            "Created consumer group '%s' on stream '%s'", group, STREAM_ORDER_CANCELS
        )
    except Exception:
        pass  # group already exists

    log.info("Order cancel consumer started: group=%s consumer=%s", group, consumer)

    # Drain any unACKed messages from previous run
    await _drain_pending(r, STREAM_ORDER_CANCELS, group, consumer, _handle_order_cancel)

    while True:
        try:
            results = await r.xreadgroup(
                group,
                consumer,
                {STREAM_ORDER_CANCELS: ">"},
                count=10,
                block=5000,
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    await _handle_order_cancel(
                        r, STREAM_ORDER_CANCELS, group, msg_id, data
                    )

        except asyncio.CancelledError:
            log.info("Order cancel consumer shutting down")
            return
        except Exception as exc:
            log.error("Order cancel consumer error: %s", exc)
            await asyncio.sleep(1)


async def _consume_order_fills() -> None:
    """
    XREADGROUP consumer that reads fill update events from Redis Stream
    and syncs avg_price / num_shares to the DB as orders partially fill.

    This ensures the DB always reflects the matching engine's actual fill
    state, not just the initial estimate from order creation.
    """
    r = get_async_redis()
    group = "api-workers"
    consumer = f"api-fill-{os.getpid()}"

    try:
        await r.xgroup_create(STREAM_ORDER_FILLS, group, id="0", mkstream=True)
        log.info(
            "Created consumer group '%s' on stream '%s'", group, STREAM_ORDER_FILLS
        )
    except Exception:
        pass

    log.info("Order fill consumer started: group=%s consumer=%s", group, consumer)

    # Drain any unACKed messages from previous run
    await _drain_pending(r, STREAM_ORDER_FILLS, group, consumer, _handle_order_fill)

    while True:
        try:
            results = await r.xreadgroup(
                group,
                consumer,
                {STREAM_ORDER_FILLS: ">"},
                count=10,
                block=5000,
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    await _handle_order_fill(r, STREAM_ORDER_FILLS, group, msg_id, data)

        except asyncio.CancelledError:
            log.info("Order fill consumer shutting down")
            return
        except Exception as exc:
            log.error("Order fill consumer error: %s", exc)
            await asyncio.sleep(1)


async def _consume_market_resolved() -> None:
    """
    XREADGROUP consumer for market resolution events (v2 spec Section 5).

    When Polymarket resolves a market, determines oracle payout and
    updates all affected orders.
    """
    r = get_async_redis()
    group = "api-workers"
    consumer = f"api-resolved-{os.getpid()}"

    try:
        await r.xgroup_create(STREAM_MARKET_RESOLVED, group, id="0", mkstream=True)
        log.info(
            "Created consumer group '%s' on stream '%s'", group, STREAM_MARKET_RESOLVED
        )
    except Exception:
        pass

    log.info("Market resolved consumer started: group=%s consumer=%s", group, consumer)
    await _drain_pending(r, STREAM_MARKET_RESOLVED, group, consumer, _handle_market_resolved)

    while True:
        try:
            results = await r.xreadgroup(
                group,
                consumer,
                {STREAM_MARKET_RESOLVED: ">"},
                count=10,
                block=5000,
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    await _handle_market_resolved(
                        r, STREAM_MARKET_RESOLVED, group, msg_id, data
                    )

        except asyncio.CancelledError:
            log.info("Market resolved consumer shutting down")
            return
        except Exception as exc:
            log.error("Market resolved consumer error: %s", exc)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start bracket exit consumer (reads from Redis Stream)
    consumer_task = asyncio.create_task(
        _consume_bracket_exits(),
        name="bracket-exit-consumer",
    )
    # Start order cancel consumer (reads from Redis Stream)
    cancel_task = asyncio.create_task(
        _consume_order_cancels(),
        name="order-cancel-consumer",
    )
    # Start order fill consumer (syncs partial fills to DB)
    fill_task = asyncio.create_task(
        _consume_order_fills(),
        name="order-fill-consumer",
    )
    # Start market resolved consumer (v2 spec Section 5)
    resolved_task = asyncio.create_task(
        _consume_market_resolved(),
        name="market-resolved-consumer",
    )

    # Start orderbook broadcaster (single Redis pub/sub for all WS clients)
    await broadcaster.start()

    # Seed achievement definitions
    try:
        from services.achievement_seeder import seed_achievements
        _db = SessionLocal()
        try:
            seed_achievements(_db)
        finally:
            _db.close()
    except Exception as exc:
        log.warning("Failed to seed achievements: %s", exc)

    # Seed default admin user if none exists
    try:
        from auth import hash_password as _hash_pw
        from models import User as _User
        _db = SessionLocal()
        try:
            if not _db.query(_User).filter(_User.is_admin == True).first():
                _admin_pw = os.getenv("ADMIN_PASSWORD", "admin123")
                _admin = _User(
                    username="admin",
                    email="admin@polyarena.local",
                    hashed_password=_hash_pw(_admin_pw),
                    is_admin=True,
                )
                _db.add(_admin)
                _db.commit()
                log.info("Seeded default admin user (username=admin)")
        finally:
            _db.close()
    except Exception as exc:
        log.warning("Failed to seed admin user: %s", exc)

    yield

    # ── Shutdown ────────────────────────────────────────────────────────────
    await broadcaster.stop()
    consumer_task.cancel()
    cancel_task.cancel()
    fill_task.cancel()
    resolved_task.cancel()
    for t in (consumer_task, cancel_task, fill_task, resolved_task):
        try:
            await t
        except asyncio.CancelledError:
            pass
    await close_async_redis()


_disable_docs = os.getenv("DISABLE_DOCS", "").strip() in ("1", "true", "yes")

app = FastAPI(
    title="PolyArena BO API",
    description="Binary Options trading dashboard — order tracking, P&L, and bot analytics.",
    version="2.0.0",
    lifespan=lifespan,
    redirect_slashes=False,
    docs_url=None if _disable_docs else "/docs",
    redoc_url=None if _disable_docs else "/redoc",
    openapi_url=None if _disable_docs else "/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    # allow_origins=[
    #     "https://arena.loritab.club",
    #     "https://loritab.club",
    #     "https://story.torilab.ai",
    # ],
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["*"])

app.include_router(auth.router, prefix="/poly-arena/auth", tags=["Auth"])
app.include_router(
    binary_options.router, prefix="/poly-arena/binary-options", tags=["Binary Options"]
)
app.include_router(bots.router, prefix="/poly-arena/bots", tags=["Bots"])
app.include_router(dashboard.router, prefix="/poly-arena/dashboard", tags=["Dashboard"])
app.include_router(admin_router.router, prefix="/poly-arena/admin", tags=["Admin"])
app.include_router(achievements_router.router, prefix="/poly-arena/achievements", tags=["Achievements"])
app.include_router(futures_router.router, prefix="/poly-arena/futures", tags=["Futures"])
app.include_router(ws_router.router, prefix="/poly-arena")
app.include_router(ws_polymarket.router, prefix="/poly-arena")


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}
