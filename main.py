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
from routers import achievements as achievements_router, auth, binary_options, bots, dashboard, ws as ws_router, ws_polymarket
from services.redis_client import get_async_redis, close_async_redis
from services.order_trace import make_trace, append_trace
from services.user_balance import record_user_balance
from services.rest_exit import (
    fetch_best_bid_from_rest,
    simulate_bracket_exit_from_rest,
)
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
                    # Tính phần chưa fill để hoàn lại
                    actual_cost = round(filled * avg_entry, 8)
                    unfilled_refund = round(max(0, bo.amount - actual_cost), 8)

                    # Cập nhật lệnh: chỉ giữ lại phần đã fill
                    bo.avg_price = avg_entry
                    bo.num_shares = filled
                    bo.amount = actual_cost
                    bo.me_order_status = "CANCELED"

                    append_trace(bo, make_trace(
                        "MATCHING", "PARTIAL_FILL_EXPIRY",
                        f"Order expired with partial fill. "
                        f"Filled: {filled:.4f} @ ${avg_entry:.4f}. "
                        f"Unfilled refund: ${unfilled_refund:.4f}.",
                        {"filled": filled, "avg_entry_price": avg_entry,
                         "actual_cost": actual_cost, "unfilled_refund": unfilled_refund,
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
                    record_user_balance(db, bo.bot_name, trade_id=bo.id)

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

                # MATCHING trace
                if status == "FILLED":
                    append_trace(bo, make_trace(
                        "MATCHING", "ORDER_FILLED",
                        f"Order fully filled. Avg Entry Price: ${avg_entry:.4f}. "
                        f"Shares: {filled:.4f}.",
                        {"avg_entry_price": avg_entry, "filled": filled,
                         "status": status},
                    ))
                elif status == "PARTIAL":
                    append_trace(bo, make_trace(
                        "MATCHING", "PARTIAL_FILL",
                        f"Partial fill: {filled:.4f} shares @ ${avg_entry:.4f}.",
                        {"avg_entry_price": avg_entry, "filled": filled,
                         "status": status},
                    ))

                db.commit()

                # Publish trace to Redis for real-time UI
                _publish_trace_sync(bo)

                log.info(
                    "Fill update: BO #%d filled=%.4f avg=%.6f status=%s",
                    bo_id,
                    filled,
                    avg_entry,
                    status,
                )

                # ── Auto-Exit: TP < entry hoặc SL > entry ──────────────
                # Khi LIMIT order FILLED mà avg_entry_price đã vi phạm
                # điều kiện TP/SL (do slippage hoặc book dynamics), tự
                # động kích hoạt bracket exit ngay lập tức.
                if (
                    status == "FILLED"
                    and bo.result == BOResult.PENDING
                    and bo.exit_trigger is None
                ):
                    tp = bo.tp_price
                    sl = bo.sl_price
                    tp_violated = tp is not None and avg_entry >= tp
                    sl_violated = sl is not None and avg_entry <= sl

                    if tp_violated or sl_violated:
                        trigger = "TP" if tp_violated else "SL"
                        cond_price = tp if tp_violated else sl
                        log.info(
                            "Auto-Exit: BO #%d %s violated at fill — "
                            "entry=%.6f %s=%.6f",
                            bo_id, trigger, avg_entry, trigger, cond_price,
                        )
                        append_trace(bo, make_trace(
                            "MONITORING", "SLIPPAGE_VIOLATION",
                            f"Post-fill check: Avg Entry ${avg_entry:.4f} "
                            f"violates {trigger} ${cond_price:.4f}. "
                            f"Triggering Auto-Exit...",
                            {"avg_entry_price": avg_entry, "trigger": trigger,
                             "condition_price": cond_price},
                        ))

                        # Resolve token_id for REST bid lookup
                        _FORECAST_MAP = {"GREEN": "UP", "RED": "DOWN"}
                        pm_dir = _FORECAST_MAP.get(
                            bo.forecast.value
                            if hasattr(bo.forecast, "value")
                            else str(bo.forecast),
                            "UP",
                        )
                        token_id = _get_token_id(
                            bo.symbol.value
                            if hasattr(bo.symbol, "value")
                            else str(bo.symbol),
                            bo.timeframe.value
                            if hasattr(bo.timeframe, "value")
                            else str(bo.timeframe),
                            pm_dir,
                        )

                        exit_done = False
                        if token_id:
                            best_bid, bid_levels = fetch_best_bid_from_rest(
                                token_id,
                            )
                            if best_bid is not None and bid_levels:
                                exit_price, exit_filled, exit_walk = (
                                    simulate_bracket_exit_from_rest(
                                        filled, bid_levels,
                                    )
                                )
                                if exit_filled > 0:
                                    # Record exit data only — profit
                                    # deferred to session-end settlement
                                    bo.exit_trigger = trigger
                                    bo.exit_price = exit_price
                                    bo.exit_filled = exit_filled
                                    bo.exit_at = datetime.now(timezone.utc)

                                    wp = bo.walk_prices or {}
                                    wp["exit"] = exit_walk
                                    bo.walk_prices = wp

                                    append_trace(bo, make_trace(
                                        "MONITORING",
                                        "AUTO_EXIT_RECORDED",
                                        f"Auto-Exit: {trigger} violated at "
                                        f"entry. Entry: ${avg_entry:.4f} → "
                                        f"Exit: ${exit_price:.4f}. "
                                        f"Shares: {exit_filled:.4f}. "
                                        f"Pending session-end settlement.",
                                        {"trigger": trigger,
                                         "entry_price": avg_entry,
                                         "exit_price": exit_price,
                                         "exit_filled": exit_filled},
                                    ))
                                    db.commit()
                                    _publish_trace_sync(bo)
                                    exit_done = True
                                    log.info(
                                        "Auto-Exit recorded (deferred): "
                                        "BO #%d %s entry=%.6f exit=%.6f "
                                        "shares=%.4f",
                                        bo_id, trigger, avg_entry,
                                        exit_price, exit_filled,
                                    )

                        if not exit_done:
                            log.warning(
                                "Auto-Exit BO #%d: no bid liquidity or "
                                "token_id unavailable — ME will monitor",
                                bo_id,
                            )
                            db.commit()
                            _publish_trace_sync(bo)

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

    yield

    # ── Shutdown ────────────────────────────────────────────────────────────
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
app.include_router(achievements_router.router, prefix="/poly-arena/achievements", tags=["Achievements"])
app.include_router(ws_router.router, prefix="/poly-arena")
app.include_router(ws_polymarket.router, prefix="/poly-arena")


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}
