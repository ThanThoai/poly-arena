import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from database import SessionLocal
from models import BalanceHistory, BinaryOption, Bot, BOResult
from routers import binary_options, bots, dashboard
from services.redis_client import get_async_redis, close_async_redis
from ws_feed_service.config import (
    STREAM_BRACKET_EXITS,
    STREAM_ORDER_CANCELS,
    STREAM_ORDER_FILLS,
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
                bo.exit_at = (
                    datetime.fromisoformat(exit_at_str)
                    if exit_at_str
                    else datetime.now(timezone.utc)
                )
                if order_id and not bo.me_order_id:
                    bo.me_order_id = order_id
                bo.me_order_status = "FILLED"

                num_shares = bo.num_shares or 0.0
                is_full_exit = exit_filled >= num_shares and num_shares > 0

                if is_full_exit:
                    profit = round((exit_price - bo.avg_price) * exit_filled, 8)
                    result = BOResult.WIN if profit >= 0 else BOResult.LOSS

                    bo.result = result
                    bo.profit = profit

                    payout = round(bo.amount + profit, 8)
                    bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
                    if bot:
                        bot.balance = round(bot.balance + payout, 8)
                        db.add(
                            BalanceHistory(
                                bot_name=bo.bot_name,
                                balance=bot.balance,
                                trade_id=bo.id,
                            )
                        )

                    # Step 1: persist to DB
                    db.commit()
                    log.info(
                        "Bracket exit settled immediately: BO #%d trigger=%s "
                        "exit_price=%.6f exit_filled=%.4f → %s profit=%.8f balance=%.2f",
                        bo_id,
                        trigger,
                        exit_price,
                        exit_filled,
                        result.value,
                        profit,
                        bot.balance if bot else float("nan"),
                    )
                else:
                    # Partial exit — scheduler settles remainder via candle
                    # Step 1: persist to DB
                    db.commit()
                    log.info(
                        "Bracket exit (partial): BO #%d trigger=%s "
                        "exit_filled=%.4f / num_shares=%.4f — pending candle settlement",
                        bo_id,
                        trigger,
                        exit_filled,
                        num_shares,
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

                    db.commit()
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
                db.commit()
                log.info(
                    "Fill update: BO #%d filled=%.4f avg=%.6f status=%s",
                    bo_id,
                    filled,
                    avg_entry,
                    status,
                )

                # ── Immediate settlement khi fill đến muộn ────────────────
                # Nếu lệnh đã FILLED và settlement_at đã qua, không cần chờ
                # scheduler — settle ngay để lệnh rời khỏi open positions.
                if status == "FILLED" and bo.settlement_at is not None:
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

    yield

    # ── Shutdown ────────────────────────────────────────────────────────────
    consumer_task.cancel()
    cancel_task.cancel()
    fill_task.cancel()
    for t in (consumer_task, cancel_task, fill_task):
        try:
            await t
        except asyncio.CancelledError:
            pass
    await close_async_redis()


app = FastAPI(
    title="PolyArena BO API",
    description="Binary Options trading dashboard — order tracking, P&L, and bot analytics.",
    version="2.0.0",
    lifespan=lifespan,
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

app.include_router(
    binary_options.router, prefix="/poly-arena/binary-options", tags=["Binary Options"]
)
app.include_router(bots.router, prefix="/poly-arena/bots", tags=["Bots"])
app.include_router(dashboard.router, prefix="/poly-arena/dashboard", tags=["Dashboard"])


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}
