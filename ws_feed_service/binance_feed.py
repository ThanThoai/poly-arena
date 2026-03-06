"""Binance Futures price feed — runs inside the WS Feed Service process.

Responsibilities:
1. Connect to Binance Futures WS for mark price updates (1s interval)
2. Write mark prices to Redis for the API to read
3. Feed prices to FuturesEngine for TP/SL/liquidation/limit order matching
4. Process engine events (fills, closes) and persist to DB + publish to Redis streams
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from services.binance_ws import BinanceFuturesWs, FUTURES_SYMBOLS
from services.futures_engine import futures_engine
from services.redis_client import get_sync_redis
from database import SessionLocal
from models import BalanceHistory, BinaryOption, Bot
from models_futures import (
    FuturesPosition, FuturesPositionStatus, FuturesSide,
    FuturesOrder, FuturesOrderStatus,
)
from config.futures_fees import (
    calc_maker_fee, calc_initial_margin, calc_liquidation_price,
)

log = logging.getLogger(__name__)

REDIS_PRICE_PREFIX = "futures:price"
STREAM_FUTURES_FILLS = "stream:futures:fills"
STREAM_FUTURES_CLOSES = "stream:futures:closes"
STREAM_MAXLEN = 10_000


def _on_price_update(symbol: str, price: float, timestamp: float) -> None:
    """Called on each mark price update from Binance WS (sync context)."""
    # Write to Redis
    try:
        r = get_sync_redis()
        key = f"{REDIS_PRICE_PREFIX}:{symbol}"
        r.hset(key, mapping={
            "price": str(price),
            "updated_at": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(),
        })
        r.expire(key, 120)
    except Exception as exc:
        log.debug("Redis write failed for %s: %s", symbol, exc)

    # Feed to futures engine — check orders/positions
    events = futures_engine.update_price(symbol, price)

    # Process events
    if events:
        _process_events(events)


def _process_events(events: list[dict]) -> None:
    """Process fills and position closes from the engine."""
    db = SessionLocal()
    r = get_sync_redis()
    try:
        for evt in events:
            try:
                if evt["type"] == "order_fill":
                    _handle_order_fill(evt, db, r)
                elif evt["type"] == "position_close":
                    _handle_position_close(evt, db, r)
                elif evt["type"] == "order_expire":
                    _handle_order_expire(evt, db)
            except Exception as exc:
                log.error("Error processing futures event %s: %s", evt.get("type"), exc)
        db.commit()
    except Exception as exc:
        log.error("Futures event batch commit failed: %s", exc)
        db.rollback()
    finally:
        db.close()


def _record_bot_equity(db, bot_name: str) -> None:
    """Record bot equity (cash + locked) in BalanceHistory."""
    from sqlalchemy import func as sa_func
    bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
    if not bot:
        return
    # BO locked
    bo_locked = (
        db.query(sa_func.coalesce(sa_func.sum(BinaryOption.amount), 0.0))
        .filter(BinaryOption.bot_name == bot_name, BinaryOption.result == "PENDING")
        .scalar()
    ) or 0.0
    # Futures locked
    fut_pos = (
        db.query(sa_func.coalesce(sa_func.sum(FuturesPosition.margin), 0.0))
        .filter(FuturesPosition.bot_name == bot_name, FuturesPosition.status == FuturesPositionStatus.OPEN)
        .scalar()
    ) or 0.0
    fut_ord = (
        db.query(sa_func.coalesce(
            sa_func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage), 0.0
        ))
        .filter(FuturesOrder.bot_name == bot_name, FuturesOrder.status == FuturesOrderStatus.PENDING)
        .scalar()
    ) or 0.0
    equity = round((bot.balance or 0) + bo_locked + fut_pos + fut_ord, 8)
    db.add(BalanceHistory(bot_name=bot_name, balance=equity))


def _handle_order_fill(evt: dict, db, r) -> None:
    """Handle a limit order fill — create position, update order, adjust balance."""
    order = db.get(FuturesOrder, evt["order_id"])
    if not order or order.status != FuturesOrderStatus.PENDING:
        return

    fill_price = evt["fill_price"]
    size = order.size
    leverage = order.leverage
    side = evt["side"]
    symbol = evt["symbol"]

    # Calculate position params
    entry_fee = calc_maker_fee(size, fill_price, evt.get("exchange", "binance"))
    margin = calc_initial_margin(size, fill_price, leverage)
    liq_price = calc_liquidation_price(fill_price, side, leverage)

    # Deduct entry fee from balance
    bot = db.query(Bot).filter(Bot.bot_name == order.bot_name).first()
    if bot:
        bot.balance = round(bot.balance - entry_fee, 8)

    # Create position
    pos = FuturesPosition(
        bot_name=order.bot_name,
        symbol=symbol,
        exchange=order.exchange,
        side=FuturesSide(side),
        status=FuturesPositionStatus.OPEN,
        size=size,
        entry_price=fill_price,
        mark_price=fill_price,
        leverage=leverage,
        margin=margin,
        liquidation_price=liq_price,
        entry_fee=entry_fee,
        tp_price=evt.get("tp_price"),
        sl_price=evt.get("sl_price"),
    )
    db.add(pos)
    db.flush()  # get pos.id

    # Update order
    order.status = FuturesOrderStatus.FILLED
    order.position_id = pos.id
    order.updated_at = datetime.now(timezone.utc)

    # Register position for monitoring
    futures_engine.register_position({
        "id": pos.id,
        "bot_name": pos.bot_name,
        "symbol": pos.symbol,
        "side": side,
        "size": pos.size,
        "entry_price": fill_price,
        "leverage": leverage,
        "margin": margin,
        "liquidation_price": liq_price,
        "tp_price": pos.tp_price,
        "sl_price": pos.sl_price,
        "exchange": pos.exchange,
    })

    # Publish to stream
    try:
        r.xadd(STREAM_FUTURES_FILLS, {
            "order_id": str(order.id),
            "position_id": str(pos.id),
            "bot_name": order.bot_name,
            "symbol": symbol,
            "side": side,
            "size": str(size),
            "fill_price": str(fill_price),
            "entry_fee": str(entry_fee),
        }, maxlen=STREAM_MAXLEN)
    except Exception as exc:
        log.debug("Failed to publish futures fill: %s", exc)

    _record_bot_equity(db, order.bot_name)

    log.info("Futures limit order #%d filled → position #%d: %s %s %.4f @ %.2f",
             order.id, pos.id, side, symbol, size, fill_price)


def _handle_position_close(evt: dict, db, r) -> None:
    """Handle a position close (TP/SL/liquidation)."""
    pos = db.get(FuturesPosition, evt["position_id"])
    if not pos or pos.status != FuturesPositionStatus.OPEN:
        return

    trigger = evt["trigger"]
    exit_price = evt["exit_price"]
    exit_fee = evt["exit_fee"]
    realized_pnl = evt["realized_pnl"]

    if trigger == "LIQ":
        pos.status = FuturesPositionStatus.LIQUIDATED
    else:
        pos.status = FuturesPositionStatus.CLOSED

    pos.exit_price = exit_price
    pos.mark_price = exit_price
    pos.exit_fee = exit_fee
    pos.realized_pnl = realized_pnl
    pos.unrealized_pnl = 0
    pos.exit_trigger = trigger
    pos.closed_at = datetime.now(timezone.utc)

    # Return margin + PnL to balance
    bot = db.query(Bot).filter(Bot.bot_name == pos.bot_name).first()
    if bot:
        if trigger == "LIQ":
            # Liquidation: lose margin, no refund (entry_fee already deducted)
            pass
        else:
            refund = round(pos.margin + realized_pnl, 8)
            bot.balance = round(bot.balance + max(0, refund), 8)

    # Publish to stream
    try:
        r.xadd(STREAM_FUTURES_CLOSES, {
            "position_id": str(pos.id),
            "bot_name": pos.bot_name,
            "symbol": pos.symbol,
            "side": evt["side"],
            "trigger": trigger,
            "exit_price": str(exit_price),
            "realized_pnl": str(realized_pnl),
        }, maxlen=STREAM_MAXLEN)
    except Exception as exc:
        log.debug("Failed to publish futures close: %s", exc)

    _record_bot_equity(db, pos.bot_name)

    log.info("Futures position #%d closed (%s): %s %s PnL=%.2f",
             pos.id, trigger, evt["side"], pos.symbol, realized_pnl)


def _handle_order_expire(evt: dict, db) -> None:
    """Handle a limit order TTL expiry — cancel and refund margin."""
    order = db.get(FuturesOrder, evt["order_id"])
    if not order or order.status != FuturesOrderStatus.PENDING:
        return

    order.status = FuturesOrderStatus.EXPIRED
    order.updated_at = datetime.now(timezone.utc)

    # Refund reserved margin
    notional = order.size * order.limit_price
    margin = round(notional / order.leverage, 8)
    bot = db.query(Bot).filter(Bot.bot_name == order.bot_name).first()
    if bot:
        bot.balance = round(bot.balance + margin, 8)

    log.info("Futures limit order #%d expired: %s %s, margin $%.2f refunded",
             order.id, order.side.value if hasattr(order.side, "value") else order.side,
             order.symbol, margin)


def _load_open_state() -> None:
    """Load open positions and pending orders from DB into the engine on startup."""
    db = SessionLocal()
    try:
        # Load open positions
        positions = db.query(FuturesPosition).filter(
            FuturesPosition.status == FuturesPositionStatus.OPEN
        ).all()
        for pos in positions:
            futures_engine.register_position({
                "id": pos.id,
                "bot_name": pos.bot_name,
                "symbol": pos.symbol,
                "side": pos.side.value if hasattr(pos.side, "value") else pos.side,
                "size": pos.size,
                "entry_price": pos.entry_price,
                "leverage": pos.leverage,
                "margin": pos.margin,
                "liquidation_price": pos.liquidation_price,
                "tp_price": pos.tp_price,
                "sl_price": pos.sl_price,
                "exchange": pos.exchange,
            })
        log.info("Loaded %d open futures positions into engine", len(positions))

        # Load pending orders
        orders = db.query(FuturesOrder).filter(
            FuturesOrder.status == FuturesOrderStatus.PENDING
        ).all()
        for order in orders:
            d = {
                "id": order.id,
                "bot_name": order.bot_name,
                "symbol": order.symbol,
                "side": order.side.value if hasattr(order.side, "value") else order.side,
                "size": order.size,
                "limit_price": order.limit_price,
                "leverage": order.leverage,
                "tp_price": order.tp_price,
                "sl_price": order.sl_price,
                "exchange": order.exchange,
            }
            if order.expires_at:
                d["expires_at"] = order.expires_at.timestamp()
            futures_engine.register_order(d)
        log.info("Loaded %d pending futures orders into engine", len(orders))
    finally:
        db.close()


async def run_binance_feed() -> None:
    """Entry point — start Binance WS feed with engine integration."""
    _load_open_state()

    ws = BinanceFuturesWs(on_price=_on_price_update)
    log.info("Starting Binance Futures price feed for: %s", ", ".join(FUTURES_SYMBOLS.keys()))
    await ws.start()
