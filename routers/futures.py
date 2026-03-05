"""Futures trading API — order placement, position management, price feeds.

Routes (all under /poly-arena/futures):
    POST   /orders              — Place market or limit order
    GET    /positions            — List open positions
    POST   /positions/{id}/close — Close a position at market
    PATCH  /positions/{id}       — Update TP/SL
    GET    /trades               — Closed trade history
    GET    /orders               — Pending limit orders
    DELETE /orders/{id}          — Cancel a pending limit order
    GET    /prices               — Current mark prices
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import get_db
from models import Bot
from models_futures import (
    FuturesPosition, FuturesPositionStatus, FuturesSide,
    FuturesOrder, FuturesOrderType, FuturesOrderStatus,
)
from config.futures_fees import (
    calc_taker_fee, calc_maker_fee, calc_initial_margin,
    calc_liquidation_price, MAX_LEVERAGE, DEFAULT_LEVERAGE,
)
from services.redis_client import get_sync_redis

log = logging.getLogger(__name__)

router = APIRouter()

SUPPORTED_SYMBOLS = {"BTC", "ETH", "SOL", "XRP"}
REDIS_PRICE_PREFIX = "futures:price"


# ── Helpers ──────────────────────────────────────────────────────────────────


def _auth_bot(api_key: str, db: Session) -> Bot:
    if not api_key:
        raise HTTPException(401, "Missing x-api-key")
    bot = db.query(Bot).filter(Bot.api_key == api_key).first()
    if not bot:
        raise HTTPException(401, "Invalid API key")
    if bot.status != "ACTIVE":
        raise HTTPException(403, f"Bot is {bot.status}")
    return bot


def _get_mark_price(symbol: str) -> Optional[float]:
    """Read latest mark price from Redis."""
    try:
        r = get_sync_redis()
        data = r.hgetall(f"{REDIS_PRICE_PREFIX}:{symbol}")
        if data and "price" in data:
            return float(data["price"])
    except Exception:
        pass
    return None


# ── Request/Response schemas ─────────────────────────────────────────────────


class PlaceOrderRequest(BaseModel):
    symbol: str = Field(..., description="BTC, ETH, SOL, XRP")
    side: str = Field(..., description="LONG or SHORT")
    amount: float = Field(..., gt=0, description="USD margin amount")
    leverage: int = Field(DEFAULT_LEVERAGE, ge=1, le=MAX_LEVERAGE)
    order_type: str = Field("MARKET", description="MARKET or LIMIT")
    limit_price: Optional[float] = Field(None, gt=0)
    tp_price: Optional[float] = Field(None, gt=0)
    sl_price: Optional[float] = Field(None, gt=0)
    ttl: Optional[int] = Field(None, ge=1, description="TTL in seconds for limit orders")
    exchange: str = Field("binance", description="Exchange (currently only binance)")
    reason: Optional[str] = Field(None, max_length=500, description="Optional trade reason/note")


class UpdatePositionRequest(BaseModel):
    tp_price: Optional[float] = Field(None, ge=0)
    sl_price: Optional[float] = Field(None, ge=0)


class ClosePositionRequest(BaseModel):
    pass  # close at market


# ── Place Order ──────────────────────────────────────────────────────────────


@router.post("/orders")
def place_order(
    req: PlaceOrderRequest,
    x_api_key: str = Header(...),
    db: Session = Depends(get_db),
):
    bot = _auth_bot(x_api_key, db)
    symbol = req.symbol.upper()

    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(400, f"Unsupported symbol: {symbol}. Supported: {', '.join(sorted(SUPPORTED_SYMBOLS))}")

    exchange = req.exchange.lower() if req.exchange else "binance"
    if exchange != "binance":
        raise HTTPException(400, f"Unsupported exchange: {exchange}. Currently only 'binance' is supported.")

    side = req.side.upper()
    if side not in ("LONG", "SHORT"):
        raise HTTPException(400, "side must be LONG or SHORT")

    order_type = req.order_type.upper()
    if order_type not in ("MARKET", "LIMIT"):
        raise HTTPException(400, "order_type must be MARKET or LIMIT")

    if order_type == "LIMIT" and not req.limit_price:
        raise HTTPException(400, "limit_price required for LIMIT orders")

    # Validate TP/SL relative to side
    if req.tp_price and req.sl_price:
        if side == "LONG":
            if req.tp_price <= req.sl_price:
                raise HTTPException(400, "LONG: tp_price must be > sl_price")
        else:
            if req.tp_price >= req.sl_price:
                raise HTTPException(400, "SHORT: tp_price must be < sl_price")

    # Check balance — amount is the margin
    margin = req.amount
    mark_price = _get_mark_price(symbol)

    if order_type == "MARKET":
        if not mark_price:
            raise HTTPException(503, f"No mark price available for {symbol}. Price feed may be down.")

        # Calculate position size from margin
        notional = margin * req.leverage
        size = round(notional / mark_price, 8)

        # Entry fee
        entry_fee = calc_taker_fee(size, mark_price)
        total_cost = margin + entry_fee

        if bot.balance < total_cost:
            raise HTTPException(400, f"Insufficient balance. Need ${total_cost:.2f}, have ${bot.balance:.2f}")

        # Calculate liquidation price
        liq_price = calc_liquidation_price(mark_price, side, req.leverage)

        # Validate TP/SL relative to entry price BEFORE deducting balance
        if req.tp_price:
            if side == "LONG" and req.tp_price <= mark_price:
                raise HTTPException(400, f"LONG: tp_price ({req.tp_price}) must be > entry ({mark_price:.2f})")
            if side == "SHORT" and req.tp_price >= mark_price:
                raise HTTPException(400, f"SHORT: tp_price ({req.tp_price}) must be < entry ({mark_price:.2f})")
        if req.sl_price:
            if side == "LONG" and req.sl_price >= mark_price:
                raise HTTPException(400, f"LONG: sl_price ({req.sl_price}) must be < entry ({mark_price:.2f})")
            if side == "SHORT" and req.sl_price <= mark_price:
                raise HTTPException(400, f"SHORT: sl_price ({req.sl_price}) must be > entry ({mark_price:.2f})")

        # Deduct from balance (after all validation passes)
        bot.balance = round(bot.balance - total_cost, 8)

        # Create position
        pos = FuturesPosition(
            bot_name=bot.bot_name,
            symbol=symbol,
            exchange=exchange,
            side=FuturesSide(side),
            status=FuturesPositionStatus.OPEN,
            size=size,
            entry_price=mark_price,
            mark_price=mark_price,
            leverage=req.leverage,
            margin=margin,
            liquidation_price=liq_price,
            unrealized_pnl=0,
            realized_pnl=0,
            entry_fee=entry_fee,
            tp_price=req.tp_price,
            sl_price=req.sl_price,
            reason=req.reason,
        )
        db.add(pos)
        db.commit()
        db.refresh(pos)

        # Register with futures engine for TP/SL/liquidation monitoring
        _register_position_in_engine(pos)

        return {
            "status": "filled",
            "position_id": pos.id,
            "symbol": symbol,
            "side": side,
            "size": size,
            "entry_price": mark_price,
            "leverage": req.leverage,
            "margin": margin,
            "entry_fee": entry_fee,
            "liquidation_price": liq_price,
            "tp_price": req.tp_price,
            "sl_price": req.sl_price,
            "balance": bot.balance,
        }

    else:
        # LIMIT order — just validate and queue
        # Estimate fee for balance check
        notional = margin * req.leverage
        est_size = round(notional / req.limit_price, 8)
        est_fee = calc_maker_fee(est_size, req.limit_price)
        total_cost = margin + est_fee

        if bot.balance < total_cost:
            raise HTTPException(400, f"Insufficient balance. Need ${total_cost:.2f}, have ${bot.balance:.2f}")

        # Reserve margin
        bot.balance = round(bot.balance - margin, 8)

        expires_at = None
        if req.ttl:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=req.ttl)

        order = FuturesOrder(
            bot_name=bot.bot_name,
            symbol=symbol,
            exchange=exchange,
            side=FuturesSide(side),
            order_type=FuturesOrderType.LIMIT,
            status=FuturesOrderStatus.PENDING,
            size=est_size,
            limit_price=req.limit_price,
            leverage=req.leverage,
            tp_price=req.tp_price,
            sl_price=req.sl_price,
            ttl=req.ttl,
            expires_at=expires_at,
            reason=req.reason,
        )
        db.add(order)
        db.commit()
        db.refresh(order)

        # Register with futures engine (include expires_at for TTL)
        _register_order_in_engine(order, expires_at=expires_at)

        return {
            "status": "pending",
            "order_id": order.id,
            "symbol": symbol,
            "side": side,
            "size": est_size,
            "limit_price": req.limit_price,
            "leverage": req.leverage,
            "margin": margin,
            "tp_price": req.tp_price,
            "sl_price": req.sl_price,
            "expires_at": expires_at.isoformat() if expires_at else None,
            "balance": bot.balance,
        }


# ── Positions ────────────────────────────────────────────────────────────────


@router.get("/positions")
def list_positions(
    status: str = Query("OPEN", description="OPEN, CLOSED, LIQUIDATED, or ALL"),
    bot_name: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(FuturesPosition)
    if status != "ALL":
        q = q.filter(FuturesPosition.status == status)
    if bot_name:
        q = q.filter(FuturesPosition.bot_name == bot_name)
    q = q.order_by(FuturesPosition.created_at.desc()).limit(limit)

    positions = q.all()

    # Update mark prices from Redis for open positions
    result = []
    for p in positions:
        d = _position_to_dict(p)
        if p.status == FuturesPositionStatus.OPEN:
            mark = _get_mark_price(p.symbol)
            if mark:
                d["mark_price"] = mark
                if p.side == FuturesSide.LONG:
                    d["unrealized_pnl"] = round((mark - p.entry_price) * p.size, 8)
                else:
                    d["unrealized_pnl"] = round((p.entry_price - mark) * p.size, 8)
        result.append(d)

    return result


@router.post("/positions/{position_id}/close")
def close_position(
    position_id: int,
    x_api_key: str = Header(...),
    db: Session = Depends(get_db),
):
    bot = _auth_bot(x_api_key, db)
    pos = db.get(FuturesPosition, position_id)
    if not pos:
        raise HTTPException(404, "Position not found")
    if pos.bot_name != bot.bot_name:
        raise HTTPException(403, "Not your position")
    if pos.status != FuturesPositionStatus.OPEN:
        raise HTTPException(400, f"Position is already {pos.status.value}")

    mark_price = _get_mark_price(pos.symbol)
    if not mark_price:
        raise HTTPException(503, f"No mark price for {pos.symbol}")

    # Calculate P&L
    if pos.side == FuturesSide.LONG:
        pnl = (mark_price - pos.entry_price) * pos.size
    else:
        pnl = (pos.entry_price - mark_price) * pos.size

    exit_fee = calc_taker_fee(pos.size, mark_price)
    realized_pnl = round(pnl - exit_fee, 8)

    # Update position
    pos.status = FuturesPositionStatus.CLOSED
    pos.exit_price = mark_price
    pos.mark_price = mark_price
    pos.exit_fee = exit_fee
    pos.realized_pnl = realized_pnl
    pos.unrealized_pnl = 0
    pos.exit_trigger = "MANUAL"
    pos.closed_at = datetime.now(timezone.utc)

    # Return margin + PnL to balance (cap at 0 — can't refund negative)
    refund = round(max(0, pos.margin + realized_pnl), 8)
    bot.balance = round(bot.balance + refund, 8)

    db.commit()

    # Remove from engine monitoring
    try:
        from services.futures_engine import futures_engine
        futures_engine.remove_position(pos.id)
    except Exception:
        pass

    return {
        "position_id": pos.id,
        "status": "CLOSED",
        "exit_price": mark_price,
        "exit_fee": exit_fee,
        "realized_pnl": realized_pnl,
        "margin_returned": pos.margin,
        "balance": bot.balance,
    }


@router.patch("/positions/{position_id}")
def update_position(
    position_id: int,
    req: UpdatePositionRequest,
    x_api_key: str = Header(...),
    db: Session = Depends(get_db),
):
    bot = _auth_bot(x_api_key, db)
    pos = db.get(FuturesPosition, position_id)
    if not pos:
        raise HTTPException(404, "Position not found")
    if pos.bot_name != bot.bot_name:
        raise HTTPException(403, "Not your position")
    if pos.status != FuturesPositionStatus.OPEN:
        raise HTTPException(400, f"Position is already {pos.status.value}")

    mark = _get_mark_price(pos.symbol) or pos.entry_price

    # Validate TP/SL
    tp = req.tp_price if req.tp_price is not None else pos.tp_price
    sl = req.sl_price if req.sl_price is not None else pos.sl_price

    if tp and tp > 0:
        if pos.side == FuturesSide.LONG and tp <= mark:
            raise HTTPException(400, f"LONG: tp_price must be > current price ({mark:.2f})")
        if pos.side == FuturesSide.SHORT and tp >= mark:
            raise HTTPException(400, f"SHORT: tp_price must be < current price ({mark:.2f})")

    if sl and sl > 0:
        if pos.side == FuturesSide.LONG and sl >= mark:
            raise HTTPException(400, f"LONG: sl_price must be < current price ({mark:.2f})")
        if pos.side == FuturesSide.SHORT and sl <= mark:
            raise HTTPException(400, f"SHORT: sl_price must be > current price ({mark:.2f})")

    if req.tp_price is not None:
        pos.tp_price = req.tp_price if req.tp_price > 0 else None
    if req.sl_price is not None:
        pos.sl_price = req.sl_price if req.sl_price > 0 else None

    db.commit()

    # Update in engine
    try:
        from services.futures_engine import futures_engine
        futures_engine.update_position_tp_sl(pos.id, pos.tp_price, pos.sl_price)
    except Exception:
        pass

    return _position_to_dict(pos)


# ── Orders ───────────────────────────────────────────────────────────────────


@router.get("/orders")
def list_orders(
    status: str = Query("PENDING", description="PENDING, FILLED, CANCELLED, EXPIRED, or ALL"),
    bot_name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(FuturesOrder)
    if status != "ALL":
        q = q.filter(FuturesOrder.status == status)
    if bot_name:
        q = q.filter(FuturesOrder.bot_name == bot_name)
    q = q.order_by(FuturesOrder.created_at.desc()).limit(limit)
    return [_order_to_dict(o) for o in q.all()]


@router.delete("/orders/{order_id}")
def cancel_order(
    order_id: int,
    x_api_key: str = Header(...),
    db: Session = Depends(get_db),
):
    bot = _auth_bot(x_api_key, db)
    order = db.get(FuturesOrder, order_id)
    if not order:
        raise HTTPException(404, "Order not found")
    if order.bot_name != bot.bot_name:
        raise HTTPException(403, "Not your order")
    if order.status != FuturesOrderStatus.PENDING:
        raise HTTPException(400, f"Order is already {order.status.value}")

    order.status = FuturesOrderStatus.CANCELLED
    order.updated_at = datetime.now(timezone.utc)

    # Refund reserved margin
    notional = order.size * order.limit_price
    margin = round(notional / order.leverage, 8)
    bot.balance = round(bot.balance + margin, 8)

    db.commit()

    # Remove from engine
    try:
        from services.futures_engine import futures_engine
        futures_engine.remove_order(order_id)
    except Exception:
        pass

    return {"order_id": order_id, "status": "CANCELLED", "margin_refunded": margin, "balance": bot.balance}


# ── Trades (closed positions) ────────────────────────────────────────────────


@router.get("/trades")
def list_trades(
    bot_name: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = Query(200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(FuturesPosition).filter(
        FuturesPosition.status.in_([FuturesPositionStatus.CLOSED, FuturesPositionStatus.LIQUIDATED])
    )
    if bot_name:
        q = q.filter(FuturesPosition.bot_name == bot_name)
    if symbol:
        q = q.filter(FuturesPosition.symbol == symbol.upper())
    q = q.order_by(FuturesPosition.closed_at.desc()).limit(limit)
    return [_trade_to_dict(p) for p in q.all()]


# ── Prices ───────────────────────────────────────────────────────────────────


@router.get("/prices")
def get_prices():
    """Current mark prices from Binance Futures."""
    r = get_sync_redis()
    result = {}
    for sym in SUPPORTED_SYMBOLS:
        data = r.hgetall(f"{REDIS_PRICE_PREFIX}:{sym}")
        if data and "price" in data:
            result[sym] = {
                "price": float(data["price"]),
                "updated_at": data.get("updated_at", ""),
            }
    return {"prices": result}


# ── Serializers ──────────────────────────────────────────────────────────────


def _position_to_dict(p: FuturesPosition) -> dict:
    return {
        "id": p.id,
        "bot_name": p.bot_name,
        "symbol": p.symbol,
        "exchange": p.exchange,
        "side": p.side.value if hasattr(p.side, "value") else p.side,
        "status": p.status.value if hasattr(p.status, "value") else p.status,
        "size": p.size,
        "entry_price": p.entry_price,
        "exit_price": p.exit_price,
        "mark_price": p.mark_price,
        "leverage": p.leverage,
        "margin": p.margin,
        "liquidation_price": p.liquidation_price,
        "unrealized_pnl": p.unrealized_pnl or 0,
        "realized_pnl": p.realized_pnl or 0,
        "entry_fee": p.entry_fee or 0,
        "exit_fee": p.exit_fee or 0,
        "tp_price": p.tp_price,
        "sl_price": p.sl_price,
        "exit_trigger": p.exit_trigger,
        "reason": p.reason,
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "closed_at": p.closed_at.isoformat() if p.closed_at else None,
    }


def _trade_to_dict(p: FuturesPosition) -> dict:
    d = _position_to_dict(p)
    d["fee"] = round((p.entry_fee or 0) + (p.exit_fee or 0), 8)
    return d


def _order_to_dict(o: FuturesOrder) -> dict:
    return {
        "id": o.id,
        "bot_name": o.bot_name,
        "symbol": o.symbol,
        "exchange": o.exchange,
        "side": o.side.value if hasattr(o.side, "value") else o.side,
        "order_type": o.order_type.value if hasattr(o.order_type, "value") else o.order_type,
        "status": o.status.value if hasattr(o.status, "value") else o.status,
        "size": o.size,
        "limit_price": o.limit_price,
        "leverage": o.leverage,
        "tp_price": o.tp_price,
        "sl_price": o.sl_price,
        "ttl": o.ttl,
        "expires_at": o.expires_at.isoformat() if o.expires_at else None,
        "reason": o.reason,
        "position_id": o.position_id,
        "created_at": o.created_at.isoformat() if o.created_at else None,
    }


# ── Engine registration helpers ──────────────────────────────────────────────


def _register_position_in_engine(pos: FuturesPosition) -> None:
    try:
        from services.futures_engine import futures_engine
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
    except Exception as exc:
        log.warning("Failed to register position #%d in engine: %s", pos.id, exc)


def _register_order_in_engine(order: FuturesOrder, expires_at=None) -> None:
    try:
        from services.futures_engine import futures_engine
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
        ea = expires_at or order.expires_at
        if ea:
            d["expires_at"] = ea.timestamp() if hasattr(ea, "timestamp") else ea
        futures_engine.register_order(d)
    except Exception as exc:
        log.warning("Failed to register order #%d in engine: %s", order.id, exc)
