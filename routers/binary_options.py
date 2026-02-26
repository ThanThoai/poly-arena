from collections import defaultdict
import json
import time
from typing import List, Optional, Tuple

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
from models import BinaryOption, Bot, BOResult, BOSymbol, BOTimeframe, BOForecast
from services.settlement import calc_settlement_time
from services.polymarket import PolymarketClient
from ws_feed_service.config import (
    PRICE_KEY_PREFIX, STALE_THRESHOLD_S, QUEUE_ORDERS_NEW,
)
from schemas import (
    BOBotStats, BOCreate, BOForecastStats, BOResponse,
    BOStats, BOTimeframeStats,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# GREEN → lấy min_ask của UP, RED → lấy min_ask của DOWN
_FORECAST_TO_STATUS = {"GREEN": "UP", "RED": "DOWN"}


# ─── helpers ──────────────────────────────────────────────────────────────────

def _compute_stats(items: list):
    wins    = sum(1 for b in items if b.result == BOResult.WIN)
    losses  = sum(1 for b in items if b.result == BOResult.LOSS)
    pending = sum(1 for b in items if b.result == BOResult.PENDING)
    decided = wins + losses
    return dict(
        total        = len(items),
        wins         = wins,
        losses       = losses,
        pending      = pending,
        win_rate     = round(wins / decided * 100, 2) if decided else 0.0,
        total_profit = round(sum(b.profit or 0 for b in items), 8),
        total_amount = round(sum(b.amount for b in items), 8),
    )


# ─── Tạo lệnh ─────────────────────────────────────────────────────────────────

def _try_redis_price(
    symbol: str, timeframe: str, pm_status: str,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Try to get best ask from Redis price cache (written by WS Feed Service).
    Returns (price, token_id) or (None, None) if unavailable/stale.
    """
    try:
        from services.redis_client import get_sync_redis
        sr = get_sync_redis()

        key = f"{PRICE_KEY_PREFIX}:{symbol}:{timeframe}:{pm_status}"
        data = sr.hgetall(key)
        if not data:
            return None, None

        # Check staleness
        updated_at = data.get("updated_at")
        if updated_at:
            age = time.time() - float(updated_at)
            if age > STALE_THRESHOLD_S:
                logger.debug(
                    "Redis price stale: %s age=%.1fs > %ds",
                    key, age, STALE_THRESHOLD_S,
                )
                return None, None

        best_ask = float(data["best_ask"])
        token_id = data["token_id"]
        logger.info(
            "Redis hit: %s %s %s → best_ask=%.4f (token=%s)",
            symbol, timeframe, pm_status, best_ask, token_id[:16],
        )
        return best_ask, token_id
    except Exception as exc:
        logger.warning("Redis price lookup failed: %s", exc)
        return None, None


@router.post("/", response_model=BOResponse, status_code=201)
def create_bo(
    payload: BOCreate,
    x_api_key: str = Header(..., alias="x-api-key", description="Bot API key"),
    db: Session = Depends(get_db),
):
    """
    Đăng ký lệnh BO mới. Xác thực bot qua header x-api-key.

    Order types:
      MARKET (limit_price=None):  fill ngay tại best_ask hiện tại
      LIMIT  (limit_price=0.xx):  chờ ask xuống đến limit_price trước expire_at

    Pricing strategy (MARKET):
      1. Try matching engine shadow orderbook — instant
      2. Fall back to REST Polymarket CLOB API

    Pricing strategy (LIMIT):
      Dùng limit_price do bot chỉ định.
      Vẫn cần token_id từ engine/REST để đặt virtual order.
    """
    order_received_at = datetime.now(timezone.utc)

    bot = db.query(Bot).filter(Bot.api_key == x_api_key, Bot.is_active == True).first()
    if not bot:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key")

    pm_status  = _FORECAST_TO_STATUS[payload.forecast.value]
    is_limit   = payload.limit_price is not None
    token_id:  Optional[str]   = None
    entry_price: float

    if is_limit:
        # ── LIMIT order: giá do bot chỉ định ─────────────────────────────────
        entry_price    = payload.limit_price  # type: ignore[assignment]
        ask_fetched_at = datetime.now(timezone.utc)
        price_source   = "limit"

        # Cần token_id để đặt virtual order — thử Redis trước, sau REST
        _, token_id = _try_redis_price(
            payload.symbol.value, payload.timeframe.value, pm_status,
        )
        if token_id is None:
            try:
                with PolymarketClient() as pm:
                    ob = pm.get_orderbook(
                        payload.symbol.value, payload.timeframe.value, pm_status,
                    )
                token_id = ob.token_id
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Polymarket unavailable: {e}")
    else:
        # ── MARKET order: lấy giá từ Redis / REST ─────────────────────────────
        price_source = "rest"
        min_ask, token_id = _try_redis_price(
            payload.symbol.value, payload.timeframe.value, pm_status,
        )
        if min_ask is not None:
            ask_fetched_at = datetime.now(timezone.utc)
            price_source   = "redis"
        else:
            try:
                with PolymarketClient() as pm:
                    ob = pm.get_orderbook(
                        payload.symbol.value, payload.timeframe.value, pm_status,
                    )
                ask_fetched_at = datetime.now(timezone.utc)
                min_ask  = ob.min_ask
                token_id = ob.token_id
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Polymarket unavailable: {e}")
        entry_price = min_ask

    num_shares    = round(payload.amount / entry_price, 8)
    settlement_at = calc_settlement_time(payload.timeframe, datetime.now(timezone.utc))
    has_bracket   = payload.tp_price is not None or payload.sl_price is not None

    latency_ms = (ask_fetched_at - order_received_at).total_seconds() * 1000
    logger.info(
        "BO %s %s %s: price=%.4f src=%s latency=%.0fms "
        "limit=%s tp=%s sl=%s",
        payload.symbol.value, payload.timeframe.value, payload.forecast.value,
        entry_price, price_source, latency_ms,
        payload.limit_price, payload.tp_price, payload.sl_price,
    )

    bo = BinaryOption(
        bot_name          = bot.bot_name,
        symbol            = payload.symbol,
        timeframe         = payload.timeframe,
        forecast          = payload.forecast,
        amount            = payload.amount,
        result            = BOResult.PENDING,
        avg_price         = entry_price,
        num_shares        = num_shares,
        reason            = payload.reason,
        order_received_at = order_received_at,
        ask_fetched_at    = ask_fetched_at,
        settlement_at     = settlement_at,
        limit_price       = payload.limit_price,
        tp_price          = payload.tp_price,
        sl_price          = payload.sl_price,
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)

    # ── Push virtual order to Redis queue (consumed by WS Feed Service) ─────
    # Push when:
    #   - LIMIT order (tracking fill + TTL expiry)
    #   - MARKET order with bracket (tracking TP/SL shadow)
    should_place_virtual = (is_limit or has_bracket) and token_id is not None
    if should_place_virtual:
        try:
            from services.redis_client import get_sync_redis
            sr = get_sync_redis()
            order_payload = json.dumps({
                "bo_id": bo.id,
                "token_id": token_id,
                "side": "BUY",
                "price": entry_price,
                "quantity": num_shares,
                "limit_price": payload.limit_price,
                "tp_price": payload.tp_price,
                "sl_price": payload.sl_price,
                "timeframe": payload.timeframe.value,
            })
            sr.lpush(QUEUE_ORDERS_NEW, order_payload)
            logger.info(
                "Virtual order queued for BO #%d: type=%s tp=%s sl=%s",
                bo.id,
                "LIMIT" if is_limit else "MARKET",
                payload.tp_price, payload.sl_price,
            )
        except Exception as exc:
            logger.error("Failed to queue virtual order for BO #%d: %s", bo.id, exc)

    db.refresh(bo)
    return bo


# ─── Danh sách lệnh ───────────────────────────────────────────────────────────

@router.get("/", response_model=List[BOResponse])
def list_bo(
    bot_name:  Optional[str]          = Query(None),
    symbol:    Optional[BOSymbol]     = Query(None),
    timeframe: Optional[BOTimeframe]  = Query(None),
    forecast:  Optional[BOForecast]   = Query(None),
    result:    Optional[BOResult]     = Query(None),
    limit:     int                    = Query(5000, ge=1, le=10000),
    offset:    int                    = Query(0,    ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(BinaryOption)
    if bot_name:
        q = q.filter(BinaryOption.bot_name.ilike(f"%{bot_name}%"))
    if symbol:
        q = q.filter(BinaryOption.symbol == symbol)
    if timeframe:
        q = q.filter(BinaryOption.timeframe == timeframe)
    if forecast:
        q = q.filter(BinaryOption.forecast == forecast)
    if result:
        q = q.filter(BinaryOption.result == result)
    return q.order_by(BinaryOption.created_at.desc()).offset(offset).limit(limit).all()


# ─── Lấy 1 lệnh ───────────────────────────────────────────────────────────────

@router.get("/{bo_id}", response_model=BOResponse)
def get_bo(bo_id: int, db: Session = Depends(get_db)):
    bo = db.get(BinaryOption, bo_id)
    if not bo:
        raise HTTPException(status_code=404, detail=f"Lệnh #{bo_id} không tồn tại")
    return bo


# ─── Thống kê tổng hợp ────────────────────────────────────────────────────────

@router.get("/stats/summary", response_model=BOStats)
def bo_stats_summary(
    bot_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(BinaryOption)
    if bot_name:
        q = q.filter(BinaryOption.bot_name.ilike(f"%{bot_name}%"))
    s = _compute_stats(q.all())
    return BOStats(**s)


# ─── Thống kê theo Bot ────────────────────────────────────────────────────────

@router.get("/stats/by-bot", response_model=List[BOBotStats])
def bo_stats_by_bot(db: Session = Depends(get_db)):
    """Thống kê chi tiết từng bot: W/L/T, win-rate, profit, ROI."""
    bos = db.query(BinaryOption).all()
    groups: dict[str, list] = defaultdict(list)
    for b in bos:
        groups[b.bot_name].append(b)

    result = []
    for bot_name, items in sorted(groups.items()):
        s = _compute_stats(items)
        roi = (s["total_profit"] / s["total_amount"] * 100) if s["total_amount"] else 0.0
        result.append(BOBotStats(**s, bot_name=bot_name, roi=round(roi, 2)))
    return sorted(result, key=lambda x: x.total_profit, reverse=True)


# ─── Thống kê theo Timeframe ──────────────────────────────────────────────────

@router.get("/stats/by-timeframe", response_model=List[BOTimeframeStats])
def bo_stats_by_timeframe(
    bot_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(BinaryOption)
    if bot_name:
        q = q.filter(BinaryOption.bot_name.ilike(f"%{bot_name}%"))
    bos = q.all()

    tf_order = ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]
    groups: dict[str, list] = defaultdict(list)
    for b in bos:
        groups[b.timeframe].append(b)

    result = []
    for tf in tf_order:
        if tf not in groups:
            continue
        items = groups[tf]
        s = _compute_stats(items)
        avg_amount = s["total_amount"] / s["total"] if s["total"] else 0.0
        result.append(BOTimeframeStats(**s, timeframe=tf, avg_amount=round(avg_amount, 8)))
    return result


# ─── Thống kê theo Forecast (GREEN/RED) ───────────────────────────────────────

@router.get("/stats/by-forecast", response_model=List[BOForecastStats])
def bo_stats_by_forecast(
    bot_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(BinaryOption)
    if bot_name:
        q = q.filter(BinaryOption.bot_name.ilike(f"%{bot_name}%"))
    bos = q.all()

    groups: dict[str, list] = defaultdict(list)
    for b in bos:
        groups[b.forecast].append(b)

    return [
        BOForecastStats(**_compute_stats(items), forecast=val)
        for val, items in sorted(groups.items())
    ]


# ─── Matching engine monitoring ──────────────────────────────────────────────

@router.get("/engine/status")
def engine_status():
    """Redis-backed health check for the WS Feed Service price cache."""
    from services.redis_client import get_sync_redis
    try:
        sr = get_sync_redis()
        keys = sr.keys(f"{PRICE_KEY_PREFIX}:*")
        prices = {}
        for key in keys:
            data = sr.hgetall(key)
            if data:
                age = time.time() - float(data.get("updated_at", 0))
                prices[key] = {
                    "best_ask": data.get("best_ask"),
                    "token_id": data.get("token_id", "")[:16],
                    "age_s": round(age, 1),
                    "stale": age > STALE_THRESHOLD_S,
                }
        return {
            "redis": "connected",
            "total_price_keys": len(keys),
            "prices": prices,
        }
    except Exception as exc:
        return {
            "redis": "error",
            "error": str(exc),
        }
