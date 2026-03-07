from collections import defaultdict
import json
import time
from decimal import Decimal
from typing import List, Optional, Tuple

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
from models import BinaryOption, Bot, BOResult, BOSymbol, BOTimeframe, BOForecast, PriceHistory
from services.polymarket import PolymarketClient
from config.timing import (
    HTTP_TIMEOUT_FAST,
    API_PRICE_CACHE_TTL_S,
)
from config.fees import estimate_max_taker_fee, taker_fee_from_levels
from ws_feed_service.config import (
    PRICE_KEY_PREFIX, STALE_THRESHOLD_S,
    ORDERBOOK_KEY_PREFIX,
)
from schemas import (
    BOBotStats, BOCreate, BOForecastStats, BOResponse,
    BOStats, BOTimeframeStats,
    SessionInfo as SessionInfoSchema, TimelineEvent, TradeInspectResponse,
)
from services.order_trace import make_trace, append_trace

logger = logging.getLogger(__name__)

router = APIRouter()

# GREEN → lấy min_ask của UP, RED → lấy min_ask của DOWN
_FORECAST_TO_STATUS = {"GREEN": "UP", "RED": "DOWN"}


# ─── helpers ──────────────────────────────────────────────────────────────────

_TF_PERIOD_S = {"M5": 300, "M15": 900}


# ── Session resolution ────────────────────────────────────────────────────────

from dataclasses import dataclass

@dataclass
class SessionInfo:
    """Result of session resolution from timestamp / session_offset."""
    candle_open: int        # Unix ts (seconds) — candle open boundary
    settlement_at: "datetime"  # candle close (UTC aware datetime)
    session_offset: int     # effective offset from *current* candle (0-3)
    is_current: bool        # True if the resolved candle is the current one
    bumped: bool = False    # True if boundary guard auto-bumped to next session


def resolve_session(
    timeframe: str,
    timestamp: Optional[int] = None,
    session_offset: int = 0,
) -> SessionInfo:
    """
    Determine which candle session an order belongs to.

    Resolution rules (in priority order):
      1. If ``timestamp`` is provided → use it to find the containing candle.
      2. Otherwise → use ``now`` + ``session_offset``.

    Constraint: the resolved candle must be the current candle or up to 3
    candles ahead.  Anything further (past or future) is rejected with ValueError.

    Parameters
    ----------
    timeframe : str
        "M5" or "M15".
    timestamp : int | None
        Optional Unix timestamp (seconds).  When set, ``session_offset``
        is ignored.
    session_offset : int
        0 = current candle, 1-3 = future candles.  Only used when
        ``timestamp`` is None.

    Returns
    -------
    SessionInfo

    Raises
    ------
    ValueError
        If timeframe is unsupported or the resolved candle is out of range.
    """
    period = _TF_PERIOD_S.get(timeframe)
    if period is None:
        raise ValueError(f"Unsupported timeframe: {timeframe!r}")

    MAX_SESSION_OFFSET = 3

    now = int(time.time())
    current_open = now - (now % period)          # candle open of the *current* candle
    max_open     = current_open + period * MAX_SESSION_OFFSET  # max allowed (3 sessions ahead)

    bumped = False

    if timestamp is not None:
        # Resolve candle from the provided timestamp
        target_open = timestamp - (timestamp % period)

        if target_open < current_open:
            raise ValueError(
                f"timestamp {timestamp} falls in a past candle "
                f"(candle_open={target_open}, current_open={current_open}). "
                f"Only current or future sessions (up to +{MAX_SESSION_OFFSET}) are accepted."
            )
        if target_open > max_open:
            raise ValueError(
                f"timestamp {timestamp} falls too far in the future "
                f"(candle_open={target_open}, max_allowed={max_open}). "
                f"Only up to {MAX_SESSION_OFFSET} sessions ahead is accepted."
            )

        effective_offset = (target_open - current_open) // period  # 0, 1, 2, or 3
        candle_open = target_open
    else:
        # Use session_offset from current time
        effective_offset = min(session_offset, MAX_SESSION_OFFSET)
        candle_open = current_open + period * effective_offset

        # Boundary guard disabled — orders near candle boundary stay in
        # the current session.  The prefetch infrastructure ensures WS
        # data is available for the next session if needed.

    # Settlement = candle close = candle_open + period
    from datetime import datetime as _dt, timezone as _tz
    settlement_at = _dt.fromtimestamp(candle_open + period, tz=_tz.utc)

    return SessionInfo(
        candle_open=candle_open,
        settlement_at=settlement_at,
        session_offset=effective_offset,
        is_current=(effective_offset == 0),
        bumped=bumped,
    )


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
        total_profit = round(sum(b.profit or 0 for b in items if b.result in (BOResult.WIN, BOResult.LOSS)), 8),
        total_amount = round(sum(b.amount for b in items), 8),
    )


# ─── Tạo lệnh ─────────────────────────────────────────────────────────────────

_DEFAULT_SLIPPAGE_TOLERANCE = 0.10

# Shared PolymarketClient for order-time REST fetches
_pm_client: Optional[PolymarketClient] = None


def _get_pm_client() -> PolymarketClient:
    """Return a shared PolymarketClient instance."""
    global _pm_client
    if _pm_client is None:
        _pm_client = PolymarketClient(timeout=HTTP_TIMEOUT_FAST)
    return _pm_client


def _resolve_token_id(
    symbol: str, timeframe: str, direction: str,
    candle_open: int = 0,
) -> Optional[str]:
    """Resolve token_id from Redis token mapping for a (symbol, tf, direction, candle_open).

    Reads ``tokens:{SYM}:{TF}`` JSON, matches candle_open to find the correct token.
    Returns None if mapping is unavailable.
    """
    try:
        from services.redis_client import get_sync_redis
        sr = get_sync_redis()
        key = f"tokens:{symbol}:{timeframe}"
        raw = sr.get(key)
        if not raw:
            return None
        data = json.loads(raw)
        dir_data = data.get(direction)
        if not dir_data:
            return None

        # Check current session
        current = dir_data.get("current")
        if current and current.get("session") == candle_open:
            return current.get("token_id")

        # Check future sessions
        for entry in dir_data.get("future", []):
            if entry.get("session") == candle_open:
                return entry.get("token_id")

        # Fallback: return current token if candle_open matches approximately
        if current and current.get("token_id"):
            return current.get("token_id")

        return None
    except Exception:
        return None


def _fetch_orderbook_from_redis(
    symbol: str, timeframe: str, direction: str,
    candle_open: int = 0,
) -> Optional[list[list]]:
    """Try to read asks from Redis orderbook snapshot. Returns None if unavailable."""
    try:
        from services.redis_client import get_sync_redis
        sr = get_sync_redis()
        key = f"{ORDERBOOK_KEY_PREFIX}:{symbol}:{timeframe}:{direction}:{candle_open}"
        data = sr.hgetall(key)
        if not data or "asks" not in data:
            return None
        asks = json.loads(data["asks"])
        return asks if asks else None
    except Exception:
        return None


def _fetch_orderbook_rest(
    symbol: str, timeframe: str, direction: str,
    candle_open: int = 0,
) -> list[list]:
    """Fetch asks from Redis orderbook (populated by WS Feed / RestPoller).

    Primary: read from Redis session-keyed orderbook snapshot.
    Fallback: resolve token_id → call Polymarket CLOB /book REST API.

    Returns sorted asks list (ascending by price) as [[price, size], ...].
    Raises HTTPException(503) if both sources are unavailable.
    """
    # 1. Primary: Redis orderbook snapshot (written by WS Feed / RestPoller every 200ms)
    redis_asks = _fetch_orderbook_from_redis(symbol, timeframe, direction, candle_open)
    if redis_asks:
        return redis_asks

    # 2. Fallback: Polymarket REST API (only if Redis has no data)
    token_id = _resolve_token_id(symbol, timeframe, direction, candle_open)
    if token_id:
        try:
            pm = _get_pm_client()
            bids_raw, asks_raw = pm.fetch_book_raw(token_id)
            if asks_raw:
                sorted_asks = sorted(asks_raw, key=lambda x: float(x["price"]))
                return [[float(a["price"]), float(a["size"])] for a in sorted_asks]
        except Exception as exc:
            logger.warning("REST orderbook fallback failed for %s: %s", token_id[:16], exc)

    raise HTTPException(
        status_code=503,
        detail="Orderbook unavailable — Redis empty and REST API fallback failed",
    )


def _fetch_asks_from_polymarket(
    symbol: str, timeframe: str, direction: str,
    candle_open: int = 0,
) -> list[list]:
    """Fetch asks directly from Polymarket REST API for order filling.

    Primary: Polymarket CLOB /book REST API (fresh orderbook, most accurate).
    Fallback: Redis orderbook snapshot (if REST fails or token not resolved).

    Returns sorted asks list (ascending by price) as [[price, size], ...].
    Raises HTTPException(503) if both sources are unavailable.
    """
    # 1. Primary: Polymarket REST API (fresh data for accurate fills)
    token_id = _resolve_token_id(symbol, timeframe, direction, candle_open)
    if token_id:
        try:
            pm = _get_pm_client()
            bids_raw, asks_raw = pm.fetch_book_raw(token_id)
            if asks_raw:
                sorted_asks = sorted(asks_raw, key=lambda x: float(x["price"]))
                return [[float(a["price"]), float(a["size"])] for a in sorted_asks]
        except Exception as exc:
            logger.warning(
                "Polymarket REST fetch failed for %s: %s — falling back to Redis",
                token_id[:16], exc,
            )

    # 2. Fallback: Redis orderbook snapshot
    redis_asks = _fetch_orderbook_from_redis(symbol, timeframe, direction, candle_open)
    if redis_asks:
        logger.info(
            "Using Redis fallback for orderbook %s:%s:%s:%d",
            symbol, timeframe, direction, candle_open,
        )
        return redis_asks

    raise HTTPException(
        status_code=503,
        detail="Orderbook unavailable — Polymarket REST API and Redis both failed",
    )


def _snapshot_best_ask(
    symbol: str, timeframe: str, direction: str,
    candle_open: int = 0,
) -> Optional[float]:
    """Fetch the best ask from Polymarket REST API.

    Returns None if unavailable.
    """
    try:
        asks = _fetch_orderbook_rest(symbol, timeframe, direction, candle_open)
        if not asks:
            return None
        return float(asks[0][0])
    except HTTPException:
        return None
    except Exception:
        return None


def _fill_market_from_snapshot(
    symbol: str, timeframe: str, direction: str, amount: float,
    slippage_tolerance: Optional[float] = None,
    limit_price: Optional[float] = None,
    candle_open: int = 0,
    order_type: str = "FAK",
    ceiling_price: Optional[float] = None,
) -> Tuple[float, float, list]:
    """
    Simulate a BUY by walking asks fetched from Polymarket REST API.

    Walks levels until ``amount`` is spent or the price cap is exceeded.

    Price cap logic:
    - MARKET orders (limit_price=None): cap = best_ask × (1 + slippage)
    - Aggressive LIMIT orders (limit_price set): cap = limit_price
    - When ceiling_price is set, it overrides the above cap with
      min(ceiling_price, original_cap). Best-price-first is guaranteed
      since asks are sorted ascending.

    Order type logic:
    - FAK (Fill-And-Kill): fill as much as possible up to ceiling_price,
      cancel the unfilled remainder immediately.
    - FOK (Fill-Or-Kill): check if the full amount can be filled within
      ceiling_price; if not, reject the entire order.

    Returns
    -------
    (avg_price, num_shares, walk_levels)
        walk_levels: list of {"price": float, "qty": float, "cost": float}

    Raises
    ------
    HTTPException(503)
        If the orderbook is unavailable or has no asks.
    HTTPException(400)
        If FOK order cannot be fully filled, or slippage exceeded.
    """
    asks = _fetch_asks_from_polymarket(symbol, timeframe, direction, candle_open)

    # Price cap: LIMIT orders use limit_price; MARKET orders use slippage from best_ask
    if limit_price is not None:
        max_price = Decimal(str(limit_price))
    else:
        slippage = slippage_tolerance if slippage_tolerance is not None else _DEFAULT_SLIPPAGE_TOLERANCE
        best_ask = Decimal(str(asks[0][0]))
        max_price = best_ask * (Decimal("1") + Decimal(str(slippage)))

    # Apply ceiling_price cap — always uses the tighter of max_price and ceiling_price.
    # Asks are sorted ascending (best price first), so walking naturally
    # fills the cheapest levels first even when ceiling_price > current market.
    if ceiling_price is not None:
        ceiling_dec = Decimal(str(ceiling_price))
        max_price = min(max_price, ceiling_dec)

    budget = Decimal(str(amount))
    total_cost = Decimal("0")
    total_qty = Decimal("0")
    walk_levels: list[dict] = []

    for price_raw, size_raw in asks:
        price = Decimal(str(price_raw))
        size = Decimal(str(size_raw))

        if price > max_price:
            break

        remaining_budget = budget - total_cost
        if remaining_budget <= 0:
            break

        max_qty_at_level = remaining_budget / price
        fill_qty = min(size, max_qty_at_level)
        level_cost = fill_qty * price

        total_cost += level_cost
        total_qty += fill_qty
        walk_levels.append({
            "price": float(price),
            "qty": float(fill_qty),
            "cost": float(level_cost),
        })

        if total_cost >= budget:
            break

    # FOK: require full budget to be spent; otherwise reject entirely
    if order_type == "FOK" and ceiling_price is not None:
        # Budget not fully consumed means insufficient liquidity under ceiling_price
        if total_cost < budget and total_qty > 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"FOK order rejected: insufficient liquidity under ceiling price "
                    f"{ceiling_price:.4f}. Available: ${float(total_cost):.4f} "
                    f"of ${amount:.4f} requested."
                ),
            )

    if total_qty == 0:
        if order_type == "FOK":
            raise HTTPException(
                status_code=400,
                detail=f"FOK order rejected: no liquidity available under ceiling price {ceiling_price}",
            )
        raise HTTPException(
            status_code=503,
            detail="Could not fill any shares from orderbook",
        )

    avg_price = float(total_cost / total_qty)
    num_shares = float(total_qty)
    return round(avg_price, 8), round(num_shares, 8), walk_levels


def _fill_limit_from_snapshot(
    symbol: str, timeframe: str, direction: str, amount: float,
    limit_price: float,
    candle_open: int = 0,
) -> Tuple[float, float, list, float]:
    """
    Fill a LIMIT order by walking asks fetched from Polymarket REST API.

    Only fills levels where ask price <= limit_price.  Any budget left
    over (because the orderbook ran out of liquidity at or below limit_price)
    is reported as ``remaining_budget`` so the caller can queue it to the
    matching engine as a passive LIMIT order.

    Returns
    -------
    (avg_price, num_shares, walk_levels, remaining_budget)
        avg_price:        0.0 if nothing filled
        num_shares:       0.0 if nothing filled
        walk_levels:      list of {"price", "qty", "cost"}
        remaining_budget: unspent amount to queue to ME

    Raises
    ------
    HTTPException(503)
        If orderbook is unavailable.
    """
    asks = _fetch_asks_from_polymarket(symbol, timeframe, direction, candle_open)

    max_price = Decimal(str(limit_price))
    budget = Decimal(str(amount))
    total_cost = Decimal("0")
    total_qty = Decimal("0")
    walk_levels: list[dict] = []

    for price_raw, size_raw in asks:
        price = Decimal(str(price_raw))
        size = Decimal(str(size_raw))

        if price > max_price:
            break

        remaining = budget - total_cost
        if remaining <= 0:
            break

        max_qty_at_level = remaining / price
        fill_qty = min(size, max_qty_at_level)
        level_cost = fill_qty * price

        total_cost += level_cost
        total_qty += fill_qty
        walk_levels.append({
            "price": float(price),
            "qty": float(fill_qty),
            "cost": float(level_cost),
        })

        if total_cost >= budget:
            break

    remaining_budget = float(budget - total_cost)
    if remaining_budget < 0:
        remaining_budget = 0.0

    avg_price = float(total_cost / total_qty) if total_qty > 0 else 0.0
    num_shares = float(total_qty)
    return (
        round(avg_price, 8),
        round(num_shares, 8),
        walk_levels,
        round(remaining_budget, 8),
    )


def _queue_order_to_session(
    bo_id: int,
    session_id: str,
    order_data: dict,
) -> None:
    """
    Push an order payload to the per-session Redis queue.

    Centralizes the queue push logic used by all order paths (passive LIMIT,
    future MARKET, prefilled bracket).

    Args:
        bo_id: BinaryOption ID for logging.
        session_id: e.g. "BTC:M5:1709313000" — determines the queue key.
        order_data: Full order payload dict (will be JSON-serialized).
    """
    from services.redis_client import get_sync_redis
    sr = get_sync_redis()
    session_queue_key = f"queue:orders:{session_id}"
    order_data["session_id"] = session_id
    sr.lpush(session_queue_key, json.dumps(order_data))
    logger.info(
        "Order queued: bo_id=%d → queue=%s",
        bo_id, session_queue_key,
    )


def _queue_prefilled_to_me(
    bo: "BinaryOption",
    avg_price: float, num_shares: float,
    payload: "BOCreate",
    direction: str = "UP",
    session_offset: int = 0,
    settlement_at: Optional[datetime] = None,
    candle_open: Optional[int] = None,
) -> None:
    """Queue a pre-filled MARKET bracket order to the matching engine via Redis."""
    try:
        if candle_open is None:
            _period = _TF_PERIOD_S.get(payload.timeframe.value, 300)
            candle_open = int(settlement_at.timestamp()) - _period if settlement_at else 0
        session_id = f"{payload.symbol.value}:{payload.timeframe.value}:{candle_open}"
        _queue_order_to_session(bo.id, session_id, {
            "bo_id": bo.id,
            "direction": direction,
            "symbol": payload.symbol.value,
            "forecast": payload.forecast.value,
            "side": "BUY",
            "prefilled": True,
            "prefilled_avg_price": avg_price,
            "prefilled_filled": num_shares,
            "tp_price": payload.tp_price,
            "sl_price": payload.sl_price,
            "timeframe": payload.timeframe.value,
            "session_offset": session_offset,
            "settlement_at": settlement_at.isoformat() if settlement_at else None,
        })

        condition_type = "TP" if payload.tp_price else "SL"
        condition_price = payload.tp_price or payload.sl_price
        append_trace(bo, make_trace(
            "MONITORING", "BRACKET_QUEUED",
            f"Active Monitoring started. Condition {condition_type} at "
            f"${condition_price:.4f}. Watching for trigger via WebSocket.",
            {"condition_type": condition_type, "condition_price": condition_price,
             "avg_entry_price": avg_price, "num_shares": num_shares},
        ))
    except Exception as exc:
        logger.error(
            "Failed to queue prefilled bracket order for BO #%d: %s",
            bo.id, exc,
        )


@router.post("", response_model=BOResponse, status_code=201)
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

    if getattr(bot, "status", "ACTIVE") != "ACTIVE":
        raise HTTPException(status_code=400, detail=f"Bot is {bot.status}. Only ACTIVE bots can trade.")

    if bot.balance < payload.amount:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient balance: {bot.balance:.2f} < {payload.amount:.2f}",
        )

    pending_traces: list[dict] = []

    # Deduct amount from balance upfront — refunded on cancel, settled on WIN/LOSS
    is_limit   = payload.limit_price is not None

    # Aggressive limit detection is deferred until after session resolution
    # so we can pass the correct candle_open to _snapshot_best_ask().
    # For now, assume taker fee for non-limit orders; limit orders recalculate below.
    is_aggressive_limit = False

    max_fee = estimate_max_taker_fee(payload.amount) if not is_limit else 0
    if bot.balance < payload.amount + max_fee:
        raise HTTPException(status_code=400, detail="Insufficient balance (including estimated fee)")
    bot.balance = round(bot.balance - payload.amount, 8)

    direction  = _FORECAST_TO_STATUS[payload.forecast.value]  # "UP" or "DOWN"

    has_bracket = payload.tp_price is not None or payload.sl_price is not None

    # ── Session resolution ────────────────────────────────────────────────
    try:
        session = resolve_session(
            timeframe=payload.timeframe.value,
            timestamp=payload.timestamp,
            session_offset=payload.session_offset or 0,
        )
    except ValueError as exc:
        bot.balance = round(bot.balance + payload.amount, 8)
        raise HTTPException(status_code=400, detail=str(exc))

    settlement_at = session.settlement_at
    session_offset = session.session_offset
    candle_open = session.candle_open
    session_id = f"{payload.symbol.value}:{payload.timeframe.value}:{candle_open}"

    # Session routing trace
    pending_traces.append(make_trace(
        "ROUTING", "SESSION_ROUTED",
        f"Order routed to session {session_id} "
        f"(offset={session_offset}, candle_open={candle_open}, "
        f"settlement={settlement_at.isoformat()}).",
        {"session_id": session_id, "candle_open": candle_open,
         "session_offset": session_offset,
         "queue_key": f"queue:orders:{session_id}",
         "is_current": session.is_current, "bumped": session.bumped},
    ))

    # ── Session availability check ───────────────────────────────────────
    # Validate that orderbook data exists for the target session.
    # Token resolution is deferred to OrderConsumer (which reads from
    # session.tokens[direction]).  The API only needs the session_id +
    # direction to route the order to the correct queue.
    snapshot_best_ask = _snapshot_best_ask(
        payload.symbol.value, payload.timeframe.value,
        direction, candle_open=candle_open,
    )

    if not session.is_current and snapshot_best_ask is None:
        # Future session with no orderbook data yet — ME hasn't created it
        bot.balance = round(bot.balance + payload.amount, 8)
        raise HTTPException(
            status_code=503,
            detail="Market for the target session is not available yet",
        )

    # For current sessions, also check that ME is running (snapshot exists)
    if session.is_current and snapshot_best_ask is None:
        bot.balance = round(bot.balance + payload.amount, 8)
        raise HTTPException(
            status_code=503,
            detail="Price data unavailable — matching engine may not be running",
        )

    ask_fetched_at = datetime.now(timezone.utc)
    latency_ms = (ask_fetched_at - order_received_at).total_seconds() * 1000

    # ── Pre-validation: ceiling_price vs Best Ask ────────────────────────────
    # ceiling_price must be >= best_ask; otherwise no fill is possible.
    if payload.ceiling_price is not None and snapshot_best_ask is not None:
        if payload.ceiling_price < snapshot_best_ask:
            bot.balance = round(bot.balance + payload.amount, 8)
            pending_traces.append(make_trace(
                "VALIDATION", "CEILING_PRICE_REJECTED",
                f"Ceiling price ${payload.ceiling_price:.4f} is below Best Ask "
                f"${snapshot_best_ask:.4f}. No fills possible. Order rejected.",
                {"ceiling_price": payload.ceiling_price, "best_ask": snapshot_best_ask,
                 "order_type": payload.order_type},
            ))
            raise HTTPException(
                status_code=400,
                detail=(
                    f"ceiling_price ({payload.ceiling_price:.4f}) is below current "
                    f"Best Ask ({snapshot_best_ask:.4f}) — no fills possible"
                ),
            )

    # ── Pre-validation: condition price vs Best Ask (v2 spec Section 2) ────
    # Prevents "logical suicide" orders where the condition is already met.
    # Runs after session resolution so we have the correct candle_open.
    if snapshot_best_ask is not None and (payload.tp_price is not None or payload.sl_price is not None):
        if payload.sl_price is not None and payload.sl_price >= snapshot_best_ask:
            bot.balance = round(bot.balance + payload.amount, 8)
            pending_traces.append(make_trace(
                "VALIDATION", "PRE_VALIDATION_FAILED",
                f"Validation Failed: SL ${payload.sl_price:.4f} must be lower than "
                f"estimated entry ${snapshot_best_ask:.4f}. Order Rejected.",
                {"sl_price": payload.sl_price, "best_ask": snapshot_best_ask},
            ))
            raise HTTPException(
                status_code=400,
                detail=(
                    f"SL price ({payload.sl_price:.4f}) must be lower than "
                    f"current Best Ask ({snapshot_best_ask:.4f})"
                ),
            )
        if payload.tp_price is not None and payload.tp_price <= snapshot_best_ask:
            bot.balance = round(bot.balance + payload.amount, 8)
            pending_traces.append(make_trace(
                "VALIDATION", "PRE_VALIDATION_FAILED",
                f"Validation Failed: TP ${payload.tp_price:.4f} must be higher than "
                f"estimated entry ${snapshot_best_ask:.4f}. Order Rejected.",
                {"tp_price": payload.tp_price, "best_ask": snapshot_best_ask},
            ))
            raise HTTPException(
                status_code=400,
                detail=(
                    f"TP price ({payload.tp_price:.4f}) must be higher than "
                    f"current Best Ask ({snapshot_best_ask:.4f})"
                ),
            )
        # Validation passed
        condition_type = "TP" if payload.tp_price is not None else "SL"
        condition_price = payload.tp_price if payload.tp_price is not None else payload.sl_price
        pending_traces.append(make_trace(
            "VALIDATION", "PRE_VALIDATION_OK",
            f"Pre-validation successful. Condition {condition_type} at "
            f"${condition_price:.4f} is valid against current Best Ask ${snapshot_best_ask:.4f}.",
            {"condition_type": condition_type, "condition_price": condition_price,
             "best_ask": snapshot_best_ask},
        ))

    # ── Aggressive limit detection (after session resolution) ─────────
    # Now we have session.candle_open so we read the correct orderbook.
    if is_limit:
        if snapshot_best_ask is not None and payload.limit_price >= snapshot_best_ask:
            is_aggressive_limit = True
            # Check balance covers estimated taker fee (actual fee deducted after fill)
            extra_fee = estimate_max_taker_fee(payload.amount)
            if bot.balance < extra_fee:
                bot.balance = round(bot.balance + payload.amount, 8)
                raise HTTPException(
                    status_code=400,
                    detail="Insufficient balance (including estimated taker fee for aggressive limit)",
                )

    # ── Future sessions ──────────────────────────────────────────────
    # Orderbook snapshots are session-keyed (orderbook:{SYM}:{TF}:{DIR}:{candle_open})
    # so both aggressive limit detection and MARKET fills use the correct
    # session's prices.  No special-casing needed.

    if is_limit and is_aggressive_limit:
        # ── AGGRESSIVE LIMIT: limit_price >= best_ask → fill what's available
        # at prices <= limit_price, queue remaining budget to ME as LIMIT.
        try:
            avg_price, num_shares, walk_levels, remaining_budget = _fill_limit_from_snapshot(
                payload.symbol.value, payload.timeframe.value, direction,
                payload.amount,
                limit_price=payload.limit_price,
                candle_open=candle_open,
            )
        except HTTPException:
            bot.balance = round(bot.balance + payload.amount, 8)
            raise

        original_amount = payload.amount

        # Apply taker fee on the filled portion
        entry_fee = taker_fee_from_levels(walk_levels) if walk_levels else 0.0
        if entry_fee > 0:
            bot.balance = round(bot.balance - entry_fee, 8)

        # Determine ME status based on whether there's a remainder to queue
        has_remainder = remaining_budget > 0
        if num_shares > 0 and has_remainder:
            me_order_status = "PARTIAL"
        elif num_shares > 0 and not has_remainder:
            me_order_status = "PREFILLED" if has_bracket else None
        else:
            me_order_status = "PENDING"

        order_traces = list(pending_traces)
        order_traces.append(make_trace(
            "MATCHING", "AGGRESSIVE_LIMIT_FILL",
            f"LIMIT Order: filled ${original_amount - remaining_budget:.4f} "
            f"of ${original_amount:.4f} at prices <= ${payload.limit_price:.4f}. "
            f"Avg Entry Price: ${avg_price:.4f}. Shares: {num_shares:.4f}. "
            f"Fee: ${entry_fee:.4f} (TAKER). "
            f"Remainder: ${remaining_budget:.4f} queued to ME."
            if has_remainder else
            f"LIMIT Order fully filled (limit ${payload.limit_price:.4f} "
            f">= best ask). Avg Entry: ${avg_price:.4f}. "
            f"Shares: {num_shares:.4f}. Fee: ${entry_fee:.4f} (TAKER).",
            {"order_type": "LIMIT", "aggressive": True,
             "limit_price": payload.limit_price,
             "avg_price": avg_price, "num_shares": num_shares,
             "walk_levels": walk_levels, "entry_fee": entry_fee,
             "amount": original_amount,
             "filled_amount": round(original_amount - remaining_budget, 8),
             "remaining_budget": remaining_budget},
        ))

        logger.info(
            "BO AGGRESSIVE LIMIT %s %s %s: amount=%.4f limit=%s "
            "avg_price=%.6f shares=%.4f fee=%.4f remainder=%.4f "
            "latency=%.0fms tp=%s sl=%s",
            payload.symbol.value, payload.timeframe.value,
            payload.forecast.value,
            original_amount, payload.limit_price,
            avg_price, num_shares, entry_fee, remaining_budget,
            latency_ms, payload.tp_price, payload.sl_price,
        )

        bo = BinaryOption(
            bot_name          = bot.bot_name,
            symbol            = payload.symbol,
            timeframe         = payload.timeframe,
            forecast          = payload.forecast,
            amount            = original_amount,
            original_amount   = original_amount,
            result            = BOResult.PENDING,
            avg_price         = avg_price if num_shares > 0 else None,
            num_shares        = num_shares if num_shares > 0 else None,
            reason            = payload.reason,
            order_received_at = order_received_at,
            ask_fetched_at    = ask_fetched_at,
            settlement_at     = settlement_at,
            limit_price       = payload.limit_price,
            tp_price          = payload.tp_price,
            sl_price          = payload.sl_price,
            ttl               = payload.ttl,
            session_offset    = session_offset,
            session_id        = session_id,
            candle_open       = candle_open,
            entry_fee         = entry_fee,
            order_type        = payload.order_type,
            ceiling_price       = payload.ceiling_price,
            me_order_status   = me_order_status,
            walk_prices       = {"entry": walk_levels} if walk_levels else None,
            traces            = order_traces if order_traces else None,
        )
        db.add(bo)
        db.commit()
        db.refresh(bo)

        # Queue remainder to ME as passive LIMIT at limit_price
        if has_remainder:
            est_qty = round(remaining_budget / payload.limit_price, 8)
            try:
                remainder_payload = {
                    "bo_id": bo.id,
                    "direction": direction,
                    "symbol": payload.symbol.value,
                    "forecast": payload.forecast.value,
                    "side": "BUY",
                    "price": payload.limit_price,
                    "expected_price": payload.limit_price,
                    "quantity": est_qty,
                    "amount": remaining_budget,
                    "limit_price": payload.limit_price,
                    "timeframe": payload.timeframe.value,
                    "ttl": payload.ttl,
                    "slippage_tolerance": payload.slippage_tolerance,
                    "session_offset": session_offset,
                    "settlement_at": settlement_at.isoformat() if settlement_at else None,
                }
                # Pass REST prefill data so OrderConsumer can merge fills
                # into the BO's existing avg_price/num_shares.
                # NOT setting "prefilled": True — this must go through
                # _process_standard_order to actually place the LIMIT order.
                if num_shares > 0:
                    remainder_payload["rest_prefill_avg"] = avg_price
                    remainder_payload["rest_prefill_filled"] = num_shares
                # Only pass tp/sl to remainder if there's NO separate bracket
                # order being queued (to avoid duplicate bracket exits).
                if not has_bracket:
                    remainder_payload["tp_price"] = payload.tp_price
                    remainder_payload["sl_price"] = payload.sl_price
                _queue_order_to_session(bo.id, session_id, remainder_payload)
                append_trace(bo, make_trace(
                    "MATCHING", "REMAINDER_QUEUED",
                    f"Unfilled ${remaining_budget:.4f} queued to ME as LIMIT "
                    f"at ${payload.limit_price:.4f}. Est qty: {est_qty:.4f}. "
                    f"Expires at TTL={payload.ttl}s or settlement.",
                    {"remaining_budget": remaining_budget,
                     "limit_price": payload.limit_price,
                     "est_qty": est_qty, "ttl": payload.ttl},
                ))
            except Exception as exc:
                logger.error(
                    "Failed to queue remainder for BO #%d: %s", bo.id, exc,
                )
            # Separately queue bracket monitoring for the already-filled portion
            if has_bracket and num_shares > 0:
                _queue_prefilled_to_me(
                    bo, avg_price, num_shares, payload,
                    direction=direction,
                    session_offset=session_offset,
                    settlement_at=settlement_at,
                    candle_open=candle_open,
                )
        elif has_bracket:
            # Fully filled + has bracket → queue for TP/SL monitoring
            _queue_prefilled_to_me(
                bo, avg_price, num_shares, payload,
                direction=direction,
                session_offset=session_offset,
                settlement_at=settlement_at,
                candle_open=candle_open,
            )

    elif is_limit:
        # ── PASSIVE LIMIT: limit_price < best_ask → queue to ME, wait for fill
        # No fee charged upfront; maker rebate applied when ME fills.
        est_qty = round(payload.amount / payload.limit_price, 8)
        entry_fee = 0.0

        order_traces = list(pending_traces)
        order_traces.append(make_trace(
            "MATCHING", "ORDER_QUEUED",
            f"LIMIT Order queued to Matching Engine. "
            f"Limit: ${payload.limit_price:.4f}. "
            f"Amount: ${payload.amount:.4f}. "
            f"Waiting for ME fill.",
            {"order_type": "LIMIT", "limit_price": payload.limit_price,
             "amount": payload.amount, "tp_price": payload.tp_price,
             "sl_price": payload.sl_price, "ttl": payload.ttl},
        ))

        logger.info(
            "BO LIMIT %s %s %s: amount=%.4f limit=%s latency=%.0fms "
            "tp=%s sl=%s → queued to ME",
            payload.symbol.value, payload.timeframe.value,
            payload.forecast.value,
            payload.amount, payload.limit_price, latency_ms,
            payload.tp_price, payload.sl_price,
        )

        bo = BinaryOption(
            bot_name          = bot.bot_name,
            symbol            = payload.symbol,
            timeframe         = payload.timeframe,
            forecast          = payload.forecast,
            amount            = payload.amount,
            original_amount   = payload.amount,
            result            = BOResult.PENDING,
            avg_price         = None,
            num_shares        = None,
            reason            = payload.reason,
            order_received_at = order_received_at,
            ask_fetched_at    = ask_fetched_at,
            settlement_at     = settlement_at,
            limit_price       = payload.limit_price,
            tp_price          = payload.tp_price,
            sl_price          = payload.sl_price,
            ttl               = payload.ttl,
            session_offset    = session_offset,
            session_id        = session_id,
            candle_open       = candle_open,
            entry_fee         = entry_fee,
            order_type        = payload.order_type,
            ceiling_price       = payload.ceiling_price,
            me_order_status   = "PENDING",
            traces            = order_traces if order_traces else None,
        )
        db.add(bo)
        db.commit()
        db.refresh(bo)

        # Queue to ME via per-session Redis queue
        try:
            _queue_order_to_session(bo.id, session_id, {
                "bo_id": bo.id,
                "direction": direction,
                "symbol": payload.symbol.value,
                "forecast": payload.forecast.value,
                "side": "BUY",
                "price": payload.limit_price,
                "expected_price": payload.limit_price,
                "quantity": est_qty,
                "amount": payload.amount,
                "limit_price": payload.limit_price,
                "tp_price": payload.tp_price,
                "sl_price": payload.sl_price,
                "timeframe": payload.timeframe.value,
                "ttl": payload.ttl,
                "slippage_tolerance": payload.slippage_tolerance,
                "session_offset": session_offset,
                "settlement_at": settlement_at.isoformat() if settlement_at else None,
            })
        except Exception as exc:
            logger.error(
                "Failed to queue order for BO #%d: %s",
                bo.id, exc,
            )

    else:
        # ── MARKET path: fill immediately from session-keyed orderbook snapshot ──
        try:
            avg_price, num_shares, walk_levels = _fill_market_from_snapshot(
                payload.symbol.value, payload.timeframe.value, direction,
                payload.amount,
                slippage_tolerance=payload.slippage_tolerance,
                candle_open=candle_open,
                order_type=payload.order_type or "FAK",
                ceiling_price=payload.ceiling_price,
            )
        except HTTPException:
            bot.balance = round(bot.balance + payload.amount, 8)
            raise

        # FAK partial fill: refund unspent budget to bot
        actual_cost = sum(lv["cost"] for lv in walk_levels)
        if actual_cost < payload.amount:
            refund = round(payload.amount - actual_cost, 8)
            bot.balance = round(bot.balance + refund, 8)
            payload.amount = round(actual_cost, 8)

        # Apply taker fee
        entry_fee = taker_fee_from_levels(walk_levels)
        if entry_fee > 0:
            bot.balance = round(bot.balance - entry_fee, 8)

        me_order_status = "PREFILLED" if has_bracket else None

        order_traces = list(pending_traces)
        order_traces.append(make_trace(
            "MATCHING", "REST_FILL",
            f"MARKET Order filled from Polymarket REST orderbook. "
            f"Avg Entry Price: ${avg_price:.4f}. "
            f"Shares: {num_shares:.4f}. "
            f"Fee: ${entry_fee:.4f} (TAKER).",
            {"order_type": "MARKET", "avg_price": avg_price,
             "num_shares": num_shares, "walk_levels": walk_levels,
             "entry_fee": entry_fee, "amount": payload.amount},
        ))

        logger.info(
            "BO MARKET %s %s %s: amount=%.4f avg_price=%.6f shares=%.4f "
            "fee=%.4f latency=%.0fms tp=%s sl=%s → filled from REST orderbook",
            payload.symbol.value, payload.timeframe.value,
            payload.forecast.value,
            payload.amount, avg_price, num_shares, entry_fee, latency_ms,
            payload.tp_price, payload.sl_price,
        )

        bo = BinaryOption(
            bot_name          = bot.bot_name,
            symbol            = payload.symbol,
            timeframe         = payload.timeframe,
            forecast          = payload.forecast,
            amount            = payload.amount,
            original_amount   = payload.amount,
            result            = BOResult.PENDING,
            avg_price         = avg_price,
            num_shares        = num_shares,
            reason            = payload.reason,
            order_received_at = order_received_at,
            ask_fetched_at    = ask_fetched_at,
            settlement_at     = settlement_at,
            limit_price       = None,
            tp_price          = payload.tp_price,
            sl_price          = payload.sl_price,
            ttl               = payload.ttl,
            session_offset    = session_offset,
            session_id        = session_id,
            candle_open       = candle_open,
            entry_fee         = entry_fee,
            order_type        = payload.order_type,
            ceiling_price       = payload.ceiling_price,
            me_order_status   = me_order_status,
            walk_prices       = {"entry": walk_levels},
            traces            = order_traces if order_traces else None,
        )
        db.add(bo)
        db.commit()
        db.refresh(bo)

        # If has bracket (TP/SL), queue as prefilled to ME for monitoring
        if has_bracket:
            _queue_prefilled_to_me(
                bo, avg_price, num_shares, payload,
                direction=direction,
                session_offset=session_offset,
                settlement_at=settlement_at,
                candle_open=candle_open,
            )

    # Persist any traces added after initial commit (e.g. from _queue_prefilled_to_me)
    db.commit()
    db.refresh(bo)

    # Publish latest trace to Redis for real-time UI
    if bo.traces:
        try:
            from services.redis_client import get_sync_redis
            from services.order_trace import publish_trace_to_redis
            sr = get_sync_redis()
            publish_trace_to_redis(sr, bo.id, bo.traces[-1])
        except Exception:
            pass

    return bo


# ─── Danh sách lệnh ───────────────────────────────────────────────────────────

@router.get("", response_model=List[BOResponse])
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


# ─── Token mapping for direct Polymarket WS ──────────────────────────────────

@router.get("/tokens")
async def get_token_mapping(
    symbol: str = Query(..., description="Symbol (BTC, ETH, SOL, XRP)"),
    timeframe: str = Query(..., description="Timeframe (M5, M15, H1)"),
):
    """
    Return Polymarket token_id mapping for a symbol/timeframe pair.

    The UI uses this to connect directly to Polymarket's WebSocket for
    real-time orderbook data, bypassing the backend WS relay.
    """
    from services.redis_client import get_async_redis

    sym = symbol.upper()
    tf = timeframe.upper()
    if sym not in ("BTC", "ETH"):
        raise HTTPException(400, f"Unsupported symbol: {sym}")
    if tf not in ("M5", "M15"):
        raise HTTPException(400, f"Unsupported timeframe: {tf}")

    r = get_async_redis()
    redis_key = f"tokens:{sym}:{tf}"
    raw = await r.get(redis_key)
    if not raw:
        raise HTTPException(404, f"No token mapping for {sym} {tf} — ws_feed_service may not be running")

    return json.loads(raw)


# ─── Volume per session ───────────────────────────────────────────────────────

@router.get("/volume")
def session_volume(
    symbol: str = Query(..., description="BTC, ETH"),
    timeframe: str = Query(..., description="M5, M15"),
    session: int = Query(..., description="candle_open Unix timestamp"),
):
    """
    Return per-minute volume data for a binary options session.

    Response: list of {minute, up_amount, down_amount, up_trades, down_trades}
    """
    from services.redis_client import get_sync_redis
    from services.volume_tracker import get_session_volume

    r = get_sync_redis()
    data = get_session_volume(r, symbol.upper(), timeframe.upper(), session)
    return {"volume": data, "symbol": symbol.upper(), "timeframe": timeframe.upper(), "session": session}


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

    tf_order = ["M1", "M5", "M15", "M30"]
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


# ─── Orderbook depth ──────────────────────────────────────────────────────

_OB_SYMBOLS = ["BTC", "ETH"]
_OB_DIRECTIONS = ["UP", "DOWN"]


@router.get("/engine/orderbook")
def engine_orderbook(
    symbol:    Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    direction: Optional[str] = Query(None),
):
    """
    Return orderbook depth (bid/ask levels).

    Source: Redis only (written by ws_feed_service from Polymarket WebSocket).
    No REST API fallback — all data comes from the live WS feed.
    Optional filters: symbol, timeframe, direction.
    """
    from services.redis_client import get_sync_redis

    target_syms = [symbol.upper()] if symbol else _OB_SYMBOLS
    target_tfs = [timeframe.upper()] if timeframe else ["M5", "M15"]
    target_dirs = [direction.upper()] if direction else _OB_DIRECTIONS

    # Build deterministic key list from filters
    combo_list = [
        (s, t, d) for s in target_syms for t in target_tfs for d in target_dirs
    ]

    orderbooks = []

    try:
        sr = get_sync_redis()
        pipe = sr.pipeline(transaction=False)
        for sym, tf, dir_ in combo_list:
            pipe.hgetall(f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{dir_}")
        results = pipe.execute()

        for combo, data in zip(combo_list, results):
            if not data:
                continue
            bids = json.loads(data.get("bids", "[]"))
            asks = json.loads(data.get("asks", "[]"))
            if bids or asks:
                sym, tf, dir_ = combo
                orderbooks.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": dir_,
                    "bids": bids,
                    "asks": asks,
                    "updated_at": data.get("updated_at"),
                })
    except Exception as exc:
        logger.warning("engine_orderbook Redis read failed: %s", exc)

    return {"orderbooks": orderbooks}


# ─── Live prices (direct REST) ─────────────────────────────────────────────

_PRICE_SYMBOLS = ["BTC", "ETH"]
_PRICE_TIMEFRAMES = ["M5", "M15"]
_PRICE_DIRECTIONS = ["UP", "DOWN"]
_PRICE_CACHE_TTL = API_PRICE_CACHE_TTL_S

# In-memory cache: {"prices": [...], "fetched_at": float}
_price_cache: dict = {"prices": [], "fetched_at": 0.0}


def _fetch_all_prices() -> list[dict]:
    """
    Fetch best_ask / best_bid for every symbol × timeframe × direction
    from Redis orderbook snapshots (populated by WS Feed / RestPoller).

    Falls back to legacy price:{SYM}:{TF}:{DIR} keys if session-keyed
    orderbook is not available.
    """
    from services.redis_client import get_sync_redis

    results: list[dict] = []
    try:
        sr = get_sync_redis()
        now_ts = int(time.time())

        for sym in _PRICE_SYMBOLS:
            for tf in _PRICE_TIMEFRAMES:
                tf_seconds = {"M5": 300, "M15": 900}.get(tf, 300)
                candle_open = now_ts - (now_ts % tf_seconds)

                for direction in _PRICE_DIRECTIONS:
                    best_ask = None
                    best_bid = None

                    # 1. Try session-keyed orderbook (primary)
                    ob_key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}:{candle_open}"
                    try:
                        ob_data = sr.hgetall(ob_key)
                        if ob_data:
                            asks_raw = ob_data.get("asks")
                            bids_raw = ob_data.get("bids")
                            if asks_raw:
                                asks = json.loads(asks_raw)
                                if asks:
                                    best_ask = float(asks[0][0])
                            if bids_raw:
                                bids = json.loads(bids_raw)
                                if bids:
                                    best_bid = float(bids[0][0])
                    except Exception:
                        pass

                    # 2. Fallback: legacy price key
                    if best_ask is None and best_bid is None:
                        try:
                            price_key = f"{PRICE_KEY_PREFIX}:{sym}:{tf}:{direction}"
                            price_data = sr.hgetall(price_key)
                            if price_data:
                                if price_data.get("best_ask"):
                                    best_ask = float(price_data["best_ask"])
                                if price_data.get("best_bid"):
                                    best_bid = float(price_data["best_bid"])
                        except Exception:
                            pass

                    if best_ask is not None or best_bid is not None:
                        results.append({
                            "symbol": sym,
                            "timeframe": tf,
                            "direction": direction,
                            "best_ask": best_ask,
                            "best_bid": best_bid,
                            "age_s": 0.0,
                            "stale": False,
                        })
    except Exception as exc:
        logger.warning("_fetch_all_prices failed: %s", exc)
    return results


@router.get("/engine/prices")
def engine_prices():
    """
    Return best_ask and best_bid for all active symbol/timeframe/direction combos.

    Reads from Redis orderbook snapshots (populated by WS Feed / RestPoller
    every 200ms) with a short in-memory cache.

    Response format:
    {
      "prices": [
        {
          "symbol": "BTC", "timeframe": "M5", "direction": "UP",
          "best_ask": 0.52, "best_bid": 0.48,
          "age_s": 2.1, "stale": false
        },
        ...
      ]
    }
    """
    global _price_cache

    now = time.time()
    age = now - _price_cache["fetched_at"]

    if age <= _PRICE_CACHE_TTL and _price_cache["prices"]:
        # Return cached prices with updated age_s
        prices = [
            {**p, "age_s": round(age, 1)}
            for p in _price_cache["prices"]
        ]
        return {"prices": prices}

    # Cache expired — fetch fresh prices
    prices = _fetch_all_prices()
    if prices:
        _price_cache = {"prices": prices, "fetched_at": time.time()}
    elif _price_cache["prices"]:
        # Fetch failed but we have old data — return it as stale
        age = now - _price_cache["fetched_at"]
        prices = [
            {**p, "age_s": round(age, 1), "stale": age > 30}
            for p in _price_cache["prices"]
        ]
        return {"prices": prices}

    return {"prices": prices}


# ── Public Trade Inspector ──────────────────────────────────────────────────

_TF_SECONDS = {"M5": 300, "M15": 900}


@router.get("/inspect/{trade_id}", response_model=TradeInspectResponse)
def inspect_trade_public(
    trade_id: int,
    db: Session = Depends(get_db),
):
    """Public trade inspector — no auth required."""
    trade = db.get(BinaryOption, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="Trade not found")

    tf_secs = _TF_SECONDS.get(
        trade.timeframe.value if hasattr(trade.timeframe, "value") else trade.timeframe, 300
    )

    # Derive the target session window from settlement_at (= candle_open + period).
    # This correctly handles A+1 orders where created_at is in candle A
    # but the order targets candle A+1.
    settle_ts = trade.settlement_at
    if settle_ts and settle_ts.tzinfo is None:
        settle_ts = settle_ts.replace(tzinfo=timezone.utc)

    created_ts = trade.created_at
    if created_ts and created_ts.tzinfo is None:
        created_ts = created_ts.replace(tzinfo=timezone.utc)

    if settle_ts:
        session_end_unix = int(settle_ts.timestamp())
        session_start_unix = session_end_unix - tf_secs
    elif created_ts:
        session_start_unix = int(created_ts.timestamp()) // tf_secs * tf_secs
        session_end_unix = session_start_unix + tf_secs
    else:
        session_start_unix = 0
        session_end_unix = tf_secs

    session_start_dt = datetime.fromtimestamp(session_start_unix, tz=timezone.utc)
    session_end_dt = datetime.fromtimestamp(session_end_unix, tz=timezone.utc)

    direction = "UP" if (trade.forecast.value if hasattr(trade.forecast, "value") else trade.forecast) == "GREEN" else "DOWN"
    symbol_val = trade.symbol.value if hasattr(trade.symbol, "value") else trade.symbol
    tf_val = trade.timeframe.value if hasattr(trade.timeframe, "value") else trade.timeframe
    trade_offset = trade.session_offset or 0

    # Try candle_ts-based lookup first (precise session tagging).
    # This correctly finds snapshots for future sessions (A+1/A+2/A+3)
    # where recorded_at is *before* the target session window.
    price_rows = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.symbol == symbol_val,
            PriceHistory.timeframe == tf_val,
            PriceHistory.direction == direction,
            PriceHistory.candle_ts == session_start_unix,
        )
        .order_by(PriceHistory.recorded_at)
        .all()
    )
    if not price_rows:
        # Fallback: legacy rows that have NO candle_ts tag.
        # Only include rows where candle_ts IS NULL to avoid pulling in
        # snapshots that belong to a different session.
        if trade_offset >= 1 and created_ts:
            fallback_start = created_ts
        else:
            fallback_start = session_start_dt
        price_rows = (
            db.query(PriceHistory)
            .filter(
                PriceHistory.symbol == symbol_val,
                PriceHistory.timeframe == tf_val,
                PriceHistory.direction == direction,
                PriceHistory.candle_ts.is_(None),
                PriceHistory.recorded_at >= fallback_start,
                PriceHistory.recorded_at <= session_end_dt,
            )
            .order_by(PriceHistory.recorded_at)
            .all()
        )

    timeline: list[dict] = []

    for t in trade.traces or []:
        timeline.append({
            "timestamp": t.get("timestamp", ""),
            "category": "trace",
            "action": t.get("action", t.get("stage", "")),
            "details": t.get("details", ""),
            "data": t.get("data"),
        })

    wp = trade.walk_prices or {}
    if wp.get("entry"):
        timeline.append({
            "timestamp": created_ts.isoformat() if created_ts else "",
            "category": "fill_entry",
            "action": "entry_fill",
            "details": f"{len(wp['entry'])} level(s), avg={trade.avg_price}",
            "data": wp["entry"],
        })
    if wp.get("exit"):
        exit_ts = trade.exit_at or trade.settlement_at or trade.updated_at
        if exit_ts and exit_ts.tzinfo is None:
            exit_ts = exit_ts.replace(tzinfo=timezone.utc)
        timeline.append({
            "timestamp": exit_ts.isoformat() if exit_ts else "",
            "category": "fill_exit",
            "action": f"exit_fill ({trade.exit_trigger or 'settlement'})",
            "details": f"{len(wp['exit'])} level(s), exit_price={trade.exit_price}",
            "data": wp["exit"],
        })

    for ph in price_rows:
        timeline.append({
            "timestamp": ph.recorded_at.isoformat() if ph.recorded_at else "",
            "category": "price",
            "action": "price_snapshot",
            "details": f"bid={ph.best_bid} ask={ph.best_ask}",
            "data": {"bids": ph.bids, "asks": ph.asks, "candle_ts": ph.candle_ts},
        })

    timeline.sort(key=lambda x: x["timestamp"])

    return TradeInspectResponse(
        trade=BOResponse.model_validate(trade),
        timeline=[TimelineEvent(**e) for e in timeline],
        session=SessionInfoSchema(
            symbol=symbol_val,
            timeframe=tf_val,
            direction=direction,
            session_start=session_start_unix,
            session_end=session_end_unix,
            session_offset=trade_offset,
        ),
    )
