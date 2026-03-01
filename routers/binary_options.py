from collections import defaultdict
from decimal import Decimal
import json
import time
from typing import List, Optional, Tuple

import logging
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
from models import BinaryOption, BalanceHistory, Bot, BOResult, BOSymbol, BOTimeframe, BOForecast
from services.settlement import calc_settlement_time
from services.polymarket import PolymarketClient
from config.timing import (
    HTTP_TIMEOUT, HTTP_TIMEOUT_FAST,
    API_PRICE_CACHE_TTL_S,
)
from ws_feed_service.config import (
    PRICE_KEY_PREFIX, STALE_THRESHOLD_S, QUEUE_ORDERS_NEW,
    ORDERBOOK_KEY_PREFIX,
)
from schemas import (
    BOBotStats, BOCreate, BOForecastStats, BOResponse,
    BOStats, BOTimeframeStats,
)
from services.order_trace import make_trace, append_trace
from services.rest_exit import (
    fetch_best_bid_from_rest as _fetch_best_bid_from_rest,
    simulate_bracket_exit_from_rest as _simulate_bracket_exit_from_rest,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# GREEN → lấy min_ask của UP, RED → lấy min_ask của DOWN
_FORECAST_TO_STATUS = {"GREEN": "UP", "RED": "DOWN"}


# ─── helpers ──────────────────────────────────────────────────────────────────

_TF_PERIOD_S = {"M5": 300, "M15": 900, "H1": 3600}


def _resolve_future_token(
    pm: PolymarketClient, symbol: str, tf: str, direction: str, session_offset: int,
) -> Optional[str]:
    """Compute future settlement_ts and resolve token_id via REST for next-session orders."""
    period = _TF_PERIOD_S.get(tf)
    if period is None or session_offset == 0:
        return None
    now = int(time.time())
    current_open = now - (now % period)
    future_ts = current_open + period * session_offset
    pm_status = _FORECAST_TO_STATUS[direction]
    return pm.get_token_id_at(symbol, tf, pm_status, future_ts)

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




def _settle_immediate_bracket_exit(
    db: Session, bo: "BinaryOption", bot: "Bot",
    trigger: str, exit_price: float, exit_filled: float,
    exit_walk: list,
) -> None:
    """
    Immediately settle a bracket exit when TP/SL condition is already met at entry.
    Updates the BO record and bot balance in a single DB commit.
    """
    now = datetime.now(timezone.utc)

    bo.exit_trigger = trigger
    bo.exit_price = exit_price
    bo.exit_filled = exit_filled
    bo.exit_at = now
    bo.me_order_status = "FILLED"

    # Persist exit walk prices
    wp = bo.walk_prices or {}
    wp["exit"] = exit_walk
    bo.walk_prices = wp

    # Calculate profit and settle
    profit = round((exit_price - bo.avg_price) * exit_filled, 8)
    result = BOResult.WIN if profit >= 0 else BOResult.LOSS

    bo.result = result
    bo.profit = profit

    payout = round(bo.amount + profit, 8)
    bot.balance = round(bot.balance + payout, 8)
    db.add(
        BalanceHistory(
            bot_name=bo.bot_name,
            balance=bot.balance,
            trade_id=bo.id,
        )
    )

    append_trace(bo, make_trace(
        "SETTLEMENT", "IMMEDIATE_BRACKET_SETTLED",
        f"Immediate bracket exit settled. {trigger} triggered at entry. "
        f"Entry: ${bo.avg_price:.4f} → Exit: ${exit_price:.4f}. "
        f"Profit: ${profit:.4f} ({result.value}). Payout: ${payout:.4f}.",
        {"trigger": trigger, "entry_price": bo.avg_price,
         "exit_price": exit_price, "exit_filled": exit_filled,
         "profit": profit, "result": result.value, "payout": payout},
    ))

    db.commit()
    logger.info(
        "Immediate bracket exit settled: BO #%d trigger=%s "
        "entry=%.6f exit=%.6f shares=%.4f → %s profit=%.8f balance=%.2f",
        bo.id, trigger, bo.avg_price, exit_price, exit_filled,
        result.value, profit, bot.balance,
    )


def _queue_prefilled_to_me(
    bo: "BinaryOption",
    avg_price: float, num_shares: float,
    payload: "BOCreate",
    token_id: Optional[str] = None,
) -> None:
    """Queue a pre-filled MARKET bracket order to the matching engine via Redis."""
    try:
        from services.redis_client import get_sync_redis
        sr = get_sync_redis()
        order_payload = json.dumps({
            "bo_id": bo.id,
            "token_id": token_id,
            "symbol": payload.symbol.value,
            "forecast": payload.forecast.value,
            "side": "BUY",
            "prefilled": True,
            "prefilled_avg_price": avg_price,
            "prefilled_filled": num_shares,
            "tp_price": payload.tp_price,
            "sl_price": payload.sl_price,
            "timeframe": payload.timeframe.value,
        })
        sr.lpush(QUEUE_ORDERS_NEW, order_payload)

        condition_type = "TP" if payload.tp_price else "SL"
        condition_price = payload.tp_price or payload.sl_price
        append_trace(bo, make_trace(
            "MONITORING", "BRACKET_QUEUED",
            f"Active Monitoring started. Condition {condition_type} at "
            f"${condition_price:.4f}. Watching for trigger via WebSocket.",
            {"condition_type": condition_type, "condition_price": condition_price,
             "avg_entry_price": avg_price, "num_shares": num_shares},
        ))

        logger.info(
            "Prefilled bracket order queued for BO #%d: "
            "avg_price=%.6f shares=%.4f tp=%s sl=%s",
            bo.id, avg_price, num_shares,
            payload.tp_price, payload.sl_price,
        )
    except Exception as exc:
        logger.error(
            "Failed to queue prefilled bracket order for BO #%d: %s",
            bo.id, exc,
        )


_DEFAULT_SLIPPAGE_TOLERANCE = 0.10  # 10%


def _try_fill_limit_from_rest(
    symbol: str, timeframe: str, pm_status: str,
    amount: float, limit_price: float,
    token_id_override: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Tuple[float, float, list]]]:
    """
    Check Polymarket REST for current asks and try to fill a LIMIT BUY.

    Returns (token_id, (avg_price, num_shares, walk_levels)) if best_ask <= limit_price.
    Returns (token_id, None) if can't fill now but token_id was resolved.
    Returns (None, None) if Polymarket REST is unavailable.
    """
    if token_id_override:
        token_id = token_id_override
    else:
        try:
            with PolymarketClient() as pm:
                ob = pm.get_orderbook(symbol, timeframe, pm_status)
                token_id = ob.token_id
        except Exception as e:
            logger.warning("LIMIT REST check failed (Polymarket unavailable): %s", e)
            return None, None

    try:
        resp = httpx.get(
            "https://clob.polymarket.com/book",
            params={"token_id": token_id},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        book = resp.json()
    except Exception as e:
        logger.warning("LIMIT REST check failed (book fetch): %s", e)
        return token_id, None

    asks = sorted(
        [
            (Decimal(str(level["price"])), Decimal(str(level["size"])))
            for level in book.get("asks", [])
            if float(level["size"]) > 0
        ],
        key=lambda x: x[0],
    )

    if not asks:
        return token_id, None

    best_ask = asks[0][0]
    limit_dec = Decimal(str(limit_price))

    if best_ask > limit_dec:
        # Can't fill now — best ask is above limit price
        logger.info(
            "LIMIT REST check: best_ask=%s > limit=%s — defer to ME",
            best_ask, limit_dec,
        )
        return token_id, None

    # Simulate fill: sweep asks up to limit_price
    remaining_budget = Decimal(str(amount))
    total_cost = Decimal("0")
    total_shares = Decimal("0")
    walk_levels: list = []

    for ask_price, ask_size in asks:
        if ask_price > limit_dec:
            break  # Don't fill above limit price
        if remaining_budget <= 0:
            break
        max_shares_at_level = remaining_budget / ask_price
        fill_shares = min(ask_size, max_shares_at_level)
        fill_cost = fill_shares * ask_price
        total_cost += fill_cost
        total_shares += fill_shares
        remaining_budget -= fill_cost
        walk_levels.append({
            "price": float(ask_price),
            "qty": float(fill_shares),
            "cost": round(float(fill_cost), 8),
        })

    if total_shares <= 0:
        return token_id, None

    avg_price = float(total_cost / total_shares)
    num_shares = float(total_shares)

    logger.info(
        "LIMIT REST fill: %s %s %s amount=%.4f limit=%.4f → "
        "avg=%.6f shares=%.4f levels=%d",
        symbol, timeframe, pm_status, amount, limit_price,
        avg_price, num_shares, len(walk_levels),
    )

    return token_id, (avg_price, num_shares, walk_levels)


def _fill_market_from_rest(
    symbol: str, timeframe: str, pm_status: str,
    amount: float, slippage_tolerance: Optional[float],
    token_id_override: Optional[str] = None,
) -> Tuple[float, float, str, list]:
    """
    Fetch full orderbook from Polymarket REST, simulate MARKET BUY fill.
    Returns (avg_price, num_shares, token_id).
    Raises HTTPException on failure.
    """
    tolerance = slippage_tolerance if slippage_tolerance is not None else _DEFAULT_SLIPPAGE_TOLERANCE

    if token_id_override:
        token_id = token_id_override
    else:
        try:
            with PolymarketClient() as pm:
                ob = pm.get_orderbook(symbol, timeframe, pm_status)
                token_id = ob.token_id
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Polymarket unavailable: {e}")

    # Fetch full book
    try:
        resp = httpx.get(
            "https://clob.polymarket.com/book",
            params={"token_id": token_id},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        book = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Polymarket book fetch failed: {e}")

    asks = sorted(
        [
            (Decimal(str(level["price"])), Decimal(str(level["size"])))
            for level in book.get("asks", [])
            if float(level["size"]) > 0
        ],
        key=lambda x: x[0],
    )

    if not asks:
        raise HTTPException(status_code=502, detail="No liquidity available on Polymarket")

    ref_price = asks[0][0]
    slippage_limit = ref_price * (1 + Decimal(str(tolerance)))
    remaining_budget = Decimal(str(amount))
    total_cost = Decimal("0")
    total_shares = Decimal("0")
    walk_levels: list = []

    for ask_price, ask_size in asks:
        if ask_price > slippage_limit:
            break
        if remaining_budget <= 0:
            break
        # Max shares we can buy at this level within budget
        max_shares_at_level = remaining_budget / ask_price
        fill_shares = min(ask_size, max_shares_at_level)
        fill_cost = fill_shares * ask_price
        total_cost += fill_cost
        total_shares += fill_shares
        remaining_budget -= fill_cost
        walk_levels.append({
            "price": float(ask_price),
            "qty": float(fill_shares),
            "cost": round(float(fill_cost), 8),
        })

    if total_shares <= 0:
        raise HTTPException(status_code=502, detail="No liquidity available within slippage tolerance")

    avg_price = float(total_cost / total_shares)
    num_shares = float(total_shares)

    logger.info(
        "REST fill: %s %s %s amount=%.4f → avg_price=%.6f shares=%.4f "
        "levels=%d slippage_limit=%.4f",
        symbol, timeframe, pm_status, amount, avg_price, num_shares,
        len(asks), float(slippage_limit),
    )

    return avg_price, num_shares, token_id, walk_levels


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

    if bot.balance < payload.amount:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient balance: {bot.balance:.2f} < {payload.amount:.2f}",
        )

    # ── Pre-validation: condition price vs Best Ask (v2 spec Section 2) ────
    # Prevents "logical suicide" orders where the condition is already met
    pending_traces: list[dict] = []
    if payload.tp_price is not None or payload.sl_price is not None:
        pm_status_val = _FORECAST_TO_STATUS[payload.forecast.value]
        best_ask_val, _ = _try_redis_price(
            payload.symbol.value, payload.timeframe.value, pm_status_val,
        )
        if best_ask_val is None:
            # Fallback to REST
            try:
                with PolymarketClient(timeout=HTTP_TIMEOUT_FAST) as pm:
                    ob = pm.get_orderbook(
                        payload.symbol.value, payload.timeframe.value, pm_status_val,
                    )
                    best_ask_val = ob.min_ask
            except Exception:
                best_ask_val = None

        if best_ask_val is not None:
            if payload.sl_price is not None and payload.sl_price >= best_ask_val:
                pending_traces.append(make_trace(
                    "VALIDATION", "PRE_VALIDATION_FAILED",
                    f"Validation Failed: SL ${payload.sl_price:.4f} must be lower than "
                    f"estimated entry ${best_ask_val:.4f}. Order Rejected.",
                    {"sl_price": payload.sl_price, "best_ask": best_ask_val},
                ))
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"SL price ({payload.sl_price:.4f}) must be lower than "
                        f"current Best Ask ({best_ask_val:.4f})"
                    ),
                )
            if payload.tp_price is not None and payload.tp_price <= best_ask_val:
                pending_traces.append(make_trace(
                    "VALIDATION", "PRE_VALIDATION_FAILED",
                    f"Validation Failed: TP ${payload.tp_price:.4f} must be higher than "
                    f"estimated entry ${best_ask_val:.4f}. Order Rejected.",
                    {"tp_price": payload.tp_price, "best_ask": best_ask_val},
                ))
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"TP price ({payload.tp_price:.4f}) must be higher than "
                        f"current Best Ask ({best_ask_val:.4f})"
                    ),
                )
            # Validation passed
            condition_type = "TP" if payload.tp_price is not None else "SL"
            condition_price = payload.tp_price if payload.tp_price is not None else payload.sl_price
            pending_traces.append(make_trace(
                "VALIDATION", "PRE_VALIDATION_OK",
                f"Pre-validation successful. Condition {condition_type} at "
                f"${condition_price:.4f} is valid against current Best Ask ${best_ask_val:.4f}.",
                {"condition_type": condition_type, "condition_price": condition_price,
                 "best_ask": best_ask_val},
            ))

    # Deduct amount from balance upfront — refunded on cancel, settled on WIN/LOSS
    bot.balance = round(bot.balance - payload.amount, 8)

    pm_status  = _FORECAST_TO_STATUS[payload.forecast.value]
    is_limit   = payload.limit_price is not None
    token_id:  Optional[str]   = None
    entry_price: float

    has_bracket = payload.tp_price is not None or payload.sl_price is not None
    session_offset = payload.session_offset or 0
    settlement_at = calc_settlement_time(
        payload.timeframe, datetime.now(timezone.utc), session_offset=session_offset,
    )

    # ── Next-session token resolution ──────────────────────────────────────
    future_token_id: Optional[str] = None
    if session_offset > 0:
        try:
            with PolymarketClient(timeout=HTTP_TIMEOUT_FAST) as pm_future:
                future_token_id = _resolve_future_token(
                    pm_future, payload.symbol.value, payload.timeframe.value,
                    payload.forecast.value, session_offset,
                )
        except Exception as exc:
            logger.warning("Future token resolution failed: %s", exc)

        if not future_token_id:
            # Refund balance and reject
            bot.balance = round(bot.balance + payload.amount, 8)
            raise HTTPException(
                status_code=503,
                detail="Future session market not available yet on Polymarket",
            )

    if is_limit:
        # ── LIMIT order: two-phase flow ─────────────────────────────────────
        # Phase 1: Check Polymarket REST for current asks.
        #   If best_ask <= limit_price → fill immediately via REST.
        #   If best_ask >  limit_price → defer to Matching Engine.
        entry_price = payload.limit_price  # type: ignore[assignment]

        token_id, rest_fill = _try_fill_limit_from_rest(
            payload.symbol.value, payload.timeframe.value, pm_status,
            payload.amount, entry_price,
            token_id_override=future_token_id,
        )

        if rest_fill is not None:
            # ── Phase 1a: Immediate REST fill ────────────────────────────
            avg_price, num_shares, walk_levels = rest_fill
            ask_fetched_at = datetime.now(timezone.utc)
            price_source = "rest_limit"

            latency_ms = (ask_fetched_at - order_received_at).total_seconds() * 1000
            logger.info(
                "BO LIMIT %s %s %s: IMMEDIATE fill via REST "
                "avg=%.6f shares=%.4f limit=%.4f latency=%.0fms tp=%s sl=%s",
                payload.symbol.value, payload.timeframe.value,
                payload.forecast.value,
                avg_price, num_shares, entry_price, latency_ms,
                payload.tp_price, payload.sl_price,
            )

            limit_traces = list(pending_traces)
            limit_traces.append(make_trace(
                "MATCHING", "LIMIT_REST_FILL",
                f"Limit Order filled immediately via REST. "
                f"Best Ask <= Limit ${entry_price:.4f}. "
                f"Avg Entry: ${avg_price:.4f}, Shares: {num_shares:.4f}.",
                {"limit_price": entry_price, "avg_entry_price": avg_price,
                 "num_shares": num_shares, "levels": len(walk_levels)},
            ))

            bo = BinaryOption(
                bot_name          = bot.bot_name,
                symbol            = payload.symbol,
                timeframe         = payload.timeframe,
                forecast          = payload.forecast,
                amount            = payload.amount,
                result            = BOResult.PENDING,
                avg_price         = avg_price,
                num_shares        = num_shares,
                reason            = payload.reason,
                order_received_at = order_received_at,
                ask_fetched_at    = ask_fetched_at,
                settlement_at     = settlement_at,
                limit_price       = payload.limit_price,
                tp_price          = payload.tp_price,
                sl_price          = payload.sl_price,
                ttl               = payload.ttl,
                session_offset    = session_offset,
                me_order_status   = "FILLED" if has_bracket else None,
                walk_prices       = {"entry": walk_levels} if walk_levels else None,
                traces            = limit_traces if limit_traces else None,
            )
            db.add(bo)
            db.commit()
            db.refresh(bo)

            # Handle bracket (TP/SL) — same logic as MARKET fill path
            if has_bracket:
                tp_already_met = payload.tp_price is not None and avg_price >= payload.tp_price
                sl_already_met = payload.sl_price is not None and avg_price <= payload.sl_price

                if tp_already_met or sl_already_met:
                    trigger = "TP" if tp_already_met else "SL"
                    condition_price = payload.tp_price if tp_already_met else payload.sl_price
                    logger.info(
                        "LIMIT fill slippage violation: BO #%d %s met at entry: "
                        "entry=%.6f %s=%s — Auto-Exit",
                        bo.id, trigger, avg_price, trigger, condition_price,
                    )
                    append_trace(bo, make_trace(
                        "MONITORING", "SLIPPAGE_VIOLATION",
                        f"Post-fill check: Avg Entry ${avg_price:.4f} violates "
                        f"{trigger} threshold ${condition_price:.4f}. "
                        f"Triggering Auto-Exit...",
                        {"avg_entry_price": avg_price, "trigger": trigger,
                         "condition_price": condition_price},
                    ))

                    best_bid, bid_levels = _fetch_best_bid_from_rest(token_id)
                    if best_bid is not None and bid_levels:
                        exit_price, exit_filled, exit_walk = (
                            _simulate_bracket_exit_from_rest(
                                num_shares, bid_levels,
                            )
                        )
                        if exit_filled > 0:
                            append_trace(bo, make_trace(
                                "MONITORING", "AUTO_EXIT_SWEEP",
                                f"Auto-Exit REST Sweep: Selling "
                                f"{exit_filled:.4f} against current Bids. "
                                f"Avg Exit: ${exit_price:.4f}.",
                                {"exit_price": exit_price,
                                 "exit_filled": exit_filled,
                                 "reason": "Slippage Violation"},
                            ))
                            _settle_immediate_bracket_exit(
                                db, bo, bot, trigger,
                                exit_price, exit_filled, exit_walk,
                            )
                        else:
                            _queue_prefilled_to_me(
                                bo, avg_price, num_shares, payload,
                                token_id=token_id,
                            )
                    else:
                        _queue_prefilled_to_me(
                            bo, avg_price, num_shares, payload,
                            token_id=token_id,
                        )
                else:
                    # Normal bracket: TP > entry / SL < entry → ME monitors
                    _queue_prefilled_to_me(
                        bo, avg_price, num_shares, payload,
                        token_id=token_id,
                    )

        else:
            # ── Phase 1b: Cannot fill now → defer to ME ──────────────────
            num_shares     = round(payload.amount / entry_price, 8)
            ask_fetched_at = datetime.now(timezone.utc)
            price_source   = "limit"

            latency_ms = (ask_fetched_at - order_received_at).total_seconds() * 1000
            logger.info(
                "BO LIMIT %s %s %s: price=%.4f src=%s latency=%.0fms "
                "tp=%s sl=%s → deferred to ME",
                payload.symbol.value, payload.timeframe.value,
                payload.forecast.value,
                entry_price, price_source, latency_ms,
                payload.tp_price, payload.sl_price,
            )

            bo = BinaryOption(
                bot_name          = bot.bot_name,
                symbol            = payload.symbol,
                timeframe         = payload.timeframe,
                forecast          = payload.forecast,
                amount            = payload.amount,
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
                me_order_status   = "PENDING",
                traces            = pending_traces if pending_traces else None,
            )
            db.add(bo)
            db.commit()
            db.refresh(bo)

            # Push LIMIT order to ME via Redis queue
            try:
                from services.redis_client import get_sync_redis
                sr = get_sync_redis()
                order_payload = json.dumps({
                    "bo_id": bo.id,
                    "token_id": token_id,
                    "symbol": payload.symbol.value,
                    "forecast": payload.forecast.value,
                    "side": "BUY",
                    "price": entry_price,
                    "expected_price": entry_price,
                    "quantity": num_shares,
                    "amount": payload.amount,
                    "limit_price": payload.limit_price,
                    "tp_price": payload.tp_price,
                    "sl_price": payload.sl_price,
                    "timeframe": payload.timeframe.value,
                    "ttl": payload.ttl,
                    "slippage_tolerance": payload.slippage_tolerance,
                })
                sr.lpush(QUEUE_ORDERS_NEW, order_payload)

                append_trace(bo, make_trace(
                    "MATCHING", "LIMIT_ORDER_QUEUED",
                    f"Limit Order queued to Matching Engine. "
                    f"Best Ask > Limit ${entry_price:.4f}. "
                    f"Waiting for ask to reach limit price.",
                    {"limit_price": entry_price, "quantity": num_shares,
                     "tp_price": payload.tp_price,
                     "sl_price": payload.sl_price,
                     "ttl": payload.ttl},
                ))
                db.commit()

                logger.info(
                    "Virtual order queued for BO #%d: type=LIMIT tp=%s sl=%s",
                    bo.id, payload.tp_price, payload.sl_price,
                )
            except Exception as exc:
                logger.error(
                    "Failed to queue virtual order for BO #%d: %s",
                    bo.id, exc,
                )

    else:
        # ── MARKET order: fill immediately via Polymarket REST API ─────────
        avg_price, num_shares, token_id, walk_levels = _fill_market_from_rest(
            payload.symbol.value, payload.timeframe.value, pm_status,
            payload.amount, payload.slippage_tolerance,
            token_id_override=future_token_id,
        )
        ask_fetched_at = datetime.now(timezone.utc)
        price_source = "rest"

        latency_ms = (ask_fetched_at - order_received_at).total_seconds() * 1000
        logger.info(
            "BO MARKET %s %s %s: avg_price=%.6f shares=%.4f src=%s "
            "latency=%.0fms tp=%s sl=%s",
            payload.symbol.value, payload.timeframe.value, payload.forecast.value,
            avg_price, num_shares, price_source, latency_ms,
            payload.tp_price, payload.sl_price,
        )

        # Add MATCHING traces for REST sweep
        market_traces = list(pending_traces)
        market_traces.append(make_trace(
            "MATCHING", "REST_SWEEP",
            f"Market Order filled. Avg Entry Price: ${avg_price:.4f}. "
            f"Total Slippage: {((avg_price - walk_levels[0]['price']) / walk_levels[0]['price'] * 100) if walk_levels else 0:.2f}%.",
            {"avg_entry_price": avg_price, "num_shares": num_shares,
             "levels": len(walk_levels)},
        ))

        bo = BinaryOption(
            bot_name          = bot.bot_name,
            symbol            = payload.symbol,
            timeframe         = payload.timeframe,
            forecast          = payload.forecast,
            amount            = payload.amount,
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
            # MARKET+bracket → "PREFILLED" (ME monitors TP/SL only)
            # MARKET without bracket → None (scheduler settles)
            me_order_status   = "PREFILLED" if has_bracket else None,
            walk_prices       = {"entry": walk_levels} if walk_levels else None,
            traces            = market_traces if market_traces else None,
        )
        db.add(bo)
        db.commit()
        db.refresh(bo)

        # Only queue to ME when MARKET order has TP/SL bracket
        if has_bracket:
            # Post-fill edge case (v2 spec Section 3.1): if avg_entry_price
            # violates the condition due to slippage, trigger Auto-Exit immediately.
            tp_already_met = payload.tp_price is not None and avg_price >= payload.tp_price
            sl_already_met = payload.sl_price is not None and avg_price <= payload.sl_price

            if tp_already_met or sl_already_met:
                # Post-fill check failed — slippage violation (v2 spec Section 4.1)
                trigger = "TP" if tp_already_met else "SL"
                condition_price = payload.tp_price if tp_already_met else payload.sl_price
                logger.info(
                    "Slippage violation: BO #%d %s already met at entry: "
                    "entry=%.6f %s=%s — triggering Auto-Exit via REST",
                    bo.id, trigger, avg_price, trigger, condition_price,
                )
                append_trace(bo, make_trace(
                    "MONITORING", "SLIPPAGE_VIOLATION",
                    f"Post-fill check failed: Avg Entry ${avg_price:.4f} violates "
                    f"{trigger} threshold ${condition_price:.4f}. Triggering Auto-Exit...",
                    {"avg_entry_price": avg_price, "trigger": trigger,
                     "condition_price": condition_price},
                ))

                best_bid, bid_levels = _fetch_best_bid_from_rest(token_id)
                if best_bid is not None and bid_levels:
                    exit_price, exit_filled, exit_walk = _simulate_bracket_exit_from_rest(
                        num_shares, bid_levels,
                    )
                    if exit_filled > 0:
                        append_trace(bo, make_trace(
                            "MONITORING", "AUTO_EXIT_SWEEP",
                            f"Auto-Exit REST Sweep: Selling {exit_filled:.4f} against "
                            f"current Bids. Avg Exit Price: ${exit_price:.4f}.",
                            {"exit_price": exit_price, "exit_filled": exit_filled,
                             "reason": "Slippage Violation"},
                        ))
                        _settle_immediate_bracket_exit(
                            db, bo, bot, trigger,
                            exit_price, exit_filled, exit_walk,
                        )
                    else:
                        logger.warning(
                            "No bid liquidity for immediate bracket exit BO #%d "
                            "— falling back to ME",
                            bo.id,
                        )
                        _queue_prefilled_to_me(
                            bo, avg_price, num_shares, payload,
                            token_id=token_id,
                        )
                else:
                    logger.warning(
                        "Failed to fetch bids for immediate bracket exit BO #%d "
                        "— falling back to ME",
                        bo.id,
                    )
                    _queue_prefilled_to_me(
                        bo, avg_price, num_shares, payload,
                        token_id=token_id,
                    )
            else:
                # Normal case: TP > entry and SL < entry → queue to ME
                _queue_prefilled_to_me(
                    bo, avg_price, num_shares, payload,
                    token_id=token_id,
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


# ─── Orderbook depth ──────────────────────────────────────────────────────

_OB_SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
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
    target_tfs = [timeframe.upper()] if timeframe else ["M5", "M15", "H1"]
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

_PRICE_SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
_PRICE_TIMEFRAMES = ["M5", "M15", "H1"]
_PRICE_DIRECTIONS = ["UP", "DOWN"]
_PRICE_CACHE_TTL = API_PRICE_CACHE_TTL_S

# In-memory cache: {"prices": [...], "fetched_at": float}
_price_cache: dict = {"prices": [], "fetched_at": 0.0}


def _fetch_all_prices() -> list[dict]:
    """
    Fetch best_ask / best_bid for every symbol × timeframe × direction
    directly from Polymarket REST API.

    Each call does:  Gamma API (slug → token_ids) → CLOB API (book → prices)
    Errors on individual combos are silently skipped.
    """
    results: list[dict] = []
    try:
        with PolymarketClient(timeout=HTTP_TIMEOUT_FAST) as pm:
            for sym in _PRICE_SYMBOLS:
                for tf in _PRICE_TIMEFRAMES:
                    for direction in _PRICE_DIRECTIONS:
                        try:
                            ob = pm.get_orderbook(sym, tf, direction)
                            results.append({
                                "symbol": sym,
                                "timeframe": tf,
                                "direction": direction,
                                "best_ask": ob.min_ask,
                                "best_bid": ob.max_bid,
                                "age_s": 0.0,
                                "stale": False,
                            })
                        except Exception:
                            pass
    except Exception as exc:
        logger.warning("_fetch_all_prices failed: %s", exc)
    return results


@router.get("/engine/prices")
def engine_prices():
    """
    Return best_ask and best_bid for all active symbol/timeframe/direction combos.

    Fetches directly from Polymarket REST API with a 5-second in-memory cache.
    This is simpler and more reliable than the Redis-based approach because it
    automatically gets prices for the current session — no dependency on WS feed,
    token rotation, or Redis state.

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
