"""
Auto-settlement service for binary options trades.

Flow:
  1. On trade creation, `calc_settlement_time` returns the close time of the
     NEXT full candle after the trade was placed.
  2. The scheduler calls `settle_pending_trades` every minute (at :05s).
  3. For each PENDING trade whose settlement_at <= now, fetch the Binance
     kline that covers that candle, compare forecast vs actual direction,
     and update result + bot balance.
"""

import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy.orm import Session

from models import BalanceHistory, BinaryOption, Bot, BOResult

logger = logging.getLogger(__name__)

_PAYOUT_RATE = 1.00  # fallback khi thiếu price_open / num_shares


def _utc(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is UTC-aware. SQLite returns naive datetimes."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

# Timeframe → duration in milliseconds
_TF_MS: dict[str, int] = {
    "M5":  5  * 60 * 1_000,
    "M15": 15 * 60 * 1_000,
    "H1":  60 * 60 * 1_000,
}

# Timeframe → Binance kline interval string
_TF_BINANCE: dict[str, str] = {
    "M5":  "5m",
    "M15": "15m",
    "H1":  "1h",
}

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

# Stuck order sweeper thresholds
_STUCK_THRESHOLD = timedelta(minutes=10)   # settlement_at + 10min
_NULL_SETTLE_THRESHOLD = timedelta(hours=2)  # created_at + 2h for orders with no settlement_at


# ── Time helpers ──────────────────────────────────────────────────────────────

def calc_settlement_time(timeframe: str, trade_time: datetime) -> datetime | None:
    """
    Return the UTC close time of the NEXT full candle after trade_time.

    Example (M5, trade at 14:23:15):
      next candle open  = 14:25:00
      next candle close = 14:30:00  ← settlement_at
    """
    interval_ms = _TF_MS.get(timeframe)
    if interval_ms is None:
        return None

    trade_ms  = int(trade_time.timestamp() * 1_000)
    settle_ms = (trade_ms // interval_ms + 1) * interval_ms
    return datetime.fromtimestamp(settle_ms / 1_000, tz=timezone.utc)


# ── Binance data ──────────────────────────────────────────────────────────────

def fetch_binance_candle(
    symbol: str,
    timeframe: str,
    settlement_at: datetime,
) -> tuple[float, float] | None:
    """
    Fetch the kline whose close time equals settlement_at.
    Returns (open_price, close_price) or None on failure.
    """
    interval = _TF_BINANCE.get(timeframe)
    if not interval:
        return None

    interval_ms  = _TF_MS[timeframe]
    open_time_ms = int(settlement_at.timestamp() * 1_000) - interval_ms
    binance_sym  = symbol.upper() + "USDT"

    try:
        resp = httpx.get(
            _BINANCE_KLINES,
            params={
                "symbol":    binance_sym,
                "interval":  interval,
                "startTime": open_time_ms,
                "limit":     1,
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logger.warning("Binance returned empty klines for %s %s", binance_sym, interval)
            return None

        # kline: [openTime, open, high, low, close, volume, closeTime, ...]
        kline       = data[0]
        open_price  = float(kline[1])
        close_price = float(kline[4])
        return open_price, close_price

    except Exception as exc:
        logger.error("Binance fetch error (%s %s): %s", binance_sym, interval, exc)
        return None


# ── Single-trade settlement helper ────────────────────────────────────────────

def _settle_single_trade(
    bo: BinaryOption,
    open_price: float,
    close_price: float,
    db: Session,
    tag: str = "",
) -> None:
    """
    Apply profit formula and update balance for a single trade.

    Handles both shadow-tracking (TP/SL bracket) and binary settlement.
    The caller is responsible for fetching the candle and committing the tx.
    """
    prefix = f"[{tag}] " if tag else ""

    # Determine candle direction (doji treated as GREEN)
    if close_price > open_price:
        candle_dir = "GREEN"
    elif close_price < open_price:
        candle_dir = "RED"
    else:
        candle_dir = "GREEN"

    # ── Choose profit formula based on profit matrix ─────────────────────
    #
    # exit_trigger is set by the matching engine callback when TP/SL fires:
    #   "TP"  → WIN  via shadow tracking: (exit_price - avg_price) × exit_filled
    #   "SL"  → LOSS via shadow tracking: (exit_price - avg_price) × exit_filled (negative)
    #   None  → binary settlement formula (no bracket exit occurred)
    #
    # Matrix:
    #   No TP, No SL            → binary (candle direction vs forecast)
    #   TP only, TP fired       → shadow profit
    #   TP only, TP not fired   → binary (candle direction vs forecast)
    #   SL only, SL fired       → shadow profit (usually negative)
    #   SL only, SL not fired   → binary (candle direction vs forecast)
    #   Both TP+SL, TP fired    → shadow profit
    #   Both TP+SL, SL fired    → shadow profit (usually negative)
    #
    # Partial exit: if exit_filled < num_shares, the remainder is settled
    # via the binary formula (candle direction).
    # ──────────────────────────────────────────────────────────────────────

    if bo.exit_trigger in ("TP", "SL") and bo.exit_price is not None and bo.exit_filled is not None:
        # Shadow tracking formula — bracket exit occurred
        shadow_profit = round((bo.exit_price - bo.avg_price) * bo.exit_filled, 8)

        # Check for partial exit: if exit_filled < num_shares, the
        # un-exited remainder settles via the candle binary formula.
        remainder = 0.0
        if bo.num_shares is not None and bo.exit_filled < bo.num_shares:
            remainder_shares = bo.num_shares - bo.exit_filled
            binary_dir = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
            if binary_dir == BOResult.WIN:
                remainder = round((1 - bo.avg_price) * remainder_shares, 8)
            else:
                # Loss on remainder: cost = avg_price × remainder_shares
                remainder = round(-bo.avg_price * remainder_shares, 8)
            logger.info(
                "%sTrade #%d partial exit: exited=%.4f remainder=%.4f "
                "shadow_pnl=%.8f remainder_pnl=%.8f",
                prefix, bo.id, bo.exit_filled, remainder_shares,
                shadow_profit, remainder,
            )

        profit = shadow_profit + remainder
        # Overall result based on total profit
        result = BOResult.WIN if profit >= 0 else BOResult.LOSS
        logger.info(
            "%sTrade #%d settled via shadow tracking: trigger=%s exit_price=%.6f "
            "avg_price=%.6f exit_filled=%.4f → profit=%.8f",
            prefix, bo.id, bo.exit_trigger, bo.exit_price, bo.avg_price, bo.exit_filled, profit,
        )
    else:
        # Binary settlement formula — no bracket fired, use candle direction
        # num_shares reflects actual filled qty (updated by fill consumer)
        result = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
        if result == BOResult.WIN:
            if bo.avg_price is not None and bo.num_shares is not None:
                profit = round((1 - bo.avg_price) * bo.num_shares, 8)
            else:
                profit = round(bo.amount * _PAYOUT_RATE, 8)
        else:
            if bo.avg_price is not None and bo.num_shares is not None:
                # Loss = cost of shares (avg_price × num_shares)
                profit = round(-bo.avg_price * bo.num_shares, 8)
            else:
                profit = -bo.amount

    bo.result      = result
    bo.profit      = profit
    bo.price_open  = open_price   # Binance candle open
    bo.price_close = close_price  # Binance candle close

    # Update bot balance
    # Amount was deducted upfront at order creation → return cost + profit
    bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
    if bot:
        payout = round(bo.amount + profit, 8)
        bot.balance = round(bot.balance + payout, 8)
        db.add(BalanceHistory(
            bot_name    = bo.bot_name,
            balance     = bot.balance,
            trade_id    = bo.id,
        ))

    logger.info(
        "%sSettled #%d %s %s forecast=%s candle=%s → %s profit=%.8f payout=%.8f balance=%.2f",
        prefix, bo.id, bo.symbol, bo.timeframe, bo.forecast,
        candle_dir, result.value, profit, payout if bot else 0,
        bot.balance if bot else float("nan"),
    )


# ── Settlement ────────────────────────────────────────────────────────────────

def settle_pending_trades(db: Session) -> None:
    """
    Find all PENDING trades whose settlement_at <= now, fetch the
    corresponding Binance candle, and resolve WIN / LOSS.
    """
    now = datetime.now(timezone.utc)

    pending = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.result == BOResult.PENDING,
            BinaryOption.settlement_at.isnot(None),
            BinaryOption.settlement_at <= now,
        )
        .all()
    )

    if not pending:
        return

    logger.info("Settling %d pending trade(s)…", len(pending))

    for bo in pending:
        # Skip cancelled orders (TTL expired)
        if bo.result != BOResult.PENDING:
            continue
        # ME never filled this order — cancel with refund immediately.
        if bo.me_order_status == "PENDING":
            logger.warning(
                "Trade #%d me_order_status still PENDING at settlement — cancelling (never filled)",
                bo.id,
            )
            bo.result = BOResult.CANCELLED
            bo.profit = 0.0
            bo.me_order_status = "CANCELED"

            bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
            if bot:
                bot.balance = round(bot.balance + bo.amount, 8)
                db.add(BalanceHistory(
                    bot_name=bo.bot_name,
                    balance=bot.balance,
                    trade_id=bo.id,
                ))
            logger.info(
                "Cancelled unfilled #%d — refunded %.2f, balance=%.2f",
                bo.id, bo.amount,
                bot.balance if bot else float("nan"),
            )
            continue
        candle = fetch_binance_candle(bo.symbol, bo.timeframe, bo.settlement_at)
        if candle is None:
            logger.warning("Skipping trade #%d — no candle data", bo.id)
            continue

        open_price, close_price = candle
        _settle_single_trade(bo, open_price, close_price, db)

    db.commit()


# ── Stuck Order Sweeper ───────────────────────────────────────────────────────

def sweep_stuck_orders(db: Session) -> int:
    """
    Catch-all sweeper for PENDING trades that the normal settlement cycle missed.

    Two categories:
      1. settlement_at IS NOT NULL and settlement_at + 10min <= now  → force-settle at market
      2. settlement_at IS NULL and created_at + 2h <= now            → cancel with refund

    Returns the number of trades resolved.
    """
    now = datetime.now(timezone.utc)
    resolved = 0

    # ── Category 1: stuck trades with settlement_at ───────────────────────
    stuck_with_settle = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.result == BOResult.PENDING,
            BinaryOption.settlement_at.isnot(None),
            BinaryOption.settlement_at <= now - _STUCK_THRESHOLD,
        )
        .all()
    )

    if stuck_with_settle:
        logger.info("[STUCK_SWEEP] Found %d stuck trade(s) with settlement_at", len(stuck_with_settle))

    for bo in stuck_with_settle:
        if bo.result != BOResult.PENDING:
            continue

        candle = fetch_binance_candle(bo.symbol, bo.timeframe, bo.settlement_at)
        if candle is not None:
            open_price, close_price = candle
            _settle_single_trade(bo, open_price, close_price, db, tag="STUCK_SWEEP")
        else:
            # Candle unavailable — cancel with refund
            logger.warning(
                "[STUCK_SWEEP] Trade #%d — no candle data, cancelling with refund (amount=%.8f)",
                bo.id, bo.amount,
            )
            bo.result = BOResult.CANCELLED
            bo.profit = 0.0

            bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
            if bot:
                bot.balance = round(bot.balance + bo.amount, 8)
                db.add(BalanceHistory(
                    bot_name = bo.bot_name,
                    balance  = bot.balance,
                    trade_id = bo.id,
                ))
            logger.info(
                "[STUCK_SWEEP] Cancelled #%d %s %s — refunded %.8f, balance=%.2f",
                bo.id, bo.symbol, bo.timeframe, bo.amount,
                bot.balance if bot else float("nan"),
            )
        resolved += 1

    # ── Category 2: orphaned trades with no settlement_at ─────────────────
    orphaned = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.result == BOResult.PENDING,
            BinaryOption.settlement_at.is_(None),
            BinaryOption.created_at <= now - _NULL_SETTLE_THRESHOLD,
        )
        .all()
    )

    if orphaned:
        logger.info("[STUCK_SWEEP] Found %d orphaned trade(s) with NULL settlement_at", len(orphaned))

    for bo in orphaned:
        if bo.result != BOResult.PENDING:
            continue

        logger.warning(
            "[STUCK_SWEEP] Trade #%d — no settlement_at, cancelling with refund (amount=%.8f)",
            bo.id, bo.amount,
        )
        bo.result = BOResult.CANCELLED
        bo.profit = 0.0

        bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
        if bot:
            bot.balance = round(bot.balance + bo.amount, 8)
            db.add(BalanceHistory(
                bot_name = bo.bot_name,
                balance  = bot.balance,
                trade_id = bo.id,
            ))
        logger.info(
            "[STUCK_SWEEP] Cancelled orphan #%d %s — refunded %.8f, balance=%.2f",
            bo.id, bo.symbol, bo.amount,
            bot.balance if bot else float("nan"),
        )
        resolved += 1

    if resolved > 0:
        db.commit()
        logger.info("[STUCK_SWEEP] Resolved %d stuck/orphaned trade(s)", resolved)

    return resolved
