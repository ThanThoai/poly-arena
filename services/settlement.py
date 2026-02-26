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
from datetime import datetime, timezone

import httpx
from sqlalchemy.orm import Session

from models import BalanceHistory, BinaryOption, Bot, BOResult

logger = logging.getLogger(__name__)

_PAYOUT_RATE = 1.00  # fallback khi thiếu price_open / num_shares

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
        candle = fetch_binance_candle(bo.symbol, bo.timeframe, bo.settlement_at)
        if candle is None:
            logger.warning("Skipping trade #%d — no candle data", bo.id)
            continue

        open_price, close_price = candle

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
        #   No TP, No SL            → binary (both WIN and LOSS)
        #   TP only, TP fired       → shadow WIN
        #   TP only, TP not fired   → binary LOSS
        #   SL only, SL fired       → shadow LOSS
        #   SL only, SL not fired   → binary WIN
        #   Both TP+SL, TP fired    → shadow WIN
        #   Both TP+SL, SL fired    → shadow LOSS
        # ──────────────────────────────────────────────────────────────────────

        if bo.exit_trigger in ("TP", "SL") and bo.exit_price is not None and bo.exit_filled is not None:
            # Shadow tracking formula — bracket exit already occurred
            result = BOResult.WIN if bo.exit_trigger == "TP" else BOResult.LOSS
            profit = round((bo.exit_price - bo.avg_price) * bo.exit_filled, 8)
            logger.info(
                "Trade #%d settled via shadow tracking: trigger=%s exit_price=%.6f "
                "avg_price=%.6f exit_filled=%.4f → profit=%.8f",
                bo.id, bo.exit_trigger, bo.exit_price, bo.avg_price, bo.exit_filled, profit,
            )
        else:
            # Binary settlement formula — no bracket fired, use candle direction
            result = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
            if result == BOResult.WIN:
                if bo.avg_price is not None and bo.num_shares is not None:
                    profit = round((1 - bo.avg_price) * bo.num_shares, 8)
                else:
                    profit = round(bo.amount * _PAYOUT_RATE, 8)
            else:
                profit = -bo.amount

        bo.result      = result
        bo.profit      = profit
        bo.price_open  = open_price   # Binance candle open
        bo.price_close = close_price  # Binance candle close

        # Update bot balance
        bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
        if bot:
            bot.balance = round(bot.balance + profit, 8)
            db.add(BalanceHistory(
                bot_name    = bo.bot_name,
                balance     = bot.balance,
                trade_id    = bo.id,
            ))

        logger.info(
            "Settled #%d %s %s forecast=%s candle=%s → %s profit=%.8f balance=%.2f",
            bo.id, bo.symbol, bo.timeframe, bo.forecast,
            candle_dir, result.value, profit,
            bot.balance if bot else float("nan"),
        )

    db.commit()
