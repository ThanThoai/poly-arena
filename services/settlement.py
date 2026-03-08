"""
Auto-settlement service for binary options trades.

Flow:
  1. On trade creation, `calc_settlement_time` returns the close time of the
     NEXT full candle after the trade was placed.
  2. The scheduler calls `settle_pending_trades` every minute (at :05s).
  3. For each PENDING trade whose settlement_at <= now, fetch the Binance
     kline that covers that candle, compare forecast vs actual direction,
     and compute result + profit.
  4. After settling all trades in the batch, `reconcile_bot_balances` recalculates
     each affected bot's balance from first principles (DB truth), fixing any
     accumulated drift from rounding or edge cases.

Balance reconciliation formula:
  balance = initial_balance + bo_realized_pnl - bo_open_locked - bo_net_fees + futures_cash_effect
  equity  = balance + bo_open_locked + futures_open_margin + futures_pending_margin
"""

import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import func
from sqlalchemy.orm import Session

from models import BalanceHistory, BinaryOption, Bot, BOResult, INITIAL_BALANCE
from models_futures import FuturesPosition, FuturesPositionStatus, FuturesOrder, FuturesOrderStatus
from services.user_balance import record_user_balance
from config.timing import (
    HTTP_TIMEOUT,
    STUCK_ORDER_THRESHOLD_MIN,
    NULL_SETTLE_THRESHOLD_HOURS,
)
from services.order_trace import make_trace, append_trace

logger = logging.getLogger(__name__)

_PAYOUT_RATE = 1.00  # fallback khi thieu price_open / num_shares


def _utc(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is UTC-aware (guards against naive datetimes from legacy data)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

# Timeframe -> duration in milliseconds
_TF_MS: dict[str, int] = {
    "M5":  5  * 60 * 1_000,
}

# Timeframe -> Binance kline interval string
_TF_BINANCE: dict[str, str] = {
    "M5":  "5m",
}

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

# Stuck order sweeper thresholds
_STUCK_THRESHOLD = timedelta(minutes=STUCK_ORDER_THRESHOLD_MIN)
_NULL_SETTLE_THRESHOLD = timedelta(hours=NULL_SETTLE_THRESHOLD_HOURS)


# -- Time helpers --------------------------------------------------------------

def calc_settlement_time(timeframe: str, trade_time: datetime, session_offset: int = 0) -> datetime | None:
    """
    Return the UTC close time of the current candle containing trade_time.

    session_offset=0 -> current candle close (default)
    session_offset=1 -> next candle close (one candle further)

    Example (M5, trade at 14:23:15, offset=0):
      current candle = 14:20 - 14:25
      settlement_at  = 14:25:00  <- close of current candle

    Example (M5, trade at 14:23:15, offset=1):
      settlement_at = 14:30:00  <- close of next candle
    """
    interval_ms = _TF_MS.get(timeframe)
    if interval_ms is None:
        return None

    trade_ms  = int(trade_time.timestamp() * 1_000)
    settle_ms = (trade_ms // interval_ms + 1 + session_offset) * interval_ms
    return datetime.fromtimestamp(settle_ms / 1_000, tz=timezone.utc)


# -- Binance data --------------------------------------------------------------

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
            timeout=HTTP_TIMEOUT,
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


# -- Single-trade settlement (profit only, no balance update) ------------------

def _settle_single_trade(
    bo: BinaryOption,
    open_price: float,
    close_price: float,
    db: Session,
    tag: str = "",
) -> None:
    """
    Compute profit and result for a single trade.

    Balance updates are NOT done here -- they are handled by
    reconcile_bot_balances() after the entire batch is settled.
    This eliminates drift from incremental payout calculations.
    """
    prefix = f"[{tag}] " if tag else ""

    # Determine candle direction (doji treated as GREEN)
    if close_price > open_price:
        candle_dir = "GREEN"
    elif close_price < open_price:
        candle_dir = "RED"
    else:
        candle_dir = "GREEN"

    # -- Choose profit formula based on profit matrix --------------------------
    #
    # exit_trigger is set by the matching engine callback when TP/SL fires:
    #   "TP"  -> shadow tracking: (exit_price - avg_price) x exit_filled
    #   "SL"  -> shadow tracking: (exit_price - avg_price) x exit_filled (negative)
    #   None  -> binary settlement formula (no bracket exit occurred)
    #
    # Partial exit: if exit_filled < num_shares, the remainder is settled
    # via the binary formula (candle direction).
    # --------------------------------------------------------------------------

    if bo.exit_trigger in ("TP", "SL") and bo.exit_price is not None and bo.exit_filled is not None:
        # Shadow tracking formula -- bracket exit occurred
        shadow_profit = round((bo.exit_price - bo.avg_price) * bo.exit_filled, 8)

        # Check for partial exit
        remainder = 0.0
        if bo.num_shares is not None and bo.exit_filled < bo.num_shares:
            remainder_shares = bo.num_shares - bo.exit_filled
            binary_dir = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
            if binary_dir == BOResult.WIN:
                remainder = round((1 - bo.avg_price) * remainder_shares, 8)
            else:
                remainder = round(-bo.avg_price * remainder_shares, 8)
            logger.info(
                "%sTrade #%d partial exit: exited=%.4f remainder=%.4f "
                "shadow_pnl=%.8f remainder_pnl=%.8f",
                prefix, bo.id, bo.exit_filled, remainder_shares,
                shadow_profit, remainder,
            )

        profit = shadow_profit + remainder
        result = BOResult.WIN if profit >= 0 else BOResult.LOSS
        logger.info(
            "%sTrade #%d settled via shadow tracking: trigger=%s exit_price=%.6f "
            "avg_price=%.6f exit_filled=%.4f -> profit=%.8f",
            prefix, bo.id, bo.exit_trigger, bo.exit_price, bo.avg_price, bo.exit_filled, profit,
        )
    else:
        # Binary settlement formula -- no bracket fired, use candle direction
        if bo.avg_price is None or bo.num_shares is None:
            # Missing fill data -- cancel (reconciliation handles balance)
            logger.warning(
                "%sTrade #%d has NULL avg_price/num_shares at settlement -- "
                "cancelling (amount=%.8f)",
                prefix, bo.id, bo.amount,
            )
            bo.result = BOResult.CANCELLED
            bo.profit = 0.0
            bo.price_open = open_price
            bo.price_close = close_price
            append_trace(bo, make_trace(
                "SETTLEMENT", "MISSING_FILL_CANCEL",
                f"Settlement reached but order has no fill data (avg_price/num_shares NULL). "
                f"Cancelled. Amount ${bo.amount:.4f} will be reconciled.",
                {"amount": bo.amount, "candle_dir": candle_dir},
            ))
            return

        result = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
        if result == BOResult.WIN:
            profit = round((1 - bo.avg_price) * bo.num_shares, 8)
        else:
            profit = round(-bo.avg_price * bo.num_shares, 8)

    bo.result      = result
    bo.profit      = profit
    bo.price_open  = open_price
    bo.price_close = close_price

    # SETTLEMENT trace
    if bo.exit_trigger in ("TP", "SL"):
        append_trace(bo, make_trace(
            "SETTLEMENT", "CANDLE_SETTLED",
            f"Settled: Candle {candle_dir} (open=${open_price:.2f}, "
            f"close=${close_price:.2f}). {bo.exit_trigger} exit -> {result.value}. "
            f"Profit: ${profit:.4f}.",
            {"candle_dir": candle_dir, "open_price": open_price,
             "close_price": close_price, "profit": profit,
             "result": result.value, "exit_trigger": bo.exit_trigger},
        ))
    else:
        append_trace(bo, make_trace(
            "SETTLEMENT", "CANDLE_SETTLED",
            f"Settled: Candle {candle_dir} (open=${open_price:.2f}, "
            f"close=${close_price:.2f}). Forecast: {bo.forecast} -> {result.value}. "
            f"Profit: ${profit:.4f}.",
            {"candle_dir": candle_dir, "open_price": open_price,
             "close_price": close_price, "forecast": str(bo.forecast),
             "profit": profit, "result": result.value},
        ))

    logger.info(
        "%sSettled #%d %s %s forecast=%s candle=%s -> %s profit=%.8f",
        prefix, bo.id, bo.symbol, bo.timeframe, bo.forecast,
        candle_dir, result.value, profit,
    )


# -- Balance reconciliation ----------------------------------------------------

def reconcile_bot_balances(
    db: Session,
    bot_names: set[str],
    settled_trade_ids: dict[str, list[int]] | None = None,
) -> dict[str, dict]:
    """
    Recalculate balance for each bot from first principles (DB truth).

    Formula (BO):
        bo_balance = initial_balance + realized_pnl - open_locked - net_fees

    Futures effects on cash (bot.balance):
        - OPEN position:      -(margin + entry_fee)
        - CLOSED position:    -(margin + entry_fee) + max(0, margin + realized_pnl)
        - LIQUIDATED:         -(margin + entry_fee)  (no refund)
        - PENDING limit order: -margin
        - EXPIRED/CANCELLED:   0 (margin refunded)

    Equity = cash + bo_open_locked + futures_pos_margin + futures_ord_margin

    Returns dict of bot_name -> reconciliation details.
    """
    # Flush pending ORM changes so aggregate queries see settled trades
    db.flush()

    results = {}

    for bot_name in bot_names:
        bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
        if not bot:
            continue

        initial = bot.initial_balance or INITIAL_BALANCE
        prev_balance = bot.balance

        # ── BO components ────────────────────────────────────────────────────
        # 1. Realized P&L: sum of profits from all settled trades
        realized_pnl = (
            db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
            .filter(
                BinaryOption.bot_name == bot_name,
                BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
            )
            .scalar()
        ) or 0.0

        # 2. Open position cost: amount locked in PENDING trades
        open_locked = (
            db.query(func.coalesce(func.sum(BinaryOption.amount), 0.0))
            .filter(
                BinaryOption.bot_name == bot_name,
                BinaryOption.result == BOResult.PENDING,
            )
            .scalar()
        ) or 0.0

        # 3. Net fees (taker positive, maker rebate negative)
        net_fees = (
            db.query(func.coalesce(func.sum(BinaryOption.entry_fee), 0.0))
            .filter(
                BinaryOption.bot_name == bot_name,
                BinaryOption.result != BOResult.CANCELLED,
                BinaryOption.entry_fee.isnot(None),
            )
            .scalar()
        ) or 0.0

        # 4. Session P&L: profit from trades settled in this batch
        trade_ids = (settled_trade_ids or {}).get(bot_name, [])
        session_pnl = 0.0
        if trade_ids:
            session_pnl = (
                db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
                .filter(BinaryOption.id.in_(trade_ids))
                .scalar()
            ) or 0.0

        # ── Futures components ───────────────────────────────────────────────
        # OPEN positions: balance was reduced by (margin + entry_fee)
        open_positions = (
            db.query(FuturesPosition)
            .filter(
                FuturesPosition.bot_name == bot_name,
                FuturesPosition.status == FuturesPositionStatus.OPEN,
            )
            .all()
        )
        fut_open_margin = sum(p.margin or 0 for p in open_positions)
        fut_open_fees = sum(p.entry_fee or 0 for p in open_positions)

        # CLOSED positions: net effect = max(0, margin + realized_pnl) - margin - entry_fee
        closed_positions = (
            db.query(FuturesPosition)
            .filter(
                FuturesPosition.bot_name == bot_name,
                FuturesPosition.status == FuturesPositionStatus.CLOSED,
            )
            .all()
        )
        fut_closed_net = 0.0
        for p in closed_positions:
            refund = max(0, (p.margin or 0) + (p.realized_pnl or 0))
            fut_closed_net += refund - (p.margin or 0) - (p.entry_fee or 0)

        # LIQUIDATED positions: lost margin + entry_fee, no refund
        fut_liq_loss = (
            db.query(func.coalesce(
                func.sum(FuturesPosition.margin + FuturesPosition.entry_fee), 0.0
            ))
            .filter(
                FuturesPosition.bot_name == bot_name,
                FuturesPosition.status == FuturesPositionStatus.LIQUIDATED,
            )
            .scalar()
        ) or 0.0

        # PENDING limit orders: margin reserved
        fut_pending_margin = (
            db.query(func.coalesce(
                func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage),
                0.0,
            ))
            .filter(
                FuturesOrder.bot_name == bot_name,
                FuturesOrder.status == FuturesOrderStatus.PENDING,
            )
            .scalar()
        ) or 0.0

        # Total futures effect on cash:
        #   open:  -(margin + entry_fee) each
        #   closed: net per position (calculated above)
        #   liquidated: -(margin + entry_fee) each
        #   pending orders: -margin
        futures_cash_effect = round(
            -(fut_open_margin + fut_open_fees)
            + fut_closed_net
            - fut_liq_loss
            - fut_pending_margin,
            8,
        )

        new_balance = round(
            initial + realized_pnl - open_locked - net_fees + futures_cash_effect,
            8,
        )
        drift = round(new_balance - (prev_balance or 0), 8)

        # Apply
        bot.balance = new_balance

        # Record balance history as equity (cash + all locked margins)
        equity = round(
            new_balance + open_locked + fut_open_margin + fut_pending_margin,
            8,
        )
        db.add(BalanceHistory(
            bot_name=bot_name,
            balance=equity,
        ))

        # Auto-pause on bankruptcy
        if bot.balance <= 0:
            bot.status = "PAUSED"
            logger.warning("Bot '%s' auto-paused: balance=%.8f", bot_name, bot.balance)

        # Record user balance
        record_user_balance(
            db, bot_name,
            pnl_amount=round(session_pnl, 8) if session_pnl else None,
        )

        if abs(drift) > 0.01:
            logger.warning(
                "RECONCILE drift '%s': %.8f -> %.8f (drift=%.8f) "
                "initial=%.2f bo_pnl=%.8f bo_locked=%.8f bo_fees=%.8f "
                "fut_effect=%.8f session_pnl=%.8f",
                bot_name, prev_balance, new_balance, drift,
                initial, realized_pnl, open_locked, net_fees,
                futures_cash_effect, session_pnl,
            )
        else:
            logger.info(
                "RECONCILE '%s': balance=%.8f equity=%.8f "
                "(initial=%.2f + bo_pnl=%.8f - bo_locked=%.8f - bo_fees=%.8f "
                "+ fut=%.8f) session_pnl=%.8f",
                bot_name, new_balance, equity, initial, realized_pnl,
                open_locked, net_fees, futures_cash_effect, session_pnl,
            )

        results[bot_name] = {
            "balance": new_balance,
            "equity": equity,
            "prev_balance": prev_balance,
            "drift": drift,
            "initial": initial,
            "realized_pnl": round(realized_pnl, 8),
            "open_locked": round(open_locked, 8),
            "net_fees": round(net_fees, 8),
            "futures_cash_effect": round(futures_cash_effect, 8),
            "session_pnl": round(session_pnl, 8),
        }

    return results


# -- Settlement ----------------------------------------------------------------

def settle_pending_trades(db: Session) -> None:
    """
    Find all PENDING trades whose settlement_at <= now, fetch the
    corresponding Binance candle, and resolve WIN / LOSS.

    After settling all trades, reconcile_bot_balances recalculates
    each affected bot's balance from DB truth.
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

    logger.info("Settling %d pending trade(s)...", len(pending))

    # Track affected bots and their settled trade IDs for reconciliation
    affected_bots: dict[str, list[int]] = {}

    for bo in pending:
        # Skip cancelled orders (TTL expired)
        if bo.result != BOResult.PENDING:
            continue
        # Skip orders already resolved by market resolution (v2 spec)
        if bo.position_closed:
            logger.info("Trade #%d skipped -- position already closed (market resolved)", bo.id)
            continue
        # ME never filled this order -- cancel immediately
        if bo.me_order_status == "PENDING":
            logger.warning(
                "Trade #%d me_order_status still PENDING at settlement -- cancelling (never filled)",
                bo.id,
            )
            bo.result = BOResult.CANCELLED
            bo.profit = 0.0
            bo.me_order_status = "CANCELED"

            append_trace(bo, make_trace(
                "SETTLEMENT", "UNFILLED_CANCEL",
                f"Order never filled at settlement time. "
                f"Cancelled. Amount ${bo.amount:.4f} will be reconciled.",
                {"amount": bo.amount},
            ))

            affected_bots.setdefault(bo.bot_name, []).append(bo.id)
            logger.info("Cancelled unfilled #%d -- amount=%.2f", bo.id, bo.amount)
            continue

        candle = fetch_binance_candle(bo.symbol, bo.timeframe, bo.settlement_at)
        if candle is None:
            logger.warning("Skipping trade #%d -- no candle data", bo.id)
            continue

        open_price, close_price = candle
        _settle_single_trade(bo, open_price, close_price, db)
        affected_bots.setdefault(bo.bot_name, []).append(bo.id)

    # Reconcile all affected bots from DB truth
    if affected_bots:
        reconcile_bot_balances(db, set(affected_bots.keys()), affected_bots)

    db.commit()

    # Check achievements for all settled trades
    try:
        from services.achievements import on_trade_resolved
        for bo in pending:
            if bo.result in (BOResult.WIN, BOResult.LOSS):
                try:
                    on_trade_resolved(bo, db)
                except Exception as exc:
                    logger.debug("Achievement check failed for BO #%d: %s", bo.id, exc)
    except Exception as exc:
        logger.debug("Achievement module unavailable: %s", exc)


# -- Stuck Order Sweeper -------------------------------------------------------

def sweep_stuck_orders(db: Session) -> int:
    """
    Catch-all sweeper for PENDING trades that the normal settlement cycle missed.

    Two categories:
      1. settlement_at IS NOT NULL and settlement_at + 10min <= now  -> force-settle
      2. settlement_at IS NULL and created_at + 2h <= now            -> cancel

    Balance is reconciled after the batch, not per-trade.
    Returns the number of trades resolved.
    """
    now = datetime.now(timezone.utc)
    resolved = 0
    affected_bots: dict[str, list[int]] = {}

    # -- Category 1: stuck trades with settlement_at ---------------------------
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
            logger.warning(
                "[STUCK_SWEEP] Trade #%d -- no candle data, cancelling (amount=%.8f)",
                bo.id, bo.amount,
            )
            bo.result = BOResult.CANCELLED
            bo.profit = 0.0

        affected_bots.setdefault(bo.bot_name, []).append(bo.id)
        resolved += 1

    # -- Category 2: orphaned trades with no settlement_at ---------------------
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
            "[STUCK_SWEEP] Trade #%d -- no settlement_at, cancelling (amount=%.8f)",
            bo.id, bo.amount,
        )
        bo.result = BOResult.CANCELLED
        bo.profit = 0.0

        affected_bots.setdefault(bo.bot_name, []).append(bo.id)
        resolved += 1

    # Reconcile all affected bots from DB truth
    if resolved > 0:
        reconcile_bot_balances(db, set(affected_bots.keys()), affected_bots)
        db.commit()
        logger.info("[STUCK_SWEEP] Resolved %d stuck/orphaned trade(s)", resolved)

        # Check achievements for swept trades
        try:
            from services.achievements import on_trade_resolved
            for bo in stuck_with_settle + orphaned:
                if bo.result in (BOResult.WIN, BOResult.LOSS):
                    try:
                        on_trade_resolved(bo, db)
                    except Exception as exc:
                        logger.debug("Achievement check failed for BO #%d: %s", bo.id, exc)
        except Exception as exc:
            logger.debug("Achievement module unavailable: %s", exc)

    return resolved
