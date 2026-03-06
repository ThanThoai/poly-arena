"""
Helper to record user-level balance history based on Realized P&L,
and periodic balance snapshots.

User realized balance = user.initial_balance + sum(profit) for all settled trades
across all of the user's bots.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import BalanceHistory, BinaryOption, Bot, BOResult, User, UserBalanceHistory, UserBalanceSnapshot
from models_futures import FuturesPosition, FuturesPositionStatus, FuturesSide, FuturesOrder, FuturesOrderStatus

logger = logging.getLogger(__name__)


def record_user_balance(
    db: Session,
    bot_name: str,
    trade_id: int | None = None,
    pnl_amount: float | None = None,
) -> None:
    """
    Compute and record the user's realized balance after a trade settles.

    Looks up the user via bot_name → bot.user_id, then computes:
      realized_balance = user.initial_balance + sum(profit for all settled trades of all user's bots)
    """
    bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
    if not bot or not bot.user_id:
        return

    user = db.query(User).filter(User.id == bot.user_id).first()
    if not user:
        return

    # Get bot names for ACTIVE bots only.
    # Deleted bots' P&L has already been absorbed into user.initial_balance
    # (see bots.py delete_bot), so including their trades here would double-count.
    user_bot_names = [
        row[0]
        for row in db.query(Bot.bot_name).filter(
            Bot.user_id == user.id, Bot.is_active == True
        ).all()
    ]

    if not user_bot_names:
        return

    # Sum of realized profits across all user's bots
    total_profit = (
        db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
        .filter(
            BinaryOption.bot_name.in_(user_bot_names),
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
            BinaryOption.profit.isnot(None),
        )
        .scalar()
    ) or 0.0

    realized_balance = round((user.initial_balance or 0) + total_profit, 8)

    db.add(UserBalanceHistory(
        user_id=user.id,
        balance=realized_balance,
        trade_id=trade_id,
        bot_id=bot.id,
        pnl_amount=pnl_amount,
    ))

    logger.info(
        "User #%d (%s) realized balance: %.2f (initial=%.2f + profit=%.2f) trade=#%s",
        user.id, user.username, realized_balance,
        user.initial_balance or 0, total_profit, trade_id,
    )


# ---------------------------------------------------------------------------
# Per-bot helpers
# ---------------------------------------------------------------------------

def _calc_bot_bo(db: Session, bot_name: str, sr) -> tuple[float, float, int]:
    """
    Calculate BO locked amount and unrealized PnL for a bot.

    Returns (bo_locked, bo_unrealized_pnl, open_count).
    """
    pending_trades = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result == BOResult.PENDING,
            BinaryOption.avg_price.isnot(None),
            BinaryOption.num_shares.isnot(None),
        )
        .all()
    )

    bo_locked = 0.0
    bo_unrealized = 0.0
    count = 0

    for bo in pending_trades:
        bo_locked += bo.amount or 0
        count += 1

        direction = "UP" if bo.forecast.value == "GREEN" else "DOWN"
        candle_open = bo.candle_open or 0

        # Session-specific orderbook key
        ob_key = f"orderbook:{bo.symbol.value}:{bo.timeframe.value}:{direction}:{candle_open}"

        try:
            ob_data = sr.hgetall(ob_key)
            bids_raw = ob_data.get("bids")
            if not bids_raw:
                # Fallback: legacy price key (current session only)
                price_data = sr.hgetall(f"price:{bo.symbol.value}:{bo.timeframe.value}:{direction}")
                best_bid_str = price_data.get("best_bid")
                if not best_bid_str:
                    continue
                best_bid = float(best_bid_str)
            else:
                bids = json.loads(bids_raw)
                if not bids:
                    continue
                best_bid = float(bids[0][0])  # top of book
        except Exception:
            continue

        open_qty = (bo.num_shares or 0) - (bo.exit_filled or 0)
        if open_qty <= 0:
            continue

        bo_unrealized += open_qty * (best_bid - (bo.avg_price or 0))

    return round(bo_locked, 8), round(bo_unrealized, 8), count


def _calc_bot_futures(db: Session, bot_name: str, sr) -> tuple[float, float, int]:
    """
    Calculate futures locked margin and unrealized PnL for a bot.

    Returns (futures_locked, futures_unrealized_pnl, open_count).
    """
    # Open positions
    open_positions = (
        db.query(FuturesPosition)
        .filter(
            FuturesPosition.bot_name == bot_name,
            FuturesPosition.status == FuturesPositionStatus.OPEN,
        )
        .all()
    )

    locked = 0.0
    unrealized = 0.0
    count = len(open_positions)

    for pos in open_positions:
        locked += pos.margin or 0

        mark = None
        try:
            data = sr.hgetall(f"futures:price:{pos.symbol}")
            if data and "price" in data:
                mark = float(data["price"])
        except Exception:
            pass

        if mark:
            if pos.side == FuturesSide.LONG:
                unrealized += (mark - pos.entry_price) * pos.size
            else:
                unrealized += (pos.entry_price - mark) * pos.size

    # Pending LIMIT orders — margin reserved
    pending_margin = (
        db.query(func.coalesce(func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage), 0.0))
        .filter(
            FuturesOrder.bot_name == bot_name,
            FuturesOrder.status == FuturesOrderStatus.PENDING,
        )
        .scalar()
    ) or 0.0
    locked += pending_margin

    return round(locked, 8), round(unrealized, 8), count


# ---------------------------------------------------------------------------
# Main snapshot function
# ---------------------------------------------------------------------------

def snapshot_all_user_balances(
    db: Session,
    session_label: Optional[str] = None,
    candle_open: Optional[int] = None,
) -> int:
    """Snapshot balance for all active users. Returns number of snapshots created."""
    from services.redis_client import get_sync_redis
    sr = get_sync_redis()

    users = db.query(User).filter(User.is_active == True).all()
    count = 0

    for user in users:
        active_bots = db.query(Bot).filter(
            Bot.user_id == user.id,
            Bot.is_active == True,
        ).all()

        bot_names = [b.bot_name for b in active_bots]

        # ── Per-bot aggregation ──
        total_bot_cash = 0.0
        total_bo_locked = 0.0
        total_bo_unrealized = 0.0
        total_futures_locked = 0.0
        total_futures_unrealized = 0.0
        total_open_bo = 0
        total_open_futures = 0

        for b in active_bots:
            total_bot_cash += b.balance or 0

            bo_locked, bo_unrealized, bo_count = _calc_bot_bo(db, b.bot_name, sr)
            total_bo_locked += bo_locked
            total_bo_unrealized += bo_unrealized
            total_open_bo += bo_count

            fut_locked, fut_unrealized, fut_count = _calc_bot_futures(db, b.bot_name, sr)
            total_futures_locked += fut_locked
            total_futures_unrealized += fut_unrealized
            total_open_futures += fut_count

            # Record bot-level equity snapshot (cash + locked)
            bot_equity = round((b.balance or 0) + bo_locked + fut_locked, 8)
            db.add(BalanceHistory(bot_name=b.bot_name, balance=bot_equity))

        # ── User-level calculations ──
        allocated = sum(b.initial_balance or 0 for b in active_bots)
        unallocated = round((user.initial_balance or 0) - allocated, 8)
        bot_cash = round(total_bot_cash, 8)
        bo_locked = round(total_bo_locked, 8)
        futures_locked = round(total_futures_locked, 8)

        equity = round(unallocated + bot_cash + bo_locked + futures_locked, 8)

        bo_unrealized_pnl = round(total_bo_unrealized, 8)
        futures_unrealized_pnl = round(total_futures_unrealized, 8)
        unrealized_pnl = round(bo_unrealized_pnl + futures_unrealized_pnl, 8)

        net_liquidation = round(equity + unrealized_pnl, 8)

        # ── Cumulative realized P&L (all-time, BO + futures) ──
        bo_realized = 0.0
        futures_realized = 0.0
        if bot_names:
            bo_realized = (
                db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
                .filter(
                    BinaryOption.bot_name.in_(bot_names),
                    BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
                )
                .scalar()
            ) or 0.0

            futures_realized = (
                db.query(func.coalesce(func.sum(FuturesPosition.realized_pnl), 0.0))
                .filter(
                    FuturesPosition.bot_name.in_(bot_names),
                    FuturesPosition.status.in_([
                        FuturesPositionStatus.CLOSED,
                        FuturesPositionStatus.LIQUIDATED,
                    ]),
                )
                .scalar()
            ) or 0.0

        cumulative_realized_pnl = round(bo_realized + futures_realized, 8)

        # ── Session realized P&L (this candle only, BO + futures) ──
        session_realized_pnl = 0.0
        if candle_open is not None and bot_names:
            session_bo = (
                db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
                .filter(
                    BinaryOption.bot_name.in_(bot_names),
                    BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
                    BinaryOption.candle_open == candle_open,
                )
                .scalar()
            ) or 0.0

            candle_start = datetime.fromtimestamp(candle_open, tz=timezone.utc)
            candle_end = datetime.fromtimestamp(candle_open + 300, tz=timezone.utc)
            session_fut = (
                db.query(func.coalesce(func.sum(FuturesPosition.realized_pnl), 0.0))
                .filter(
                    FuturesPosition.bot_name.in_(bot_names),
                    FuturesPosition.status.in_([
                        FuturesPositionStatus.CLOSED,
                        FuturesPositionStatus.LIQUIDATED,
                    ]),
                    FuturesPosition.closed_at >= candle_start,
                    FuturesPosition.closed_at < candle_end,
                )
                .scalar()
            ) or 0.0

            session_realized_pnl = round(session_bo + session_fut, 8)

        # ── Delta vs previous snapshot ──
        prev_snap = (
            db.query(UserBalanceSnapshot)
            .filter(UserBalanceSnapshot.user_id == user.id)
            .order_by(UserBalanceSnapshot.recorded_at.desc())
            .first()
        )
        prev_net_liq = prev_snap.net_liquidation if prev_snap else None
        snapshot_delta = round(net_liquidation - prev_net_liq, 8) if prev_net_liq is not None else None

        db.add(UserBalanceSnapshot(
            user_id=user.id,
            session_id=session_label,
            candle_open=candle_open,
            unallocated=unallocated,
            bot_cash=bot_cash,
            bo_locked=bo_locked,
            futures_locked=futures_locked,
            equity=equity,
            bo_unrealized_pnl=bo_unrealized_pnl,
            futures_unrealized_pnl=futures_unrealized_pnl,
            unrealized_pnl=unrealized_pnl,
            net_liquidation=net_liquidation,
            cumulative_realized_pnl=cumulative_realized_pnl,
            session_realized_pnl=session_realized_pnl,
            snapshot_delta=snapshot_delta,
            active_bot_count=len(active_bots),
            open_bo_count=total_open_bo,
            open_futures_count=total_open_futures,
            recorded_at=datetime.now(timezone.utc),
        ))
        count += 1

        logger.info(
            "Snapshot user #%d: equity=%.2f net_liq=%.2f unrealized=%.2f "
            "session_pnl=%.2f delta=%s bots=%d bo=%d fut=%d",
            user.id, equity, net_liquidation, unrealized_pnl,
            session_realized_pnl, snapshot_delta,
            len(active_bots), total_open_bo, total_open_futures,
        )

    if count:
        db.commit()

    return count
