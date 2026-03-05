"""
Helper to record user-level balance history based on Realized P&L,
and periodic balance snapshots.

User realized balance = user.initial_balance + sum(profit) for all settled trades
across all of the user's bots.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import BalanceHistory, BinaryOption, Bot, BOResult, User, UserBalanceHistory, UserBalanceSnapshot

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


def _calc_bot_unrealized_pnl(db: Session, bot_name: str, sr) -> float:
    """
    Calculate unrealized P&L for all PENDING trades of a bot.

    For each open position:
      open_qty = num_shares - (exit_filled or 0)
      unrealized += open_qty * (best_bid - avg_price)

    best_bid is fetched from Redis key price:{SYM}:{TF}:{DIR}
    where DIR = UP if forecast == GREEN, DOWN if forecast == RED.
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

    total_unrealized = 0.0
    for bo in pending_trades:
        direction = "UP" if bo.forecast.value == "GREEN" else "DOWN"
        price_key = f"price:{bo.symbol.value}:{bo.timeframe.value}:{direction}"

        try:
            price_data = sr.hgetall(price_key)
            best_bid_str = price_data.get("best_bid")
            if not best_bid_str:
                continue
            best_bid = float(best_bid_str)
        except Exception:
            continue

        open_qty = (bo.num_shares or 0) - (bo.exit_filled or 0)
        if open_qty <= 0:
            continue

        unrealized = open_qty * (best_bid - (bo.avg_price or 0))
        total_unrealized += unrealized

    return round(total_unrealized, 8)


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

        # Calculate unrealized P&L and equity per bot
        total_unrealized = 0.0
        bot_equity_sum = 0.0
        for b in active_bots:
            unrealized = _calc_bot_unrealized_pnl(db, b.bot_name, sr)
            total_unrealized += unrealized
            bot_equity = round((b.balance or 0) + unrealized, 8)
            bot_equity_sum += bot_equity

            # Record bot-level equity snapshot (balance + unrealized)
            db.add(BalanceHistory(
                bot_name=b.bot_name,
                balance=bot_equity,
            ))

        total_unrealized = round(total_unrealized, 8)
        bot_balance = round(bot_equity_sum, 8)
        allocated = sum(b.initial_balance or 0 for b in active_bots)
        available = round((user.initial_balance or 0) - allocated, 8)
        balance = round(bot_balance + available, 8)

        # Previous balance from most recent snapshot
        prev_snap = (
            db.query(UserBalanceSnapshot)
            .filter(UserBalanceSnapshot.user_id == user.id)
            .order_by(UserBalanceSnapshot.recorded_at.desc())
            .first()
        )
        prev_balance = prev_snap.balance if prev_snap else None

        # Session P&L: balance delta from previous snapshot
        session_pnl = round(balance - prev_balance, 8) if prev_balance is not None else None

        # Bot P&L: sum of trade profits settled in this candle session
        bot_pnl = None
        if candle_open is not None:
            user_bot_names = [b.bot_name for b in active_bots]
            if user_bot_names:
                bot_pnl = (
                    db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
                    .filter(
                        BinaryOption.bot_name.in_(user_bot_names),
                        BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
                        BinaryOption.candle_open == candle_open,
                    )
                    .scalar()
                ) or 0.0

        db.add(UserBalanceSnapshot(
            user_id=user.id,
            balance=balance,
            bot_balance=bot_balance,
            available=available,
            session_id=session_label,
            session_pnl=session_pnl,
            prev_balance=prev_balance,
            bot_pnl=bot_pnl,
            unrealized_pnl=total_unrealized,
            recorded_at=datetime.now(timezone.utc),
        ))
        count += 1

    if count:
        db.commit()

    return count
