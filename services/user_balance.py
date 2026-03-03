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

from models import BinaryOption, Bot, BOResult, User, UserBalanceHistory, UserBalanceSnapshot

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


def snapshot_all_user_balances(db: Session, session_label: Optional[str] = None) -> int:
    """Snapshot balance for all active users. Returns number of snapshots created."""
    users = db.query(User).filter(User.is_active == True).all()
    count = 0

    for user in users:
        active_bots = db.query(Bot).filter(
            Bot.user_id == user.id,
            Bot.is_active == True,
        ).all()

        bot_balance = round(sum(b.balance or 0 for b in active_bots), 8)
        allocated = sum(b.initial_balance or 0 for b in active_bots)
        available = round((user.initial_balance or 0) - allocated, 8)
        balance = round(bot_balance + available, 8)

        db.add(UserBalanceSnapshot(
            user_id=user.id,
            balance=balance,
            bot_balance=bot_balance,
            available=available,
            session_id=session_label,
            recorded_at=datetime.now(timezone.utc),
        ))
        count += 1

    if count:
        db.commit()

    return count
