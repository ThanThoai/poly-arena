"""
Helper to record user-level balance history based on Realized P&L.

User realized balance = user.initial_balance + sum(profit) for all settled trades
across all of the user's bots.
"""

import logging

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import BinaryOption, Bot, BOResult, User, UserBalanceHistory

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

    # Get all bot names belonging to this user
    user_bot_names = [
        row[0]
        for row in db.query(Bot.bot_name).filter(Bot.user_id == user.id).all()
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
