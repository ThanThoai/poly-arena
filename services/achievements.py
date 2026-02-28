"""
Achievement evaluation service.

Each checker is registered via the @achievement decorator and called
after a trade is resolved. Checkers return a metadata dict on match
or None to skip.
"""

import logging
from functools import wraps
from typing import Callable, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import (
    AchievementDefinition,
    BalanceHistory,
    BinaryOption,
    Bot,
    BotAchievement,
    BOResult,
)

logger = logging.getLogger(__name__)

# slug → checker function
_CHECKERS: dict[str, Callable] = {}


def achievement(slug: str):
    """Decorator that registers an achievement checker."""
    def decorator(fn):
        _CHECKERS[slug] = fn
        @wraps(fn)
        def wrapper(*a, **kw):
            return fn(*a, **kw)
        return wrapper
    return decorator


def _get_recent_results(bot_name: str, db: Session, limit: int = 50) -> list[BinaryOption]:
    """Get recent settled trades (WIN/LOSS) ordered by newest first."""
    return (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
        )
        .order_by(BinaryOption.id.desc())
        .limit(limit)
        .all()
    )


def _get_consecutive_streak(bot_name: str, db: Session, result: BOResult, limit: int = 50) -> int:
    """Count consecutive recent trades with the given result (from newest)."""
    trades = _get_recent_results(bot_name, db, limit)
    count = 0
    for t in trades:
        if t.result == result:
            count += 1
        else:
            break
    return count


# ── Minimum stake: use $1 as the "minimum allowable" threshold ───────────
_MIN_STAKE = 1.0


# ── Checkers ──────────────────────────────────────────────────────────────


@achievement("peak-buyer")
def _check_peak_buyer(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """Purchase token at Price > $0.98 and result is LOSS."""
    if bo.result != BOResult.LOSS:
        return None
    if bo.avg_price is not None and bo.avg_price > 0.98:
        return {"avg_price": bo.avg_price, "trade_id": bo.id}
    return None


@achievement("blind-sniper")
def _check_blind_sniper(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """Win a trade with Price < $0.10 at entry."""
    if bo.result != BOResult.WIN:
        return None
    if bo.avg_price is not None and bo.avg_price < 0.10:
        return {"avg_price": bo.avg_price, "trade_id": bo.id}
    return None


@achievement("pink-slip-seeker")
def _check_pink_slip_seeker(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """Single trade stake > 90% of initial balance."""
    if bot.initial_balance and bot.initial_balance > 0:
        ratio = bo.amount / bot.initial_balance
        if ratio > 0.90:
            return {"amount": bo.amount, "initial_balance": bot.initial_balance, "ratio": round(ratio, 4)}
    return None


@achievement("the-martyr")
def _check_the_martyr(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """100% wallet stake + LOSS → balance effectively zero."""
    if bo.result != BOResult.LOSS:
        return None
    if bot.balance is not None and bot.balance <= 0:
        return {"final_balance": bot.balance, "trade_id": bo.id}
    return None


@achievement("golden-incense")
def _check_golden_incense(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """10 consecutive losses."""
    if bo.result != BOResult.LOSS:
        return None
    streak = _get_consecutive_streak(bo.bot_name, db, BOResult.LOSS)
    if streak >= 10:
        return {"loss_streak": streak}
    return None


@achievement("anti-midas")
def _check_anti_midas(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """15 consecutive losses."""
    if bo.result != BOResult.LOSS:
        return None
    streak = _get_consecutive_streak(bo.bot_name, db, BOResult.LOSS)
    if streak >= 15:
        return {"loss_streak": streak}
    return None


@achievement("immortal-sniper")
def _check_immortal_sniper(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """5 consecutive wins with avg_price < 0.15."""
    if bo.result != BOResult.WIN:
        return None
    trades = _get_recent_results(bo.bot_name, db, limit=50)
    count = 0
    for t in trades:
        if t.result == BOResult.WIN and t.avg_price is not None and t.avg_price < 0.15:
            count += 1
        else:
            break
    if count >= 5:
        return {"win_streak": count, "latest_avg_price": bo.avg_price}
    return None


@achievement("dust-collector")
def _check_dust_collector(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """100 trades where amount < $1.00 each."""
    if bo.amount >= _MIN_STAKE:
        return None
    dust_count = (
        db.query(func.count(BinaryOption.id))
        .filter(
            BinaryOption.bot_name == bo.bot_name,
            BinaryOption.amount < _MIN_STAKE,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
        )
        .scalar()
    )
    if dust_count >= 100:
        return {"dust_trades": dust_count}
    return None


@achievement("penny-pincher")
def _check_penny_pincher(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """50 consecutive min-stake trades (amount <= _MIN_STAKE)."""
    trades = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bo.bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
        )
        .order_by(BinaryOption.id.desc())
        .limit(50)
        .all()
    )
    count = 0
    for t in trades:
        if t.amount <= _MIN_STAKE:
            count += 1
        else:
            break
    if count >= 50:
        return {"consecutive_min_stake": count}
    return None


@achievement("phoenix-down")
def _check_phoenix_down(bo: BinaryOption, bot: Bot, db: Session) -> Optional[dict]:
    """Balance recovered from < $1 back to initial_balance."""
    if bot.balance is None or bot.initial_balance is None:
        return None
    if bot.balance < bot.initial_balance:
        return None
    # Check if balance history ever dipped below $1
    min_balance = (
        db.query(func.min(BalanceHistory.balance))
        .filter(BalanceHistory.bot_name == bot.bot_name)
        .scalar()
    )
    if min_balance is not None and min_balance < 1.0:
        return {
            "min_balance": min_balance,
            "current_balance": bot.balance,
            "initial_balance": bot.initial_balance,
        }
    return None


# ── Main entry point ─────────────────────────────────────────────────────


def on_trade_resolved(bo: BinaryOption, db: Session) -> list[str]:
    """
    Evaluate all achievement checkers for a resolved trade.
    Returns list of newly awarded slugs.
    """
    if bo.result not in (BOResult.WIN, BOResult.LOSS):
        return []

    bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
    if not bot:
        return []

    # Load already-earned slugs for this bot
    earned = set(
        row[0]
        for row in (
            db.query(AchievementDefinition.slug)
            .join(BotAchievement, BotAchievement.achievement_id == AchievementDefinition.id)
            .filter(BotAchievement.bot_id == bot.id)
            .all()
        )
    )

    # Load definition lookup
    defs = {
        d.slug: d
        for d in db.query(AchievementDefinition).all()
    }

    awarded = []
    for slug, checker in _CHECKERS.items():
        if slug in earned:
            continue
        defn = defs.get(slug)
        if defn is None:
            continue
        try:
            meta = checker(bo, bot, db)
        except Exception as exc:
            logger.warning("Achievement checker '%s' failed: %s", slug, exc)
            continue
        if meta is not None:
            db.add(BotAchievement(
                bot_id=bot.id,
                achievement_id=defn.id,
                metadata_=meta,
            ))
            awarded.append(slug)
            logger.info("Achievement unlocked: bot=%s slug=%s", bot.bot_name, slug)

    if awarded:
        db.commit()

    return awarded
