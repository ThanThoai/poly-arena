"""Achievement API endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import get_db
from models import AchievementDefinition, Bot, BotAchievement
from schemas import AchievementDefinitionResponse, BotAchievementResponse

router = APIRouter()


def _enrich(ba: BotAchievement, bot: Bot, defn: AchievementDefinition) -> dict:
    return {
        "id": ba.id,
        "bot_id": ba.bot_id,
        "bot_name": bot.bot_name,
        "achievement_id": ba.achievement_id,
        "slug": defn.slug,
        "name": defn.name,
        "description": defn.description,
        "tier": defn.tier,
        "earned_at": ba.earned_at,
        "metadata_": ba.metadata_,
    }


@router.get("", response_model=list[AchievementDefinitionResponse])
def list_achievements(db: Session = Depends(get_db)):
    """List all achievement definitions."""
    return db.query(AchievementDefinition).order_by(AchievementDefinition.id).all()


@router.get("/bot/{bot_id}", response_model=list[BotAchievementResponse])
def bot_achievements(bot_id: int, db: Session = Depends(get_db)):
    """List achievements earned by a specific bot."""
    rows = (
        db.query(BotAchievement, Bot, AchievementDefinition)
        .join(Bot, Bot.id == BotAchievement.bot_id)
        .join(AchievementDefinition, AchievementDefinition.id == BotAchievement.achievement_id)
        .filter(BotAchievement.bot_id == bot_id)
        .order_by(BotAchievement.earned_at.desc())
        .all()
    )
    return [_enrich(ba, bot, defn) for ba, bot, defn in rows]


@router.get("/all-bots")
def all_bot_achievements(db: Session = Depends(get_db)):
    """Return all bot achievements grouped by bot_id — single query instead of N+1."""
    rows = (
        db.query(BotAchievement, Bot, AchievementDefinition)
        .join(Bot, Bot.id == BotAchievement.bot_id)
        .join(AchievementDefinition, AchievementDefinition.id == BotAchievement.achievement_id)
        .order_by(BotAchievement.earned_at.desc())
        .all()
    )
    grouped: dict[int, list] = {}
    for ba, bot, defn in rows:
        grouped.setdefault(ba.bot_id, []).append(_enrich(ba, bot, defn))
    return grouped
