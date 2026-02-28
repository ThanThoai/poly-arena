"""Achievement API endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import AchievementDefinition, Bot, BotAchievement, User
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


@router.get("/", response_model=list[AchievementDefinitionResponse])
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


@router.get("/my", response_model=list[BotAchievementResponse])
def my_achievements(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all achievements across all bots owned by the current user."""
    rows = (
        db.query(BotAchievement, Bot, AchievementDefinition)
        .join(Bot, Bot.id == BotAchievement.bot_id)
        .join(AchievementDefinition, AchievementDefinition.id == BotAchievement.achievement_id)
        .filter(Bot.user_id == user.id)
        .order_by(BotAchievement.earned_at.desc())
        .all()
    )
    return [_enrich(ba, bot, defn) for ba, bot, defn in rows]
