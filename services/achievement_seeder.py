"""
Seed achievement definitions into the database.

Idempotent: existing slugs are skipped.
"""

import logging
from sqlalchemy.orm import Session
from models import AchievementDefinition

logger = logging.getLogger(__name__)

DEFINITIONS = [
    {
        "slug": "peak-buyer",
        "name": "Đại Sứ Đu Đỉnh",
        "description": "The view is great from up here, isn't it?",
        "tier": "SILVER",
        "category": "Banter & Bad Luck",
    },
    {
        "slug": "blind-sniper",
        "name": "Sát Thủ Mù",
        "description": "You didn't see the chart, but you felt the win.",
        "tier": "GOLD",
        "category": "Holy Prophet & Assassin",
    },
    {
        "slug": "pink-slip-seeker",
        "name": "Sổ Đỏ Diver",
        "description": "One green candle to rule them all, or back to the streets.",
        "tier": "SILVER",
        "category": "Size Matters",
    },
    {
        "slug": "the-martyr",
        "name": "Kẻ Tử Vì Đạo",
        "description": "You went down with the ship. Respect.",
        "tier": "PLATINUM",
        "category": "How the Steel Was Tempered",
    },
    {
        "slug": "golden-incense",
        "name": "Bát Nhang Vàng",
        "description": "A beacon of hope for people betting against you.",
        "tier": "SILVER",
        "category": "Binary Trauma",
    },
    {
        "slug": "anti-midas",
        "name": "Bàn Tay Midas Ngược",
        "description": "Everything you touch turns to... well, not gold.",
        "tier": "GOLD",
        "category": "Banter & Bad Luck",
    },
    {
        "slug": "immortal-sniper",
        "name": "The Immortal Sniper",
        "description": "Winning is easy when you're this lucky.",
        "tier": "PLATINUM",
        "category": "Holy Prophet & Assassin",
    },
    {
        "slug": "dust-collector",
        "name": "Vua Ve Chai",
        "description": "Collecting pennies like they're infinity stones.",
        "tier": "BRONZE",
        "category": "Vagabond Style",
    },
    {
        "slug": "penny-pincher",
        "name": "Chúa Tể Cò Con",
        "description": "50 trades for the price of one Banh Mi.",
        "tier": "BRONZE",
        "category": "Size Matters",
    },
    {
        "slug": "phoenix-down",
        "name": "Trỗi Dậy Từ Tro Tàn",
        "description": "Account balance: $0.01. Current status: Legend.",
        "tier": "PLATINUM",
        "category": "How the Steel Was Tempered",
    },
]


def seed_achievements(db: Session) -> int:
    """Insert missing achievement definitions. Returns count of newly inserted."""
    existing_slugs = {
        row[0]
        for row in db.query(AchievementDefinition.slug).all()
    }
    added = 0
    for defn in DEFINITIONS:
        if defn["slug"] not in existing_slugs:
            db.add(AchievementDefinition(**defn))
            added += 1
    if added:
        db.commit()
        logger.info("Seeded %d achievement definition(s)", added)
    return added
