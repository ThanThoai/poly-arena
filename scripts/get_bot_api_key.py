"""Get bot API key by bot_id.

Usage:
    python scripts/get_bot_api_key.py <bot_id>
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import Bot


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/get_bot_api_key.py <bot_id>")
        sys.exit(1)

    bot_id = int(sys.argv[1])
    db = SessionLocal()
    try:
        bot = db.query(Bot).filter(Bot.id == bot_id).first()
        if not bot:
            print(f"Bot #{bot_id} not found")
            sys.exit(1)
        print(f"Bot #{bot.id} ({bot.bot_name}): {bot.api_key}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
