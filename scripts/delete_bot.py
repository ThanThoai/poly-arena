#!/usr/bin/env python3
"""
Xóa bot theo tên hoặc ID, kèm toàn bộ lệnh & balance history của bot đó.

Usage:
    python scripts/delete_bot.py 42              # xoá bot có id=42
    python scripts/delete_bot.py my-bot          # xoá bot có tên "my-bot"
    python scripts/delete_bot.py 42 --yes        # bỏ qua xác nhận
    python scripts/delete_bot.py --list          # liệt kê tất cả bot
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import SessionLocal, engine, Base
from models import BalanceHistory, BinaryOption, Bot, BotAchievement


def list_bots() -> None:
    """Liệt kê tất cả bot."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        bots = db.query(Bot).order_by(Bot.id).all()
        if not bots:
            print("Không có bot nào.")
            return

        print(f"{'ID':>5}  {'Status':<8}  {'Balance':>12}  {'Name'}")
        print("-" * 55)
        for b in bots:
            status = b.status if b.is_active else "DELETED"
            print(f"{b.id:>5}  {status:<8}  ${b.balance:>10,.2f}  {b.bot_name}")
        print(f"\nTổng: {len(bots)} bot(s)")
    finally:
        db.close()


def find_bot(db, identifier: str) -> Bot | None:
    """Tìm bot theo ID (nếu là số) hoặc theo tên."""
    if identifier.isdigit():
        return db.query(Bot).filter(Bot.id == int(identifier)).first()
    return db.query(Bot).filter(Bot.bot_name == identifier).first()


def delete_bot(identifier: str, skip_confirm: bool = False) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        bot = find_bot(db, identifier)
        if not bot:
            print(f"Bot '{identifier}' không tồn tại.")
            return

        trades = db.query(BinaryOption).filter(BinaryOption.bot_name == bot.bot_name).count()
        history = db.query(BalanceHistory).filter(BalanceHistory.bot_name == bot.bot_name).count()
        achievements = db.query(BotAchievement).filter(BotAchievement.bot_id == bot.id).count()

        print(f"ID        : {bot.id}")
        print(f"Bot       : {bot.bot_name}")
        print(f"Status    : {bot.status if bot.is_active else 'DELETED'}")
        print(f"Balance   : ${bot.balance:,.2f}")
        print(f"Trades    : {trades}")
        print(f"Bal. hist : {history}")
        print(f"Achieve.  : {achievements}")

        if not skip_confirm:
            print()
            answer = input(f"Xóa bot #{bot.id} '{bot.bot_name}' và toàn bộ dữ liệu liên quan? [y/N] ").strip().lower()
            if answer != "y":
                print("Hủy.")
                return

        db.query(BinaryOption).filter(BinaryOption.bot_name == bot.bot_name).delete()
        db.query(BalanceHistory).filter(BalanceHistory.bot_name == bot.bot_name).delete()
        db.query(BotAchievement).filter(BotAchievement.bot_id == bot.id).delete()
        db.delete(bot)
        db.commit()

        print(f"\nDone: bot #{bot.id} '{bot.bot_name}' — {trades} trades, {history} balance records, {achievements} achievements.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xóa bot theo tên hoặc ID")
    parser.add_argument("identifier", nargs="?", help="Bot ID (số) hoặc bot_name")
    parser.add_argument("--yes", action="store_true", help="Bỏ qua xác nhận")
    parser.add_argument("--list", action="store_true", help="Liệt kê tất cả bot")
    args = parser.parse_args()

    if args.list:
        list_bots()
    elif args.identifier:
        delete_bot(args.identifier, skip_confirm=args.yes)
    else:
        parser.print_help()
