#!/usr/bin/env python3
"""
Xóa bot theo tên, kèm toàn bộ lệnh & balance history của bot đó.

Usage:
    python delete_bot.py <bot_name>
    python delete_bot.py <bot_name> --yes   # bỏ qua xác nhận
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from database import SessionLocal, engine, Base
from models import BalanceHistory, BinaryOption, Bot


def delete_bot(bot_name: str, skip_confirm: bool = False) -> None:
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
        if not bot:
            print(f"Bot '{bot_name}' không tồn tại.")
            return

        trades  = db.query(BinaryOption).filter(BinaryOption.bot_name == bot_name).count()
        history = db.query(BalanceHistory).filter(BalanceHistory.bot_name == bot_name).count()

        print(f"Bot       : {bot.bot_name}")
        print(f"Balance   : ${bot.balance:,.2f}")
        print(f"Lệnh      : {trades}")
        print(f"Bal. hist : {history}")

        if not skip_confirm:
            print()
            answer = input(f"Xóa bot '{bot_name}' và toàn bộ dữ liệu liên quan? [y/N] ").strip().lower()
            if answer != "y":
                print("Hủy.")
                return

        db.query(BinaryOption).filter(BinaryOption.bot_name == bot_name).delete()
        db.query(BalanceHistory).filter(BalanceHistory.bot_name == bot_name).delete()
        db.delete(bot)
        db.commit()

        print(f"\n✓ Đã xóa bot '{bot_name}', {trades} lệnh, {history} balance records.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xóa bot theo tên")
    parser.add_argument("bot_name", help="Tên bot cần xóa")
    parser.add_argument("--yes", action="store_true", help="Bỏ qua xác nhận")
    args = parser.parse_args()

    delete_bot(args.bot_name, skip_confirm=args.yes)
