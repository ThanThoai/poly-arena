#!/usr/bin/env python3
"""
Reset trades & balance — giữ nguyên bots và accounts.

Thực hiện:
  • Xóa toàn bộ bảng binary_options
  • Xóa toàn bộ bảng balance_history
  • Xóa toàn bộ bảng bot_achievements
  • Reset balance của mỗi bot về initial_balance
  • KHÔNG xóa bots hay users

Usage:
    python bots/reset_db.py
    python bots/reset_db.py --yes   # bỏ qua xác nhận
"""

import argparse
import sys
from pathlib import Path

# Thêm project root vào sys.path để import được database/models
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from database import SessionLocal, engine, Base
from models import Bot


def reset(skip_confirm: bool = False) -> None:
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        bots = db.query(Bot).all()
        if bots:
            print(f"Tìm thấy {len(bots)} bot (sẽ giữ nguyên):")
            for b in bots:
                print(f"  • {b.bot_name}  balance: ${b.balance:,.2f} → ${b.initial_balance:,.2f}")

        if not skip_confirm:
            print()
            answer = input(
                "Xóa tất cả trades, balance history, achievements và reset balance?\n"
                "Bots và users sẽ được giữ nguyên. [y/N] "
            ).strip().lower()
            if answer != "y":
                print("Hủy.")
                return

        r_ach = db.execute(text("DELETE FROM bot_achievements"))
        r_bh  = db.execute(text("DELETE FROM balance_history"))
        r_ubh = db.execute(text("DELETE FROM user_balance_history"))
        r_ubs = db.execute(text("DELETE FROM user_balance_snapshots"))
        r_bo  = db.execute(text("DELETE FROM binary_options"))

        for b in bots:
            b.balance = b.initial_balance

        db.commit()

        print()
        print(f"✓ Đã xóa {r_bo.rowcount} lệnh")
        print(f"✓ Đã xóa {r_bh.rowcount} balance history records")
        print(f"✓ Đã xóa {r_ubh.rowcount} user balance history records")
        print(f"✓ Đã xóa {r_ubs.rowcount} user balance snapshot records")
        print(f"✓ Đã xóa {r_ach.rowcount} achievements")
        print(f"✓ Reset balance {len(bots)} bot về initial_balance")
        print("✓ Bots và users được giữ nguyên")
        print("\nDone.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset trades & balance, giữ nguyên bots & users")
    parser.add_argument("--yes", action="store_true", help="Bỏ qua xác nhận")
    args = parser.parse_args()

    reset(skip_confirm=args.yes)
