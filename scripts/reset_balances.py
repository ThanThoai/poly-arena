#!/usr/bin/env python3
"""
Reset trades & balance về trạng thái ban đầu, GIỮ NGUYÊN thông tin bots.

Thực hiện:
  • Xóa toàn bộ bảng binary_options
  • Xóa toàn bộ bảng balance_history
  • Reset balance của mỗi bot về initial_balance (mặc định 10,000)

Usage:
    python reset_balances.py
    python reset_balances.py --yes   # bỏ qua xác nhận
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import text
from database import SessionLocal, engine, Base
from models import Bot, INITIAL_BALANCE


def reset(skip_confirm: bool = False) -> None:
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        bots = db.query(Bot).all()
        if not bots:
            print("Không có bot nào trong DB.")
        else:
            print(f"Tìm thấy {len(bots)} bot:")
            for b in bots:
                print(f"  • {b.bot_name}  balance hiện tại: ${b.balance:,.2f}")

        if not skip_confirm:
            print()
            answer = input(
                "Xóa tất cả lệnh & balance history, reset balance về "
                f"${INITIAL_BALANCE:,.0f}? [y/N] "
            ).strip().lower()
            if answer != "y":
                print("Hủy.")
                return

        r_bo  = db.execute(text("DELETE FROM binary_options"))
        r_bh  = db.execute(text("DELETE FROM balance_history"))

        for b in bots:
            b.balance = b.initial_balance

        db.commit()

        print()
        print(f"✓ Đã xóa {r_bo.rowcount} lệnh")
        print(f"✓ Đã xóa {r_bh.rowcount} balance history records")
        print(f"✓ Reset balance {len(bots)} bot về initial_balance")
        for b in bots:
            print(f"  • {b.bot_name}  → ${b.initial_balance:,.2f}")
        print("\nDone.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset trades & balance, giữ nguyên bot")
    parser.add_argument("--yes", action="store_true", help="Bỏ qua xác nhận")
    args = parser.parse_args()

    reset(skip_confirm=args.yes)
