#!/usr/bin/env python3
"""
Reset toàn bộ DB về trạng thái ban đầu.

Xóa:
  • Tất cả lệnh trong bảng binary_options
  • Tất cả bot trong bảng bots
  • Cache api_key tại bots/bots_config.json

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

CONFIG_FILE = Path(__file__).parent / "bots_config.json"


def reset(skip_confirm: bool = False) -> None:
    if not skip_confirm:
        answer = input("Xóa toàn bộ dữ liệu DB? [y/N] ").strip().lower()
        if answer != "y":
            print("Hủy.")
            return

    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        r_bo  = db.execute(text("DELETE FROM binary_options"))
        r_bot = db.execute(text("DELETE FROM bots"))
        db.commit()
        print(f"Đã xóa {r_bo.rowcount} lệnh, {r_bot.rowcount} bot.")
    finally:
        db.close()

    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()
        print(f"Đã xóa cache api_key: {CONFIG_FILE}")

    print("Done — DB sạch, sẵn sàng chạy lại.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset toàn bộ DB PolyArena")
    parser.add_argument("--yes", action="store_true", help="Bỏ qua xác nhận")
    args = parser.parse_args()

    reset(skip_confirm=args.yes)
