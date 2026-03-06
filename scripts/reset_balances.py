#!/usr/bin/env python3
"""
Reset bot & user balances và xoá lịch sử lệnh.

Thực hiện:
  1. Xoá toàn bộ binary_options (lịch sử lệnh)
  2. Xoá toàn bộ balance_history (snapshot balance bot)
  3. Xoá toàn bộ user_balance_history (snapshot balance user)
  4. Reset mỗi bot: balance = initial_balance = 10000, status = ACTIVE
  5. Reset mỗi user: initial_balance = 50000 - tổng initial_balance các bot sở hữu

Usage:
    python scripts/reset_balances.py          # dry-run (chỉ hiển thị)
    python scripts/reset_balances.py --apply  # thực hiện thay đổi
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from database import SessionLocal, engine, Base
from datetime import datetime, timezone
from models import Bot, User, UserBalanceSnapshot

BOT_BALANCE = 10000.0
USER_TOTAL = 50000.0


def reset(apply: bool = False) -> None:
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        # ── Counts ──
        trade_count = db.execute(text("SELECT count(*) FROM binary_options")).scalar()
        bh_count = db.execute(text("SELECT count(*) FROM balance_history")).scalar()
        ubh_count = db.execute(text("SELECT count(*) FROM user_balance_history")).scalar()
        ubs_count = db.execute(text("SELECT count(*) FROM user_balance_snapshots")).scalar()

        bots = db.query(Bot).filter(Bot.is_active == True).all()
        users = db.query(User).all()

        prefix = "[DRY-RUN] " if not apply else ""
        print(f"{prefix}Reset summary:")
        print(f"  Trades to delete:              {trade_count}")
        print(f"  BalanceHistory to delete:       {bh_count}")
        print(f"  UserBalanceHistory to delete:   {ubh_count}")
        print(f"  UserBalanceSnapshots to delete: {ubs_count}")
        print(f"  Active bots to reset:           {len(bots)}")
        print(f"  Users to reset:                 {len(users)}")
        print()

        # ── Bot details ──
        for bot in bots:
            print(f"  Bot '{bot.bot_name}': balance {bot.balance:.2f} → {BOT_BALANCE:.2f}")

        # ── User details ──
        for user in users:
            user_bots = [b for b in bots if b.user_id == user.id]
            total_allocated = len(user_bots) * BOT_BALANCE
            available = USER_TOTAL - total_allocated
            print(
                f"  User '{user.username}': initial_balance {user.initial_balance:.2f} → {USER_TOTAL:.2f} "
                f"(available: {available:.0f} = {USER_TOTAL:.0f} - {len(user_bots)} bots × {BOT_BALANCE:.0f})"
            )

        if not apply:
            print("\nPass --apply to execute.")
            return

        # ── 1. Xoá lịch sử lệnh ──
        r_bo = db.execute(text("DELETE FROM binary_options"))
        print(f"\n  Deleted {r_bo.rowcount} trades")

        # ── 2. Xoá balance history ──
        r_bh = db.execute(text("DELETE FROM balance_history"))
        print(f"  Deleted {r_bh.rowcount} balance history records")

        # ── 3. Xoá user balance history ──
        r_ubh = db.execute(text("DELETE FROM user_balance_history"))
        print(f"  Deleted {r_ubh.rowcount} user balance history records")

        # ── 3b. Xoá user balance snapshots ──
        r_ubs = db.execute(text("DELETE FROM user_balance_snapshots"))
        print(f"  Deleted {r_ubs.rowcount} user balance snapshot records")

        # ── 4. Reset bots ──
        for bot in bots:
            bot.balance = BOT_BALANCE
            bot.initial_balance = BOT_BALANCE
            bot.status = "ACTIVE"
        print(f"  Reset {len(bots)} bots → balance={BOT_BALANCE:.0f}")

        # ── 5. Reset users & tạo bản ghi snapshot ban đầu ──
        for user in users:
            user.initial_balance = USER_TOTAL
            user_bots = [b for b in bots if b.user_id == user.id]
            total_allocated = len(user_bots) * BOT_BALANCE
            available = USER_TOTAL - total_allocated
            db.add(UserBalanceSnapshot(
                user_id=user.id,
                session_id=None,
                candle_open=None,
                unallocated=available,
                bot_cash=total_allocated,
                bo_locked=0,
                futures_locked=0,
                equity=USER_TOTAL,
                bo_unrealized_pnl=0,
                futures_unrealized_pnl=0,
                unrealized_pnl=0,
                net_liquidation=USER_TOTAL,
                cumulative_realized_pnl=0,
                session_realized_pnl=0,
                snapshot_delta=None,
                active_bot_count=len(user_bots),
                open_bo_count=0,
                open_futures_count=0,
                recorded_at=datetime.now(timezone.utc),
            ))
            print(f"  User '{user.username}': equity={USER_TOTAL:.0f}, bot_cash={total_allocated:.0f}, unallocated={available:.0f}")
        print(f"  Reset {len(users)} users → initial_balance={USER_TOTAL:.0f} + snapshot saved")

        db.commit()
        print("\nDone.")

    except Exception as exc:
        db.rollback()
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset bot/user balances, xoá lịch sử lệnh")
    parser.add_argument("--apply", action="store_true", help="Thực hiện thay đổi (mặc định: dry-run)")
    args = parser.parse_args()

    reset(apply=args.apply)
