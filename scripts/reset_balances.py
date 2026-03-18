#!/usr/bin/env python3
"""
Reset bot & user balances và xoá lịch sử lệnh.

Thực hiện:
  1. Xoá toàn bộ binary_options (lịch sử lệnh BO)
  2. Xoá toàn bộ futures_orders (lệnh futures)
  3. Xoá toàn bộ futures_positions (vị thế futures)
  4. Xoá toàn bộ balance_history (snapshot balance bot)
  5. Xoá toàn bộ user_balance_history (snapshot balance user)
  6. Xoá toàn bộ user_balance_snapshots
  7. Xoá toàn bộ bot_settlement_ledger
  8. Xoá toàn bộ bot_achievements
  9. Reset mỗi bot: balance = initial_balance = 10000, status = ACTIVE
 10. Reset mỗi user: initial_balance = 50000 - tổng initial_balance các bot sở hữu

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
from models import Bot, User, UserBalanceSnapshot, BotSettlementLedger

BOT_BALANCE = 10000.0
USER_TOTAL = 50000.0


def _safe_count(db, table: str) -> int:
    """Return row count, 0 if table does not exist."""
    try:
        return db.execute(text(f"SELECT count(*) FROM {table}")).scalar()
    except Exception:
        db.rollback()
        return 0


def _safe_delete(db, table: str) -> int:
    """Delete all rows, return rowcount. Skip if table missing."""
    try:
        r = db.execute(text(f"DELETE FROM {table}"))
        return r.rowcount
    except Exception:
        db.rollback()
        return 0


def reset(apply: bool = False) -> None:
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        # ── Counts ──
        tables = [
            ("binary_options",        "BO trades"),
            ("futures_orders",        "Futures orders"),
            ("futures_positions",     "Futures positions"),
            ("balance_history",       "Balance history"),
            ("user_balance_history",  "User balance history"),
            ("user_balance_snapshots","User balance snapshots"),
            ("bot_settlement_ledger", "Bot settlement ledger"),
            ("bot_achievements",      "Bot achievements"),
        ]

        counts = {}
        for tbl, _ in tables:
            counts[tbl] = _safe_count(db, tbl)

        bots = db.query(Bot).filter(Bot.is_active == True).all()
        users = db.query(User).all()

        prefix = "[DRY-RUN] " if not apply else ""
        print(f"{prefix}Reset summary:")
        for tbl, label in tables:
            c = counts[tbl]
            flag = "" if c == 0 else " ⚠"
            print(f"  {label + ' to delete:':<35} {c}{flag}")
        print(f"  {'Active bots to reset:':<35} {len(bots)}")
        print(f"  {'Users to reset:':<35} {len(users)}")
        print()

        # ── Bot details (with ID) ──
        print("  ── Bots ──")
        for bot in bots:
            owner = next((u.username for u in users if u.id == bot.user_id), "?")
            print(
                f"  [{bot.id}] {bot.bot_name:<20} "
                f"owner={owner:<12} "
                f"balance {bot.balance:.2f} → {BOT_BALANCE:.2f}"
            )

        # ── User details ──
        print()
        print("  ── Users ──")
        for user in users:
            user_bots = [b for b in bots if b.user_id == user.id]
            bot_ids = ", ".join(str(b.id) for b in user_bots) or "none"
            total_allocated = len(user_bots) * BOT_BALANCE
            available = USER_TOTAL - total_allocated
            print(
                f"  [{user.id}] {user.username:<15} "
                f"initial_balance {user.initial_balance:.2f} → {USER_TOTAL:.2f} "
                f"(bots: {bot_ids})"
            )

        if not apply:
            print("\nPass --apply to execute.")
            return

        # ── Delete in dependency order ──
        print()
        for tbl, label in tables:
            n = _safe_delete(db, tbl)
            print(f"  Deleted {n} {label.lower()}")

        # ── Reset bots ──
        for bot in bots:
            bot.balance = BOT_BALANCE
            bot.initial_balance = BOT_BALANCE
            bot.status = "ACTIVE"
        print(f"  Reset {len(bots)} bots → balance={BOT_BALANCE:.0f}")

        # ── Reset users & tạo snapshot ban đầu ──
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
