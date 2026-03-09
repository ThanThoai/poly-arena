#!/usr/bin/env python3
"""
Reset tất cả bot: xoá trade (BO + futures), balance history, achievements → reset balance.

Thực hiện:
  1. Xoá toàn bộ binary_options (lịch sử lệnh BO)
  2. Xoá toàn bộ futures_orders (lệnh limit pending)
  3. Xoá toàn bộ futures_positions (lịch sử vị thế futures)
  4. Xoá toàn bộ balance_history (snapshot balance bot)
  5. Xoá toàn bộ bot_achievements
  6. Xoá toàn bộ user_balance_history + user_balance_snapshots
  7. Reset mỗi bot: balance = initial_balance = 10000, status = ACTIVE
  8. Reset mỗi user: initial_balance phù hợp với số bot × 10000

Hỗ trợ filter theo user hoặc bot:
    python scripts/reset_bots.py                        # dry-run tất cả
    python scripts/reset_bots.py --apply                # thực hiện tất cả
    python scripts/reset_bots.py --user trader-1 --apply
    python scripts/reset_bots.py --bot trader-1-aggressive --apply
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone
from sqlalchemy import text
from database import SessionLocal, engine, Base
from models import (
    BalanceHistory, BinaryOption, Bot, BotAchievement, BotSettlementLedger, BOResult,
    User, UserBalanceHistory, UserBalanceSnapshot,
)
from models_futures import FuturesPosition, FuturesOrder

BOT_BALANCE = 10_000.0


def reset(
    apply: bool = False,
    user_filter: str | None = None,
    bot_filter: str | None = None,
) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        # -- Resolve target bots --
        bot_q = db.query(Bot).filter(Bot.is_active == True)

        if bot_filter:
            bot_q = bot_q.filter(Bot.bot_name == bot_filter)
        elif user_filter:
            user = db.query(User).filter(User.username == user_filter).first()
            if not user:
                print(f"User '{user_filter}' not found.")
                return
            bot_q = bot_q.filter(Bot.user_id == user.id)

        bots = bot_q.all()
        if not bots:
            print("No matching bots found.")
            return

        bot_names = [b.bot_name for b in bots]
        bot_ids = [b.id for b in bots]

        # -- Count affected rows --
        bo_count = (
            db.query(BinaryOption)
            .filter(BinaryOption.bot_name.in_(bot_names))
            .count()
        )
        fut_pos_count = (
            db.query(FuturesPosition)
            .filter(FuturesPosition.bot_name.in_(bot_names))
            .count()
        )
        fut_ord_count = (
            db.query(FuturesOrder)
            .filter(FuturesOrder.bot_name.in_(bot_names))
            .count()
        )
        bh_count = (
            db.query(BalanceHistory)
            .filter(BalanceHistory.bot_name.in_(bot_names))
            .count()
        )
        ach_count = (
            db.query(BotAchievement)
            .filter(BotAchievement.bot_id.in_(bot_ids))
            .count()
        )
        bsl_count = (
            db.query(BotSettlementLedger)
            .filter(BotSettlementLedger.bot_name.in_(bot_names))
            .count()
        )

        prefix = "[DRY-RUN] " if not apply else ""
        scope = f"bot={bot_filter}" if bot_filter else f"user={user_filter}" if user_filter else "ALL"
        print(f"{prefix}Reset scope: {scope}")
        print(f"  Bots to reset:              {len(bots)}")
        print(f"  Binary options to delete:   {bo_count}")
        print(f"  Futures positions to delete: {fut_pos_count}")
        print(f"  Futures orders to delete:   {fut_ord_count}")
        print(f"  BalanceHistory to delete:   {bh_count}")
        print(f"  Achievements to delete:     {ach_count}")
        print(f"  Settlement ledger to delete: {bsl_count}")
        print()

        for bot in bots:
            print(f"  {bot.bot_name}: ${bot.balance:,.2f} -> ${BOT_BALANCE:,.2f}")

        if not apply:
            print("\nPass --apply to execute.")
            return

        # -- 1. Delete binary options --
        deleted_bo = (
            db.query(BinaryOption)
            .filter(BinaryOption.bot_name.in_(bot_names))
            .delete(synchronize_session="fetch")
        )
        print(f"\n  Deleted {deleted_bo} binary options")

        # -- 2. Delete futures orders (before positions due to FK) --
        deleted_fo = (
            db.query(FuturesOrder)
            .filter(FuturesOrder.bot_name.in_(bot_names))
            .delete(synchronize_session="fetch")
        )
        print(f"  Deleted {deleted_fo} futures orders")

        # -- 3. Delete futures positions --
        deleted_fp = (
            db.query(FuturesPosition)
            .filter(FuturesPosition.bot_name.in_(bot_names))
            .delete(synchronize_session="fetch")
        )
        print(f"  Deleted {deleted_fp} futures positions")

        # -- 4. Delete balance history --
        deleted_bh = (
            db.query(BalanceHistory)
            .filter(BalanceHistory.bot_name.in_(bot_names))
            .delete(synchronize_session="fetch")
        )
        print(f"  Deleted {deleted_bh} balance history records")

        # -- 5. Delete achievements --
        deleted_ach = (
            db.query(BotAchievement)
            .filter(BotAchievement.bot_id.in_(bot_ids))
            .delete(synchronize_session="fetch")
        )
        print(f"  Deleted {deleted_ach} achievement records")

        # -- 5b. Delete settlement ledger --
        deleted_bsl = (
            db.query(BotSettlementLedger)
            .filter(BotSettlementLedger.bot_name.in_(bot_names))
            .delete(synchronize_session="fetch")
        )
        print(f"  Deleted {deleted_bsl} settlement ledger records")

        # -- 6. Reset bot balances --
        for bot in bots:
            bot.balance = BOT_BALANCE
            bot.initial_balance = BOT_BALANCE
            bot.status = "ACTIVE"
        print(f"  Reset {len(bots)} bots -> balance=${BOT_BALANCE:,.0f}")

        # -- 7. Reset affected users --
        affected_user_ids = set(b.user_id for b in bots if b.user_id)
        for uid in affected_user_ids:
            user = db.query(User).filter(User.id == uid).first()
            if not user:
                continue

            all_user_bots = (
                db.query(Bot)
                .filter(Bot.user_id == uid, Bot.is_active == True)
                .all()
            )
            total_allocated = sum(b.initial_balance or 0 for b in all_user_bots)
            user.initial_balance = total_allocated

            # Clean user-level history
            db.query(UserBalanceHistory).filter(UserBalanceHistory.user_id == uid).delete(synchronize_session="fetch")
            db.query(UserBalanceSnapshot).filter(UserBalanceSnapshot.user_id == uid).delete(synchronize_session="fetch")

            # Seed fresh snapshot
            db.add(UserBalanceSnapshot(
                user_id=uid,
                session_id=None,
                candle_open=None,
                unallocated=0.0,
                bot_cash=total_allocated,
                bo_locked=0,
                futures_locked=0,
                equity=total_allocated,
                bo_unrealized_pnl=0,
                futures_unrealized_pnl=0,
                unrealized_pnl=0,
                net_liquidation=total_allocated,
                cumulative_realized_pnl=0,
                session_realized_pnl=0,
                snapshot_delta=None,
                active_bot_count=len(all_user_bots),
                open_bo_count=0,
                open_futures_count=0,
                recorded_at=datetime.now(timezone.utc),
            ))
            print(f"  User '{user.username}': initial={total_allocated:,.0f} ({len(all_user_bots)} bots x balance)")

        db.commit()
        print("\nDone.")

    except Exception as exc:
        db.rollback()
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset bot trades & balances -> $10,000",
    )
    parser.add_argument("--apply", action="store_true", help="Execute (default: dry-run)")
    parser.add_argument("--user", type=str, default=None, help="Only reset bots of this user")
    parser.add_argument("--bot", type=str, default=None, help="Only reset this specific bot")
    args = parser.parse_args()

    reset(apply=args.apply, user_filter=args.user, bot_filter=args.bot)
