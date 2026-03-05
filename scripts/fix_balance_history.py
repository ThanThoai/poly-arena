#!/usr/bin/env python3
"""
Kiểm tra và sửa dữ liệu BalanceHistory (snapshot equity của bot) cho lệnh BO.

Equity đúng tại thời điểm T:
  equity = initial_balance
           + sum(profit) for BO settled <= T
           - sum(entry_fee) for non-CANCELLED BO created <= T

Usage:
    python scripts/fix_balance_history.py                    # dry-run tất cả
    python scripts/fix_balance_history.py --apply            # thực hiện sửa
    python scripts/fix_balance_history.py --bot my-bot -v    # chi tiết 1 bot
    python scripts/fix_balance_history.py --user trader-1    # bots của user
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func
from sqlalchemy.orm import Session as SASession
from datetime import datetime

from database import SessionLocal, engine, Base
from models import (
    BalanceHistory, BinaryOption, Bot, BOResult, User,
)

TOLERANCE = 0.01


def _calc_expected_equity(db: SASession, bot_name: str, at: datetime, initial: float) -> dict:
    """Tính equity đúng tại thời điểm T từ BO trade history."""

    bo_realized = (
        db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
            BinaryOption.settlement_at <= at,
        )
        .scalar()
    ) or 0.0

    bo_fees = (
        db.query(func.coalesce(func.sum(BinaryOption.entry_fee), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result != BOResult.CANCELLED,
            BinaryOption.entry_fee.isnot(None),
            BinaryOption.created_at <= at,
        )
        .scalar()
    ) or 0.0

    equity = round(initial + bo_realized - bo_fees, 8)

    return {
        "equity": equity,
        "bo_realized": round(bo_realized, 8),
        "bo_fees": round(bo_fees, 8),
    }


def check_and_fix(
    apply: bool = False,
    user_filter: str | None = None,
    bot_filter: str | None = None,
    verbose: bool = False,
) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
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

        prefix = "[DRY-RUN] " if not apply else ""
        scope = (
            f"bot={bot_filter}" if bot_filter
            else f"user={user_filter}" if user_filter
            else "ALL"
        )
        print(f"{prefix}Checking BalanceHistory (BO only) — scope: {scope}")
        print(f"  Bots: {len(bots)}\n")

        total_records = 0
        total_wrong = 0
        total_updated = 0

        for bot in bots:
            initial = bot.initial_balance or 0

            records = (
                db.query(BalanceHistory)
                .filter(BalanceHistory.bot_name == bot.bot_name)
                .order_by(BalanceHistory.recorded_at.asc())
                .all()
            )

            if not records:
                if verbose:
                    print(f"  {bot.bot_name}: no records")
                continue

            bot_wrong = 0
            bot_records = len(records)
            total_records += bot_records

            for rec in records:
                at = rec.recorded_at
                if not at:
                    continue

                result = _calc_expected_equity(db, bot.bot_name, at, initial)
                expected = result["equity"]
                stored = round(rec.balance or 0, 8)
                diff = round(expected - stored, 8)

                if abs(diff) > TOLERANCE:
                    bot_wrong += 1
                    total_wrong += 1

                    if verbose:
                        print(
                            f"    [{rec.id}] {at.strftime('%Y-%m-%d %H:%M:%S')} "
                            f"stored=${stored:,.2f}  expected=${expected:,.2f}  "
                            f"diff={diff:+,.2f}  "
                            f"(bo_pnl={result['bo_realized']:+,.2f} "
                            f"bo_fees={result['bo_fees']:,.2f})"
                        )

                    if apply:
                        rec.balance = expected
                        total_updated += 1

            status = f"WRONG ({bot_wrong})" if bot_wrong else "OK"
            print(f"  {bot.bot_name}: {bot_records} records [{status}]")

        print(f"\n{prefix}Summary:")
        print(f"  Total records checked: {total_records}")
        print(f"  Incorrect records:     {total_wrong}")

        if apply and total_updated:
            db.commit()
            print(f"  Updated records:       {total_updated}")
            print("\nDone.")
        elif total_wrong and not apply:
            print("\nPass --apply to fix incorrect records.")
        else:
            print("\nAll records are correct.")

    except Exception as exc:
        db.rollback()
        print(f"\nError: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check & fix bot BalanceHistory snapshots (BO only)",
    )
    parser.add_argument("--apply", action="store_true", help="Fix incorrect records (default: dry-run)")
    parser.add_argument("--user", type=str, default=None, help="Only check bots of this user")
    parser.add_argument("--bot", type=str, default=None, help="Only check this specific bot")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show details for each incorrect record")
    args = parser.parse_args()

    check_and_fix(
        apply=args.apply,
        user_filter=args.user,
        bot_filter=args.bot,
        verbose=args.verbose,
    )
