#!/usr/bin/env python3
"""
Backfill balance_history from existing trade data.

For each bot:
  1. Insert a record at initial_balance (using bot.created_at as timestamp).
  2. Replay all settled trades in chronological order, inserting a record
     after each one with the running balance.

Skips bots that already have history (safe to re-run).

Usage:
    python backfill_balance_history.py
    python backfill_balance_history.py --force   # overwrite existing history
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from database import Base, SessionLocal, engine
from models import BalanceHistory, BinaryOption, BOResult, Bot

_PAYOUT_RATE = 1.00


def backfill(force: bool = False) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        bots = db.query(Bot).order_by(Bot.created_at).all()
        if not bots:
            print("No bots found.")
            return

        for bot in bots:
            existing = (
                db.query(BalanceHistory)
                .filter(BalanceHistory.bot_name == bot.bot_name)
                .count()
            )

            if existing and not force:
                print(f"  Skip  {bot.bot_name!r:20s} — already has {existing} record(s)")
                continue

            if existing and force:
                db.query(BalanceHistory).filter(
                    BalanceHistory.bot_name == bot.bot_name
                ).delete()
                db.flush()

            # Settled trades in chronological order
            trades = (
                db.query(BinaryOption)
                .filter(
                    BinaryOption.bot_name == bot.bot_name,
                    BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
                    BinaryOption.profit.isnot(None),
                )
                .order_by(BinaryOption.created_at.asc())
                .all()
            )

            records = []

            # Initial balance snapshot
            records.append(BalanceHistory(
                bot_name    = bot.bot_name,
                balance     = bot.initial_balance,
                trade_id    = None,
                recorded_at = bot.created_at,
            ))

            balance = bot.initial_balance
            for t in trades:
                balance = round(balance + t.profit, 8)
                records.append(BalanceHistory(
                    bot_name    = bot.bot_name,
                    balance     = balance,
                    trade_id    = t.id,
                    recorded_at = t.created_at,
                ))

            db.bulk_save_objects(records)
            db.flush()
            print(
                f"  Done  {bot.bot_name!r:20s} — {len(records)} records "
                f"(final balance: {balance:,.2f})"
            )

        db.commit()
        total = db.query(BalanceHistory).count()
        print(f"\nBackfill complete — {total} total records in balance_history.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill balance_history table")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and re-create history for bots that already have records",
    )
    args = parser.parse_args()

    try:
        backfill(force=args.force)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
