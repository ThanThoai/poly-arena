#!/usr/bin/env python3
"""
Backfill bot_settlement_ledger from existing settled trades.

For each bot:
  1. Query all settled trades (WIN/LOSS) ordered by settlement_at.
  2. Group trades by settlement batch (same settlement_at timestamp).
  3. Write ONE aggregated ledger row per batch, chaining prev_balance → new_balance.

Skips bots that already have ledger entries (safe to re-run).

Usage:
    python scripts/backfill_settlement_ledger.py
    python scripts/backfill_settlement_ledger.py --force       # overwrite existing ledger
    python scripts/backfill_settlement_ledger.py --bot mybot   # backfill single bot
    python scripts/backfill_settlement_ledger.py --dry-run     # preview without writing
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Base, SessionLocal, engine
from models import (
    BinaryOption,
    BOResult,
    Bot,
    BotSettlementLedger,
)


def backfill(force: bool = False, bot_filter: str | None = None, dry_run: bool = False) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        q = db.query(Bot).order_by(Bot.created_at)
        if bot_filter:
            q = q.filter(Bot.bot_name == bot_filter)
        bots = q.all()

        if not bots:
            print("No bots found." if not bot_filter else f"Bot '{bot_filter}' not found.")
            return

        total_created = 0

        for bot in bots:
            existing = (
                db.query(BotSettlementLedger)
                .filter(BotSettlementLedger.bot_name == bot.bot_name)
                .count()
            )

            if existing and not force:
                print(f"  Skip  {bot.bot_name!r:20s} — already has {existing} ledger record(s)")
                continue

            if existing and force:
                if not dry_run:
                    db.query(BotSettlementLedger).filter(
                        BotSettlementLedger.bot_name == bot.bot_name,
                    ).delete()
                    db.flush()
                print(f"  Clear {bot.bot_name!r:20s} — removed {existing} existing record(s)")

            # Fetch all settled trades for this bot, ordered chronologically
            trades = (
                db.query(BinaryOption)
                .filter(
                    BinaryOption.bot_name == bot.bot_name,
                    BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
                )
                .order_by(BinaryOption.settlement_at.asc(), BinaryOption.id.asc())
                .all()
            )

            if not trades:
                print(f"  Skip  {bot.bot_name!r:20s} — no settled trades")
                continue

            # Group trades by settlement_at timestamp (one batch = one ledger row)
            batch_trades: dict[object, list[BinaryOption]] = defaultdict(list)
            batch_order: list[object] = []

            for t in trades:
                key = t.settlement_at
                if key not in batch_trades:
                    batch_order.append(key)
                batch_trades[key].append(t)

            # Chain balance forward
            initial = bot.initial_balance or 0
            ledger_prev = initial
            records = []

            for settled_at in batch_order:
                batch = batch_trades[settled_at]

                total_profit = round(sum(t.profit or 0 for t in batch), 8)
                total_fee = round(sum(t.entry_fee or 0 for t in batch), 8)
                delta = round(total_profit - total_fee, 8)
                new_bal = round(ledger_prev + delta, 8)

                wins = sum(1 for t in batch if t.result == BOResult.WIN)
                losses = sum(1 for t in batch if t.result == BOResult.LOSS)

                if delta > 0:
                    session_result = "WIN"
                elif delta < 0:
                    session_result = "LOSS"
                else:
                    session_result = "BREAKEVEN"

                record = BotSettlementLedger(
                    bot_name=bot.bot_name,
                    prev_balance=ledger_prev,
                    total_profit=total_profit,
                    total_fee=total_fee,
                    delta=delta,
                    new_balance=new_bal,
                    session_result=session_result,
                    trade_count=len(batch),
                    win_count=wins,
                    loss_count=losses,
                    trade_ids=[t.id for t in batch],
                    settled_at=settled_at,
                )
                records.append(record)
                ledger_prev = new_bal

            if not dry_run:
                db.bulk_save_objects(records)
                db.flush()

            total_created += len(records)
            final_bal = records[-1].new_balance if records else initial
            total_delta = round(final_bal - initial, 2)
            sign = "+" if total_delta >= 0 else ""

            print(
                f"  {'(dry) ' if dry_run else ''}Done  {bot.bot_name!r:20s} — "
                f"{len(records)} batch(es), "
                f"{len(trades)} trade(s), "
                f"init=${initial:,.2f} → final=${final_bal:,.2f} ({sign}{total_delta:,.2f})"
            )

        if not dry_run:
            db.commit()

        total = db.query(BotSettlementLedger).count()
        print(f"\nBackfill {'(dry-run) ' if dry_run else ''}complete — "
              f"{total_created} created, {total} total records in bot_settlement_ledger.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill bot_settlement_ledger from existing settled trades",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and re-create ledger for bots that already have records",
    )
    parser.add_argument(
        "--bot",
        type=str,
        default=None,
        help="Backfill only a specific bot (by name)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview backfill without writing to database",
    )
    args = parser.parse_args()

    try:
        backfill(force=args.force, bot_filter=args.bot, dry_run=args.dry_run)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
