#!/usr/bin/env python3
"""
Backfill balance_history from existing trade data (BO + Futures).

For each bot:
  1. Insert a record at initial_balance (using bot.created_at as timestamp).
  2. Collect all equity-changing events (BO settlements, futures closes) in
     chronological order.
  3. For each event, compute equity = cash + all locked margins.

Skips bots that already have history (safe to re-run).

Usage:
    python scripts/backfill_balance_history.py
    python scripts/backfill_balance_history.py --force   # overwrite existing history
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Base, SessionLocal, engine
from models import BalanceHistory, BinaryOption, BOResult, Bot
from models_futures import (
    FuturesPosition, FuturesPositionStatus,
    FuturesOrder, FuturesOrderStatus,
)
from sqlalchemy import func


def _calc_equity_at(db, bot_name: str, initial: float, at) -> float:
    """Calculate bot equity at a given timestamp using the same logic as settlement."""
    # BO realized P&L
    bo_realized = (
        db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
            BinaryOption.settlement_at <= at,
        )
        .scalar()
    ) or 0.0

    # BO fees
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

    # BO open locked at T (PENDING trades that exist at time T)
    bo_open_locked = (
        db.query(func.coalesce(func.sum(BinaryOption.amount), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result == BOResult.PENDING,
            BinaryOption.created_at <= at,
        )
        .scalar()
    ) or 0.0

    # Futures OPEN at T
    open_positions = (
        db.query(FuturesPosition)
        .filter(
            FuturesPosition.bot_name == bot_name,
            FuturesPosition.status == FuturesPositionStatus.OPEN,
            FuturesPosition.created_at <= at,
        )
        .all()
    )
    fut_open_margin = sum(p.margin or 0 for p in open_positions)
    fut_open_fees = sum(p.entry_fee or 0 for p in open_positions)

    # Futures CLOSED at T
    closed_positions = (
        db.query(FuturesPosition)
        .filter(
            FuturesPosition.bot_name == bot_name,
            FuturesPosition.status == FuturesPositionStatus.CLOSED,
            FuturesPosition.closed_at <= at,
        )
        .all()
    )
    fut_closed_net = 0.0
    for p in closed_positions:
        refund = max(0, (p.margin or 0) + (p.realized_pnl or 0))
        fut_closed_net += refund - (p.margin or 0) - (p.entry_fee or 0)

    # Futures LIQUIDATED at T
    fut_liq_loss = (
        db.query(func.coalesce(
            func.sum(FuturesPosition.margin + FuturesPosition.entry_fee), 0.0
        ))
        .filter(
            FuturesPosition.bot_name == bot_name,
            FuturesPosition.status == FuturesPositionStatus.LIQUIDATED,
            FuturesPosition.closed_at <= at,
        )
        .scalar()
    ) or 0.0

    # Futures PENDING orders at T
    fut_pending_margin = (
        db.query(func.coalesce(
            func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage),
            0.0,
        ))
        .filter(
            FuturesOrder.bot_name == bot_name,
            FuturesOrder.status == FuturesOrderStatus.PENDING,
            FuturesOrder.created_at <= at,
        )
        .scalar()
    ) or 0.0

    futures_cash_effect = (
        -(fut_open_margin + fut_open_fees)
        + fut_closed_net
        - fut_liq_loss
        - fut_pending_margin
    )

    cash = initial + bo_realized - bo_open_locked - bo_fees + futures_cash_effect
    equity = cash + bo_open_locked + fut_open_margin + fut_pending_margin

    return round(equity, 8)


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

            initial = bot.initial_balance or 0

            # Collect all equity-changing events with timestamps
            # BO settlements
            bo_events = (
                db.query(BinaryOption.settlement_at)
                .filter(
                    BinaryOption.bot_name == bot.bot_name,
                    BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
                    BinaryOption.settlement_at.isnot(None),
                )
                .all()
            )
            # Futures closes
            fut_events = (
                db.query(FuturesPosition.closed_at)
                .filter(
                    FuturesPosition.bot_name == bot.bot_name,
                    FuturesPosition.status.in_([
                        FuturesPositionStatus.CLOSED,
                        FuturesPositionStatus.LIQUIDATED,
                    ]),
                    FuturesPosition.closed_at.isnot(None),
                )
                .all()
            )

            # Combine and sort unique timestamps
            timestamps = sorted(set(
                [row[0] for row in bo_events if row[0]] +
                [row[0] for row in fut_events if row[0]]
            ))

            records = []

            # Initial balance snapshot
            records.append(BalanceHistory(
                bot_name=bot.bot_name,
                balance=initial,
                trade_id=None,
                recorded_at=bot.created_at,
            ))

            # One snapshot per event timestamp
            for ts in timestamps:
                equity = _calc_equity_at(db, bot.bot_name, initial, ts)
                records.append(BalanceHistory(
                    bot_name=bot.bot_name,
                    balance=equity,
                    trade_id=None,
                    recorded_at=ts,
                ))

            db.bulk_save_objects(records)
            db.flush()

            final = records[-1].balance if records else initial
            print(
                f"  Done  {bot.bot_name!r:20s} — {len(records)} records "
                f"(final equity: {final:,.2f})"
            )

        db.commit()
        total = db.query(BalanceHistory).count()
        print(f"\nBackfill complete — {total} total records in balance_history.")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill balance_history table (BO + Futures)")
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
