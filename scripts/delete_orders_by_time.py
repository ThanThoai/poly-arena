#!/usr/bin/env python3
"""
Delete orders in a time range, refund balances, and clean up related history.

Cleans up:
  1. binary_options in the time range
  2. balance_history entries referencing those orders (trade_id)
  3. bot_settlement_ledger entries referencing those orders (trade_ids JSON)
  4. user_balance_history entries referencing those orders (trade_id)

Then refunds each bot's balance (REST + WS pools) to reverse the effect of
the deleted orders.

Usage:
    # Dry-run (default): show what would be deleted
    python scripts/delete_orders_by_time.py --from "2026-03-26 17:00:00" --to "2026-03-26 18:30:00"

    # Filter by bot, symbol, or fill source
    python scripts/delete_orders_by_time.py --from "2026-03-26 17:00:00" --to "2026-03-26 18:30:00" --bot "Ace Jr"
    python scripts/delete_orders_by_time.py --from "2026-03-26 17:00:00" --to "2026-03-26 18:30:00" --symbol BTC
    python scripts/delete_orders_by_time.py --from "2026-03-26 17:00:00" --to "2026-03-26 18:30:00" --source WS

    # Apply changes
    python scripts/delete_orders_by_time.py --from "2026-03-26 17:00:00" --to "2026-03-26 18:30:00" --apply

All timestamps are interpreted as UTC.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone
from sqlalchemy import and_
from database import SessionLocal
from models import (
    BalanceHistory,
    BinaryOption,
    Bot,
    BotSettlementLedger,
    BOResult,
    User,
    UserBalanceHistory,
)


def parse_utc(s: str) -> datetime:
    """Parse a datetime string as UTC."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {s!r}  (expected YYYY-MM-DD HH:MM:SS UTC)")


def run(
    dt_from: datetime,
    dt_to: datetime,
    apply: bool = False,
    bot_filter: str | None = None,
    symbol_filter: str | None = None,
    fill_source_filter: str | None = None,
) -> None:
    db = SessionLocal()

    try:
        # ── 1. Find orders to delete ─────────────────────────────────────
        q = db.query(BinaryOption).filter(
            and_(
                BinaryOption.created_at >= dt_from,
                BinaryOption.created_at <= dt_to,
            )
        )
        if bot_filter:
            q = q.filter(BinaryOption.bot_name == bot_filter)
        if symbol_filter:
            q = q.filter(BinaryOption.symbol == symbol_filter.upper())
        if fill_source_filter:
            q = q.filter(BinaryOption.fill_source == fill_source_filter.upper())

        orders = q.order_by(BinaryOption.id).all()

        if not orders:
            print("No orders found in the specified range.")
            return

        # Collect order IDs and bot names
        order_ids: list[int] = []
        bot_names: set[str] = set()
        bot_refunds: dict[str, dict] = {}  # bot_name → {rest, ws, count}

        print(f"{'ID':>7}  {'Bot':25s}  {'Sym':4s}  {'TF':3s}  {'FC':5s}  {'Src':4s}  {'Result':8s}  {'Amount':>10s}  {'Fee':>8s}  {'Profit':>10s}  Created (UTC)")
        print("-" * 130)

        for bo in orders:
            order_ids.append(bo.id)
            bot_names.add(bo.bot_name)

            amount = bo.original_amount or bo.amount or 0
            fee = bo.entry_fee or 0
            profit = bo.profit or 0
            source = bo.fill_source or "?"
            result = bo.result.value if bo.result else "?"
            created = bo.created_at.strftime("%Y-%m-%d %H:%M:%S") if bo.created_at else "?"

            print(
                f"{bo.id:>7}  {bo.bot_name:25s}  {bo.symbol.value:4s}  {bo.timeframe.value:3s}  "
                f"{bo.forecast.value:5s}  {source:4s}  {result:8s}  "
                f"{amount:>10.4f}  {fee:>8.4f}  {profit:>10.4f}  {created}"
            )

            # Refund = amount + fee deducted at order time
            # For settled orders, also reverse the profit/loss already applied
            refund = amount + fee
            if bo.result in (BOResult.WIN, BOResult.LOSS) and profit != 0:
                refund -= profit

            entry = bot_refunds.setdefault(bo.bot_name, {"rest": 0.0, "ws": 0.0, "count": 0})
            entry["count"] += 1
            if source == "WS":
                entry["ws"] += refund
            else:
                entry["rest"] += refund

        order_ids_set = set(order_ids)

        # ── 2. Find related history to clean up ──────────────────────────

        # balance_history: trade_id references BO id
        bh_count = db.query(BalanceHistory).filter(
            BalanceHistory.trade_id.in_(order_ids)
        ).count()

        # user_balance_history: trade_id references BO id
        ubh_count = db.query(UserBalanceHistory).filter(
            UserBalanceHistory.trade_id.in_(order_ids)
        ).count()

        # bot_settlement_ledger: trade_ids JSON array contains BO ids
        # Must scan all ledger entries for affected bots and check JSON
        ledger_entries = db.query(BotSettlementLedger).filter(
            BotSettlementLedger.bot_name.in_(bot_names)
        ).all()

        # Ledger entries that reference ANY of our order IDs
        affected_ledger_ids: list[int] = []
        for le in ledger_entries:
            if le.trade_ids:
                if any(tid in order_ids_set for tid in le.trade_ids):
                    affected_ledger_ids.append(le.id)

        # ── 3. Summary ───────────────────────────────────────────────────
        print()
        print(f"Orders to delete:          {len(orders)}")
        print(f"BalanceHistory to delete:  {bh_count}")
        print(f"UserBalanceHistory to del: {ubh_count}")
        print(f"SettlementLedger to del:   {len(affected_ledger_ids)}")
        print()

        # Refund summary per bot
        print(f"{'Bot':25s}  {'Orders':>7s}  {'Refund REST':>12s}  {'Refund WS':>12s}  {'Total':>12s}")
        print("-" * 80)
        for bot_name, info in sorted(bot_refunds.items()):
            total = info["rest"] + info["ws"]
            print(
                f"{bot_name:25s}  {info['count']:>7d}  "
                f"{info['rest']:>12.4f}  {info['ws']:>12.4f}  {total:>12.4f}"
            )

        if not apply:
            print()
            print("[DRY RUN] No changes made. Add --apply to execute.")
            return

        # ── 4. Apply: delete history, delete orders, refund balances ─────
        print()
        print("Applying changes...")

        # Delete related history first (FK-safe order)
        del_bh = db.query(BalanceHistory).filter(
            BalanceHistory.trade_id.in_(order_ids)
        ).delete(synchronize_session="fetch")
        print(f"  Deleted {del_bh} balance_history row(s)")

        del_ubh = db.query(UserBalanceHistory).filter(
            UserBalanceHistory.trade_id.in_(order_ids)
        ).delete(synchronize_session="fetch")
        print(f"  Deleted {del_ubh} user_balance_history row(s)")

        if affected_ledger_ids:
            del_sl = db.query(BotSettlementLedger).filter(
                BotSettlementLedger.id.in_(affected_ledger_ids)
            ).delete(synchronize_session="fetch")
            print(f"  Deleted {del_sl} bot_settlement_ledger row(s)")

        # Delete orders
        del_bo = q.delete(synchronize_session="fetch")
        print(f"  Deleted {del_bo} binary_options row(s)")

        # Refund bot balances
        for bot_name, info in bot_refunds.items():
            bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
            if not bot:
                print(f"  WARNING: Bot {bot_name!r} not found — skipping refund")
                continue

            old_bal = bot.balance or 0
            old_rest = bot.balance_rest
            old_ws = bot.balance_ws

            if info["rest"] != 0 and old_rest is not None:
                bot.balance_rest = round(old_rest + info["rest"], 8)
            elif info["rest"] != 0:
                bot.balance = round(old_bal + info["rest"], 8)

            if info["ws"] != 0 and old_ws is not None:
                bot.balance_ws = round(old_ws + info["ws"], 8)

            # Keep balance synced with balance_rest
            if bot.balance_rest is not None:
                bot.balance = bot.balance_rest

            print(
                f"  {bot_name}: balance {old_bal:.4f} → {bot.balance:.4f}"
                f"  (rest: {old_rest} → {bot.balance_rest},"
                f" ws: {old_ws} → {bot.balance_ws})"
            )

        db.commit()
        print(f"\nDone. Deleted {del_bo} order(s), cleaned up history, refunded {len(bot_refunds)} bot(s).")

    except Exception as exc:
        db.rollback()
        print(f"ERROR: {exc}")
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Delete orders in a UTC time range, refund bot balances, and clean up related history.",
    )
    parser.add_argument("--from", dest="dt_from", required=True, help="Start time UTC (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--to", dest="dt_to", required=True, help="End time UTC (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--bot", dest="bot", default=None, help="Filter by bot_name")
    parser.add_argument("--symbol", dest="symbol", default=None, help="Filter by symbol (BTC, ETH, ...)")
    parser.add_argument("--source", dest="source", default=None, help="Filter by fill_source (REST, WS)")
    parser.add_argument("--apply", action="store_true", help="Actually delete and refund (default: dry-run)")
    args = parser.parse_args()

    dt_from = parse_utc(args.dt_from)
    dt_to = parse_utc(args.dt_to)

    print(f"Time range: {dt_from.isoformat()} → {dt_to.isoformat()} (UTC)")
    print(f"Filters: bot={args.bot or 'all'}, symbol={args.symbol or 'all'}, source={args.source or 'all'}")
    print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}")
    print()

    run(
        dt_from=dt_from,
        dt_to=dt_to,
        apply=args.apply,
        bot_filter=args.bot,
        symbol_filter=args.symbol,
        fill_source_filter=args.source,
    )


if __name__ == "__main__":
    main()
