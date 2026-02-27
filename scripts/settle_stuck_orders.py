#!/usr/bin/env python3
"""
Settle stuck orders — force-settle PENDING orders whose settlement_at has passed.

Handles all edge cases:
  - me_order_status stuck at "PENDING" (WS Feed Service not running)
  - settlement_at is NULL
  - Binance candle data unavailable (retry with backoff)

Usage:
    # Dry run (default) — preview what would be settled
    python scripts/settle_stuck_orders.py

    # Actually settle
    python scripts/settle_stuck_orders.py --execute

    # Only settle orders older than 1 hour
    python scripts/settle_stuck_orders.py --execute --min-age 3600

    # Only settle specific bot
    python scripts/settle_stuck_orders.py --execute --bot mybot

    # Cancel unfilled orders (me_order_status=PENDING, no fill)
    python scripts/settle_stuck_orders.py --execute --cancel-unfilled
"""

import argparse
import logging
import os
import sys
import time

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from sqlalchemy import inspect, text

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import BalanceHistory, BinaryOption, Bot, BOResult
from services.settlement import fetch_binance_candle

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("settle-stuck")

# Columns that may be missing from older DBs — add them if needed
_MIGRATIONS = [
    ("limit_price",     "REAL"),
    ("tp_price",        "REAL"),
    ("sl_price",        "REAL"),
    ("exit_price",      "REAL"),
    ("exit_trigger",    "VARCHAR(20)"),
    ("exit_filled",     "REAL"),
    ("exit_at",         "DATETIME"),
    ("me_order_id",     "VARCHAR(64)"),
    ("me_order_status", "VARCHAR(20)"),
    ("ttl",             "INTEGER"),
]


def _make_engine(db_url: str):
    """Create engine + session factory for the given DB URL."""
    connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
    eng = create_engine(db_url, connect_args=connect_args)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=eng)
    return eng, session_factory


def _ensure_schema(eng):
    """Create tables and add any missing columns."""
    from database import Base
    Base.metadata.create_all(bind=eng)
    insp = inspect(eng)
    if "binary_options" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("binary_options")}
    with eng.begin() as conn:
        for col, dtype in _MIGRATIONS:
            if col not in existing:
                log.info("Migration: adding binary_options.%s", col)
                conn.execute(text(f"ALTER TABLE binary_options ADD COLUMN {col} {dtype}"))
                existing.add(col)


def _aware(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is timezone-aware (SQLite returns naive datetimes)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def get_stuck_orders(db, min_age_s: float, bot_name: str | None):
    """Query all PENDING orders whose settlement_at has passed."""
    now = datetime.now(timezone.utc)
    q = (
        db.query(BinaryOption)
        .filter(BinaryOption.result == BOResult.PENDING)
    )
    if bot_name:
        q = q.filter(BinaryOption.bot_name == bot_name)

    orders = q.order_by(BinaryOption.id).all()

    stuck = []
    for bo in orders:
        settle_at = _aware(bo.settlement_at)
        created_at = _aware(bo.created_at)

        if settle_at is None:
            # No settlement time — use created_at as reference
            age = (now - created_at).total_seconds() if created_at else 999999
            if age >= min_age_s:
                stuck.append(bo)
            continue

        # settlement_at is set — must be in the past
        age = (now - settle_at).total_seconds()
        if age >= min_age_s:
            stuck.append(bo)

    return stuck


def print_summary(orders):
    """Print a summary table of stuck orders."""
    if not orders:
        log.info("No stuck orders found.")
        return

    now = datetime.now(timezone.utc)
    log.info("Found %d stuck order(s):\n", len(orders))

    header = f"{'ID':>6}  {'Bot':<16}  {'Symbol':<5}  {'TF':<4}  {'Forecast':<8}  "
    header += f"{'Amount':>10}  {'AvgPrice':>9}  {'Shares':>10}  {'ME Status':<10}  "
    header += f"{'Exit':>5}  {'Settle At':<20}  {'Age':>10}"
    print(header)
    print("-" * len(header))

    for bo in orders:
        settle_at = _aware(bo.settlement_at)
        created_at = _aware(bo.created_at)
        settle_str = settle_at.strftime("%Y-%m-%d %H:%M") if settle_at else "NULL"
        if settle_at:
            age_s = (now - settle_at).total_seconds()
        elif created_at:
            age_s = (now - created_at).total_seconds()
        else:
            age_s = 0

        # Format age human-readable
        if age_s < 3600:
            age_str = f"{age_s / 60:.0f}m"
        elif age_s < 86400:
            age_str = f"{age_s / 3600:.1f}h"
        else:
            age_str = f"{age_s / 86400:.1f}d"

        sym = bo.symbol.value if hasattr(bo.symbol, "value") else bo.symbol
        tf = bo.timeframe.value if hasattr(bo.timeframe, "value") else bo.timeframe
        fc = bo.forecast.value if hasattr(bo.forecast, "value") else bo.forecast

        print(
            f"#{bo.id:>5}  {bo.bot_name:<16}  {sym:<5}  {tf:<4}  {fc:<8}  "
            f"${bo.amount:>9.2f}  {bo.avg_price or 0:>9.4f}  "
            f"{bo.num_shares or 0:>10.4f}  {bo.me_order_status or 'NULL':<10}  "
            f"{bo.exit_trigger or '—':>5}  {settle_str:<20}  {age_str:>10}"
        )
    print()


def settle_order(db, bo, dry_run: bool) -> str:
    """
    Settle a single stuck order. Returns action taken as string.
    Uses the same logic as services/settlement.py but without the
    me_order_status gate.
    """
    if bo.settlement_at is None:
        return "SKIP (no settlement_at)"

    sym = bo.symbol.value if hasattr(bo.symbol, "value") else bo.symbol
    tf = bo.timeframe.value if hasattr(bo.timeframe, "value") else bo.timeframe
    fc = bo.forecast.value if hasattr(bo.forecast, "value") else bo.forecast

    # Fetch Binance candle with retry
    candle = None
    for attempt in range(3):
        candle = fetch_binance_candle(sym, tf, bo.settlement_at)
        if candle is not None:
            break
        if attempt < 2:
            time.sleep(1)

    if candle is None:
        return "SKIP (no candle data)"

    open_price, close_price = candle

    # Candle direction
    if close_price > open_price:
        candle_dir = "GREEN"
    elif close_price < open_price:
        candle_dir = "RED"
    else:
        candle_dir = "GREEN"

    # Profit formula — reuse settlement logic
    if (
        bo.exit_trigger in ("TP", "SL")
        and bo.exit_price is not None
        and bo.exit_filled is not None
    ):
        # Shadow tracking (bracket exit occurred)
        shadow_profit = round((bo.exit_price - bo.avg_price) * bo.exit_filled, 8)
        remainder = 0.0
        if bo.num_shares is not None and bo.exit_filled < bo.num_shares:
            remainder_shares = bo.num_shares - bo.exit_filled
            binary_dir = BOResult.WIN if candle_dir == fc else BOResult.LOSS
            if binary_dir == BOResult.WIN:
                remainder = round((1 - bo.avg_price) * remainder_shares, 8)
            else:
                remainder = round(-bo.avg_price * remainder_shares, 8)
        profit = shadow_profit + remainder
        result = BOResult.WIN if profit >= 0 else BOResult.LOSS
        method = f"shadow ({bo.exit_trigger})"
    else:
        # Binary formula
        result = BOResult.WIN if candle_dir == fc else BOResult.LOSS
        if result == BOResult.WIN:
            if bo.avg_price is not None and bo.num_shares is not None:
                profit = round((1 - bo.avg_price) * bo.num_shares, 8)
            else:
                profit = round(bo.amount * 1.0, 8)
        else:
            if bo.avg_price is not None and bo.num_shares is not None:
                profit = round(-bo.avg_price * bo.num_shares, 8)
            else:
                profit = -bo.amount
        method = "binary"

    action = (
        f"{result.value} profit={profit:+.4f} "
        f"candle={candle_dir} ({method}) "
        f"open={open_price:.2f} close={close_price:.2f}"
    )

    if dry_run:
        return f"[DRY RUN] → {action}"

    # Apply settlement
    bo.result = result
    bo.profit = profit
    bo.price_open = open_price
    bo.price_close = close_price

    # Update bot balance
    # Amount was deducted upfront at order creation → return cost + profit
    bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
    if bot:
        payout = round(bo.amount + profit, 8)
        bot.balance = round(bot.balance + payout, 8)
        db.add(
            BalanceHistory(
                bot_name=bo.bot_name,
                balance=bot.balance,
                trade_id=bo.id,
            )
        )

    return f"SETTLED → {action}"


def cancel_unfilled_order(_db, bo, dry_run: bool) -> str:
    """
    Cancel an unfilled order (me_order_status=PENDING, never matched).
    Returns action taken as string.
    """
    if bo.me_order_status != "PENDING":
        return "SKIP (already processed by ME)"

    action = "CANCELLED (unfilled, me_order_status=PENDING)"

    if dry_run:
        return f"[DRY RUN] → {action}"

    bo.result = BOResult.CANCELLED
    bo.profit = 0.0
    bo.me_order_status = "CANCELED"

    return action


def main():
    parser = argparse.ArgumentParser(
        description="Settle stuck PENDING orders",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually settle orders (default is dry run)",
    )
    parser.add_argument(
        "--min-age",
        type=float,
        default=0,
        help="Only settle orders older than N seconds past settlement_at (default: 0)",
    )
    parser.add_argument(
        "--bot",
        type=str,
        default=None,
        help="Only settle orders for a specific bot",
    )
    parser.add_argument(
        "--cancel-unfilled",
        action="store_true",
        help="Cancel (instead of settle) orders with me_order_status=PENDING that were never filled",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="SQLite database path (e.g. test_orders.db). Overrides DATABASE_URL env var.",
    )
    args = parser.parse_args()

    # Resolve database URL
    if args.db:
        db_url = f"sqlite:///./{args.db}"
    else:
        db_url = os.getenv("DATABASE_URL", "sqlite:///./orders.db")
    log.info("Using database: %s", db_url)

    eng, SessionFactory = _make_engine(db_url)
    _ensure_schema(eng)

    dry_run = not args.execute
    if dry_run:
        log.info("=== DRY RUN MODE === (use --execute to apply changes)")
    else:
        log.warning("=== EXECUTE MODE === changes will be committed to DB")

    db = SessionFactory()
    try:
        orders = get_stuck_orders(db, args.min_age, args.bot)
        print_summary(orders)

        if not orders:
            return

        settled = 0
        cancelled = 0
        skipped = 0

        for bo in orders:
            if args.cancel_unfilled and bo.me_order_status == "PENDING":
                action = cancel_unfilled_order(db, bo, dry_run)
                log.info("  #%d %s %s → %s", bo.id, bo.bot_name, bo.symbol, action)
                if "CANCELLED" in action:
                    cancelled += 1
                else:
                    skipped += 1
            else:
                action = settle_order(db, bo, dry_run)
                log.info("  #%d %s %s → %s", bo.id, bo.bot_name, bo.symbol, action)
                if "SETTLED" in action or "DRY RUN" in action:
                    settled += 1
                else:
                    skipped += 1

        if not dry_run:
            db.commit()
            log.info("DB committed.")

        print()
        log.info(
            "Summary: %d settled, %d cancelled, %d skipped (total: %d)",
            settled, cancelled, skipped, len(orders),
        )

    except Exception as exc:
        log.error("Error: %s", exc, exc_info=True)
        db.rollback()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
