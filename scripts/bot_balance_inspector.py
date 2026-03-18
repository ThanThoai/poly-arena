#!/usr/bin/env python3
"""Interactive bot balance history inspector.

Usage:
    python scripts/bot_balance_inspector.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone
from sqlalchemy import func as sa_func

from database import SessionLocal
from models import Bot, BalanceHistory, BinaryOption, BOResult
from models_futures import FuturesPosition, FuturesPositionStatus, FuturesOrder, FuturesOrderStatus


def get_bot_locked(db, bot_name: str) -> dict:
    """Return locked amounts breakdown for a bot."""
    bo_locked = (
        db.query(sa_func.coalesce(sa_func.sum(BinaryOption.amount), 0.0))
        .filter(BinaryOption.bot_name == bot_name, BinaryOption.result == BOResult.PENDING)
        .scalar()
    ) or 0.0

    fut_pos_margin = (
        db.query(sa_func.coalesce(sa_func.sum(FuturesPosition.margin), 0.0))
        .filter(FuturesPosition.bot_name == bot_name, FuturesPosition.status == FuturesPositionStatus.OPEN)
        .scalar()
    ) or 0.0

    fut_ord_margin = (
        db.query(sa_func.coalesce(
            sa_func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage), 0.0
        ))
        .filter(FuturesOrder.bot_name == bot_name, FuturesOrder.status == FuturesOrderStatus.PENDING)
        .scalar()
    ) or 0.0

    return {
        "bo_locked": float(bo_locked),
        "fut_pos_margin": float(fut_pos_margin),
        "fut_ord_margin": float(fut_ord_margin),
        "total_locked": float(bo_locked + fut_pos_margin + fut_ord_margin),
    }


def list_bots(db):
    """List all bots with summary info."""
    bots = db.query(Bot).order_by(Bot.id).all()
    if not bots:
        print("  No bots found.")
        return []

    print(f"\n  {'ID':<5} {'Bot Name':<25} {'Status':<10} {'Initial':>12} {'Cash':>12} {'Created'}")
    print(f"  {'─'*5} {'─'*25} {'─'*10} {'─'*12} {'─'*12} {'─'*20}")
    for b in bots:
        created = b.created_at.strftime("%Y-%m-%d %H:%M") if b.created_at else "N/A"
        status = b.status or ("ACTIVE" if b.is_active else "INACTIVE")
        print(f"  {b.id:<5} {b.bot_name:<25} {status:<10} {b.initial_balance or 0:>12,.2f} {b.balance or 0:>12,.2f} {created}")

    return bots


def show_bot_detail(db, bot: Bot):
    """Show detailed balance history for a bot."""
    locked = get_bot_locked(db, bot.bot_name)
    cash = float(bot.balance or 0)
    equity = cash + locked["total_locked"]

    print(f"\n  ╔══ {bot.bot_name} (ID: {bot.id}) ══")
    print(f"  ║ Initial Balance : ${bot.initial_balance or 0:>12,.2f}")
    print(f"  ║ Current Cash    : ${cash:>12,.2f}")
    print(f"  ║ BO Locked       : ${locked['bo_locked']:>12,.2f}")
    print(f"  ║ Fut Pos Margin  : ${locked['fut_pos_margin']:>12,.2f}")
    print(f"  ║ Fut Ord Margin  : ${locked['fut_ord_margin']:>12,.2f}")
    print(f"  ║ Total Locked    : ${locked['total_locked']:>12,.2f}")
    print(f"  ║ Current Equity  : ${equity:>12,.2f}")
    pnl = equity - (bot.initial_balance or 0)
    pnl_pct = (pnl / (bot.initial_balance or 1)) * 100
    sign = "+" if pnl >= 0 else ""
    print(f"  ║ P&L             : {sign}${pnl:>11,.2f} ({sign}{pnl_pct:.2f}%)")
    print(f"  ╚{'═' * 50}")

    # Balance history from DB
    records = (
        db.query(BalanceHistory)
        .filter(BalanceHistory.bot_name == bot.bot_name)
        .order_by(BalanceHistory.recorded_at.asc())
        .all()
    )

    # Build full timeline: seed + DB records + current
    seed_ts = bot.created_at
    seed_bal = float(bot.initial_balance or 0)
    now = datetime.now(timezone.utc)

    print(f"\n  Balance History ({len(records)} DB records + seed + current = {len(records) + 2} total):\n")
    print(f"  {'#':<5} {'Time (UTC)':<22} {'Balance':>14} {'Change':>12} {'Source':<10}")
    print(f"  {'─'*5} {'─'*22} {'─'*14} {'─'*12} {'─'*10}")

    prev_bal = None

    # Seed record
    ts_str = seed_ts.strftime("%Y-%m-%d %H:%M:%S") if seed_ts else "N/A"
    print(f"  {'S':<5} {ts_str:<22} ${seed_bal:>13,.2f} {'':>12} {'seed':<10}")
    prev_bal = seed_bal

    # DB records
    for i, r in enumerate(records, 1):
        ts_str = r.recorded_at.strftime("%Y-%m-%d %H:%M:%S") if r.recorded_at else "N/A"
        bal = float(r.balance)
        change = bal - prev_bal if prev_bal is not None else 0
        ch_sign = "+" if change >= 0 else ""
        ch_str = f"{ch_sign}{change:,.2f}" if abs(change) > 0.005 else "—"
        trade_info = f"t#{r.trade_id}" if r.trade_id else "snapshot"
        print(f"  {i:<5} {ts_str:<22} ${bal:>13,.2f} {ch_str:>12} {trade_info:<10}")
        prev_bal = bal

    # Current record
    change = equity - prev_bal if prev_bal is not None else 0
    ch_sign = "+" if change >= 0 else ""
    ch_str = f"{ch_sign}{change:,.2f}" if abs(change) > 0.005 else "—"
    ts_str = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"  {'C':<5} {ts_str:<22} ${equity:>13,.2f} {ch_str:>12} {'current':<10}")

    # Gap analysis
    print(f"\n  Gap Analysis:")
    all_times = []
    if seed_ts:
        all_times.append(("seed", seed_ts))
    for r in records:
        if r.recorded_at:
            all_times.append(("db", r.recorded_at))
    all_times.append(("current", now))

    max_gap = None
    max_gap_sec = 0
    for i in range(1, len(all_times)):
        gap = (all_times[i][1] - all_times[i - 1][1]).total_seconds()
        if gap > max_gap_sec:
            max_gap_sec = gap
            max_gap = (all_times[i - 1], all_times[i])

    if max_gap and max_gap_sec > 0:
        hours = max_gap_sec / 3600
        from_ts = max_gap[0][1].strftime("%m-%d %H:%M")
        to_ts = max_gap[1][1].strftime("%m-%d %H:%M")
        print(f"  Largest gap: {hours:.1f}h ({from_ts} → {to_ts})")

    if len(all_times) >= 2:
        total_span = (all_times[-1][1] - all_times[0][1]).total_seconds() / 3600
        print(f"  Total span : {total_span:.1f}h")
        if len(records) > 0:
            avg_interval = total_span / len(records)
            print(f"  Avg interval: {avg_interval:.1f}h ({avg_interval * 60:.0f}min)")


def main():
    db = SessionLocal()
    try:
        print("\n  ═══ Bot Balance Inspector ═══")

        while True:
            bots = list_bots(db)
            if not bots:
                break

            bot_map = {b.id: b for b in bots}
            bot_name_map = {b.bot_name.lower(): b for b in bots}

            print(f"\n  Enter bot ID or name (q to quit): ", end="")
            choice = input().strip()

            if choice.lower() in ("q", "quit", "exit"):
                break

            bot = None
            if choice.isdigit():
                bot = bot_map.get(int(choice))
            if not bot:
                bot = bot_name_map.get(choice.lower())

            if not bot:
                print(f"  Bot '{choice}' not found.")
                continue

            show_bot_detail(db, bot)
            print(f"\n  Press Enter to continue...", end="")
            input()

    except KeyboardInterrupt:
        print("\n")
    finally:
        db.close()


if __name__ == "__main__":
    main()
