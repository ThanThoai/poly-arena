#!/usr/bin/env python3
"""
Xoá 1 order của bot, rồi reconcile lại balance + backfill snapshot.

Usage:
    python scripts/delete_order.py --list-bots                          # list bots
    python scripts/delete_order.py <bot_name_or_id>                     # list orders
    python scripts/delete_order.py <bot_name_or_id> --delete 42         # dry-run xoá order #42
    python scripts/delete_order.py <bot_name_or_id> --delete 42 --apply # thực hiện xoá

Flow:
  1. List tất cả orders đã settle (WIN/LOSS) với profit
  2. User chọn --delete <order_id>
  3. Xoá order khỏi DB
  4. Reconcile lại bot.balance từ first principles
  5. Rebuild toàn bộ balance_history + settlement_ledger từ remaining orders
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy import func
from database import SessionLocal, engine, Base
from models import (
    BalanceHistory, BinaryOption, Bot, BotSettlementLedger, BOResult,
    User, UserBalanceHistory,
)


def list_bots(db) -> None:
    """List tất cả bots."""
    bots = db.query(Bot).order_by(Bot.id).all()
    if not bots:
        print("Không có bot nào.")
        return

    print(f"\n{'ID':>5}  {'Bot Name':<30}  {'Status':<8}  {'Balance':>12}  {'User'}")
    print("-" * 80)
    for b in bots:
        status = b.status if b.is_active else "DELETED"
        user = db.query(User).filter(User.id == b.user_id).first() if b.user_id else None
        username = user.username if user else "—"
        print(f"{b.id:>5}  {b.bot_name:<30}  {status:<8}  ${float(b.balance or 0):>10,.2f}  {username}")
    print(f"\nTổng: {len(bots)} bot(s)")


def find_bot(db, identifier: str) -> Bot | None:
    if identifier.isdigit():
        return db.query(Bot).filter(Bot.id == int(identifier)).first()
    return db.query(Bot).filter(Bot.bot_name == identifier).first()


def list_orders(db, bot: Bot) -> None:
    """List tất cả orders đã settle của bot."""
    orders = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot.bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS, BOResult.TIE]),
        )
        .order_by(BinaryOption.created_at.desc())
        .all()
    )

    if not orders:
        print("Không có order đã settle.")
        return

    print(f"\nBot: {bot.bot_name} (id={bot.id})  Balance: ${bot.balance:,.2f}")
    print(f"{'ID':>6}  {'Result':<6}  {'Profit':>12}  {'Amount':>10}  "
          f"{'Symbol':<5}  {'TF':<4}  {'Forecast':<5}  {'AvgPrice':>8}  {'Created'}")
    print("-" * 105)

    total_profit = Decimal("0")
    for o in orders:
        profit = o.profit or 0
        total_profit += Decimal(str(profit))
        created = o.created_at.strftime("%Y-%m-%d %H:%M") if o.created_at else "?"
        avg_price = f"{o.avg_price:.4f}" if o.avg_price else "N/A"
        print(
            f"{o.id:>6}  {o.result.value:<6}  "
            f"{'$':>1}{profit:>+11,.4f}  "
            f"${float(o.amount or 0):>9,.2f}  "
            f"{o.symbol.value:<5}  {o.timeframe.value:<4}  "
            f"{o.forecast.value:<5}  {avg_price:>8}  {created}"
        )

    print("-" * 105)
    print(f"Total settled: {len(orders)} orders   Net profit: ${total_profit:>+,.4f}")

    # Also show pending
    pending = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot.bot_name,
            BinaryOption.result == BOResult.PENDING,
        )
        .count()
    )
    cancelled = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot.bot_name,
            BinaryOption.result == BOResult.CANCELLED,
        )
        .count()
    )
    print(f"PENDING: {pending}   CANCELLED: {cancelled}")


def reconcile_balance(db, bot: Bot) -> dict:
    """
    Reconcile bot balance from first principles (same formula as settlement.py).
    Assumes caller has already flushed any pending deletes.
    """
    bot_name = bot.bot_name
    initial = float(bot.initial_balance or 10_000.0)

    # 1. Realized P&L: sum of profits from all settled trades
    realized_pnl = (
        db.query(func.coalesce(func.sum(BinaryOption.profit), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
        )
        .scalar()
    ) or 0.0

    # 2. Open locked (PENDING)
    open_locked = (
        db.query(func.coalesce(func.sum(BinaryOption.amount), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result == BOResult.PENDING,
        )
        .scalar()
    ) or 0.0

    # 3. Net fees from all non-CANCELLED orders
    net_fees = (
        db.query(func.coalesce(func.sum(BinaryOption.entry_fee), 0.0))
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result != BOResult.CANCELLED,
            BinaryOption.entry_fee.isnot(None),
        )
        .scalar()
    ) or 0.0

    # 4. Futures effect
    futures_cash_effect = 0.0
    try:
        from models_futures import FuturesPosition, FuturesOrder, FuturesPositionStatus, FuturesOrderStatus

        open_positions = (
            db.query(FuturesPosition)
            .filter(
                FuturesPosition.bot_name == bot_name,
                FuturesPosition.status == FuturesPositionStatus.OPEN,
            )
            .all()
        )
        fut_open_margin = sum(p.margin or 0 for p in open_positions)
        fut_open_fees = sum(p.entry_fee or 0 for p in open_positions)

        closed_positions = (
            db.query(FuturesPosition)
            .filter(
                FuturesPosition.bot_name == bot_name,
                FuturesPosition.status == FuturesPositionStatus.CLOSED,
            )
            .all()
        )
        fut_closed_net = 0.0
        for p in closed_positions:
            refund = max(0, (p.margin or 0) + (p.realized_pnl or 0))
            fut_closed_net += refund - (p.margin or 0) - (p.entry_fee or 0)

        fut_liq_loss = (
            db.query(func.coalesce(
                func.sum(FuturesPosition.margin + FuturesPosition.entry_fee), 0.0
            ))
            .filter(
                FuturesPosition.bot_name == bot_name,
                FuturesPosition.status == FuturesPositionStatus.LIQUIDATED,
            )
            .scalar()
        ) or 0.0

        fut_pending_margin = (
            db.query(func.coalesce(
                func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage),
                0.0,
            ))
            .filter(
                FuturesOrder.bot_name == bot_name,
                FuturesOrder.status == FuturesOrderStatus.PENDING,
            )
            .scalar()
        ) or 0.0

        futures_cash_effect = round(
            -(fut_open_margin + fut_open_fees)
            + fut_closed_net
            - fut_liq_loss
            - fut_pending_margin,
            8,
        )
    except ImportError:
        pass

    new_balance = round(initial + realized_pnl - open_locked - net_fees + futures_cash_effect, 8)
    equity = round(new_balance + open_locked, 8)

    return {
        "initial": initial,
        "realized_pnl": round(realized_pnl, 8),
        "open_locked": round(open_locked, 8),
        "net_fees": round(net_fees, 8),
        "futures_cash_effect": round(futures_cash_effect, 8),
        "new_balance": new_balance,
        "equity": equity,
    }


def rebuild_balance_history(db, bot: Bot) -> int:
    """
    Xoá toàn bộ balance_history cũ của bot rồi rebuild từ order history.
    Replay tất cả settled orders theo thời gian settlement, tính running balance.
    Returns số snapshot đã tạo.
    """
    bot_name = bot.bot_name
    initial = float(bot.initial_balance or 10_000.0)

    # Xoá balance history cũ
    deleted = db.query(BalanceHistory).filter(
        BalanceHistory.bot_name == bot_name
    ).delete(synchronize_session="fetch")

    # Lấy tất cả settled orders, sort theo thời gian settlement
    # Ưu tiên settlement_at > updated_at > created_at
    orders = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
        )
        .order_by(
            func.coalesce(
                BinaryOption.settlement_at,
                BinaryOption.updated_at,
                BinaryOption.created_at,
            ).asc(),
            BinaryOption.id.asc(),
        )
        .all()
    )

    # Seed initial snapshot
    db.add(BalanceHistory(
        bot_name=bot_name,
        balance=initial,
        recorded_at=orders[0].created_at if orders else datetime.now(timezone.utc),
    ))

    if not orders:
        return 1

    cumulative_pnl = 0.0
    cumulative_fees = 0.0
    count = 1  # initial snapshot

    for order in orders:
        cumulative_pnl += float(order.profit or 0)
        cumulative_fees += float(order.entry_fee or 0)
        balance = round(initial + cumulative_pnl - cumulative_fees, 8)
        ts = order.settlement_at or order.updated_at or order.created_at or datetime.now(timezone.utc)
        db.add(BalanceHistory(
            bot_name=bot_name,
            balance=balance,
            trade_id=order.id,
            recorded_at=ts,
        ))
        count += 1

    return count


def rebuild_settlement_ledger(db, bot: Bot) -> int:
    """
    Xoá và rebuild settlement ledger từ orders.
    Logic giống backfill_settlement_ledger.py: group by settlement_at timestamp.
    """
    bot_name = bot.bot_name
    initial = float(bot.initial_balance or 10_000.0)

    db.query(BotSettlementLedger).filter(
        BotSettlementLedger.bot_name == bot_name
    ).delete(synchronize_session="fetch")

    # Fetch settled trades, order by settlement_at (same as backfill script)
    trades = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.bot_name == bot_name,
            BinaryOption.result.in_([BOResult.WIN, BOResult.LOSS]),
        )
        .order_by(BinaryOption.settlement_at.asc(), BinaryOption.id.asc())
        .all()
    )

    if not trades:
        return 0

    # Group by settlement_at timestamp (one batch = one ledger row)
    # Preserve insertion order via batch_order list
    batch_trades: dict[object, list] = defaultdict(list)
    batch_order: list[object] = []

    for t in trades:
        key = t.settlement_at
        if key not in batch_trades:
            batch_order.append(key)
        batch_trades[key].append(t)

    # Chain balance forward
    prev_balance = initial
    count = 0

    for settled_at in batch_order:
        batch = batch_trades[settled_at]

        total_profit = round(sum(float(t.profit or 0) for t in batch), 8)
        total_fee = round(sum(float(t.entry_fee or 0) for t in batch), 8)
        delta = round(total_profit - total_fee, 8)
        new_balance = round(prev_balance + delta, 8)

        wins = sum(1 for t in batch if t.result == BOResult.WIN)
        losses = sum(1 for t in batch if t.result == BOResult.LOSS)

        if delta > 0:
            session_result = "WIN"
        elif delta < 0:
            session_result = "LOSS"
        else:
            session_result = "BREAKEVEN"

        db.add(BotSettlementLedger(
            bot_name=bot_name,
            prev_balance=prev_balance,
            total_profit=total_profit,
            total_fee=total_fee,
            delta=delta,
            new_balance=new_balance,
            session_result=session_result,
            trade_count=len(batch),
            win_count=wins,
            loss_count=losses,
            trade_ids=[t.id for t in batch],
            settled_at=settled_at,
        ))
        prev_balance = new_balance
        count += 1

    return count


def delete_order(bot_identifier: str, order_id: int, apply: bool = False) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        bot = find_bot(db, bot_identifier)
        if not bot:
            print(f"Bot '{bot_identifier}' không tồn tại.")
            return

        order = (
            db.query(BinaryOption)
            .filter(BinaryOption.id == order_id, BinaryOption.bot_name == bot.bot_name)
            .first()
        )
        if not order:
            print(f"Order #{order_id} không tồn tại hoặc không thuộc bot '{bot.bot_name}'.")
            return

        prefix = "[DRY-RUN] " if not apply else ""
        prev_balance = float(bot.balance or 0)

        # Show order details
        print(f"\n{prefix}Xoá order #{order.id} của bot '{bot.bot_name}':")
        print(f"  Symbol    : {order.symbol.value} {order.timeframe.value}")
        print(f"  Forecast  : {order.forecast.value}")
        print(f"  Result    : {order.result.value}")
        print(f"  Amount    : ${float(order.amount or 0):,.2f}")
        print(f"  Avg Price : {order.avg_price}")
        print(f"  Shares    : {order.num_shares}")
        print(f"  Profit    : ${float(order.profit or 0):+,.4f}")
        print(f"  Entry Fee : ${float(order.entry_fee or 0):,.4f}")
        print(f"  Session   : {order.session_id}")
        print(f"  Created   : {order.created_at}")

        print(f"\n{prefix}Balance trước khi xoá: ${prev_balance:,.4f}")

        # Delete order + flush để reconcile thấy DB đã xoá
        db.delete(order)
        db.flush()

        # Reconcile từ first principles (order đã bị xoá khỏi query results)
        recon = reconcile_balance(db, bot)

        print(f"\n{prefix}Balance sau khi xoá + reconcile:")
        print(f"  Initial       : ${recon['initial']:>12,.2f}")
        print(f"  Realized PnL  : ${recon['realized_pnl']:>+12,.4f}")
        print(f"  Open locked   : ${recon['open_locked']:>12,.4f}")
        print(f"  Net fees      : ${recon['net_fees']:>12,.4f}")
        print(f"  Futures effect: ${recon['futures_cash_effect']:>+12,.4f}")
        print(f"  New balance   : ${recon['new_balance']:>12,.4f}")
        print(f"  Equity        : ${recon['equity']:>12,.4f}")
        print(f"  Delta         : ${recon['new_balance'] - prev_balance:>+12,.4f}")

        if not apply:
            print(f"\nPass --apply để thực hiện xoá.")
            db.rollback()
            return

        # Apply new balance
        bot.balance = recon["new_balance"]

        # Rebuild balance history
        snapshot_count = rebuild_balance_history(db, bot)
        print(f"\n  Rebuilt {snapshot_count} balance history snapshots")

        # Rebuild settlement ledger
        ledger_count = rebuild_settlement_ledger(db, bot)
        print(f"  Rebuilt {ledger_count} settlement ledger records")

        # Clear user balance history (will be rebuilt by next scheduler snapshot)
        if bot.user_id:
            cleared = db.query(UserBalanceHistory).filter(
                UserBalanceHistory.user_id == bot.user_id
            ).delete(synchronize_session="fetch")
            print(f"  Cleared {cleared} user balance history records (user_id={bot.user_id})")

        # Auto-unpause if balance restored
        if bot.balance > 0 and bot.status == "PAUSED":
            bot.status = "ACTIVE"
            print(f"  Bot status: PAUSED -> ACTIVE")

        db.commit()
        print(f"\nDone. Order #{order_id} đã xoá. New balance: ${recon['new_balance']:,.4f}")

    except Exception as exc:
        db.rollback()
        print(f"\nError: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Xoá 1 order của bot, reconcile balance + rebuild snapshots",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-bots                                   # list tất cả bots
  %(prog)s trader-1-aggressive                          # list orders của bot
  %(prog)s trader-1-aggressive --delete 42              # dry-run xoá order #42
  %(prog)s trader-1-aggressive --delete 42 --apply      # thực hiện xoá
        """,
    )
    parser.add_argument("bot", nargs="?", default=None, help="Bot name hoặc ID")
    parser.add_argument("--list-bots", action="store_true", help="Liệt kê tất cả bots")
    parser.add_argument("--delete", type=int, metavar="ORDER_ID", help="Order ID cần xoá")
    parser.add_argument("--apply", action="store_true", help="Thực hiện (default: dry-run)")
    args = parser.parse_args()

    Base.metadata.create_all(bind=engine)

    if args.list_bots:
        db = SessionLocal()
        try:
            list_bots(db)
        finally:
            db.close()
        return

    if not args.bot:
        parser.print_help()
        return

    if args.delete:
        delete_order(args.bot, args.delete, apply=args.apply)
    else:
        db = SessionLocal()
        try:
            bot = find_bot(db, args.bot)
            if not bot:
                print(f"Bot '{args.bot}' không tồn tại.")
                sys.exit(1)
            list_orders(db, bot)
        finally:
            db.close()


if __name__ == "__main__":
    main()
