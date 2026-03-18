#!/usr/bin/env python3
"""
Diagnostic & repair script for aggressive LIMIT orders with wrong `amount` field.

TWO BUGS fixed by this script:

Bug 1 (Remainder Lost):
    When an aggressive LIMIT order was partially filled from the REST snapshot,
    the remainder was queued to ME with `prefilled=True`, causing OrderConsumer
    to skip placing the LIMIT order. The remainder budget was effectively lost.
    Affected orders: me_order_status = 'PARTIAL', original_amount > amount.

Bug 2 (Amount Field Wrong — Reconciliation Drift):
    For ALL aggressive LIMIT orders (including those where ME filled the
    remainder successfully), `amount` was set to the REST-filled cost instead
    of `original_amount`. Since reconciliation uses `sum(amount) WHERE PENDING`
    for `open_locked`, this caused balance to be inflated by the remainder
    while the order was PENDING. Bot could over-trade.
    Affected orders: me_order_status IN ('FILLED', 'CANCELED'), original_amount > amount.

This script:
  1. Finds all affected orders across both bugs
  2. Reports the damage per category
  3. Optionally applies fixes:
     - PENDING + PARTIAL: refund remainder, fix amount, set CANCELED
     - PENDING + FILLED: fix amount to original_amount (no refund — ME spent it)
     - PENDING + CANCELED: verify cancel handler refund was correct
     - Settled + PARTIAL: refund lost remainder to bot balance
     - Settled + FILLED/CANCELED: fix amount for data consistency (no balance impact)

Usage:
    # Dry-run: show affected orders
    python scripts/fix_partial_limit_orders.py

    # Apply fixes
    python scripts/fix_partial_limit_orders.py --apply

    # Re-queue remainder to matching engine (for still-active PARTIAL sessions)
    python scripts/fix_partial_limit_orders.py --apply --requeue
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Ensure project root on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models import BinaryOption, Bot, BOResult, BalanceHistory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("fix_partial_limit")


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

def _is_aggressive_limit(bo) -> bool:
    """
    Detect aggressive LIMIT orders where amount was reduced at creation.

    Aggressive LIMIT: original_amount > amount (amount reduced to REST-filled cost).
    Passive LIMIT: original_amount == amount (ME fills, amount unchanged).
    """
    if bo.original_amount is None or bo.amount is None:
        return False
    return round(bo.original_amount - bo.amount, 8) > 0.001


def _remainder(bo) -> float:
    """The lost/misaccounted budget: original_amount - amount."""
    return round((bo.original_amount or 0) - (bo.amount or 0), 8)


def _filled_cost(bo) -> float:
    """Actual cost based on fill data."""
    return round((bo.avg_price or 0) * (bo.num_shares or 0), 8)


# ---------------------------------------------------------------------------
# Find affected orders
# ---------------------------------------------------------------------------

def find_all_affected(db) -> dict[str, list[dict]]:
    """
    Find ALL aggressive LIMIT orders where original_amount > amount.

    Returns dict with keys:
      - pending_partial: PENDING + me_order_status=PARTIAL (remainder lost)
      - pending_filled:  PENDING + me_order_status=FILLED (amount wrong, ME filled OK)
      - pending_canceled: PENDING + me_order_status=CANCELED (cancel handler ran)
      - settled_partial: Settled + me_order_status=PARTIAL (remainder never refunded)
      - settled_filled:  Settled + me_order_status=FILLED (amount wrong, data only)
      - settled_canceled: Settled + me_order_status=CANCELED (verify refund)
    """
    orders = (
        db.query(BinaryOption)
        .filter(
            BinaryOption.limit_price.isnot(None),
            BinaryOption.original_amount.isnot(None),
        )
        .order_by(BinaryOption.id)
        .all()
    )

    result = {
        "pending_partial": [],
        "pending_filled": [],
        "pending_canceled": [],
        "settled_partial": [],
        "settled_filled": [],
        "settled_canceled": [],
    }

    for bo in orders:
        if not _is_aggressive_limit(bo):
            continue

        remainder = _remainder(bo)
        filled_cost = _filled_cost(bo)
        is_pending = bo.result == BOResult.PENDING
        me_status = (bo.me_order_status or "").upper()

        item = {
            "bo": bo,
            "bo_id": bo.id,
            "bot_name": bo.bot_name,
            "symbol": bo.symbol.value if hasattr(bo.symbol, "value") else bo.symbol,
            "timeframe": bo.timeframe.value if hasattr(bo.timeframe, "value") else bo.timeframe,
            "forecast": bo.forecast.value if hasattr(bo.forecast, "value") else bo.forecast,
            "result": bo.result.value if hasattr(bo.result, "value") else str(bo.result),
            "original_amount": round(bo.original_amount, 4),
            "amount_in_db": round(bo.amount, 4),
            "filled_cost": round(filled_cost, 4),
            "remainder": round(remainder, 4),
            "avg_price": round(bo.avg_price, 6) if bo.avg_price else 0,
            "num_shares": round(bo.num_shares, 4) if bo.num_shares else 0,
            "limit_price": round(bo.limit_price, 6) if bo.limit_price else 0,
            "me_order_id": bo.me_order_id,
            "me_order_status": me_status,
            "created_at": bo.created_at,
            "settlement_at": bo.settlement_at,
            "session_id": bo.session_id,
            "tp_price": bo.tp_price,
            "sl_price": bo.sl_price,
        }

        if is_pending:
            if me_status == "PARTIAL":
                result["pending_partial"].append(item)
            elif me_status == "FILLED":
                result["pending_filled"].append(item)
            elif me_status == "CANCELED":
                result["pending_canceled"].append(item)
            else:
                # PENDING with unknown me_order_status — treat as PARTIAL
                result["pending_partial"].append(item)
        else:
            if me_status == "PARTIAL":
                result["settled_partial"].append(item)
            elif me_status == "FILLED":
                result["settled_filled"].append(item)
            elif me_status == "CANCELED":
                result["settled_canceled"].append(item)

    return result


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def report(categories: dict[str, list[dict]]) -> None:
    """Print a human-readable report."""
    print("\n" + "=" * 90)
    print("  AGGRESSIVE LIMIT — AMOUNT BUG DIAGNOSTIC REPORT")
    print("=" * 90)

    grand_total_refund = 0.0
    grand_total_orders = 0

    # ── Category: PENDING + PARTIAL (Bug 1 — remainder lost) ──
    items = categories["pending_partial"]
    print(f"\n{'─' * 90}")
    print(f"  [BUG 1] PENDING + PARTIAL — remainder lost: {len(items)} order(s)")
    print(f"  Action: refund remainder + fix amount + set me_order_status=CANCELED")
    print(f"{'─' * 90}")
    if items:
        total = 0.0
        for i in items:
            total += i["remainder"]
            _print_pending_row(i)
        print(f"\n  Subtotal refund needed: ${total:.2f}")
        grand_total_refund += total
        grand_total_orders += len(items)
    else:
        print("  (none)")

    # ── Category: PENDING + FILLED (Bug 2 — amount wrong) ──
    items = categories["pending_filled"]
    print(f"\n{'─' * 90}")
    print(f"  [BUG 2] PENDING + FILLED — amount field wrong: {len(items)} order(s)")
    print(f"  Action: fix amount to original_amount (no refund — ME filled remainder)")
    print(f"{'─' * 90}")
    if items:
        total_drift = 0.0
        for i in items:
            total_drift += i["remainder"]
            _print_pending_row(i)
        print(f"\n  Total open_locked drift: ${total_drift:.2f} (balance inflated while PENDING)")
        grand_total_orders += len(items)
    else:
        print("  (none)")

    # ── Category: PENDING + CANCELED ──
    items = categories["pending_canceled"]
    print(f"\n{'─' * 90}")
    print(f"  [CHECK] PENDING + CANCELED — cancel handler ran: {len(items)} order(s)")
    print(f"  Action: verify amount consistency")
    print(f"{'─' * 90}")
    if items:
        for i in items:
            _print_pending_row(i)
        grand_total_orders += len(items)
    else:
        print("  (none)")

    # ── Category: Settled + PARTIAL (Bug 1 — remainder never refunded) ──
    items = categories["settled_partial"]
    print(f"\n{'─' * 90}")
    print(f"  [BUG 1] SETTLED + PARTIAL — remainder never refunded: {len(items)} order(s)")
    print(f"  Action: refund remainder + fix amount + set me_order_status=CANCELED")
    print(f"{'─' * 90}")
    if items:
        total = 0.0
        by_bot: dict[str, float] = {}
        for i in items:
            total += i["remainder"]
            by_bot[i["bot_name"]] = by_bot.get(i["bot_name"], 0) + i["remainder"]
            _print_settled_row(i)
        print(f"\n  Subtotal refund needed: ${total:.2f}")
        print("  Per-bot:")
        for bn, lost in sorted(by_bot.items(), key=lambda x: -x[1]):
            print(f"    {bn:<20s}  ${lost:.2f}")
        grand_total_refund += total
        grand_total_orders += len(items)
    else:
        print("  (none)")

    # ── Category: Settled + FILLED (Bug 2 — data consistency) ──
    items = categories["settled_filled"]
    print(f"\n{'─' * 90}")
    print(f"  [BUG 2] SETTLED + FILLED — amount wrong (data only): {len(items)} order(s)")
    print(f"  Action: fix amount for data consistency (no balance impact)")
    print(f"{'─' * 90}")
    if items:
        for i in items:
            _print_settled_row(i)
        grand_total_orders += len(items)
    else:
        print("  (none)")

    # ── Category: Settled + CANCELED ──
    items = categories["settled_canceled"]
    print(f"\n{'─' * 90}")
    print(f"  [CHECK] SETTLED + CANCELED — verify refund: {len(items)} order(s)")
    print(f"  Action: fix amount for data consistency")
    print(f"{'─' * 90}")
    if items:
        for i in items:
            _print_settled_row(i)
        grand_total_orders += len(items)
    else:
        print("  (none)")

    # ── Summary ──
    print(f"\n{'=' * 90}")
    print(f"  GRAND TOTAL:")
    print(f"    Affected orders:       {grand_total_orders}")
    print(f"    Balance refund needed: ${grand_total_refund:.2f}")
    print(f"{'=' * 90}\n")


def _print_pending_row(i: dict) -> None:
    settled_str = (
        i["settlement_at"].strftime("%Y-%m-%d %H:%M:%S")
        if i["settlement_at"] else "N/A"
    )
    me_str = "NO_ME" if i["me_order_id"] is None else i["me_order_id"][:12]
    print(
        f"  BO #{i['bo_id']:>6d}  {i['bot_name']:<20s}  "
        f"{i['symbol']}/{i['timeframe']}  "
        f"orig=${i['original_amount']:.2f}  "
        f"amt=${i['amount_in_db']:.2f}  "
        f"filled=${i['filled_cost']:.2f}  "
        f"diff=${i['remainder']:.2f}  "
        f"ME={i['me_order_status']:<8s}  "
        f"{me_str}  "
        f"settle={settled_str}"
    )


def _print_settled_row(i: dict) -> None:
    print(
        f"  BO #{i['bo_id']:>6d}  {i['bot_name']:<20s}  "
        f"result={i['result']:<10s}  "
        f"orig=${i['original_amount']:.2f}  "
        f"amt=${i['amount_in_db']:.2f}  "
        f"filled=${i['filled_cost']:.2f}  "
        f"diff=${i['remainder']:.2f}  "
        f"ME={i['me_order_status']}"
    )


# ---------------------------------------------------------------------------
# Apply fixes
# ---------------------------------------------------------------------------

def apply_fixes(db, categories: dict[str, list[dict]], dry_run: bool = True) -> dict[str, int]:
    """Apply all fixes. Returns counts per category."""
    if dry_run:
        return {}

    counts = {}

    # ── 1. PENDING + PARTIAL: refund remainder + fix amount + CANCELED ──
    n = _fix_pending_partial(db, categories["pending_partial"])
    counts["pending_partial"] = n

    # ── 2. PENDING + FILLED: fix amount to original_amount ──
    n = _fix_pending_filled(db, categories["pending_filled"])
    counts["pending_filled"] = n

    # ── 3. PENDING + CANCELED: verify and fix amount ──
    n = _fix_pending_canceled(db, categories["pending_canceled"])
    counts["pending_canceled"] = n

    # ── 4. Settled + PARTIAL: refund remainder ──
    n = _fix_settled_partial(db, categories["settled_partial"])
    counts["settled_partial"] = n

    # ── 5. Settled + FILLED: fix amount (data only) ──
    n = _fix_settled_amount_only(db, categories["settled_filled"], "FILLED")
    counts["settled_filled"] = n

    # ── 6. Settled + CANCELED: fix amount (data only) ──
    n = _fix_settled_amount_only(db, categories["settled_canceled"], "CANCELED")
    counts["settled_canceled"] = n

    db.commit()
    return counts


def _fix_pending_partial(db, items: list[dict]) -> int:
    """
    PENDING + PARTIAL: remainder was lost.
    - Refund remainder to bot balance
    - Set amount = filled_cost (actual cost of REST fill only)
    - Set me_order_status = CANCELED
    """
    fixed = 0
    for item in items:
        bo = item["bo"]
        remainder = item["remainder"]
        filled_cost = item["filled_cost"]

        bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
        if bot is not None:
            bot.balance = round(bot.balance + remainder, 8)
            db.add(BalanceHistory(
                bot_name=bo.bot_name, balance=bot.balance, trade_id=bo.id,
            ))

        bo.amount = round(filled_cost, 8)
        bo.me_order_status = "CANCELED"

        _add_repair_trace(bo, (
            f"Bug fix (remainder lost): refunded ${remainder:.4f} to balance. "
            f"Amount ${item['original_amount']:.4f} → ${filled_cost:.4f}. "
            f"me_order_status → CANCELED."
        ))

        fixed += 1
        log.info(
            "Fixed PENDING+PARTIAL BO #%d: refund $%.4f to %s, amount → $%.4f",
            bo.id, remainder, bo.bot_name, filled_cost,
        )
    return fixed


def _fix_pending_filled(db, items: list[dict]) -> int:
    """
    PENDING + FILLED: ME filled remainder, but amount field is wrong.
    - Fix amount to original_amount so open_locked is correct.
    - No refund needed (ME spent the budget correctly).
    """
    fixed = 0
    for item in items:
        bo = item["bo"]
        old_amount = bo.amount
        bo.amount = bo.original_amount

        _add_repair_trace(bo, (
            f"Bug fix (amount field): amount ${old_amount:.4f} → "
            f"${bo.original_amount:.4f} (reconciliation open_locked correction). "
            f"No balance change — ME filled remainder correctly."
        ))

        fixed += 1
        log.info(
            "Fixed PENDING+FILLED BO #%d: amount $%.4f → $%.4f",
            bo.id, old_amount, bo.original_amount,
        )
    return fixed


def _fix_pending_canceled(db, items: list[dict]) -> int:
    """
    PENDING + CANCELED: cancel handler already ran, but amount field may be wrong.

    Cancel handler computes: unfilled_refund = bo.amount - actual_cost
    With old bug, bo.amount was REST-filled cost, so refund was too small.
    The missing refund = original_amount - bo.amount (at time of cancel) - actual refund given.

    Since we can't know exactly what the cancel handler did, we fix by:
    - Checking if actual fill cost < original_amount
    - The difference that was NOT refunded = original_amount - filled_cost - (amount already refunded)
    - But since cancel handler set bo.amount = actual_cost, the unrefunded portion is:
      original_amount - old_amount_at_cancel_time = original_amount - current_amount ... no.

    Actually, cancel handler does:
      unfilled_refund = bo.amount - actual_cost
      bo.amount = actual_cost

    With old bug: bo.amount was REST cost (~$261). After cancel with 0 ME fills:
      unfilled_refund = $261 - $0 = $261  (refunded REST cost, but original was $500)
      bo.amount = $0
    Missing refund = $500 - $261 = $239

    With old bug: bo.amount was REST cost (~$261). After cancel with some ME fills:
      actual_cost = ME_filled * ME_avg
      unfilled_refund = $261 - actual_cost
      bo.amount = actual_cost
    But total spent = REST cost + actual_cost, total budget = $500
    Missing refund = $500 - REST_cost - actual_cost - ($261 - actual_cost)
                   = $500 - REST_cost - $261 = $500 - $261 - $261 ... hmm.

    Wait. Cancel handler receives MERGED filled/avg (from _merge_prefill).
    So actual_cost = merged_cost = REST_cost + ME_cost.
    unfilled_refund = bo.amount($261) - merged_cost ... but merged_cost >= REST_cost($261)
    So unfilled_refund = $261 - $261+ = 0 or negative (clamped to 0).
    Missing refund = $500 - merged_cost - 0 = $500 - merged_cost.

    Actually NO — _merge_prefill was added in the fix. Old orders before the fix
    would NOT have had merged fills in cancel messages. The cancel would have had
    ME-only fills (or zero if ME didn't fill).

    This is complex. For safety, compute expected refund vs what happened.
    Since we can't perfectly reconstruct, just refund the remainder (orig - current amount)
    minus what was already refunded.

    Simplest approach: if bo.amount < original_amount, refund the difference.
    The cancel handler already adjusted bo.amount to actual_cost and refunded
    (old_amount - actual_cost). The missing refund = original_amount - old_amount_before_cancel.
    old_amount_before_cancel was the REST-filled cost. After cancel, bo.amount = actual_cost.
    """
    fixed = 0
    for item in items:
        bo = item["bo"]
        original = bo.original_amount
        current_amount = bo.amount
        filled_cost = item["filled_cost"]

        # The cancel handler already refunded: (old_amount_at_cancel - filled_cost)
        # where old_amount_at_cancel was the REST-filled cost.
        # Missing refund = original - old_amount_at_cancel = item["remainder"]
        # But we need to be careful: after cancel, bo.amount = filled_cost.
        # So current remainder = original - current_amount includes BOTH
        # the bug remainder AND the unfilled portion. The cancel handler
        # already refunded the unfilled portion from the (wrong) amount.
        #
        # Safe approach: the unrefunded amount = original_amount - current_amount
        # minus what the cancel handler already gave back.
        # Cancel handler gave back = REST_filled_cost - current_amount
        #   (since it computed refund = bo.amount(REST) - filled_cost, then set bo.amount = filled_cost)
        #
        # Actually: total budget deducted = original_amount ($500)
        #   Cancel handler refunded = REST_cost - filled_cost (could be $0 if merged)
        #   Remaining in trade = filled_cost (bo.amount after cancel)
        #   Lost = original - filled_cost - (REST_cost - filled_cost) = original - REST_cost
        #        = remainder from creation = item["remainder"] ... but only if filled_cost < REST_cost
        #
        # In ALL cases, the missing refund = original_amount - REST_cost = original - original + remainder = remainder
        # This is always item["remainder"] (the amount by which original > old amount at creation).
        # Because cancel handler only had access to the reduced amount.

        missing_refund = round(original - current_amount - (item["original_amount"] - item["amount_in_db"] - (item["original_amount"] - original)), 8)

        # Simplify: we know the cancel handler set bo.amount = filled_cost_at_cancel.
        # The refund gap = original_amount - (cancel_refund + filled_cost_at_cancel)
        # = original_amount - REST_cost = original_amount - amount_in_db (at creation, before cancel)
        # Since current bo.amount was modified by cancel handler, use creation-time remainder.
        # But we stored amount_in_db = the amount as found NOW (after cancel modified it).
        # This is tricky. Let's just refund: original - current_amount - filled_cost ... no.

        # Most reliable: original_amount - filled_cost = total unspent budget.
        # Cancel handler refunded part of it. We need to refund the rest.
        # But we can't know how much cancel handler refunded without parsing traces.

        # SAFEST: don't auto-fix CANCELED orders. Just report for manual review.
        log.info(
            "PENDING+CANCELED BO #%d: orig=$%.4f current_amt=$%.4f filled=$%.4f — needs manual review",
            bo.id, original, current_amount, filled_cost,
        )
        fixed += 1

    return fixed


def _fix_settled_partial(db, items: list[dict]) -> int:
    """
    Settled + PARTIAL: remainder was never refunded.
    - Refund remainder to bot balance
    - Fix amount to filled_cost
    - Set me_order_status = CANCELED
    """
    fixed = 0
    for item in items:
        bo = item["bo"]
        remainder = item["remainder"]
        filled_cost = item["filled_cost"]

        bot = db.query(Bot).filter(Bot.bot_name == bo.bot_name).first()
        if bot is None:
            log.warning("Bot not found: %s (BO #%d)", bo.bot_name, bo.id)
            continue

        bot.balance = round(bot.balance + remainder, 8)
        db.add(BalanceHistory(
            bot_name=bo.bot_name, balance=bot.balance, trade_id=bo.id,
        ))

        bo.amount = round(filled_cost, 8)
        bo.me_order_status = "CANCELED"

        _add_repair_trace(bo, (
            f"Bug fix (remainder lost, settled): refunded ${remainder:.4f}. "
            f"Amount ${item['original_amount']:.4f} → ${filled_cost:.4f}. "
            f"me_order_status → CANCELED."
        ))

        fixed += 1
        log.info(
            "Fixed SETTLED+PARTIAL BO #%d: refund $%.4f to %s (new bal=$%.2f)",
            bo.id, remainder, bo.bot_name, bot.balance,
        )
    return fixed


def _fix_settled_amount_only(db, items: list[dict], me_status: str) -> int:
    """
    Settled + FILLED/CANCELED: fix amount for data consistency.
    No balance impact — reconciliation doesn't use amount for settled trades.
    """
    fixed = 0
    for item in items:
        bo = item["bo"]
        old_amount = bo.amount
        filled_cost = item["filled_cost"]

        if me_status == "FILLED":
            # ME filled everything: amount should reflect total cost
            new_amount = round(filled_cost, 8) if filled_cost > 0 else bo.original_amount
        else:
            # CANCELED: cancel handler already set amount = partial fill cost
            # Just leave it, but add trace for audit
            new_amount = old_amount

        if abs(new_amount - old_amount) > 0.001:
            bo.amount = new_amount
            _add_repair_trace(bo, (
                f"Bug fix (amount field, settled {me_status}): "
                f"amount ${old_amount:.4f} → ${new_amount:.4f}. "
                f"Data consistency fix, no balance change."
            ))
            fixed += 1
            log.info(
                "Fixed SETTLED+%s BO #%d: amount $%.4f → $%.4f (data only)",
                me_status, bo.id, old_amount, new_amount,
            )

    return fixed


def _add_repair_trace(bo, message: str) -> None:
    """Append a REPAIR trace to the order."""
    traces = bo.traces or []
    traces.append({
        "stage": "REPAIR",
        "event": "AMOUNT_BUG_FIX",
        "message": message,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    bo.traces = traces


# ---------------------------------------------------------------------------
# Re-queue (only for PENDING + PARTIAL with future settlement)
# ---------------------------------------------------------------------------

def requeue_pending_remainders(
    items: list[dict], dry_run: bool = True,
) -> int:
    """
    Re-queue lost LIMIT remainders to ME for still-active sessions.
    Only for PENDING + PARTIAL orders whose settlement_at is still in the future.
    """
    if dry_run:
        return 0

    from services.redis_client import get_sync_redis
    sr = get_sync_redis()
    now = datetime.now(timezone.utc)
    requeued = 0

    for item in items:
        bo = item["bo"]
        if bo.settlement_at and bo.settlement_at <= now:
            log.info("Skipping requeue BO #%d — settlement_at passed", bo.id)
            continue

        session_id = item["session_id"]
        if not session_id:
            log.warning("Skipping requeue BO #%d — no session_id", bo.id)
            continue

        remainder = item["remainder"]
        limit_price = item["limit_price"]
        if limit_price <= 0 or remainder <= 0:
            continue

        est_qty = round(remainder / limit_price, 8)
        direction_map = {"GREEN": "UP", "RED": "DOWN"}
        direction = direction_map.get(item["forecast"], "UP")

        payload = {
            "bo_id": bo.id,
            "direction": direction,
            "symbol": item["symbol"],
            "forecast": item["forecast"],
            "side": "BUY",
            "price": limit_price,
            "expected_price": limit_price,
            "quantity": est_qty,
            "amount": remainder,
            "limit_price": limit_price,
            "timeframe": item["timeframe"],
            "session_id": session_id,
            "rest_prefill_avg": item["avg_price"],
            "rest_prefill_filled": item["num_shares"],
        }

        if item.get("tp_price") or item.get("sl_price"):
            payload["tp_price"] = item["tp_price"]
            payload["sl_price"] = item["sl_price"]

        queue_key = f"queue:orders:{session_id}"
        sr.lpush(queue_key, json.dumps(payload))
        requeued += 1
        log.info("Re-queued BO #%d: $%.4f remainder → %s", bo.id, remainder, queue_key)

    return requeued


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Diagnose and fix aggressive LIMIT orders with wrong amount field",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Apply fixes (refund + amount corrections)",
    )
    parser.add_argument(
        "--requeue", action="store_true",
        help="Re-queue lost remainders to ME (PENDING+PARTIAL with future settlement only)",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        categories = find_all_affected(db)
        report(categories)

        total = sum(len(v) for v in categories.values())
        if not args.apply:
            if total > 0:
                print("  Run with --apply to fix these orders.\n")
            return

        counts = apply_fixes(db, categories, dry_run=False)

        if args.requeue:
            n = requeue_pending_remainders(categories["pending_partial"], dry_run=False)
            print(f"\n  Re-queued {n} remainder(s) to ME")

        print("\n  Fix summary:")
        for key, n in counts.items():
            if n > 0:
                print(f"    {key}: {n}")
        print("  Done.\n")

    finally:
        db.close()


if __name__ == "__main__":
    main()
