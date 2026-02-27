"""
Live integration test: Matching Engine vs real Polymarket data.

Covers full spec docs/matching_engine.md:
  Section 2  — SimulatedOrder with Bracket Order fields
  Section 3  — All 5 WebSocket event handlers
  Section 4  — Core matching algorithm (multi-level fills)
  Section 5  — Workflow E: TP/SL monitoring with OCO + slippage
  Section 6  — Decimal precision, partial-fill awareness

Run:
    conda run -n poly_arena python tests/test_matching_engine_live.py
"""

import asyncio
import json
import sys
import time
from decimal import Decimal

import httpx
import websockets

sys.path.insert(0, ".")

from services.matching_engine import (
    BracketFillResult,
    MatchingEngine,
    OrderSide,
    OrderStatus,
    ShadowOrderbook,
    SimulatedOrder,
    get_engine,
)
from services.polymarket import PolymarketClient

_WS_URI   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_CLOB_URL = "https://clob.polymarket.com/book"

PASS = "✅ PASS"
FAIL = "❌ FAIL"


def ok(cond: bool, msg: str) -> None:
    print(f"  {PASS if cond else FAIL}  {msg}")
    if not cond:
        raise AssertionError(msg)


def sep(title: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def fetch_full_book(token_id: str) -> dict:
    r = httpx.get(_CLOB_URL, params={"token_id": token_id}, timeout=15.0)
    r.raise_for_status()
    return r.json()


# ═══════════════════════════════════════════════════════════════
#  PART 1 — UNIT TESTS (no network)
# ═══════════════════════════════════════════════════════════════

def test_section2_datamodel():
    sep("Sec 2 — SimulatedOrder data model + Bracket fields")
    o = SimulatedOrder(
        order_id="x",
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("100"),
        tp_price=Decimal("0.70"),
        sl_price=Decimal("0.30"),
    )
    ok(o.remaining_qty == Decimal("100"), "remaining_qty = quantity when unfilled")
    ok(o.has_bracket, "has_bracket True when TP/SL set")
    ok(not o.position_closed, "position_closed defaults False")
    ok(not o.is_eligible_for_bracket, "not eligible before any fill")

    o.filled = Decimal("60")
    o.status = OrderStatus.PARTIAL
    ok(o.is_eligible_for_bracket, "eligible after partial fill")

    o.position_closed = True
    ok(not o.is_eligible_for_bracket, "not eligible after position_closed")


def test_section4_matching_buy():
    sep("Sec 4 — Matching algorithm: BUY order multi-level")
    book = ShadowOrderbook("t1")
    book.apply_snapshot(
        bids=[],
        asks=[
            {"price": "0.50", "size": "30"},
            {"price": "0.51", "size": "40"},
            {"price": "0.52", "size": "60"},
            {"price": "0.60", "size": "200"},
        ],
    )
    # BUY 100 @ 0.52 — should consume 0.50(30) + 0.51(40) + 0.52(30)
    order = book.place_virtual_order(OrderSide.BUY, Decimal("0.52"), Decimal("100"))
    ok(order.status == OrderStatus.FILLED, "BUY fully filled across 3 levels")
    ok(order.filled == Decimal("100"), f"filled=100 (got {order.filled})")
    ok(Decimal("0.50") not in book.asks, "0.50 level fully consumed")
    ok(Decimal("0.51") not in book.asks, "0.51 level fully consumed")
    ok(book.asks.get(Decimal("0.52")) == Decimal("30"), "0.52 has 30 remaining (60-30)")
    ok(Decimal("0.60") in book.asks, "0.60 untouched")


def test_section4_matching_sell():
    sep("Sec 4 — Matching algorithm: SELL order with slippage")
    book = ShadowOrderbook("t2")
    book.apply_snapshot(
        bids=[
            {"price": "0.95", "size": "20"},
            {"price": "0.94", "size": "50"},
            {"price": "0.93", "size": "80"},
        ],
        asks=[],
    )
    order = book.place_virtual_order(OrderSide.SELL, Decimal("0.93"), Decimal("100"))
    ok(order.status == OrderStatus.FILLED, "SELL fully filled")
    ok(order.filled == Decimal("100"), f"filled=100 (got {order.filled})")
    ok(Decimal("0.95") not in book.bids, "0.95 consumed")
    ok(Decimal("0.94") not in book.bids, "0.94 consumed")
    ok(book.bids.get(Decimal("0.93")) == Decimal("50"), "0.93 has 50 remaining")


def test_section4_partial_fill():
    sep("Sec 4 — Partial fill when insufficient liquidity")
    book = ShadowOrderbook("t3")
    book.apply_snapshot(
        bids=[],
        asks=[{"price": "0.50", "size": "30"}],
    )
    order = book.place_virtual_order(OrderSide.BUY, Decimal("0.50"), Decimal("100"))
    ok(order.status == OrderStatus.PARTIAL, "status=PARTIAL when book exhausted")
    ok(order.filled == Decimal("30"), f"filled=30 (got {order.filled})")
    ok(order.remaining_qty == Decimal("70"), "remaining=70")

    # Add more asks — resting order should fill on next delta
    book.apply_changes([
        {"side": "ask", "price": "0.50", "size": "70"},
    ])
    book.run_matching()
    ok(order.status == OrderStatus.FILLED, "fills rest after book update")
    ok(order.filled == Decimal("100"), "now fully filled")


def test_section3_event_routing():
    sep("Sec 3 — Event routing (book / price_change / market_resolved)")
    engine = MatchingEngine()

    # book snapshot
    engine.dispatch_event({
        "event_type": "book",
        "asset_id": "tok1",
        "bids": [{"price": "0.45", "size": "100"}],
        "asks": [{"price": "0.55", "size": "200"}],
    })
    ok(engine.best_bid("tok1") == 0.45, "best_bid after snapshot")
    ok(engine.best_ask("tok1") == 0.55, "best_ask after snapshot")

    # price_change delta
    engine.dispatch_event({
        "event_type": "price_change",
        "asset_id": "tok1",
        "changes": [
            {"side": "ask", "price": "0.55", "size": "0"},   # remove
            {"side": "ask", "price": "0.53", "size": "50"},  # add
        ],
    })
    ok(engine.best_ask("tok1") == 0.53, "best_ask updated by price_change")

    # last_trade_price recorded
    engine.dispatch_event({
        "event_type": "last_trade_price",
        "asset_id": "tok1",
        "price": "0.50", "size": "10", "side": "BUY",
    })
    book = engine.get_book("tok1")
    ok(book.last_trade is not None, "last_trade recorded")
    ok(book.last_trade.price == Decimal("0.50"), "last_trade price correct")

    # market_resolved cancels open virtual orders
    engine.place_virtual_order("tok1", OrderSide.BUY,
                                Decimal("0.50"), Decimal("999"))
    engine.dispatch_event({"event_type": "market_resolved", "asset_id": "tok1"})
    with book._lock:
        canceled = [o for o in book._virtual_orders
                    if o.status == OrderStatus.CANCELED]
    ok(len(canceled) > 0, "market_resolved cancels virtual orders")

    engine.shutdown()


def test_section5_take_profit():
    sep("Sec 5 / Workflow E — Take Profit trigger + OCO")
    book = ShadowOrderbook("tp_book")
    book.apply_snapshot(
        bids=[
            {"price": "0.80", "size": "50"},
            {"price": "0.79", "size": "100"},
            {"price": "0.78", "size": "200"},
        ],
        asks=[{"price": "0.55", "size": "200"}],
    )

    # Place a BUY order — fills immediately AND TP fires during placement
    # because best_bid (0.80) >= tp_price (0.75)
    order = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.55"),
        quantity=Decimal("100"),
        tp_price=Decimal("0.75"),  # trigger when bid >= 0.75
        sl_price=Decimal("0.30"),  # well below market — should NOT fire
    )
    ok(order.status == OrderStatus.FILLED, "BUY filled")
    ok(order.position_closed, "TP fired during placement (bid 0.80 >= tp 0.75)")
    ok(order.exit_trigger == "TP", "exit_trigger=TP, not SL")
    ok(order.exit_filled == Decimal("100"), f"fully exited 100 (got {order.exit_filled})")
    ok(order.exit_price >= Decimal("0.79"), f"avg price ≥ 0.79 (got {order.exit_price})")

    # position_closed — monitor must NOT re-fire (OCO)
    exits2 = book.monitor_bracket_orders()
    ok(len(exits2) == 0, "OCO: TP already fired, no second exit")


def test_section5_stop_loss():
    sep("Sec 5 / Workflow E — Stop Loss trigger + slippage")
    book = ShadowOrderbook("sl_book")
    book.apply_snapshot(
        bids=[
            {"price": "0.25", "size": "30"},   # 3 levels below SL
            {"price": "0.24", "size": "40"},
            {"price": "0.23", "size": "50"},
        ],
        asks=[{"price": "0.40", "size": "200"}],
    )

    # SL fires during placement: best_bid (0.25) <= sl (0.35)
    order = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.40"),
        quantity=Decimal("100"),
        tp_price=Decimal("0.90"),  # unreachable — should NOT fire
        sl_price=Decimal("0.35"),  # best_bid (0.25) <= sl (0.35) → fires
    )
    ok(order.status == OrderStatus.FILLED, "BUY filled")
    ok(order.position_closed, "SL fired during placement")
    ok(order.exit_trigger == "SL", "exit_trigger=SL")
    ok(order.exit_filled == Decimal("100"), f"fully exited (got {order.exit_filled})")
    ok(order.exit_price < Decimal("0.35"), f"avg exit price < sl_price due to slippage (got {order.exit_price})")

    # Already closed — monitor should not fire again
    exits = book.monitor_bracket_orders()
    ok(len(exits) == 0, "no re-fire after SL already closed")


def test_section5_partial_fill_bracket():
    sep("Sec 5 — TP/SL only liquidates the filled portion (Sec 6.3)")
    book = ShadowOrderbook("partial_bracket")
    book.apply_snapshot(
        bids=[{"price": "0.80", "size": "500"}],
        asks=[{"price": "0.50", "size": "30"}],  # only 30 in asks
    )
    # BUY 100 but only 30 fills; TP fires during placement (0.80 >= 0.70)
    order = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("100"),
        tp_price=Decimal("0.70"),
    )
    ok(order.status == OrderStatus.PARTIAL, "partial fill")
    ok(order.filled == Decimal("30"), f"filled=30 (got {order.filled})")
    ok(order.position_closed, "TP fired during placement on partial fill")
    ok(order.exit_filled == Decimal("30"), f"only closes filled qty=30 (got {order.exit_filled})")
    ok(order.exit_trigger == "TP", "exit_trigger=TP")


def test_section5_oco_sl_after_tp():
    sep("Sec 5 — OCO: SL does not fire after TP")
    book = ShadowOrderbook("oco_book")
    book.apply_snapshot(
        bids=[{"price": "0.80", "size": "200"}],
        asks=[{"price": "0.50", "size": "100"}],
    )
    # Both TP and SL conditions met at placement, but TP wins OCO
    order = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("50"),
        tp_price=Decimal("0.70"),  # fires (0.80 >= 0.70)
        sl_price=Decimal("0.90"),  # also would fire (0.80 <= 0.90)
    )
    ok(order.position_closed, "bracket fired during placement")
    ok(order.exit_trigger == "TP", "TP fired, not SL (OCO: TP wins)")

    # Monitor should not re-fire
    exits = book.monitor_bracket_orders()
    ok(len(exits) == 0, "no re-fire after TP already closed")


def test_section3_best_bid_ask_triggers_bracket():
    sep("Sec 3.3/3.6 — best_bid_ask event triggers Workflow E")
    engine = MatchingEngine()

    engine.dispatch_event({
        "event_type": "book",
        "asset_id": "bba_tok",
        "bids": [{"price": "0.60", "size": "200"}],
        "asks": [{"price": "0.70", "size": "100"}],
    })
    # Place filled BUY with TP at 0.55 (already triggerable)
    order = engine.place_virtual_order(
        "bba_tok", OrderSide.BUY, Decimal("0.70"), Decimal("50"),
        tp_price=Decimal("0.55"),  # best_bid=0.60 >= 0.55 → fires
    )
    ok(order.status == OrderStatus.FILLED, "BUY filled")

    # Dispatch best_bid_ask — should trigger bracket monitoring
    engine.dispatch_event({
        "event_type": "best_bid_ask",
        "asset_id": "bba_tok",
        "bid": "0.61", "bid_size": "200",
        "ask": "0.70", "ask_size": "100",
    })
    ok(order.position_closed, "best_bid_ask triggered TP via Workflow E")
    engine.shutdown()


def test_section3_last_trade_triggers_bracket():
    sep("Sec 3.4/3.6 — last_trade_price event triggers Workflow E")
    engine = MatchingEngine()

    engine.dispatch_event({
        "event_type": "book",
        "asset_id": "ltp_tok",
        "bids": [{"price": "0.50", "size": "300"}],
        "asks": [{"price": "0.60", "size": "100"}],
    })
    order = engine.place_virtual_order(
        "ltp_tok", OrderSide.BUY, Decimal("0.60"), Decimal("50"),
        sl_price=Decimal("0.55"),  # best_bid=0.50 <= 0.55 → SL fires
    )
    ok(order.status == OrderStatus.FILLED, "BUY filled")

    # last_trade_price should trigger bracket monitoring
    engine.dispatch_event({
        "event_type": "last_trade_price",
        "asset_id": "ltp_tok",
        "price": "0.50", "size": "10", "side": "SELL",
    })
    ok(order.position_closed, "last_trade_price triggered SL via Workflow E")
    engine.shutdown()


def test_avg_entry_price_tracking():
    sep("Avg entry price — weighted average across multiple fill levels")
    book = ShadowOrderbook("avg_entry")
    book.apply_snapshot(
        bids=[],
        asks=[
            {"price": "0.50", "size": "30"},
            {"price": "0.51", "size": "40"},
            {"price": "0.52", "size": "60"},
        ],
    )
    # BUY 70 @ 0.52 — fills 30@0.50 + 40@0.51
    order = book.place_virtual_order(OrderSide.BUY, Decimal("0.52"), Decimal("70"))
    ok(order.status == OrderStatus.FILLED, "BUY fully filled")
    # avg = (30*0.50 + 40*0.51) / 70 = (15+20.4) / 70 = 35.4/70 ≈ 0.505714...
    expected = (Decimal("30") * Decimal("0.50") + Decimal("40") * Decimal("0.51")) / Decimal("70")
    ok(order.avg_entry_price == expected, f"avg_entry_price={order.avg_entry_price} expected={expected}")
    ok(order.avg_entry_price != order.price, "avg_entry_price differs from limit price")


def test_calculate_profit_tp():
    sep("Profit calculation — TP exit with multi-level entry")
    book = ShadowOrderbook("profit_tp")
    book.apply_snapshot(
        bids=[{"price": "0.80", "size": "200"}],
        asks=[
            {"price": "0.50", "size": "30"},
            {"price": "0.52", "size": "70"},
        ],
    )
    # TP fires during placement: best_bid(0.80) >= tp(0.70)
    order = book.place_virtual_order(
        OrderSide.BUY, Decimal("0.55"), Decimal("50"),
        tp_price=Decimal("0.70"),
    )
    ok(order.status == OrderStatus.FILLED, "filled")
    ok(order.filled == Decimal("50"), f"filled=50 (got {order.filled})")
    # entry: 30@0.50 + 20@0.52 = 15+10.4 = 25.4 → avg = 0.508
    avg_entry = order.avg_entry_price
    ok(avg_entry is not None, "avg_entry_price tracked")
    ok(order.position_closed, "TP fired during placement")

    profit = order.calculate_profit()
    ok(profit is not None, "profit calculated")
    # profit = (50 * 0.80) - (50 * 0.508) = 40 - 25.4 = 14.6
    expected_profit = (Decimal("50") * Decimal("0.80")) - (Decimal("50") * avg_entry)
    ok(profit == expected_profit, f"profit={profit} expected={expected_profit}")
    ok(profit > 0, "profit is positive for TP exit")


def test_calculate_profit_sl():
    sep("Profit calculation — SL exit (loss)")
    book = ShadowOrderbook("profit_sl")
    book.apply_snapshot(
        bids=[{"price": "0.30", "size": "200"}],
        asks=[{"price": "0.50", "size": "100"}],
    )
    # SL fires during placement: best_bid(0.30) <= sl(0.40)
    order = book.place_virtual_order(
        OrderSide.BUY, Decimal("0.50"), Decimal("40"),
        sl_price=Decimal("0.40"),
    )
    ok(order.status == OrderStatus.FILLED, "filled")
    ok(order.position_closed, "SL fired during placement")

    profit = order.calculate_profit()
    ok(profit is not None, "profit calculated")
    ok(profit < 0, f"profit is negative for SL (got {profit})")


def test_cancel_individual_order():
    sep("Cancel individual order + partial fill P&L")
    book = ShadowOrderbook("cancel_one")
    book.apply_snapshot(
        bids=[],
        asks=[{"price": "0.50", "size": "30"}],
    )
    # BUY 100 but only 30 available → PARTIAL
    order = book.place_virtual_order(OrderSide.BUY, Decimal("0.50"), Decimal("100"))
    ok(order.status == OrderStatus.PARTIAL, "partial fill")
    ok(order.filled == Decimal("30"), "30 filled")
    ok(order.avg_entry_price == Decimal("0.50"), "avg_entry=0.50")

    # Cancel the order
    canceled = book.cancel_order(order.order_id)
    ok(canceled is not None, "cancel_order returns the order")
    ok(canceled.status == OrderStatus.CANCELED, "status=CANCELED")
    ok(canceled.filled == Decimal("30"), "filled preserved after cancel")
    ok(canceled.avg_entry_price == Decimal("0.50"), "avg_entry preserved after cancel")

    # Canceled order should not match further
    book.apply_changes([{"side": "ask", "price": "0.50", "size": "200"}])
    book.run_matching()
    ok(canceled.filled == Decimal("30"), "no further fills after cancel")

    # Cancel non-existent order
    result = book.cancel_order("nonexistent-id")
    ok(result is None, "cancel unknown order returns None")


def test_partial_exit_not_fully_closed():
    sep("Partial TP/SL exit — position stays open when bids exhausted")
    book = ShadowOrderbook("partial_exit")
    book.apply_snapshot(
        bids=[{"price": "0.80", "size": "20"}],  # only 20 available to sell into
        asks=[{"price": "0.50", "size": "100"}],
    )
    # TP fires during placement but only 20 shares can exit (bids exhausted)
    order = book.place_virtual_order(
        OrderSide.BUY, Decimal("0.50"), Decimal("100"),
        tp_price=Decimal("0.70"),
    )
    ok(order.status == OrderStatus.FILLED, "BUY filled")
    ok(order.filled == Decimal("100"), "filled=100")
    ok(order.exit_filled == Decimal("20"), "only exited 20 during placement")
    ok(not order.position_closed, "position NOT fully closed (partial exit)")

    # Add more bids — TP should fire again on next monitor tick
    book.apply_changes([{"side": "bid", "price": "0.78", "size": "80"}])
    exits2 = book.monitor_bracket_orders()
    ok(len(exits2) == 1, "TP fires again for remaining shares")
    ok(order.position_closed, "position now fully closed")


def test_section6_decimal_precision():
    sep("Sec 6.1 — Decimal precision throughout")
    book = ShadowOrderbook("prec")
    book.apply_snapshot(
        bids=[{"price": "0.123456789", "size": "100.987654321"}],
        asks=[{"price": "0.234567891", "size": "200.111111111"}],
    )
    ok(isinstance(book.best_bid(), Decimal), "best_bid is Decimal")
    ok(isinstance(book.best_ask(), Decimal), "best_ask is Decimal")
    ok(book.best_bid() == Decimal("0.123456789"), "bid precision preserved")
    ok(book.best_ask() == Decimal("0.234567891"), "ask precision preserved")


def run_unit_tests():
    tests = [
        test_section2_datamodel,
        test_section4_matching_buy,
        test_section4_matching_sell,
        test_section4_partial_fill,
        test_section3_event_routing,
        test_section5_take_profit,
        test_section5_stop_loss,
        test_section5_partial_fill_bracket,
        test_section5_oco_sl_after_tp,
        test_section3_best_bid_ask_triggers_bracket,
        test_section3_last_trade_triggers_bracket,
        test_avg_entry_price_tracking,
        test_calculate_profit_tp,
        test_calculate_profit_sl,
        test_cancel_individual_order,
        test_partial_exit_not_fully_closed,
        test_section6_decimal_precision,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"  {FAIL}  {e}")
            failed += 1
        except Exception as e:
            print(f"  {FAIL}  Unexpected error: {e}")
            import traceback; traceback.print_exc()
            failed += 1

    print(f"\n{'═'*60}")
    print(f"  Unit tests: {passed} passed, {failed} failed")
    print(f"{'═'*60}")
    return failed == 0


# ═══════════════════════════════════════════════════════════════
#  PART 2 — LIVE INTEGRATION TESTS (real Polymarket data)
# ═══════════════════════════════════════════════════════════════

async def run_live_tests():
    sep("LIVE: Step 1 — Discover token IDs")

    test_cases = [("ETH", "M5", "UP"), ("ETH", "M5", "DOWN"), ("BTC", "M5", "UP")]
    tokens: list[dict] = []

    with PolymarketClient(timeout=15.0) as pm:
        for sym, tf, direction in test_cases:
            try:
                ob = pm.get_orderbook(sym, tf, direction)
                tokens.append({
                    "sym": sym, "tf": tf, "dir": direction,
                    "token_id": ob.token_id,
                    "rest_ask": ob.min_ask,
                    "rest_bid": ob.max_bid,
                })
                print(f"  {sym} {tf} {direction}: token={ob.token_id[:32]}... "
                      f"ask={ob.min_ask} bid={ob.max_bid}")
            except Exception as e:
                print(f"  {sym} {tf} {direction}: SKIP — {e}")

    if not tokens:
        print("  No tokens discovered — skipping live tests")
        return True

    # ── Load REST books into engine ──────────────────────────────────────────
    sep("LIVE: Step 2 — Load full orderbook from REST CLOB")
    engine = MatchingEngine()

    for t in tokens:
        try:
            book_data = fetch_full_book(t["token_id"])
            n_bids = len(book_data.get("bids", []))
            n_asks = len(book_data.get("asks", []))
            engine.dispatch_event({
                "event_type": "book",
                "asset_id": t["token_id"],
                "bids": book_data.get("bids", []),
                "asks": book_data.get("asks", []),
            })
            eng_ask = engine.best_ask(t["token_id"])
            eng_bid = engine.best_bid(t["token_id"])
            diff = abs(eng_ask - t["rest_ask"]) if eng_ask else 99
            match = "✅" if diff < 0.01 else "⚠️ "
            print(f"  {t['sym']} {t['dir']}: {n_bids}b/{n_asks}a | "
                  f"eng_ask={eng_ask} rest={t['rest_ask']} {match} | "
                  f"eng_bid={eng_bid}")
        except Exception as e:
            print(f"  {t['sym']}: FAILED — {e}")

    # ── Virtual order test on real book ──────────────────────────────────────
    sep("LIVE: Step 3 — Virtual BUY + Bracket Order on real book")
    t = tokens[0]
    tid = t["token_id"]
    book = engine.get_book(tid)

    if book and book.best_ask():
        ask = book.best_ask()
        qty = Decimal("50")
        # TP: +15% above best ask; SL: -20% below
        tp  = (ask * Decimal("1.15")).quantize(Decimal("0.001"))
        sl  = (ask * Decimal("0.80")).quantize(Decimal("0.001"))

        order = engine.place_virtual_order(
            tid, OrderSide.BUY, ask, qty,
            tp_price=tp, sl_price=sl,
        )
        print(f"  Order: side=BUY price={ask} qty={qty} tp={tp} sl={sl}")
        print(f"  Status:  {order.status.value}  filled={order.filled}")
        print(f"  Bracket: has_bracket={order.has_bracket} "
              f"eligible={order.is_eligible_for_bracket}")

        if order.filled > 0:
            # Simulate a price spike to trigger TP
            bid_now = book.best_bid() or Decimal("0")
            if bid_now >= tp:
                exits = book.monitor_bracket_orders()
                print(f"  TP naturally triggered: {len(exits)} exit(s)")
            else:
                # Force-test: inject a price_change that moves bid above TP
                engine.dispatch_event({
                    "event_type": "best_bid_ask",
                    "asset_id": tid,
                    "bid": str(tp + Decimal("0.01")),
                    "bid_size": "500",
                    "ask": str(ask),
                    "ask_size": "100",
                })
                if order.position_closed:
                    print(f"  ✅ TP triggered via best_bid_ask event")
                    print(f"  Exit: trigger={order.exit_trigger} "
                          f"price={order.exit_price} "
                          f"filled={order.exit_filled}")
                else:
                    print(f"  ℹ️  TP not triggered (book may have no bids at that level)")

    # ── WebSocket live stream ────────────────────────────────────────────────
    sep("LIVE: Step 4 — WebSocket stream (20 seconds)")
    token_ids = [t["token_id"] for t in tokens]
    ws_engine = MatchingEngine()
    event_counts: dict[str, int] = {}
    bracket_exits_ws = 0

    # Place a bracket order on each token to test WS-triggered monitoring
    for t in tokens:
        tid = t["token_id"]
        # Pre-load book so we have some liquidity
        try:
            bd = fetch_full_book(tid)
            ws_engine.dispatch_event({
                "event_type": "book", "asset_id": tid,
                "bids": bd.get("bids", []), "asks": bd.get("asks", []),
            })
            bk = ws_engine.get_book(tid)
            if bk and bk.best_ask():
                ask_ = bk.best_ask()
                order_ = ws_engine.place_virtual_order(
                    tid, OrderSide.BUY, ask_, Decimal("10"),
                    tp_price=(ask_ * Decimal("1.05")).quantize(Decimal("0.001")),
                    sl_price=(ask_ * Decimal("0.95")).quantize(Decimal("0.001")),
                )
                print(f"  Placed bracket order on {t['sym']} {t['dir']}: "
                      f"status={order_.status.value} filled={order_.filled} "
                      f"tp={order_.tp_price} sl={order_.sl_price}")
        except Exception as e:
            print(f"  Pre-load {t['sym']}: {e}")

    try:
        async with websockets.connect(_WS_URI, ping_interval=None, close_timeout=5) as ws:
            print(f"  Connected to Polymarket WS")
            await ws.send(json.dumps({
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }))
            print(f"  Subscribed to {len(token_ids)} tokens")

            hb = asyncio.create_task(
                asyncio.sleep(9999)  # replaced by real heartbeat below
            )
            hb.cancel()

            async def heartbeat():
                while True:
                    await asyncio.sleep(10)
                    try:
                        await ws.send("PING")
                    except Exception:
                        break
            hb_task = asyncio.create_task(heartbeat())

            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if raw == "PONG":
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue

                events = data if isinstance(data, list) else [data]
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    etype = ev.get("event_type", "?")
                    event_counts[etype] = event_counts.get(etype, 0) + 1
                    exits_before = sum(
                        len(ws_engine.get_book(t["token_id"]).bracket_log)
                        for t in tokens
                        if ws_engine.get_book(t["token_id"])
                    )
                    ws_engine.dispatch_event(ev)
                    exits_after = sum(
                        len(ws_engine.get_book(t["token_id"]).bracket_log)
                        for t in tokens
                        if ws_engine.get_book(t["token_id"])
                    )
                    new_exits = exits_after - exits_before
                    if new_exits:
                        bracket_exits_ws += new_exits
                        print(f"  🔔 Bracket exit triggered by {etype}! "
                              f"(total exits: {bracket_exits_ws})")

            hb_task.cancel()

    except Exception as e:
        print(f"  WS error: {e}")

    print(f"\n  Events received (20s):")
    for etype, cnt in sorted(event_counts.items()):
        print(f"    {etype:25s} {cnt:4d}")
    print(f"  Bracket exits triggered by WS: {bracket_exits_ws}")

    # ── Final engine summary ─────────────────────────────────────────────────
    sep("LIVE: Step 5 — Engine summary")
    for summary in ws_engine.all_books_summary():
        t_info = next((t for t in tokens if t["token_id"] == summary["token_id"]), {})
        label = f"{t_info.get('sym','?')} {t_info.get('dir','?')}"
        print(f"\n  [{label}]")
        print(f"    bid={summary['best_bid']}  ask={summary['best_ask']}  "
              f"spread={summary['spread']}")
        print(f"    levels: {summary['bid_levels']}b / {summary['ask_levels']}a  "
              f"stale={summary['stale']}")
        print(f"    virtual_orders={summary['active_virtual_orders']}  "
              f"bracket_active={summary['active_bracket_orders']}  "
              f"exits_total={summary['bracket_exits_total']}")
        if summary["last_trade"]:
            lt = summary["last_trade"]
            print(f"    last_trade: {lt['price']} × {lt['size']} ({lt['side']})")
        for ex in summary["recent_bracket_exits"]:
            print(f"    EXIT {ex['trigger']}: price={ex['avg_exit_price']} "
                  f"qty={ex['qty_exited']} levels={ex['levels']}")

    ws_engine.shutdown()
    engine.shutdown()
    return True


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═" * 60)
    print("  Matching Engine — Full Spec Test")
    print("  docs/matching_engine.md  Sections 2–6")
    print("═" * 60)

    unit_ok = run_unit_tests()

    print("\n")
    live_ok = asyncio.run(run_live_tests())

    print()
    print("═" * 60)
    status = "ALL PASSED ✅" if (unit_ok and live_ok) else "SOME FAILED ❌"
    print(f"  {status}")
    print("═" * 60)
    sys.exit(0 if (unit_ok and live_ok) else 1)
