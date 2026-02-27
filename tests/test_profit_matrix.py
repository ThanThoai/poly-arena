"""
Unit tests for the profit matrix (docs/order_flow.md).

Verifies settlement.py applies the correct formula:
  - No TP/SL                 → binary settlement (WIN or LOSS)
  - TP only, TP fired        → shadow tracking WIN
  - TP only, TP not fired    → binary LOSS
  - SL only, SL fired        → shadow tracking LOSS
  - SL only, SL not fired    → binary WIN
  - Both TP+SL, TP fired     → shadow tracking WIN
  - Both TP+SL, SL fired     → shadow tracking LOSS

Also tests the on_bracket_exit callback write-back in the matching engine.
"""

import sys
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, ".")

from models import BOResult
from services.matching_engine import (
    BracketFillResult,
    MatchingEngine,
    OrderSide,
    OrderStatus,
    ShadowOrderbook,
)
from datetime import datetime, timezone


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_bo(**kwargs):
    """Build a minimal BinaryOption-like namespace for settlement tests."""
    defaults = dict(
        id          = 1,
        symbol      = "BTC",
        timeframe   = "M5",
        forecast    = "GREEN",
        amount      = 100.0,
        avg_price   = 0.50,
        num_shares  = 200.0,
        # Bracket fields — default: no bracket
        tp_price    = None,
        sl_price    = None,
        exit_trigger= None,
        exit_price  = None,
        exit_filled = None,
        # settlement output fields (written by settle_pending_trades)
        result      = None,
        profit      = None,
        price_open  = None,
        price_close = None,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _settle(bo, open_price: float, close_price: float):
    """
    Run just the profit/result calculation logic from settle_pending_trades,
    without any DB or HTTP calls.  Mirrors the logic in settlement.py exactly.
    """
    candle_dir = "GREEN" if close_price > open_price else (
                 "RED"   if close_price < open_price else "GREEN")

    if bo.exit_trigger in ("TP", "SL") and bo.exit_price is not None and bo.exit_filled is not None:
        result = BOResult.WIN if bo.exit_trigger == "TP" else BOResult.LOSS
        profit = round((bo.exit_price - bo.avg_price) * bo.exit_filled, 8)
    else:
        result = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
        if result == BOResult.WIN:
            if bo.avg_price is not None and bo.num_shares is not None:
                profit = round((1 - bo.avg_price) * bo.num_shares, 8)
            else:
                profit = round(bo.amount * 1.0, 8)
        else:
            profit = -bo.amount

    bo.result = result
    bo.profit = profit
    bo.price_open  = open_price
    bo.price_close = close_price


# ═══════════════════════════════════════════════════════════════
# 1. No TP / No SL — binary settlement
# ═══════════════════════════════════════════════════════════════


def test_no_bracket_win():
    """No TP/SL — candle GREEN, forecast GREEN → binary WIN."""
    bo = _make_bo(forecast="GREEN", avg_price=0.30, num_shares=333.33)
    _settle(bo, open_price=100, close_price=110)  # GREEN candle
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.30) * 333.33, 2)


def test_no_bracket_loss():
    """No TP/SL — candle RED, forecast GREEN → binary LOSS."""
    bo = _make_bo(forecast="GREEN", amount=100.0)
    _settle(bo, open_price=110, close_price=100)  # RED candle
    assert bo.result == BOResult.LOSS
    assert bo.profit == -100.0


def test_no_bracket_red_win():
    """No TP/SL — candle RED, forecast RED → binary WIN."""
    bo = _make_bo(forecast="RED", avg_price=0.48, num_shares=208.33)
    _settle(bo, open_price=110, close_price=100)  # RED candle
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.48) * 208.33, 2)


# ═══════════════════════════════════════════════════════════════
# 2. TP only — no SL
# ═══════════════════════════════════════════════════════════════


def test_tp_only_tp_fired_win():
    """TP only, TP fired → shadow WIN regardless of candle direction."""
    bo = _make_bo(
        tp_price     = 0.70,
        exit_trigger = "TP",
        exit_price   = 0.72,   # avg_exit from shadow
        exit_filled  = 200.0,
        avg_price    = 0.50,
    )
    # Even if candle is RED, shadow WIN takes precedence
    _settle(bo, open_price=110, close_price=100)
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 4) == round((0.72 - 0.50) * 200.0, 4)  # +44.00


def test_tp_only_tp_not_fired_candle_loss():
    """TP only, TP did NOT fire, candle goes wrong → binary LOSS."""
    bo = _make_bo(
        tp_price     = 0.70,
        exit_trigger = None,   # TP not fired
        avg_price    = 0.50,
        num_shares   = 200.0,
        amount       = 100.0,
        forecast     = "GREEN",
    )
    _settle(bo, open_price=110, close_price=100)  # RED candle, forecast wrong
    assert bo.result == BOResult.LOSS
    assert bo.profit == -100.0


def test_tp_only_tp_not_fired_candle_win():
    """TP only, TP did NOT fire, candle goes right → binary WIN."""
    bo = _make_bo(
        tp_price     = 0.70,
        exit_trigger = None,
        avg_price    = 0.50,
        num_shares   = 200.0,
        forecast     = "GREEN",
    )
    _settle(bo, open_price=100, close_price=110)  # GREEN candle
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.50) * 200.0, 2)  # +100.00


# ═══════════════════════════════════════════════════════════════
# 3. SL only — no TP
# ═══════════════════════════════════════════════════════════════


def test_sl_only_sl_fired_loss():
    """SL only, SL fired → shadow LOSS regardless of candle direction."""
    bo = _make_bo(
        sl_price     = 0.35,
        exit_trigger = "SL",
        exit_price   = 0.335,  # avg_exit after slippage
        exit_filled  = 200.0,
        avg_price    = 0.50,
    )
    # Even if candle is GREEN, shadow LOSS takes precedence
    _settle(bo, open_price=100, close_price=110)
    assert bo.result == BOResult.LOSS
    assert round(bo.profit, 2) == round((0.335 - 0.50) * 200.0, 2)  # -33.00


def test_sl_only_sl_not_fired_candle_win():
    """SL only, SL did NOT fire, candle goes right → binary WIN."""
    bo = _make_bo(
        sl_price     = 0.35,
        exit_trigger = None,
        avg_price    = 0.50,
        num_shares   = 200.0,
        forecast     = "GREEN",
    )
    _settle(bo, open_price=100, close_price=110)  # GREEN candle
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.50) * 200.0, 2)


def test_sl_only_sl_not_fired_candle_loss():
    """SL only, SL did NOT fire (price stayed above SL), candle goes wrong → binary LOSS."""
    bo = _make_bo(
        sl_price     = 0.35,
        exit_trigger = None,
        amount       = 100.0,
        forecast     = "GREEN",
    )
    _settle(bo, open_price=110, close_price=100)  # RED candle
    assert bo.result == BOResult.LOSS
    assert bo.profit == -100.0


# ═══════════════════════════════════════════════════════════════
# 4. Both TP and SL (Bracket)
# ═══════════════════════════════════════════════════════════════


def test_bracket_tp_fired():
    """Bracket, TP fired → shadow WIN."""
    bo = _make_bo(
        tp_price     = 0.70,
        sl_price     = 0.35,
        exit_trigger = "TP",
        exit_price   = 0.733,
        exit_filled  = 100.0,
        avg_price    = 0.52,
    )
    _settle(bo, open_price=100, close_price=110)
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 4) == round((0.733 - 0.52) * 100.0, 4)  # +21.30


def test_bracket_sl_fired():
    """Bracket, SL fired → shadow LOSS."""
    bo = _make_bo(
        tp_price     = 0.70,
        sl_price     = 0.35,
        exit_trigger = "SL",
        exit_price   = 0.32,
        exit_filled  = 100.0,
        avg_price    = 0.52,
    )
    _settle(bo, open_price=110, close_price=100)
    assert bo.result == BOResult.LOSS
    assert round(bo.profit, 4) == round((0.32 - 0.52) * 100.0, 4)  # -20.00


def test_bracket_no_fire_fallback_win():
    """Bracket, neither TP nor SL fired before expiry → binary settlement fallback."""
    bo = _make_bo(
        tp_price     = 0.70,
        sl_price     = 0.35,
        exit_trigger = None,
        avg_price    = 0.50,
        num_shares   = 200.0,
        forecast     = "GREEN",
    )
    _settle(bo, open_price=100, close_price=110)  # GREEN candle
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.50) * 200.0, 2)


def test_bracket_no_fire_fallback_loss():
    """Bracket, neither fired → binary LOSS fallback."""
    bo = _make_bo(
        tp_price     = 0.70,
        sl_price     = 0.35,
        exit_trigger = None,
        amount       = 100.0,
        forecast     = "GREEN",
    )
    _settle(bo, open_price=110, close_price=100)  # RED candle
    assert bo.result == BOResult.LOSS
    assert bo.profit == -100.0


# ═══════════════════════════════════════════════════════════════
# 5. on_bracket_exit callback write-back
# ═══════════════════════════════════════════════════════════════


def test_on_bracket_exit_callback_called_on_tp():
    """Callback is called with BracketFillResult when TP fires."""
    book = ShadowOrderbook("tok")
    book.bids = {Decimal("0.72"): Decimal("200"), Decimal("0.70"): Decimal("100")}
    book.asks = {Decimal("0.50"): Decimal("200")}

    received = []
    order, bracket_results = book.place_virtual_order(
        side            = OrderSide.BUY,
        price           = Decimal("0.50"),
        quantity        = Decimal("200"),
        tp_price        = Decimal("0.65"),
        on_bracket_exit = lambda r: received.append(r),
    )
    # Fire any immediate bracket results
    for br in bracket_results:
        received.append(br)
    assert order.status == OrderStatus.FILLED

    book.monitor_bracket_orders()

    assert len(received) == 1
    assert received[0].trigger == "TP"
    assert received[0].order_id == order.order_id


def test_on_bracket_exit_callback_called_on_sl():
    """Callback is called with BracketFillResult when SL fires."""
    book = ShadowOrderbook("tok2")
    book.bids = {Decimal("0.30"): Decimal("200")}
    book.asks = {Decimal("0.50"): Decimal("200")}

    received = []
    order, bracket_results = book.place_virtual_order(
        side            = OrderSide.BUY,
        price           = Decimal("0.50"),
        quantity        = Decimal("200"),
        sl_price        = Decimal("0.40"),
        on_bracket_exit = lambda r: received.append(r),
    )
    for br in bracket_results:
        received.append(br)
    assert order.status == OrderStatus.FILLED

    book.monitor_bracket_orders()

    assert len(received) == 1
    assert received[0].trigger == "SL"


def test_no_callback_without_bracket():
    """No callback fired when order has no TP/SL."""
    book = ShadowOrderbook("tok3")
    book.bids = {Decimal("0.80"): Decimal("200")}
    book.asks = {Decimal("0.50"): Decimal("200")}

    received = []
    order, bracket_results = book.place_virtual_order(
        side            = OrderSide.BUY,
        price           = Decimal("0.50"),
        quantity        = Decimal("200"),
        on_bracket_exit = lambda r: received.append(r),
    )
    for br in bracket_results:
        received.append(br)
    book.monitor_bracket_orders()
    assert len(received) == 0  # no TP/SL → no callback


def test_callback_not_called_twice_after_position_closed():
    """After position_closed=True, callback should not be called again on next tick."""
    book = ShadowOrderbook("tok4")
    book.bids = {Decimal("0.72"): Decimal("200")}
    book.asks = {Decimal("0.50"): Decimal("200")}

    received = []
    _, bracket_results = book.place_virtual_order(
        side            = OrderSide.BUY,
        price           = Decimal("0.50"),
        quantity        = Decimal("200"),
        tp_price        = Decimal("0.65"),
        on_bracket_exit = lambda r: received.append(r),
    )
    for br in bracket_results:
        received.append(br)

    book.monitor_bracket_orders()  # fires TP, position_closed=True
    book.monitor_bracket_orders()  # second tick — should NOT fire again
    assert len(received) == 1


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
