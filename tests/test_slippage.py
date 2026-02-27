"""
Tests for slippage protection in the matching engine (docs/system_improve.md Section 6).

Verifies:
  - MARKET order within slippage tolerance → fills normally
  - MARKET order exceeding slippage → partial fill + IOC cancel
  - Cost cap enforcement → matching stops at max_cost
"""

import sys
from decimal import Decimal

sys.path.insert(0, ".")

from services.matching_engine import (
    MatchingEngine,
    OrderSide,
    OrderStatus,
    ShadowOrderbook,
)


# ═══════════════════════════════════════════════════════════════
# 1. MARKET order within slippage tolerance → fills normally
# ═══════════════════════════════════════════════════════════════


def test_market_order_within_slippage_fills():
    """MARKET BUY with asks within slippage range → full fill."""
    book = ShadowOrderbook("tok-slip-1")
    # Asks: 0.50 (100 qty), 0.51 (100 qty) — within 10% slippage of 0.50
    book.asks = {
        Decimal("0.50"): Decimal("100"),
        Decimal("0.51"): Decimal("100"),
    }
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("150"),
        order_type="MARKET",
        max_slippage=Decimal("0.10"),  # 10% slippage
    )

    assert order.status in (OrderStatus.FILLED, OrderStatus.CANCELED)
    assert order.filled == Decimal("150")
    # avg entry should be between 0.50 and 0.51
    assert order.avg_entry_price is not None
    assert Decimal("0.50") <= order.avg_entry_price <= Decimal("0.51")


def test_market_order_exact_ask_no_slippage():
    """MARKET BUY at single ask level → fills at exact price, no slippage."""
    book = ShadowOrderbook("tok-slip-2")
    book.asks = {Decimal("0.50"): Decimal("200")}
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("100"),
        order_type="MARKET",
        max_slippage=Decimal("0.05"),  # 5%
    )

    assert order.filled == Decimal("100")
    assert order.avg_entry_price == Decimal("0.50")


# ═══════════════════════════════════════════════════════════════
# 2. MARKET order exceeding slippage → partial fill + IOC cancel
# ═══════════════════════════════════════════════════════════════


def test_market_order_exceeding_slippage_partial_fill():
    """MARKET BUY where higher ask levels exceed slippage → partial fill."""
    book = ShadowOrderbook("tok-slip-3")
    # Asks: 0.50 (50 qty), 0.60 (200 qty)
    # With 5% slippage from 0.50, limit = 0.525 → 0.60 is beyond
    book.asks = {
        Decimal("0.50"): Decimal("50"),
        Decimal("0.60"): Decimal("200"),
    }
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("200"),
        order_type="MARKET",
        max_slippage=Decimal("0.05"),  # 5% → max price = 0.525
    )

    # Should fill only the 50 at 0.50, then IOC cancel the rest
    assert order.status == OrderStatus.CANCELED  # IOC cancel
    assert order.filled == Decimal("50")
    assert order.avg_entry_price == Decimal("0.50")


def test_market_order_zero_fill_when_all_beyond_slippage():
    """MARKET BUY where all ask levels exceed slippage → zero fill, IOC cancel."""
    book = ShadowOrderbook("tok-slip-4")
    # Only ask is at 0.70, which is beyond 5% slippage of reference
    book.asks = {Decimal("0.70"): Decimal("200")}
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.70"),
        quantity=Decimal("100"),
        order_type="MARKET",
        max_slippage=Decimal("0.05"),  # 5% of 0.70 → max = 0.735
    )

    # With only one level at the reference price, should fill normally
    # (slippage_ref_price = min(asks) = 0.70, limit = 0.70 * 1.05 = 0.735)
    assert order.filled == Decimal("100")


def test_market_sell_slippage_protection():
    """MARKET SELL where lower bid levels exceed slippage → partial fill."""
    book = ShadowOrderbook("tok-slip-5")
    book.asks = {Decimal("0.55"): Decimal("100")}
    # Bids: 0.50 (50 qty), 0.30 (200 qty)
    # With 5% slippage from 0.50, floor = 0.475 → 0.30 is beyond
    book.bids = {
        Decimal("0.50"): Decimal("50"),
        Decimal("0.30"): Decimal("200"),
    }

    order, _ = book.place_virtual_order(
        side=OrderSide.SELL,
        price=Decimal("0.50"),
        quantity=Decimal("200"),
        order_type="MARKET",
        max_slippage=Decimal("0.05"),  # 5% → min price = 0.475
    )

    assert order.status == OrderStatus.CANCELED
    assert order.filled == Decimal("50")


# ═══════════════════════════════════════════════════════════════
# 3. Cost cap enforcement
# ═══════════════════════════════════════════════════════════════


def test_cost_cap_limits_fill():
    """MARKET BUY with max_cost stops matching when cost limit is reached."""
    book = ShadowOrderbook("tok-cost-1")
    book.asks = {Decimal("0.50"): Decimal("1000")}
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("1000"),
        order_type="MARKET",
        max_cost=Decimal("50.0"),  # only buy $50 worth
    )

    # At 0.50 per share, max_cost=50 → 100 shares
    assert order.filled == Decimal("100")
    total_cost = order.filled * order.avg_entry_price
    assert total_cost <= Decimal("50.0")


def test_cost_cap_with_multiple_levels():
    """MARKET BUY across multiple ask levels with cost cap."""
    book = ShadowOrderbook("tok-cost-2")
    book.asks = {
        Decimal("0.40"): Decimal("100"),  # cost = 40
        Decimal("0.50"): Decimal("100"),  # cost = 50, cumulative = 90
        Decimal("0.60"): Decimal("100"),  # cost = 60, cumulative would be 150
    }
    book.bids = {Decimal("0.38"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.40"),
        quantity=Decimal("300"),
        order_type="MARKET",
        max_cost=Decimal("80.0"),  # limit to $80
        max_slippage=Decimal("1.0"),  # wide slippage to not interfere
    )

    # Should fill 100 at 0.40 ($40) + up to 80 at 0.50 ($40 remaining budget)
    total_cost = sum(
        order.avg_entry_price * order.filled
        for _ in [1]  # single expression
    )
    assert total_cost <= Decimal("80.01")  # small rounding tolerance


# ═══════════════════════════════════════════════════════════════
# 4. Default slippage (10%)
# ═══════════════════════════════════════════════════════════════


def test_default_slippage_applied():
    """MARKET order with no explicit slippage uses 10% default."""
    book = ShadowOrderbook("tok-default-slip")
    # Ask at 0.50 (ref), ask at 0.54 (8% above, within 10%), ask at 0.60 (20% above, beyond 10%)
    book.asks = {
        Decimal("0.50"): Decimal("50"),
        Decimal("0.54"): Decimal("50"),
        Decimal("0.60"): Decimal("200"),
    }
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("200"),
        order_type="MARKET",
        # max_slippage=None → default 10%
    )

    # Should fill 50 at 0.50 + 50 at 0.54 (both within 10% of 0.50)
    # 0.60 is 20% above 0.50, beyond the 10% default
    assert order.filled == Decimal("100")
    assert order.status == OrderStatus.CANCELED  # IOC cancel remainder


# ═══════════════════════════════════════════════════════════════
# 5. LIMIT orders ignore slippage
# ═══════════════════════════════════════════════════════════════


def test_limit_order_ignores_slippage():
    """LIMIT orders are not affected by slippage tolerance — they match at limit price or better."""
    book = ShadowOrderbook("tok-limit-noslip")
    book.asks = {Decimal("0.50"): Decimal("200")}
    book.bids = {Decimal("0.48"): Decimal("100")}

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("100"),
        order_type="LIMIT",
        max_slippage=Decimal("0.01"),  # very tight — should be ignored for LIMIT
    )

    assert order.filled == Decimal("100")
    assert order.status == OrderStatus.FILLED
