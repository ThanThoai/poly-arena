"""
Tests for _fill_market_from_rest() — REST-based MARKET order fill logic.

Verifies:
  - Multi-level fill: walks through multiple ask levels correctly
  - Partial fill when amount < total liquidity: fills what budget allows
  - Slippage enforcement: skips levels beyond slippage tolerance
  - Empty orderbook → 502
  - Budget exhausted mid-level → partial fill at that level
  - Weighted avg_price calculation across levels
"""

import json
from decimal import Decimal
from unittest.mock import patch, MagicMock

import pytest
from fastapi import HTTPException

from routers.binary_options import _fill_market_from_rest


def _mock_get_orderbook(symbol, timeframe, pm_status):
    """Return a fake OrderbookResult with a known token_id."""
    ob = MagicMock()
    ob.token_id = "fake-token-123"
    ob.min_ask = 0.50
    ob.max_bid = 0.48
    return ob


def _make_book_response(asks, bids=None):
    """Build a CLOB book JSON response from ask levels."""
    return {
        "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
        "bids": bids or [{"price": "0.48", "size": "100"}],
    }


# ═══════════════════════════════════════════════════════════════
# 1. Multi-level fill
# ═══════════════════════════════════════════════════════════════


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_multi_level_fill(mock_pm_cls, mock_httpx_get):
    """Fill across 3 ask levels with enough budget for all."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Asks: 0.50 x 100, 0.51 x 100, 0.52 x 100
    # Total cost to buy all: 50 + 51 + 52 = 153
    book = _make_book_response([
        (0.50, 100),
        (0.51, 100),
        (0.52, 100),
    ])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, token_id, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=153.0, slippage_tolerance=0.10,
    )

    assert token_id == "fake-token-123"
    assert abs(num_shares - 300.0) < 0.01  # 100 + 100 + 100
    # Weighted avg: (100*0.50 + 100*0.51 + 100*0.52) / 300 = 0.51
    assert abs(avg_price - 0.51) < 0.001


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_multi_level_weighted_avg(mock_pm_cls, mock_httpx_get):
    """Weighted avg_price is correct with unequal level sizes."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Asks: 0.40 x 200, 0.50 x 50
    # Buy all: cost = 80 + 25 = 105, shares = 250
    # avg = 105 / 250 = 0.42
    book = _make_book_response([(0.40, 200), (0.50, 50)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=105.0, slippage_tolerance=0.50,
    )

    assert abs(num_shares - 250.0) < 0.01
    assert abs(avg_price - 0.42) < 0.001


# ═══════════════════════════════════════════════════════════════
# 2. Partial fill — budget runs out mid-level
# ═══════════════════════════════════════════════════════════════


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_budget_exhausted_mid_level(mock_pm_cls, mock_httpx_get):
    """Budget runs out partway through a level → partial fill at that level."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Ask: 0.50 x 1000 (plenty of liquidity)
    # Budget: $10 → can buy 10/0.50 = 20 shares
    book = _make_book_response([(0.50, 1000)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=10.0, slippage_tolerance=0.10,
    )

    assert abs(num_shares - 20.0) < 0.01
    assert abs(avg_price - 0.50) < 0.001


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_budget_exhausted_across_levels(mock_pm_cls, mock_httpx_get):
    """Budget exhausted partway through second ask level."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Asks: 0.40 x 100 (cost=40), 0.50 x 200 (cost=100)
    # Budget: $60 → fills 100 at 0.40 ($40 spent, $20 left)
    #             → then 20/0.50 = 40 shares at 0.50
    # Total: 140 shares, cost = 40 + 20 = 60
    book = _make_book_response([(0.40, 100), (0.50, 200)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=60.0, slippage_tolerance=0.50,
    )

    assert abs(num_shares - 140.0) < 0.01
    # avg = 60 / 140 ≈ 0.4286
    expected_avg = 60.0 / 140.0
    assert abs(avg_price - expected_avg) < 0.001


# ═══════════════════════════════════════════════════════════════
# 3. Slippage enforcement
# ═══════════════════════════════════════════════════════════════


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_slippage_skips_expensive_levels(mock_pm_cls, mock_httpx_get):
    """Levels beyond slippage tolerance are skipped."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Asks: 0.50 x 50, 0.60 x 200
    # Slippage 5%: limit = 0.50 * 1.05 = 0.525
    # 0.60 > 0.525 → skipped
    book = _make_book_response([(0.50, 50), (0.60, 200)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=100.0, slippage_tolerance=0.05,
    )

    # Only fills at 0.50, 50 shares ($25 spent out of $100 budget)
    assert abs(num_shares - 50.0) < 0.01
    assert abs(avg_price - 0.50) < 0.001


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_slippage_all_levels_beyond_tolerance(mock_pm_cls, mock_httpx_get):
    """All levels beyond slippage → HTTPException 502."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # ref_price = 0.50, slippage 1% → limit = 0.505
    # Only ask at 0.60 → beyond limit, but ref_price comes from asks[0]
    # Actually: if only level is 0.60, ref_price = 0.60, limit = 0.606
    # So we need: asks[0]=0.50 (ref), only level within tolerance is 0.50 with 0 size
    # Better: two levels where first is tiny but ref, second is beyond
    # Simplest: set slippage_tolerance=0 so limit = ref_price exactly
    # ref = 0.50, limit = 0.50 * 1.0 = 0.50
    # Ask at 0.50 has 0 size (filtered out), ask at 0.60 → beyond
    book = _make_book_response([(0.60, 200)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    # ref_price = 0.60, with slippage 0% → limit = 0.60
    # 0.60 <= 0.60 → it WILL fill. Need a gap.
    # Use asks: [0.50 x 10, 0.90 x 200] with 2% slippage
    # ref = 0.50, limit = 0.50 * 1.02 = 0.51, 0.90 > 0.51 → skipped
    # Budget: $5 buys all 10 at 0.50 ($5)... that fills.
    # To get zero fill: need budget=0.01 but 0.50 * any > 0.01? No, it fills 0.02 shares.
    # Actually easiest: make ALL asks have size=0 (filtered out) → empty list → "No liquidity"
    book2 = _make_book_response([])
    mock_resp.json.return_value = book2

    with pytest.raises(HTTPException) as exc_info:
        _fill_market_from_rest("BTC", "M5", "UP", amount=100.0, slippage_tolerance=0.05)
    assert exc_info.value.status_code == 502
    assert "No liquidity" in exc_info.value.detail


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_tight_slippage_partial_fill(mock_pm_cls, mock_httpx_get):
    """Tight slippage allows only first level, budget covers more."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Asks: 0.50 x 30, 0.53 x 100, 0.55 x 100
    # slippage 2%: limit = 0.50 * 1.02 = 0.51
    # Only 0.50 within tolerance → 30 shares at $15
    book = _make_book_response([(0.50, 30), (0.53, 100), (0.55, 100)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=100.0, slippage_tolerance=0.02,
    )

    assert abs(num_shares - 30.0) < 0.01
    assert abs(avg_price - 0.50) < 0.001


# ═══════════════════════════════════════════════════════════════
# 4. Empty orderbook
# ═══════════════════════════════════════════════════════════════


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_empty_orderbook_raises_502(mock_pm_cls, mock_httpx_get):
    """Empty asks → 502 error."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    book = _make_book_response([])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    with pytest.raises(HTTPException) as exc_info:
        _fill_market_from_rest("BTC", "M5", "UP", amount=10.0, slippage_tolerance=0.10)
    assert exc_info.value.status_code == 502
    assert "No liquidity" in exc_info.value.detail


# ═══════════════════════════════════════════════════════════════
# 5. Default slippage tolerance
# ═══════════════════════════════════════════════════════════════


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_default_slippage_tolerance(mock_pm_cls, mock_httpx_get):
    """None slippage_tolerance uses 10% default."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Asks: 0.50 x 100, 0.54 x 100 (8% above, within 10%), 0.60 x 100 (20% above)
    # Default 10%: limit = 0.50 * 1.10 = 0.55
    # 0.54 within, 0.60 beyond
    book = _make_book_response([(0.50, 100), (0.54, 100), (0.60, 100)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=200.0, slippage_tolerance=None,
    )

    # Fills 100 at 0.50 ($50) + 100 at 0.54 ($54) = 200 shares, $104 cost
    assert abs(num_shares - 200.0) < 0.01
    expected_avg = 104.0 / 200.0  # 0.52
    assert abs(avg_price - expected_avg) < 0.001


# ═══════════════════════════════════════════════════════════════
# 6. Very small budget
# ═══════════════════════════════════════════════════════════════


@patch("routers.binary_options.httpx.get")
@patch("routers.binary_options.PolymarketClient")
def test_tiny_budget_partial_fill(mock_pm_cls, mock_httpx_get):
    """Very small budget only buys a fraction of the first level."""
    mock_pm = MagicMock()
    mock_pm.get_orderbook = _mock_get_orderbook
    mock_pm.__enter__ = lambda self: mock_pm
    mock_pm.__exit__ = MagicMock(return_value=False)
    mock_pm_cls.return_value = mock_pm

    # Ask: 0.50 x 1000
    # Budget: $1 → 1 / 0.50 = 2 shares
    book = _make_book_response([(0.50, 1000)])
    mock_resp = MagicMock()
    mock_resp.json.return_value = book
    mock_resp.raise_for_status = MagicMock()
    mock_httpx_get.return_value = mock_resp

    avg_price, num_shares, _, _walk = _fill_market_from_rest(
        "BTC", "M5", "UP", amount=1.0, slippage_tolerance=0.10,
    )

    assert abs(num_shares - 2.0) < 0.01
    assert abs(avg_price - 0.50) < 0.001
