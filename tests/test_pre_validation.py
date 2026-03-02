"""
Tests for Pre-Validation Against Best Ask (v2 spec Section 2).

Verifies:
  - TP > best_ask → accepted
  - TP <= best_ask → rejected 400
  - SL < best_ask → accepted
  - SL >= best_ask → rejected 400
"""

import pytest

pytestmark = pytest.mark.skip(reason="TP/SL feature temporarily disabled")

import pytest
from unittest.mock import patch, MagicMock
from decimal import Decimal

from models import Bot, BOResult


@pytest.fixture
def bot_with_key(db):
    """Create a bot with sufficient balance."""
    import secrets
    bot = Bot(
        bot_name="pre-val-bot",
        api_key=secrets.token_urlsafe(32),
        is_active=True,
        balance=10000.0,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


def _mock_redis_price(best_ask: float):
    """Mock _try_redis_price to return a fixed best_ask."""
    return patch(
        "routers.binary_options._try_redis_price",
        return_value=(best_ask, "fake-token-id"),
    )


def _mock_fill_market():
    """Mock _fill_market_from_rest to return a simple fill."""
    return patch(
        "routers.binary_options._fill_market_from_rest",
        return_value=(0.50, 20.0, "fake-token-id", [{"price": 0.50, "qty": 20.0, "cost": 10.0}]),
    )


def test_tp_above_best_ask_accepted(client, bot_with_key):
    """TP price > best_ask should pass pre-validation."""
    with _mock_redis_price(0.50), _mock_fill_market():
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
                "tp_price": 0.70,  # > 0.50 best_ask
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 201, resp.json()
    data = resp.json()
    assert data["tp_price"] == 0.70


def test_tp_at_best_ask_rejected(client, bot_with_key):
    """TP price <= best_ask should be rejected."""
    with _mock_redis_price(0.50):
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
                "tp_price": 0.50,  # == 0.50 best_ask → rejected
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 400
    assert "TP price" in resp.json()["detail"]
    assert "must be higher" in resp.json()["detail"]


def test_tp_below_best_ask_rejected(client, bot_with_key):
    """TP price < best_ask should be rejected."""
    with _mock_redis_price(0.50):
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
                "tp_price": 0.40,  # < 0.50 best_ask → rejected
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 400


def test_sl_below_best_ask_accepted(client, bot_with_key):
    """SL price < best_ask should pass pre-validation."""
    with _mock_redis_price(0.50), _mock_fill_market():
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
                "sl_price": 0.30,  # < 0.50 best_ask
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 201, resp.json()
    data = resp.json()
    assert data["sl_price"] == 0.30


def test_sl_at_best_ask_rejected(client, bot_with_key):
    """SL price >= best_ask should be rejected."""
    with _mock_redis_price(0.50):
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
                "sl_price": 0.50,  # == 0.50 best_ask → rejected
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 400
    assert "SL price" in resp.json()["detail"]
    assert "must be lower" in resp.json()["detail"]


def test_sl_above_best_ask_rejected(client, bot_with_key):
    """SL price > best_ask should be rejected."""
    with _mock_redis_price(0.60):
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
                "sl_price": 0.70,  # > 0.60 best_ask → rejected
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 400


def test_no_condition_skips_prevalidation(client, bot_with_key):
    """Order without TP or SL should skip pre-validation entirely."""
    with _mock_fill_market():
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 10.0,
            },
            headers={"x-api-key": bot_with_key.api_key},
        )
    assert resp.status_code == 201
