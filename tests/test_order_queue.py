"""
Tests for Redis order queue (LPUSH in create_bo → BRPOP in OrderConsumer).

Verifies:
  - create_bo pushes order JSON to queue:orders:new
  - OrderConsumer pops and places virtual order in matching engine
  - Bracket callback publishes to stream:bracket:exits
"""

import json
from unittest.mock import patch, MagicMock

import pytest

from ws_feed_service.config import QUEUE_ORDERS_NEW


def _mock_fill_market_from_rest(symbol, timeframe, pm_status, amount, slippage_tolerance):
    """Mock REST fill returning predictable values."""
    walk_levels = [{"price": 0.52, "qty": round(amount / 0.52, 8), "cost": amount}]
    return (0.52, round(amount / 0.52, 8), "fake-token-btc-m5-up", walk_levels)


# ── create_bo → LPUSH ────────────────────────────────────────────────────────


@patch("routers.binary_options._try_fill_limit_from_rest", return_value=None)
def test_create_bo_limit_order_pushes_to_queue(mock_rest, client, test_bot, fake_sync_redis):
    """LIMIT order where best_ask > limit should LPUSH to queue:orders:new."""
    bot_name, api_key = test_bot

    # Seed Redis with token_id so _get_token_id_from_redis() returns a value.
    # In production this is written by the WS Feed Service (TokenRegistry).
    fake_sync_redis.hset("price:BTC:M5:UP", mapping={
        "token_id": "fake-token-btc-m5-up",
        "best_ask": "0.52",
        "updated_at": "9999999999",
    })

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["limit_price"] == 0.45

    # Check Redis queue
    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 1

    raw = fake_sync_redis.rpop(QUEUE_ORDERS_NEW)
    order = json.loads(raw)
    assert order["bo_id"] == data["id"]
    assert order["side"] == "BUY"
    assert order["limit_price"] == 0.45
    assert order["timeframe"] == "M5"


def _mock_try_fill_limit(symbol, timeframe, pm_status, amount, limit_price):
    """Mock REST fill for LIMIT order that CAN fill now."""
    walk_levels = [{"price": 0.42, "qty": round(amount / 0.42, 8), "cost": amount}]
    return (0.42, round(amount / 0.42, 8), "fake-token-btc-m5-up", walk_levels)


@patch("routers.binary_options._try_fill_limit_from_rest", side_effect=_mock_try_fill_limit)
def test_create_bo_limit_immediate_fill(mock_rest, client, test_bot, fake_sync_redis):
    """LIMIT order where best_ask <= limit should fill immediately via REST."""
    bot_name, api_key = test_bot

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["limit_price"] == 0.45
    # Should be filled immediately
    assert data["avg_price"] == pytest.approx(0.42, abs=0.01)
    assert data["num_shares"] is not None
    # No queue push — filled at REST level, no bracket
    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 0


@patch("routers.binary_options._try_fill_limit_from_rest", side_effect=_mock_try_fill_limit)
def test_create_bo_limit_immediate_fill_with_bracket(mock_rest, client, test_bot, fake_sync_redis):
    """LIMIT order with TP that fills immediately should push prefilled to ME."""
    bot_name, api_key = test_bot

    fake_sync_redis.hset("price:BTC:M5:UP", mapping={
        "token_id": "fake-token-btc-m5-up",
        "best_ask": "0.42",
        "updated_at": "9999999999",
    })

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,
            "tp_price": 0.70,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["avg_price"] == pytest.approx(0.42, abs=0.01)
    assert data["me_order_status"] == "FILLED"

    # Bracket → should push prefilled order to ME for TP monitoring
    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 1

    raw = fake_sync_redis.rpop(QUEUE_ORDERS_NEW)
    order = json.loads(raw)
    assert order["prefilled"] is True
    assert order["tp_price"] == 0.70


@patch("routers.binary_options._try_redis_price", return_value=(0.52, "fake-token-btc-m5-up"))
@patch("routers.binary_options._fill_market_from_rest", side_effect=_mock_fill_market_from_rest)
def test_create_bo_market_with_bracket_pushes_to_queue(mock_fill, mock_price, client, test_bot, fake_sync_redis):
    """MARKET order with TP should fill via REST then push prefilled order to queue.

    v2 spec: single condition policy — only TP or SL, not both.
    Pre-validation needs best_ask to validate TP > best_ask.
    """
    bot_name, api_key = test_bot

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.70,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    # MARKET orders are now filled immediately via REST
    assert data["avg_price"] == 0.52
    assert data["num_shares"] is not None
    assert data["me_order_status"] == "PREFILLED"

    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 1

    raw = fake_sync_redis.rpop(QUEUE_ORDERS_NEW)
    order = json.loads(raw)
    assert order["prefilled"] is True
    assert order["prefilled_avg_price"] == 0.52
    assert order["tp_price"] == 0.70
    assert order["sl_price"] is None


@patch("routers.binary_options._fill_market_from_rest", side_effect=_mock_fill_market_from_rest)
def test_create_bo_market_no_bracket_skips_queue(mock_fill, client, test_bot, fake_sync_redis):
    """Plain MARKET order (no TP/SL) fills via REST and does NOT push to queue."""
    bot_name, api_key = test_bot

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    # MARKET orders are now filled immediately via REST
    assert data["avg_price"] == 0.52
    assert data["num_shares"] is not None
    assert data["me_order_status"] is None

    # No queue push — scheduler settles, no ME involvement
    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 0
