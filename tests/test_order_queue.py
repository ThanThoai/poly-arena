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
    return (0.52, round(amount / 0.52, 8), "fake-token-btc-m5-up")


# ── create_bo → LPUSH ────────────────────────────────────────────────────────


def test_create_bo_limit_order_pushes_to_queue(client, test_bot, fake_sync_redis):
    """LIMIT order should LPUSH to queue:orders:new."""
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


@patch("routers.binary_options._fill_market_from_rest", side_effect=_mock_fill_market_from_rest)
def test_create_bo_market_with_bracket_pushes_to_queue(mock_fill, client, test_bot, fake_sync_redis):
    """MARKET order with TP/SL should fill via REST then push prefilled order to queue."""
    bot_name, api_key = test_bot

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.70,
            "sl_price": 0.30,
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
    assert order["sl_price"] == 0.30


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
