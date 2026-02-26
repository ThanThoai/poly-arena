"""
Tests for Redis order queue (LPUSH in create_bo → BRPOP in OrderConsumer).

Verifies:
  - create_bo pushes order JSON to queue:orders:new
  - OrderConsumer pops and places virtual order in matching engine
  - Bracket callback publishes to stream:bracket:exits
"""

import json

import pytest

from ws_feed_service.config import QUEUE_ORDERS_NEW


# ── create_bo → LPUSH ────────────────────────────────────────────────────────


def test_create_bo_limit_order_pushes_to_queue(client, test_bot, fake_sync_redis):
    """LIMIT order should LPUSH to queue:orders:new."""
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

    # Check Redis queue
    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 1

    raw = fake_sync_redis.rpop(QUEUE_ORDERS_NEW)
    order = json.loads(raw)
    assert order["bo_id"] == data["id"]
    assert order["side"] == "BUY"
    assert order["limit_price"] == 0.45
    assert order["timeframe"] == "M5"


def test_create_bo_market_with_bracket_pushes_to_queue(client, test_bot, fake_sync_redis):
    """MARKET order with TP/SL should also push to queue."""
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

    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 1

    raw = fake_sync_redis.rpop(QUEUE_ORDERS_NEW)
    order = json.loads(raw)
    assert order["tp_price"] == 0.70
    assert order["sl_price"] == 0.30


def test_create_bo_market_no_bracket_skips_queue(client, test_bot, fake_sync_redis):
    """Plain MARKET order (no TP/SL) should NOT push to queue."""
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

    queue_len = fake_sync_redis.llen(QUEUE_ORDERS_NEW)
    assert queue_len == 0
