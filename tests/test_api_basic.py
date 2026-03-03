"""
Basic API tests (health, CRUD) using test DB and fake Redis.

Verifies the FastAPI app starts correctly with the test environment.
"""

import json
import time
import pytest

from models import BOResult
from ws_feed_service.config import ORDERBOOK_KEY_PREFIX
from config.timing import TF_SECONDS


def _current_candle_open(tf="M5"):
    period_s = TF_SECONDS[tf]
    now_ts = int(time.time())
    return now_ts - (now_ts % period_s)


def _seed_orderbook(redis_client, symbol="BTC", tf="M5", direction="UP"):
    """Seed a fake orderbook snapshot in Redis using session-keyed key."""
    candle_open = _current_candle_open(tf)
    key = f"{ORDERBOOK_KEY_PREFIX}:{symbol}:{tf}:{direction}:{candle_open}"
    asks = [[0.52, 500.0], [0.53, 300.0], [0.54, 200.0]]
    redis_client.hset(key, mapping={
        "asks": json.dumps(asks),
        "bids": json.dumps([[0.51, 400.0], [0.50, 600.0]]),
        "updated_at": str(time.time()),
    })


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


def test_create_bo_market_order(client, test_bot, fake_sync_redis):
    """MARKET order should fill immediately from orderbook snapshot."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis)

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
    assert data["bot_name"] == bot_name
    assert data["symbol"] == "BTC"
    assert data["timeframe"] == "M5"
    assert data["forecast"] == "GREEN"
    assert data["amount"] == 10.0
    assert data["result"] == BOResult.PENDING.value
    assert data["avg_price"] is not None
    assert data["avg_price"] > 0
    assert data["num_shares"] is not None
    assert data["me_order_status"] is None  # No bracket → not queued to ME


def test_create_bo_invalid_api_key(client):
    """Invalid API key should return 401."""
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": "bad-key"},
    )
    assert resp.status_code == 401


def test_list_bo_empty(client):
    """Should return empty list when no BOs exist."""
    resp = client.get("/poly-arena/binary-options/")
    assert resp.status_code == 200
    assert resp.json() == []


def test_stats_summary_empty(client):
    """Stats should work with empty DB."""
    resp = client.get("/poly-arena/binary-options/stats/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["wins"] == 0
    assert data["losses"] == 0
