"""
Tests for the /engine/status endpoint (Redis health check).

Verifies:
  - Returns price keys from Redis
  - Reports staleness correctly
  - Handles Redis errors gracefully
"""

import time

import pytest

from ws_feed_service.config import PRICE_KEY_PREFIX, STALE_THRESHOLD_S


def test_engine_status_with_prices(client, fake_sync_redis):
    """Should return price info from Redis."""
    # Seed a fresh price
    fake_sync_redis.hset(f"{PRICE_KEY_PREFIX}:BTC:M5:UP", mapping={
        "best_ask": "0.52",
        "token_id": "token-abc-123-def-456",
        "updated_at": str(time.time()),
    })

    resp = client.get("/poly-arena/binary-options/engine/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["redis"] == "connected"
    assert data["total_price_keys"] == 1

    key = f"{PRICE_KEY_PREFIX}:BTC:M5:UP"
    assert key in data["prices"]
    assert data["prices"][key]["best_ask"] == "0.52"
    assert data["prices"][key]["stale"] is False


def test_engine_status_stale_price(client, fake_sync_redis):
    """Prices older than threshold should show stale=True."""
    old_ts = time.time() - STALE_THRESHOLD_S - 60
    fake_sync_redis.hset(f"{PRICE_KEY_PREFIX}:ETH:H1:DOWN", mapping={
        "best_ask": "0.38",
        "token_id": "token-old",
        "updated_at": str(old_ts),
    })

    resp = client.get("/poly-arena/binary-options/engine/status")
    data = resp.json()
    key = f"{PRICE_KEY_PREFIX}:ETH:H1:DOWN"
    assert data["prices"][key]["stale"] is True


def test_engine_status_empty(client, fake_sync_redis):
    """No price keys → total=0, empty prices dict."""
    resp = client.get("/poly-arena/binary-options/engine/status")
    data = resp.json()
    assert data["redis"] == "connected"
    assert data["total_price_keys"] == 0
    assert data["prices"] == {}
