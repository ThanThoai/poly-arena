"""
Tests for Redis price cache (RedisWriter → _try_redis_price).

Verifies:
  - RedisWriter writes price data to correct Redis keys
  - _try_redis_price reads and validates cache entries
  - Stale prices are treated as cache miss
  - Missing keys return (None, None)
"""

import time

import pytest

from ws_feed_service.config import PRICE_KEY_PREFIX, STALE_THRESHOLD_S
from ws_feed_service.redis_writer import RedisWriter


# ── RedisWriter.update_price ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_price_writes_hash(fake_async_redis):
    """RedisWriter should HSET price:{SYM}:{TF}:{DIR} with best_ask, token_id, updated_at."""
    writer = RedisWriter(fake_async_redis)
    writer.register_token_mapping({
        ("BTC", "M5", "UP"): "token-btc-m5-up-001",
    })

    await writer.update_price("token-btc-m5-up-001", 0.5200)

    key = f"{PRICE_KEY_PREFIX}:BTC:M5:UP"
    data = await fake_async_redis.hgetall(key)
    assert data["best_ask"] == "0.52"
    assert data["token_id"] == "token-btc-m5-up-001"
    assert "updated_at" in data
    # TTL should be set
    ttl = await fake_async_redis.ttl(key)
    assert ttl > 0


@pytest.mark.asyncio
async def test_update_price_multiple_combos(fake_async_redis):
    """One token_id can map to multiple (sym, tf, dir) combos."""
    writer = RedisWriter(fake_async_redis)
    writer.register_token_mapping({
        ("BTC", "M5", "UP"): "token-abc",
        ("BTC", "M5", "DOWN"): "token-def",
    })

    await writer.update_price("token-abc", 0.60)
    await writer.update_price("token-def", 0.40)

    up_data = await fake_async_redis.hgetall(f"{PRICE_KEY_PREFIX}:BTC:M5:UP")
    down_data = await fake_async_redis.hgetall(f"{PRICE_KEY_PREFIX}:BTC:M5:DOWN")
    assert up_data["best_ask"] == "0.6"
    assert down_data["best_ask"] == "0.4"


@pytest.mark.asyncio
async def test_update_price_none_is_noop(fake_async_redis):
    """update_price with None best_ask should not write anything."""
    writer = RedisWriter(fake_async_redis)
    writer.register_token_mapping({
        ("BTC", "M5", "UP"): "token-abc",
    })

    await writer.update_price("token-abc", None)

    key = f"{PRICE_KEY_PREFIX}:BTC:M5:UP"
    data = await fake_async_redis.hgetall(key)
    assert data == {}


@pytest.mark.asyncio
async def test_update_price_unknown_token_is_noop(fake_async_redis):
    """update_price for unregistered token_id should not write."""
    writer = RedisWriter(fake_async_redis)
    writer.register_token_mapping({})

    await writer.update_price("unknown-token", 0.55)

    keys = await fake_async_redis.keys(f"{PRICE_KEY_PREFIX}:*")
    assert len(keys) == 0


# ── _snapshot_best_ask ────────────────────────────────────────────────────────


def test_snapshot_best_ask_hit(fake_sync_redis):
    """_snapshot_best_ask should return best_ask from session-keyed orderbook."""
    import json
    from ws_feed_service.config import ORDERBOOK_KEY_PREFIX
    candle_open = 1709313000
    key = f"{ORDERBOOK_KEY_PREFIX}:ETH:M15:DOWN:{candle_open}"
    fake_sync_redis.hset(key, mapping={
        "asks": json.dumps([[0.45, 500.0], [0.46, 300.0]]),
        "bids": json.dumps([[0.44, 400.0]]),
        "updated_at": str(time.time()),
    })

    from routers.binary_options import _snapshot_best_ask
    price = _snapshot_best_ask("ETH", "M15", "DOWN", candle_open=candle_open)
    assert price == 0.45


def test_snapshot_best_ask_miss(fake_sync_redis):
    """_snapshot_best_ask should return None when key doesn't exist."""
    from routers.binary_options import _snapshot_best_ask
    price = _snapshot_best_ask("BTC", "M5", "UP", candle_open=9999999999)
    assert price is None


def test_snapshot_best_ask_empty_asks(fake_sync_redis):
    """_snapshot_best_ask should return None when asks list is empty."""
    import json
    from ws_feed_service.config import ORDERBOOK_KEY_PREFIX
    candle_open = 1709313000
    key = f"{ORDERBOOK_KEY_PREFIX}:BTC:M5:UP:{candle_open}"
    fake_sync_redis.hset(key, mapping={
        "asks": json.dumps([]),
        "bids": json.dumps([[0.50, 600.0]]),
        "updated_at": str(time.time()),
    })

    from routers.binary_options import _snapshot_best_ask
    price = _snapshot_best_ask("BTC", "M5", "UP", candle_open=candle_open)
    assert price is None
