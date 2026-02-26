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


# ── _try_redis_price ─────────────────────────────────────────────────────────


def test_try_redis_price_hit(fake_sync_redis):
    """_try_redis_price should return (price, token_id) for fresh cache."""
    key = f"{PRICE_KEY_PREFIX}:ETH:M15:DOWN"
    fake_sync_redis.hset(key, mapping={
        "best_ask": "0.4500",
        "token_id": "token-eth-m15-down-999",
        "updated_at": str(time.time()),
    })

    from routers.binary_options import _try_redis_price
    price, token_id = _try_redis_price("ETH", "M15", "DOWN")
    assert price == 0.45
    assert token_id == "token-eth-m15-down-999"


def test_try_redis_price_miss(fake_sync_redis):
    """_try_redis_price should return (None, None) when key doesn't exist."""
    from routers.binary_options import _try_redis_price
    price, token_id = _try_redis_price("SOL", "H1", "UP")
    assert price is None
    assert token_id is None


def test_try_redis_price_stale(fake_sync_redis):
    """Prices older than STALE_THRESHOLD_S should be treated as miss."""
    key = f"{PRICE_KEY_PREFIX}:BTC:M5:UP"
    stale_ts = time.time() - STALE_THRESHOLD_S - 10  # 10s beyond threshold
    fake_sync_redis.hset(key, mapping={
        "best_ask": "0.5000",
        "token_id": "token-old",
        "updated_at": str(stale_ts),
    })

    from routers.binary_options import _try_redis_price
    price, token_id = _try_redis_price("BTC", "M5", "UP")
    assert price is None
    assert token_id is None
