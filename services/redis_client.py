"""
Redis client factory — singleton async and sync clients.

Uses REDIS_URL env var (default: redis://localhost:6379).
"""

import os
from typing import Optional

import redis
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_async_client: Optional[aioredis.Redis] = None
_sync_client: Optional[redis.Redis] = None


def get_async_redis() -> aioredis.Redis:
    """Return the async Redis singleton (create on first call)."""
    global _async_client
    if _async_client is None:
        _async_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _async_client


def get_sync_redis() -> redis.Redis:
    """Return the sync Redis singleton (create on first call)."""
    global _sync_client
    if _sync_client is None:
        _sync_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _sync_client


async def close_async_redis() -> None:
    """Close the async client (call on shutdown)."""
    global _async_client
    if _async_client is not None:
        await _async_client.aclose()
        _async_client = None


def close_sync_redis() -> None:
    """Close the sync client (call on shutdown)."""
    global _sync_client
    if _sync_client is not None:
        _sync_client.close()
        _sync_client = None
