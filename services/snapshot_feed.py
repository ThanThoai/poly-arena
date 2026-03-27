"""
SnapshotFeedConsumer — runs inside the FastAPI process.

Connects to Polymarket WS independently of the WS Feed Service process,
builds an in-memory SnapshotStore, and registers it as the global singleton
so the WS fill path in binary_options.py can read snapshots directly.

Token IDs are discovered from Redis (written by the WS Feed Service).
The consumer refreshes its subscription when token IDs change in Redis.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

from services.snapshot_store import SnapshotStore, set_store
from services.ws_feed import PolymarketFeed

log = logging.getLogger(__name__)

_SYMBOLS = ["BTC"]
_TIMEFRAMES = ["M5", "M15"]
_DIRECTIONS = ["UP", "DOWN"]
_TOKEN_REFRESH_INTERVAL_S = 30  # re-check Redis for new token_ids every 30s


def _read_all_token_ids() -> list[str]:
    """Read current token_ids from Redis token mapping keys."""
    try:
        from services.redis_client import get_sync_redis
        sr = get_sync_redis()
        ids: set[str] = set()
        for sym in _SYMBOLS:
            for tf in _TIMEFRAMES:
                key = f"tokens:{sym}:{tf}"
                raw = sr.get(key)
                if not raw:
                    continue
                data = json.loads(raw)
                for direction in _DIRECTIONS:
                    dir_data = data.get(direction)
                    if not dir_data:
                        continue
                    current = dir_data.get("current", {})
                    if current.get("token_id"):
                        ids.add(current["token_id"])
                    for entry in dir_data.get("future", []):
                        if entry.get("token_id"):
                            ids.add(entry["token_id"])
        return list(ids)
    except Exception as exc:
        log.warning("SnapshotFeedConsumer: failed to read token_ids from Redis: %s", exc)
        return []


class SnapshotFeedConsumer:
    """
    Lightweight WS consumer that lives inside the FastAPI process.

    Maintains a SnapshotStore by subscribing to the same Polymarket WS
    endpoint as WsFeedPoller. Only purpose: serve WS MARKET fills.
    """

    def __init__(self) -> None:
        self._store = SnapshotStore()
        self._feed: Optional[PolymarketFeed] = None
        self._token_ids: list[str] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def _on_event(self, event: dict) -> None:
        try:
            self._store.handle_event(event)
        except Exception as exc:
            log.debug("SnapshotFeedConsumer event error: %s", exc)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        set_store(self._store)
        self._task = asyncio.create_task(self._run(), name="snapshot-feed-consumer")
        log.info("SnapshotFeedConsumer started")

    async def stop(self) -> None:
        self._running = False
        if self._feed is not None:
            await self._feed.stop()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("SnapshotFeedConsumer stopped")

    async def _run(self) -> None:
        """Main loop: start feed, then periodically refresh token subscription."""
        # Wait for WS Feed Service to populate Redis token mappings
        token_ids = await self._wait_for_tokens()
        if not token_ids:
            log.warning("SnapshotFeedConsumer: no token_ids found, retrying in background")

        self._token_ids = token_ids
        self._feed = PolymarketFeed(list(self._token_ids), on_event=self._on_event)
        await self._feed.start()
        log.info("SnapshotFeedConsumer subscribed to %d token(s)", len(self._token_ids))

        while self._running:
            await asyncio.sleep(_TOKEN_REFRESH_INTERVAL_S)
            await self._refresh_tokens()

    async def _wait_for_tokens(self, timeout_s: float = 30.0) -> list[str]:
        """Poll Redis until token_ids are available or timeout."""
        elapsed = 0.0
        while elapsed < timeout_s:
            ids = _read_all_token_ids()
            if ids:
                return ids
            await asyncio.sleep(2.0)
            elapsed += 2.0
        return _read_all_token_ids()

    async def _refresh_tokens(self) -> None:
        """Check Redis for new token_ids; update subscription if changed."""
        try:
            new_ids = set(_read_all_token_ids())
            current_ids = set(self._token_ids)
            added = new_ids - current_ids
            removed = current_ids - new_ids

            if not added and not removed:
                return

            log.info(
                "SnapshotFeedConsumer: token refresh +%d -%d",
                len(added), len(removed),
            )

            # Evict removed tokens from store
            if removed:
                with self._store._lock:
                    for t in removed:
                        self._store._store.pop(t, None)

            self._token_ids = list(new_ids)
            if self._feed is not None:
                self._feed.token_ids = self._token_ids
                # Trigger resubscribe on the existing WS connection
                if self._feed._ws is not None and self._feed._running:
                    asyncio.create_task(self._feed._resubscribe(self._token_ids))
        except Exception as exc:
            log.warning("SnapshotFeedConsumer token refresh error: %s", exc)


# Module-level singleton for FastAPI lifespan
_consumer: Optional[SnapshotFeedConsumer] = None


async def start_snapshot_feed() -> None:
    """Call from FastAPI lifespan startup."""
    global _consumer
    _consumer = SnapshotFeedConsumer()
    await _consumer.start()


async def stop_snapshot_feed() -> None:
    """Call from FastAPI lifespan shutdown."""
    global _consumer
    if _consumer is not None:
        await _consumer.stop()
        _consumer = None
