"""
Shared orderbook broadcaster — single Redis pub/sub connection, parse once,
fan-out to all connected WebSocket clients via asyncio.Queue.

Also provides:
- SnapshotCache: coalesced Redis reads with short TTL
- TokenDiscoveryCache: coalesced Polymarket token discovery with threadpool
"""

import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import redis.asyncio as aioredis

from config.timing import TF_SECONDS
from services.redis_client import get_async_redis, REDIS_URL
from ws_feed_service.config import ORDERBOOK_KEY_PREFIX

logger = logging.getLogger(__name__)

_OB_SYMBOLS = ["BTC", "ETH"]
_OB_DIRECTIONS = ["UP", "DOWN"]
_OB_TIMEFRAMES = ["M5", "M15"]


# ── Parsed message dataclass ────────────────────────────────────────────────

@dataclass
class ParsedOrderbookMessage:
    symbol: str
    timeframe: str
    direction: str
    session: int | None
    bids: list
    asks: list
    updated_at: str | None
    raw_payload: dict = field(repr=False)


# ── OrderbookBroadcaster ────────────────────────────────────────────────────

class OrderbookBroadcaster:
    """Single Redis pub/sub → parse once → fan-out via asyncio.Queue per client."""

    def __init__(self):
        self._subscribers: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._pubsub_redis: aioredis.Redis | None = None
        self._pubsub = None
        self._running = False

    async def start(self) -> None:
        """Connect to Redis pub/sub and start the listener loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._listen_loop(), name="orderbook-broadcaster")
        logger.info("OrderbookBroadcaster started")

    async def stop(self) -> None:
        """Shut down the broadcaster."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._cleanup_pubsub()
        logger.info("OrderbookBroadcaster stopped")

    @property
    def subscriber_count(self) -> int:
        """Number of currently connected subscribers."""
        return len(self._subscribers)

    @property
    def is_connected(self) -> bool:
        """Whether the pub/sub connection is active."""
        return self._running and self._pubsub is not None

    async def subscribe(self) -> asyncio.Queue:
        """Register a new client — returns a Queue that receives ParsedOrderbookMessage."""
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        async with self._lock:
            self._subscribers.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        """Remove a client queue."""
        async with self._lock:
            self._subscribers.discard(q)

    async def _listen_loop(self) -> None:
        """Main loop: connect to Redis pub/sub, parse messages, fan out."""
        while self._running:
            try:
                await self._connect_pubsub()
                async for raw_msg in self._pubsub.listen():
                    if not self._running:
                        break
                    if raw_msg["type"] != "message":
                        continue
                    parsed = self._parse_message(raw_msg["data"])
                    if parsed is None:
                        continue
                    await self._fan_out(parsed)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("Broadcaster pub/sub error: %s — reconnecting in 1s", exc)
                await self._cleanup_pubsub()
                await asyncio.sleep(1)

    async def _connect_pubsub(self) -> None:
        """Create a dedicated Redis connection for pub/sub."""
        self._pubsub_redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        self._pubsub = self._pubsub_redis.pubsub()
        await self._pubsub.subscribe("orderbook:updates")
        logger.info("Broadcaster subscribed to orderbook:updates")

    async def _cleanup_pubsub(self) -> None:
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe("orderbook:updates")
                await self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None
        if self._pubsub_redis:
            try:
                await self._pubsub_redis.aclose()
            except Exception:
                pass
            self._pubsub_redis = None

    @staticmethod
    def _parse_message(data: str) -> ParsedOrderbookMessage | None:
        """Parse raw JSON once → structured message."""
        try:
            payload = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return None

        try:
            bids_raw = payload["bids"]
            asks_raw = payload["asks"]
            bids = json.loads(bids_raw) if isinstance(bids_raw, str) else bids_raw
            asks = json.loads(asks_raw) if isinstance(asks_raw, str) else asks_raw
        except (json.JSONDecodeError, KeyError):
            return None

        return ParsedOrderbookMessage(
            symbol=payload.get("symbol", ""),
            timeframe=payload.get("timeframe", ""),
            direction=payload.get("direction", ""),
            session=payload.get("session"),
            bids=bids,
            asks=asks,
            updated_at=payload.get("updated_at"),
            raw_payload=payload,
        )

    async def _fan_out(self, msg: ParsedOrderbookMessage) -> None:
        """Send to all subscriber queues. Drop oldest on full (backpressure)."""
        async with self._lock:
            subscribers = list(self._subscribers)
        for q in subscribers:
            if q.full():
                try:
                    q.get_nowait()  # drop oldest
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass  # shouldn't happen after drop, but be safe


# ── SnapshotCache ───────────────────────────────────────────────────────────

class SnapshotCache:
    """Coalesced orderbook snapshot reads with short TTL.

    Multiple clients requesting the same snapshot within the TTL window
    share a single Redis read via an asyncio.Future.
    """

    def __init__(self, ttl: float = 2.0):
        self._ttl = ttl
        self._cache: dict[str, tuple[float, list[dict]]] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def get_snapshot(
        self,
        r: aioredis.Redis,
        symbol: str | None,
        timeframe: str | None,
    ) -> list[dict]:
        """Return snapshot entries for the given filter.

        Results are cached for ``_ttl`` seconds. Concurrent callers for the
        same key coalesce on a single Future.
        """
        cache_key = f"{symbol or '*'}:{timeframe or '*'}"
        now = time.monotonic()

        pending_fut: asyncio.Future | None = None
        is_owner = False

        async with self._lock:
            if cache_key in self._cache:
                ts, data = self._cache[cache_key]
                if now - ts < self._ttl:
                    return data

            if cache_key in self._pending:
                pending_fut = self._pending[cache_key]
            else:
                pending_fut = asyncio.get_event_loop().create_future()
                self._pending[cache_key] = pending_fut
                is_owner = True

        # Await outside lock — either we own the fetch or we wait on someone else's
        if not is_owner:
            return await pending_fut

        try:
            result = await self._fetch_snapshot(r, symbol, timeframe)
            async with self._lock:
                self._cache[cache_key] = (time.monotonic(), result)
                self._pending.pop(cache_key, None)
            pending_fut.set_result(result)
            return result
        except Exception as exc:
            async with self._lock:
                self._pending.pop(cache_key, None)
            pending_fut.set_exception(exc)
            raise

    @staticmethod
    async def _fetch_snapshot(
        r: aioredis.Redis,
        symbol: str | None,
        timeframe: str | None,
    ) -> list[dict]:
        """Read current + future session orderbook state from Redis."""
        syms = [symbol.upper()] if symbol else _OB_SYMBOLS
        tfs = [timeframe.upper()] if timeframe else _OB_TIMEFRAMES
        dirs = _OB_DIRECTIONS

        combos = [(s, t, d) for s in syms for t in tfs for d in dirs]
        if not combos:
            return []

        entries: list[dict] = []

        # Legacy keys (current session)
        pipe = r.pipeline(transaction=False)
        for sym, tf, dir_ in combos:
            pipe.hgetall(f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{dir_}")

        try:
            results = await pipe.execute()
        except Exception as exc:
            logger.warning("Snapshot cache Redis read failed: %s", exc)
            return []

        for (sym, tf, dir_), data in zip(combos, results):
            if not data:
                continue
            try:
                bids = json.loads(data.get("bids", "[]"))
                asks = json.loads(data.get("asks", "[]"))
            except (json.JSONDecodeError, TypeError):
                continue
            if not bids and not asks:
                continue
            entries.append({
                "type": "snapshot",
                "symbol": sym,
                "timeframe": tf,
                "direction": dir_,
                "bids": bids,
                "asks": asks,
                "updated_at": data.get("updated_at"),
            })

        # Future session keys (scan orderbook:{sym}:{tf}:{dir}:{ts})
        for sym, tf, dir_ in combos:
            pattern = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{dir_}:*"
            try:
                session_keys: list[str] = []
                async for key in r.scan_iter(match=pattern, count=50):
                    session_keys.append(key if isinstance(key, str) else key.decode())
                if not session_keys:
                    continue

                pipe2 = r.pipeline(transaction=False)
                for sk in session_keys:
                    pipe2.hgetall(sk)
                session_results = await pipe2.execute()

                for sk, data in zip(session_keys, session_results):
                    if not data:
                        continue
                    parts = sk.split(":")
                    if len(parts) < 5:
                        continue
                    try:
                        candle_ts = int(parts[-1])
                    except (ValueError, IndexError):
                        continue
                    try:
                        bids = json.loads(data.get("bids", "[]"))
                        asks = json.loads(data.get("asks", "[]"))
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if not bids and not asks:
                        continue
                    entries.append({
                        "type": "snapshot",
                        "symbol": sym,
                        "timeframe": tf,
                        "direction": dir_,
                        "session": candle_ts,
                        "bids": bids,
                        "asks": asks,
                        "updated_at": data.get("updated_at"),
                    })
            except Exception as exc:
                logger.debug(
                    "Snapshot cache future session scan failed for %s:%s:%s: %s",
                    sym, tf, dir_, exc,
                )

        return entries


# ── TokenDiscoveryCache ─────────────────────────────────────────────────────

class TokenDiscoveryCache:
    """Coalesced Polymarket token discovery with TTL.

    First caller runs the threadpool work; concurrent callers await the same
    Future. Results cached for ``ttl`` seconds.
    """

    def __init__(self, ttl: float = 30.0, max_workers: int = 4):
        self._ttl = ttl
        self._cache: dict[str, tuple[float, dict]] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="pm-discover",
        )

    async def discover(self, symbol: str, timeframe: str) -> dict | None:
        """Discover tokens for (symbol, timeframe). Coalesced + cached."""
        cache_key = f"{symbol.upper()}:{timeframe.upper()}"
        now = time.monotonic()

        pending_fut: asyncio.Future | None = None
        is_owner = False

        async with self._lock:
            if cache_key in self._cache:
                ts, data = self._cache[cache_key]
                if now - ts < self._ttl:
                    return data

            if cache_key in self._pending:
                pending_fut = self._pending[cache_key]
            else:
                pending_fut = asyncio.get_event_loop().create_future()
                self._pending[cache_key] = pending_fut
                is_owner = True

        if not is_owner:
            return await pending_fut

        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._executor,
                _discover_tokens_sync,
                symbol.upper(),
                timeframe.upper(),
            )
            async with self._lock:
                if result is not None:
                    self._cache[cache_key] = (time.monotonic(), result)
                self._pending.pop(cache_key, None)
            pending_fut.set_result(result)
            return result
        except Exception as exc:
            async with self._lock:
                self._pending.pop(cache_key, None)
            pending_fut.set_exception(exc)
            raise

    async def invalidate(self, symbol: str | None = None, timeframe: str | None = None) -> None:
        """Invalidate cached entries. If symbol/timeframe given, only that key."""
        async with self._lock:
            if symbol and timeframe:
                cache_key = f"{symbol.upper()}:{timeframe.upper()}"
                self._cache.pop(cache_key, None)
            else:
                self._cache.clear()


def _discover_tokens_sync(symbol: str, timeframe: str) -> dict | None:
    """Discover token IDs for current + 2 future sessions (runs in threadpool)."""
    from services.polymarket import PolymarketClient

    tf_upper = timeframe.upper()
    period = TF_SECONDS.get(tf_upper, 300)
    now = int(time.time())
    current_ts = now - (now % period)
    sessions = [current_ts, current_ts + period, current_ts + period * 2]

    token_map: dict[str, dict] = {}
    reverse_map: dict[tuple[str, int], str] = {}

    client = PolymarketClient()
    try:
        for ts in sessions:
            for direction in ("UP", "DOWN"):
                try:
                    token_id = client.get_token_id_at(symbol, timeframe, direction, ts)
                    if token_id:
                        token_map[token_id] = {
                            "direction": direction,
                            "session": ts,
                        }
                        reverse_map[(direction, ts)] = token_id
                except Exception as exc:
                    logger.debug(
                        "Token discovery failed: %s/%s/%s/%d: %s",
                        symbol, timeframe, direction, ts, exc,
                    )
    finally:
        client.close()

    if not token_map:
        return None

    return {
        "token_map": token_map,
        "reverse_map": reverse_map,
        "period": period,
    }


# ── Module-level singletons ─────────────────────────────────────────────────

broadcaster = OrderbookBroadcaster()
snapshot_cache = SnapshotCache(ttl=2.0)
token_cache = TokenDiscoveryCache(ttl=30.0, max_workers=4)
