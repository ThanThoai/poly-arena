"""
Token Registry — discovers and auto-refreshes Polymarket token IDs.

Why this is needed
──────────────────
Each Polymarket candle market has a UNIQUE token_id per candle period because
the slug encodes the candle's settlement timestamp:

    M5  slug:  btc-updown-5m-{settlement_ts}
    M15 slug:  btc-updown-15m-{settlement_ts}
    H1  slug:  bitcoin-up-or-down-{month}-{day}-{hour}{am|pm}-et

where settlement_ts = candle open Unix timestamp (changes every candle).

Timeline (M5 example):
    12:10:00  candle opens  → token_id = abc...  subscribed
    12:14:58  still same candle
    12:15:00  NEW candle    → NEW token_id = def...
    12:15:05  (+ 5s offset) → TokenRegistry detects boundary, fetches def...,
                               calls on_new_tokens(["def..."]) → WS re-subscribes

Usage
─────
    registry = TokenRegistry(on_new_tokens=lambda ids: feed.add_tokens(ids))
    token_ids = registry.discover_all()   # synchronous, call before event loop
    await registry.start()                # starts background refresh loop
    ...
    await registry.stop()
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from config.timing import (
    HTTP_TIMEOUT,
    HTTP_TIMEOUT_DISCOVERY,
    TOKEN_PREFETCH_CANDLES,
    TOKEN_REFRESH_OFFSET_S,
    TOKEN_REFRESH_MAX_RETRIES,
    TOKEN_REFRESH_RETRY_DELAY_S,
    TF_SECONDS as _TF_SECONDS,
)
from services.polymarket import PolymarketClient

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_SYMBOLS: list[str]    = ["BTC", "ETH", "SOL", "XRP"]
_DIRECTIONS: list[str] = ["UP", "DOWN"]
_TIMEFRAMES: list[str] = ["M5", "M15", "H1"]

_PREFETCH_CANDLES: int = TOKEN_PREFETCH_CANDLES

_REFRESH_OFFSET_S: int   = TOKEN_REFRESH_OFFSET_S
_REFRESH_MAX_RETRIES: int = TOKEN_REFRESH_MAX_RETRIES
_REFRESH_RETRY_DELAY: int = TOKEN_REFRESH_RETRY_DELAY_S


# ── TokenRegistry ─────────────────────────────────────────────────────────────


class TokenRegistry:
    """
    Maps (symbol, timeframe, direction) → current token_id.

    Runs a background asyncio task that wakes up at each candle boundary
    (+ REFRESH_OFFSET_S), re-fetches token_ids via Polymarket REST, and
    calls `on_new_tokens` for any IDs that changed so the WS Feed can
    subscribe to the new markets.
    """

    def __init__(
        self,
        on_new_tokens: Optional[Callable[[list[str]], None]] = None,
        symbols:    Optional[list[str]] = None,
        timeframes: Optional[list[str]] = None,
    ) -> None:
        self._on_new_tokens = on_new_tokens
        self._symbols    = [s.upper() for s in (symbols    or _SYMBOLS)]
        self._timeframes = [t.upper() for t in (timeframes or _TIMEFRAMES)]
        # (symbol, timeframe, direction) → token_id  (current candle)
        self._mapping: dict[tuple[str, str, str], str] = {}
        # (symbol, timeframe, direction) → [token_id_1, ..., token_id_N]
        # future candles (index 0 = next candle, index 4 = 5th candle ahead)
        self._future_mapping: dict[tuple[str, str, str], list[str]] = {}
        # token_id → candle_open_ts (session timestamp for each token)
        self._token_sessions: dict[str, int] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # token_id → (bids, asks) from REST discovery — used to seed Redis
        self._initial_books: dict[str, tuple[list[tuple[float, float]], list[tuple[float, float]]]] = {}

    # ── Public helpers ────────────────────────────────────────────────────────

    def get_token_id(self, symbol: str, timeframe: str, direction: str) -> Optional[str]:
        """Return the current token_id for (symbol, timeframe, direction), or None."""
        key = (symbol.upper(), timeframe.upper(), direction.upper())
        return self._mapping.get(key)

    def get_future_token_ids(
        self, symbol: str, timeframe: str, direction: str,
    ) -> list[str]:
        """Return prefetched token_ids for the next N candles (may be fewer if some failed)."""
        key = (symbol.upper(), timeframe.upper(), direction.upper())
        return list(self._future_mapping.get(key, []))

    def all_token_ids(self) -> list[str]:
        """Return all currently-known token_ids (current + future, deduplicated)."""
        ids: list[str] = list(self._mapping.values())
        for future_ids in self._future_mapping.values():
            ids.extend(future_ids)
        return list(dict.fromkeys(ids))

    def get_all_token_mapping(self) -> dict[str, list[tuple[str, str, str, int]]]:
        """
        Return reverse map: token_id → [(sym, tf, dir, candle_ts), ...].

        Includes both current and future tokens with their session timestamps.
        """
        result: dict[str, list[tuple[str, str, str, int]]] = {}
        for (sym, tf, direction), token_id in self._mapping.items():
            ts = self._token_sessions.get(token_id, 0)
            result.setdefault(token_id, []).append((sym, tf, direction, ts))
        for (sym, tf, direction), future_ids in self._future_mapping.items():
            for token_id in future_ids:
                ts = self._token_sessions.get(token_id, 0)
                result.setdefault(token_id, []).append((sym, tf, direction, ts))
        return result

    def pop_initial_books(self) -> dict[str, tuple[list[tuple[float, float]], list[tuple[float, float]]]]:
        """Return and clear initial book data collected during discovery/refresh.

        Returns: {token_id: (bids, asks)} where bids/asks are [(price, size), ...].
        """
        books = dict(self._initial_books)
        self._initial_books.clear()
        return books

    def get_current_candle_open(self, tf: str) -> int:
        """Return the candle-open timestamp for the current candle of the given timeframe."""
        period_s = _TF_SECONDS[tf.upper()]
        now = int(datetime.now(timezone.utc).timestamp())
        return now - (now % period_s)

    # ── Initial discovery ─────────────────────────────────────────────────────

    def discover_all(self) -> list[str]:
        """
        Fetch token_ids for all (symbol × timeframe × direction) combos
        for the CURRENT candle + next N future candles.
        Blocking — call before starting the event loop.

        Returns a list of all discovered token_ids (current + future).
        Missing combos are skipped with a warning so a single REST error
        does not abort startup.
        """
        discovered: list[str] = []

        try:
            with PolymarketClient(timeout=HTTP_TIMEOUT_DISCOVERY) as pm:
                for sym in self._symbols:
                    for tf in self._timeframes:
                        for direction in _DIRECTIONS:
                            # ── Current candle ──
                            try:
                                ob = pm.get_orderbook(sym, tf, direction)
                                key = (sym, tf, direction)
                                self._mapping[key] = ob.token_id
                                discovered.append(ob.token_id)
                                # Store initial book depth for Redis seeding
                                if ob.bids is not None and ob.asks is not None:
                                    self._initial_books[ob.token_id] = (ob.bids, ob.asks)
                                # Track session timestamp for current candle
                                period_s = _TF_SECONDS[tf]
                                now_ts = int(time.time())
                                candle_open = now_ts - (now_ts % period_s)
                                self._token_sessions[ob.token_id] = candle_open
                                logger.info(
                                    "Token discovered: %s %s %s → %s",
                                    sym, tf, direction, ob.token_id[:24],
                                )
                            except Exception as exc:
                                logger.warning(
                                    "Token discovery skipped: %s %s %s — %s",
                                    sym, tf, direction, exc,
                                )
                                continue

                            # ── Future candles ──
                            self._prefetch_future(pm, sym, tf, direction, discovered)
        except Exception as exc:
            logger.error("PolymarketClient error during discovery: %s", exc)

        total_expected = len(self._symbols) * len(self._timeframes) * len(_DIRECTIONS)
        logger.info(
            "TokenRegistry: discovered %d current + %d future token IDs (expected %d combos)",
            len(self._mapping), sum(len(v) for v in self._future_mapping.values()),
            total_expected,
        )
        return discovered

    def _prefetch_future(
        self,
        pm: PolymarketClient,
        sym: str,
        tf: str,
        direction: str,
        discovered: list[str],
    ) -> None:
        """Prefetch token_ids for the next N future candles."""
        tf_norm = {"M5": "5m", "M15": "15m", "H1": "1h"}.get(tf, tf.lower())
        future_timestamps = pm._future_settlements(tf_norm, _PREFETCH_CANDLES)
        key = (sym, tf, direction)
        future_ids: list[str] = []

        for i, ts in enumerate(future_timestamps):
            try:
                token_id = pm.get_token_id_at(sym, tf, direction, ts)
                if token_id:
                    future_ids.append(token_id)
                    discovered.append(token_id)
                    # Track session timestamp for future candle
                    self._token_sessions[token_id] = ts
                    logger.info(
                        "Future token [+%d]: %s %s %s ts=%d → %s",
                        i + 1, sym, tf, direction, ts, token_id[:24],
                    )
                else:
                    logger.debug(
                        "Future token [+%d]: %s %s %s ts=%d — not available yet",
                        i + 1, sym, tf, direction, ts,
                    )
            except Exception as exc:
                logger.debug(
                    "Future token [+%d]: %s %s %s ts=%d — %s",
                    i + 1, sym, tf, direction, ts, exc,
                )

        self._future_mapping[key] = future_ids

    # ── Background refresh loop ───────────────────────────────────────────────

    async def start(self) -> None:
        """Start the background refresh task (non-blocking)."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._refresh_loop(), name="token-registry-refresh")
        logger.info("TokenRegistry refresh loop started")

    async def stop(self) -> None:
        """Stop the background refresh task."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("TokenRegistry stopped")

    async def _refresh_loop(self) -> None:
        """
        Main loop:
          1. Calculate next refresh timestamp = next candle boundary + offset
             for each timeframe.
          2. Sleep until the earliest one.
          3. Refresh all timeframes whose boundary just passed.
          4. Repeat.
        """
        while self._running:
            now_ts = datetime.now(timezone.utc).timestamp()

            # ── Compute next refresh time for each tracked timeframe ──────────
            schedule: list[tuple[float, str]] = []
            for tf in self._timeframes:
                period_s = _TF_SECONDS[tf]
                next_boundary_ts = (math.floor(now_ts / period_s) + 1) * period_s
                refresh_ts       = next_boundary_ts + _REFRESH_OFFSET_S
                schedule.append((refresh_ts, tf))

            schedule.sort()
            next_ts, _ = schedule[0]

            sleep_s = next_ts - now_ts
            if sleep_s > 0:
                logger.debug(
                    "TokenRegistry: next refresh in %.0fs  (%s)",
                    sleep_s, _tf_labels(schedule),
                )
                try:
                    await asyncio.sleep(sleep_s)
                except asyncio.CancelledError:
                    return

            if not self._running:
                return

            # ── Refresh timeframes whose boundary is now (±30s tolerance) ────
            now_ts = datetime.now(timezone.utc).timestamp()
            for refresh_ts, tf in schedule:
                if abs(refresh_ts - now_ts) <= 30:
                    await self._refresh_timeframe_with_retry(tf)

    async def _refresh_timeframe_with_retry(self, tf: str) -> None:
        """
        Retry fetching new token_ids up to _REFRESH_MAX_RETRIES times.

        Polymarket may take a few seconds after candle close to publish
        the new market.  Each retry waits _REFRESH_RETRY_DELAY seconds.

        IMPORTANT: At a candle boundary we EXPECT token_ids to change.
        If the first fetch returns the same token_ids as before, Polymarket
        hasn't published the new market yet — we must keep retrying, NOT
        treat "same tokens" as success.
        """
        logger.info("Candle boundary — refreshing tokens for %s", tf)

        for attempt in range(1, _REFRESH_MAX_RETRIES + 1):
            new_ids = await self._fetch_timeframe(tf)

            if new_ids is not None:
                if new_ids:
                    # Token IDs actually changed → new session discovered
                    logger.info(
                        "TokenRegistry [%s] attempt %d/%d: %d new token(s)",
                        tf, attempt, _REFRESH_MAX_RETRIES, len(new_ids),
                    )
                    if self._on_new_tokens:
                        self._on_new_tokens(new_ids)
                    return  # success — new tokens found
                else:
                    # Fetch succeeded but token_ids are unchanged.
                    # At candle boundary this means Polymarket hasn't
                    # published the new market yet — keep retrying.
                    logger.warning(
                        "TokenRegistry [%s] attempt %d/%d: same tokens (market not rotated yet), "
                        "retrying in %ds",
                        tf, attempt, _REFRESH_MAX_RETRIES, _REFRESH_RETRY_DELAY,
                    )
                    # Fall through to retry below

            else:
                # fetch returned None → all requests failed
                logger.warning(
                    "TokenRegistry [%s] attempt %d/%d: market not ready, retrying in %ds",
                    tf, attempt, _REFRESH_MAX_RETRIES, _REFRESH_RETRY_DELAY,
                )

            try:
                await asyncio.sleep(_REFRESH_RETRY_DELAY)
            except asyncio.CancelledError:
                return

        logger.error(
            "TokenRegistry [%s]: could not refresh after %d attempts — "
            "WS Feed will use stale token IDs until next candle",
            tf, _REFRESH_MAX_RETRIES,
        )

    async def _fetch_timeframe(self, tf: str) -> Optional[list[str]]:
        """
        Fetch token_ids for all symbols/directions for the given timeframe.
        Also refreshes future candle token_ids in the background.

        Returns:
          list[str]  — list of *changed* current candle token_ids
                       (may be empty if no rotation detected)
          None       — all requests failed (market not published yet)

        Side effect: updates self._future_mapping with the next N candle token_ids.
        """

        def _blocking_fetch() -> tuple[list[str], bool]:
            """Run in executor — blocking HTTP calls."""
            local_new: list[str] = []
            local_ok = False

            with PolymarketClient(timeout=HTTP_TIMEOUT) as pm:
                for sym in self._symbols:
                    for direction in _DIRECTIONS:
                        # ── Current candle ──
                        try:
                            ob = pm.get_orderbook(sym, tf, direction)
                            key = (sym, tf, direction)
                            old_id = self._mapping.get(key)
                            self._mapping[key] = ob.token_id
                            # Store initial book depth for Redis seeding
                            if ob.bids is not None and ob.asks is not None:
                                self._initial_books[ob.token_id] = (ob.bids, ob.asks)
                            # Track session timestamp for rotated candle
                            period_s = _TF_SECONDS[tf]
                            now_epoch = int(time.time())
                            self._token_sessions[ob.token_id] = now_epoch - (now_epoch % period_s)
                            local_ok = True
                            if ob.token_id != old_id:
                                local_new.append(ob.token_id)
                                logger.info(
                                    "Token rotated: %s %s %s  %s → %s",
                                    sym, tf, direction,
                                    (old_id or "none")[:16],
                                    ob.token_id[:16],
                                )
                        except Exception as exc:
                            logger.warning(
                                "Token refresh failed: %s %s %s — %s",
                                sym, tf, direction, exc,
                            )
                            continue

                        # ── Future candles (prefetch silently) ──
                        _scratch: list[str] = []
                        self._prefetch_future(pm, sym, tf, direction, _scratch)

            return local_new, local_ok

        loop = asyncio.get_event_loop()
        new_token_ids, any_success = await loop.run_in_executor(None, _blocking_fetch)

        if not any_success:
            return None   # signal: market not ready, caller should retry
        return new_token_ids


# ── Helpers ───────────────────────────────────────────────────────────────────


def _tf_labels(schedule: list[tuple[float, str]]) -> str:
    """Format schedule as human-readable string for logging."""
    now = datetime.now(timezone.utc).timestamp()
    parts = [f"{tf}+{ts - now:.0f}s" for ts, tf in schedule]
    return ", ".join(parts)


def next_refresh_times(timeframes: Optional[list[str]] = None) -> dict[str, datetime]:
    """
    Return the next refresh datetime for each timeframe (for monitoring/debug).

    Example:
        {
          "M5":  datetime(2024, 1, 1, 12, 15, 5, tzinfo=utc),
          "M15": datetime(2024, 1, 1, 12, 15, 5, tzinfo=utc),
          "H1":  datetime(2024, 1, 1, 13,  0, 5, tzinfo=utc),
        }
    """
    tfs = timeframes or _TIMEFRAMES
    now_ts = datetime.now(timezone.utc).timestamp()
    result = {}
    for tf in tfs:
        period_s = _TF_SECONDS[tf]
        next_boundary_ts = (math.floor(now_ts / period_s) + 1) * period_s
        refresh_ts = next_boundary_ts + _REFRESH_OFFSET_S
        result[tf] = datetime.fromtimestamp(refresh_ts, tz=timezone.utc)
    return result
