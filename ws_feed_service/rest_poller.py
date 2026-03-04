"""
REST Poller — periodic orderbook fetching from Polymarket CLOB REST API.

Replaces WS-driven matching entirely: every N ms, fetch orderbook data from
the REST API for all active sessions, apply snapshots, run matching, and
write prices + orderbook depth to Redis for the UI.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Optional

from services.polymarket import PolymarketClient
from services.session_manager import SessionManager
from services.session_engine import SessionState
from config.timing import REST_POLL_INTERVAL_S, REST_POLL_TIMEOUT_S, REST_POLL_MAX_CONCURRENT

logger = logging.getLogger(__name__)

# Re-export for convenience
REST_POLL_INTERVAL = REST_POLL_INTERVAL_S
REST_POLL_TIMEOUT = REST_POLL_TIMEOUT_S


class RestPoller:
    """
    Periodically polls Polymarket REST API for orderbook data, applies to
    the matching engine, and writes prices/depth to Redis.

    Two modes of token selection per cycle:
    - **Matching tokens**: sessions with pending/bracket orders → apply snapshot + run matching
    - **All active tokens**: all non-archived sessions → write prices to Redis for UI
    """

    def __init__(
        self,
        session_manager: SessionManager,
        writer=None,
        pm_client: Optional[PolymarketClient] = None,
        interval: float = REST_POLL_INTERVAL,
    ) -> None:
        self._sm = session_manager
        self._writer = writer  # RedisWriter (optional, for price/depth publishing)
        self._pm = pm_client or PolymarketClient(timeout=REST_POLL_TIMEOUT)
        self._interval = interval
        self._running = False
        self._poll_count = 0

    async def start(self) -> None:
        """Start the polling loop."""
        self._running = True
        logger.info(
            "RestPoller started (interval=%.1fs, max_concurrent=%d)",
            self._interval, REST_POLL_MAX_CONCURRENT,
        )
        while self._running:
            try:
                await self._poll_cycle()
            except Exception as exc:
                logger.error("RestPoller poll cycle error: %s", exc, exc_info=True)
            await asyncio.sleep(self._interval)

    async def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False
        logger.info("RestPoller stopped after %d poll cycles", self._poll_count)

    async def _poll_cycle(self) -> None:
        """One poll cycle: fetch orderbooks, run matching, write to Redis."""
        all_tokens = self._get_all_active_tokens()
        if not all_tokens:
            return

        self._poll_count += 1
        matching_tokens = self._get_matching_tokens()

        # Try batch fetch first, fall back to individual fetches
        book_map = await self._fetch_books_batch(all_tokens)
        if book_map is None:
            book_map = await self._fetch_books_individual(all_tokens)

        # Process results
        applied = 0
        for token_id, (bids, asks) in book_map.items():
            # Always write to Redis for UI display
            await self._write_to_redis(token_id, bids, asks)

            # Apply to matching engine only for tokens with active orders
            if token_id in matching_tokens:
                if self._apply_to_sessions(token_id, bids, asks):
                    applied += 1

        if self._poll_count % 50 == 0:
            logger.info(
                "RestPoller: cycle #%d — polled %d token(s), matched %d",
                self._poll_count, len(all_tokens), applied,
            )

    async def _fetch_books_batch(self, token_ids: list[str]) -> dict[str, tuple[list[dict], list[dict]]] | None:
        """Batch-fetch all orderbooks in a single POST /books call. Returns None on failure."""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self._pm.fetch_books_batch, token_ids
            )
        except Exception as exc:
            logger.warning("RestPoller: batch fetch failed, falling back to individual: %s", exc)
            return None

    async def _fetch_books_individual(self, token_ids: list[str]) -> dict[str, tuple[list[dict], list[dict]]]:
        """Fallback: fetch orderbooks individually with concurrency limit."""
        semaphore = asyncio.Semaphore(REST_POLL_MAX_CONCURRENT)

        async def _fetch_with_limit(token_id: str):
            async with semaphore:
                return await self._fetch_book(token_id)

        results = await asyncio.gather(
            *[_fetch_with_limit(tid) for tid in token_ids],
            return_exceptions=True,
        )

        book_map: dict[str, tuple[list[dict], list[dict]]] = {}
        for token_id, result in zip(token_ids, results):
            if isinstance(result, Exception):
                logger.warning(
                    "RestPoller: failed to fetch book for %s: %s",
                    token_id[:16], result,
                )
                continue
            book_map[token_id] = result
        return book_map

    def _get_all_active_tokens(self) -> list[str]:
        """Return all token_ids from non-archived sessions."""
        tokens: set[str] = set()
        with self._sm._lock:
            engines = list(self._sm._engines.values())

        for engine in engines:
            if engine.state in (SessionState.ACTIVE, SessionState.PREFETCH):
                for direction in engine.books:
                    token_id = engine.get_token_id(direction)
                    if token_id:
                        tokens.add(token_id)
        return list(tokens)

    def _get_matching_tokens(self) -> set[str]:
        """Return token_ids for sessions that have pending/bracket orders."""
        active_tokens: set[str] = set()
        with self._sm._lock:
            engines = list(self._sm._engines.values())

        for engine in engines:
            if engine.state not in (SessionState.ACTIVE, SessionState.PREFETCH):
                continue
            for direction, book in engine.books.items():
                if book.has_pending_orders() or book.has_bracket_orders():
                    token_id = engine.get_token_id(direction)
                    if token_id:
                        active_tokens.add(token_id)
        return active_tokens

    async def _fetch_book(self, token_id: str) -> tuple[list[dict], list[dict]]:
        """Fetch orderbook from Polymarket REST API."""
        loop = asyncio.get_event_loop()
        bids, asks = await loop.run_in_executor(
            None, self._pm.fetch_book_raw, token_id
        )
        return bids, asks

    async def _write_to_redis(self, token_id: str, bids: list[dict], asks: list[dict]) -> None:
        """Write prices and orderbook depth to Redis for UI display."""
        if self._writer is None:
            return

        try:
            # Convert to Decimal tuples for RedisWriter
            dec_bids = [
                (Decimal(str(b["price"])), Decimal(str(b["size"])))
                for b in sorted(bids, key=lambda x: float(x["price"]), reverse=True)
            ]
            dec_asks = [
                (Decimal(str(a["price"])), Decimal(str(a["size"])))
                for a in sorted(asks, key=lambda x: float(x["price"]))
            ]

            # Write orderbook depth
            await self._writer.update_orderbook(token_id, dec_bids, dec_asks)

            # Write best prices
            best_ask = float(dec_asks[0][0]) if dec_asks else None
            best_bid = float(dec_bids[0][0]) if dec_bids else None
            if best_ask is not None:
                await self._writer.update_price(token_id, best_ask, best_bid)

        except Exception as exc:
            logger.debug("RestPoller: Redis write failed for %s: %s", token_id[:16], exc)

    def _apply_to_sessions(self, token_id: str, bids: list[dict], asks: list[dict]) -> bool:
        """Apply fetched orderbook to matching engine sessions. Returns True if applied."""
        sessions = self._sm.get_sessions_for_token(token_id)
        applied = False
        for session in sessions:
            if session.state == SessionState.ARCHIVED:
                continue
            book = session.get_book_for_token(token_id)
            if book is None:
                continue
            # Apply snapshot (replaces orderbook data)
            book.apply_snapshot(bids, asks)
            # Run matching + bracket monitoring
            session.try_match_pending(book)
            applied = True
        return applied
