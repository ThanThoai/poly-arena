"""
WsFeedPoller — WebSocket-based orderbook source wrapping PolymarketFeed.

Drop-in alternative to RestPoller: receives real-time orderbook events via
WebSocket instead of polling REST every 200ms.  Same integration points:
RedisWriter for price/depth publishing, SessionManager for matching.

Includes a staleness guard: if a token hasn't received a WS event within
WS_STALE_THRESHOLD_S (default 15s), a REST fallback fetch is triggered so
every session stays fresh.

Usage:
    poller = WsFeedPoller(session_manager, writer, token_ids)
    await poller.start()
    ...
    await poller.stop()
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional

from services.ws_feed import PolymarketFeed, PolymarketFeedPool
from services.session_manager import SessionManager
from services.session_engine import SessionState
from services.volume_tracker import record_trade_volume
from config.timing import WS_STALE_THRESHOLD_S, WS_RECONNECT_STALE_MS, REST_POLL_TIMEOUT_S

logger = logging.getLogger(__name__)


class WsFeedPoller:
    """
    Wraps PolymarketFeed with the same integration as RestPoller:
    applies orderbook snapshots to sessions and writes prices to Redis.

    Runs a background staleness check every WS_STALE_THRESHOLD_S seconds.
    Tokens that haven't received a WS update within the threshold are
    refreshed via a single REST call (fallback).
    """

    POOL_SIZE = 3  # number of concurrent WS connections

    def __init__(
        self,
        session_manager: SessionManager,
        writer=None,
        token_ids: Optional[list[str]] = None,
        pool_size: int = POOL_SIZE,
    ) -> None:
        self._sm = session_manager
        self._writer = writer
        self._pool_size = pool_size
        self._feed = PolymarketFeedPool(
            token_ids=token_ids or [],
            on_event=self._on_event,
            pool_size=pool_size,
        )
        self._event_count = 0
        self._trade_count = 0
        # Track last WS update time per token for staleness detection
        self._last_ws_update: dict[str, float] = {}
        self._stale_check_task: Optional[asyncio.Task] = None
        self._rest_fallback_count = 0

    async def start(self) -> None:
        """Start the underlying WebSocket feed pool + staleness guard + reconnect monitor."""
        logger.info(
            "WsFeedPoller starting — %d WS connections, %d token(s), stale=%ds, reconnect=%dms",
            self._pool_size, len(self._feed.token_ids), WS_STALE_THRESHOLD_S, WS_RECONNECT_STALE_MS,
        )
        self._stale_check_task = asyncio.ensure_future(self._stale_check_loop())
        self._reconnect_monitor_task = asyncio.ensure_future(self._reconnect_monitor_loop())
        await self._feed.start()

    async def stop(self) -> None:
        """Stop the underlying WebSocket feed + staleness guard + reconnect monitor."""
        if self._stale_check_task is not None:
            self._stale_check_task.cancel()
        if hasattr(self, '_reconnect_monitor_task') and self._reconnect_monitor_task is not None:
            self._reconnect_monitor_task.cancel()
        await self._feed.stop()
        logger.info(
            "WsFeedPoller stopped after %d event(s), %d REST fallback(s)",
            self._event_count, self._rest_fallback_count,
        )

    def add_tokens(self, token_ids: list[str]) -> None:
        """Add new token IDs to the live WebSocket subscription."""
        self._feed.add_tokens(token_ids)

    # ── Event handling ────────────────────────────────────────────────────

    def _on_event(self, event: dict) -> None:
        """
        Dispatch a Polymarket WebSocket event.

        Event types:
        - ``book``: full orderbook snapshot → write to Redis + apply to sessions
        - ``price_change``: incremental price update → write to Redis only
        - ``last_trade_price``: trade execution → record in sessions + write to Redis
        """
        event_type = event.get("event_type")
        if event_type == "book":
            self._handle_book(event)
        elif event_type == "price_change":
            self._handle_price_change(event)
        elif event_type == "last_trade_price":
            self._handle_last_trade(event)

    def _touch_token(self, token_id: str) -> None:
        """Record that a WS event was received for this token."""
        self._last_ws_update[token_id] = time.monotonic()

    def _handle_book(self, event: dict) -> None:
        """Handle a full orderbook snapshot from WebSocket."""
        asset_id = event.get("asset_id")
        if not asset_id:
            return

        self._touch_token(asset_id)
        self._event_count += 1

        # Parse bids/asks from WS event
        raw_bids = event.get("bids", [])
        raw_asks = event.get("asks", [])

        # Convert to Decimal tuples for RedisWriter
        dec_bids = [
            (Decimal(str(b["price"])), Decimal(str(b["size"])))
            for b in sorted(raw_bids, key=lambda x: float(x["price"]), reverse=True)
        ]
        dec_asks = [
            (Decimal(str(a["price"])), Decimal(str(a["size"])))
            for a in sorted(raw_asks, key=lambda x: float(x["price"]))
        ]

        # Schedule async Redis writes on the event loop
        if self._writer is not None:
            asyncio.ensure_future(
                self._writer.update_orderbook(asset_id, dec_bids, dec_asks),
            )
            best_ask = float(dec_asks[0][0]) if dec_asks else None
            best_bid = float(dec_bids[0][0]) if dec_bids else None
            if best_ask is not None:
                asyncio.ensure_future(
                    self._writer.update_price(asset_id, best_ask, best_bid),
                )

        # Apply snapshot to matching engine sessions
        self._apply_to_sessions(asset_id, raw_bids, raw_asks)

        if self._event_count % 200 == 0:
            logger.info(
                "WsFeedPoller: %d book events processed so far", self._event_count,
            )

    def _handle_price_change(self, event: dict) -> None:
        """Handle incremental price changes — update Redis prices + apply to sessions."""
        if self._writer is None:
            return

        # Polymarket uses "price_changes" key: list of dicts
        # Each: {asset_id, price, size, side, hash, best_bid, best_ask}
        price_changes = event.get("price_changes", [])
        if not price_changes:
            return

        # Group changes by asset_id and extract best bid/ask
        for change in price_changes:
            asset_id = change.get("asset_id")
            if not asset_id:
                continue

            self._touch_token(asset_id)
            best_bid = float(change["best_bid"]) if "best_bid" in change else None
            best_ask = float(change["best_ask"]) if "best_ask" in change else None

            if best_ask is not None or best_bid is not None:
                asyncio.ensure_future(
                    self._writer.update_price(asset_id, best_ask, best_bid),
                )

            # Apply incremental update to matching engine sessions
            price = change.get("price")
            size = change.get("size")
            side = change.get("side", "")
            if price is not None and size is not None:
                # Map Polymarket sides to ShadowOrderbook sides
                book_side = "bid" if side.upper() == "BUY" else "ask"
                delta = [{"side": book_side, "price": price, "size": size}]
                sessions = self._sm.get_sessions_for_token(asset_id)
                for session in sessions:
                    if session.state == SessionState.ARCHIVED:
                        continue
                    book = session.get_book_for_token(asset_id)
                    if book is not None:
                        book.apply_changes(delta)

    def _handle_last_trade(self, event: dict) -> None:
        """
        Handle a last_trade_price event — record trade in sessions and write to Redis.

        Event fields: asset_id, price, size, side
        Records the trade on the ShadowOrderbook (updates last_trade) and triggers
        bracket monitoring (TP/SL may fire on trade price).
        """
        asset_id = event.get("asset_id")
        if not asset_id:
            return

        self._touch_token(asset_id)
        self._trade_count += 1
        price = event.get("price", "0")
        size = event.get("size", "0")
        side = event.get("side", "")

        # Write last trade to Redis
        if self._writer is not None:
            asyncio.ensure_future(
                self._writer.update_last_trade(asset_id, price, size, side),
            )

            # Record volume per session for this token
            self._record_volume(asset_id, float(price), float(size))

        # Apply to matching engine sessions — record_trade + bracket monitoring
        sessions = self._sm.get_sessions_for_token(asset_id)
        for session in sessions:
            if session.state == SessionState.ARCHIVED:
                continue
            book = session.get_book_for_token(asset_id)
            if book is None:
                continue
            book.record_trade(price=price, size=size, side=side)
            book.monitor_bracket_orders()

        if self._trade_count % 500 == 0:
            logger.info(
                "WsFeedPoller: %d trade events processed so far", self._trade_count,
            )

    def _record_volume(self, token_id: str, price: float, size: float) -> None:
        """Record trade volume for all sessions mapped to this token."""
        if self._writer is None:
            return

        # Resolve (symbol, timeframe, direction, candle_ts) from writer's session token map
        combos = self._writer._session_token_map.get(token_id)
        if not combos:
            # Fallback: use legacy token map (current session only)
            legacy = self._writer._token_map.get(token_id)
            if legacy:
                for sym, tf, direction in legacy:
                    candle_ts = self._writer._current_sessions.get(tf, 0)
                    if candle_ts:
                        asyncio.ensure_future(
                            record_trade_volume(
                                self._writer._r,
                                symbol=sym, timeframe=tf,
                                candle_ts=candle_ts, direction=direction,
                                price=price, size=size,
                            )
                        )
            return

        for sym, tf, direction, candle_ts in combos:
            asyncio.ensure_future(
                record_trade_volume(
                    self._writer._r,
                    symbol=sym, timeframe=tf,
                    candle_ts=candle_ts, direction=direction,
                    price=price, size=size,
                )
            )

    def _apply_to_sessions(
        self, token_id: str, bids: list[dict], asks: list[dict],
    ) -> None:
        """Apply orderbook snapshot to matching engine sessions (same as RestPoller)."""
        sessions = self._sm.get_sessions_for_token(token_id)
        for session in sessions:
            if session.state == SessionState.ARCHIVED:
                continue
            book = session.get_book_for_token(token_id)
            if book is None:
                continue
            book.apply_snapshot(bids, asks)
            session.try_match_pending(book)

    # ── Staleness guard ────────────────────────────────────────────────────

    def _get_all_active_tokens(self) -> set[str]:
        """Return all token_ids from non-archived sessions."""
        tokens: set[str] = set()
        for engine in self._sm.list_sessions():
            if engine.state in (SessionState.ACTIVE, SessionState.PREFETCH):
                for direction in engine.books:
                    token_id = engine.get_token_id(direction)
                    if token_id:
                        tokens.add(token_id)
        return tokens

    async def _stale_check_loop(self) -> None:
        """
        Periodically check all active tokens for staleness.

        If a token hasn't received a WS event in WS_STALE_THRESHOLD_S,
        fetch its orderbook from REST and apply to both Redis and matching
        engine, ensuring every session stays fresh.
        """
        # Wait one threshold before first check to give WS time to connect
        await asyncio.sleep(WS_STALE_THRESHOLD_S)

        self._rest_pm = None
        try:
            while True:
                try:
                    await self._check_and_refresh_stale()
                except Exception as exc:
                    logger.error("WsFeedPoller stale check error: %s", exc)
                await asyncio.sleep(WS_STALE_THRESHOLD_S)
        except asyncio.CancelledError:
            pass
        finally:
            if self._rest_pm is not None:
                try:
                    self._rest_pm.close()
                except Exception:
                    pass

    async def _check_and_refresh_stale(self) -> None:
        """Find stale tokens and refresh them via REST."""
        from services.polymarket import PolymarketClient

        now = time.monotonic()
        active_tokens = self._get_all_active_tokens()
        stale_tokens: list[str] = []

        for token_id in active_tokens:
            last = self._last_ws_update.get(token_id, 0.0)
            if now - last > WS_STALE_THRESHOLD_S:
                stale_tokens.append(token_id)

        if not stale_tokens:
            return

        logger.warning(
            "WsFeedPoller: %d/%d token(s) stale (>%ds) — REST fallback",
            len(stale_tokens), len(active_tokens), WS_STALE_THRESHOLD_S,
        )

        # Lazy-init REST client
        if self._rest_pm is None:
            self._rest_pm = PolymarketClient(timeout=REST_POLL_TIMEOUT_S)

        loop = asyncio.get_running_loop()
        for token_id in stale_tokens:
            try:
                bids, asks, _ts = await loop.run_in_executor(
                    None, self._rest_pm.fetch_book_raw, token_id,
                )

                # Write to Redis
                if self._writer is not None:
                    dec_bids = [
                        (Decimal(str(b["price"])), Decimal(str(b["size"])))
                        for b in sorted(bids, key=lambda x: float(x["price"]), reverse=True)
                    ]
                    dec_asks = [
                        (Decimal(str(a["price"])), Decimal(str(a["size"])))
                        for a in sorted(asks, key=lambda x: float(x["price"]))
                    ]
                    await self._writer.update_orderbook(token_id, dec_bids, dec_asks)
                    best_ask = float(dec_asks[0][0]) if dec_asks else None
                    best_bid = float(dec_bids[0][0]) if dec_bids else None
                    if best_ask is not None:
                        await self._writer.update_price(token_id, best_ask, best_bid)

                # Apply to matching engine
                self._apply_to_sessions(token_id, bids, asks)

                # Mark as refreshed so we don't re-fetch on next tick
                self._touch_token(token_id)
                self._rest_fallback_count += 1

                logger.info(
                    "WsFeedPoller: REST fallback refreshed %s (%d bids, %d asks)",
                    token_id[:16], len(bids), len(asks),
                )
            except Exception as exc:
                logger.warning(
                    "WsFeedPoller: REST fallback failed for %s: %s",
                    token_id[:16], exc,
                )

        # Prune _last_ws_update for tokens no longer active
        stale_keys = [k for k in self._last_ws_update if k not in active_tokens]
        for k in stale_keys:
            del self._last_ws_update[k]

    # ── Fast reconnect monitor (1000ms staleness) ─────────────────────

    async def _reconnect_monitor_loop(self) -> None:
        """
        Fast check every 1s: if ANY active token hasn't received a WS event
        in WS_RECONNECT_STALE_MS (1000ms), force-reconnect the WebSocket.

        This catches scenarios where the WS connection is alive (PONG ok)
        but data has stopped flowing for a specific market.
        """
        threshold_s = WS_RECONNECT_STALE_MS / 1000.0
        # Give WS time to connect and deliver first events
        await asyncio.sleep(max(3.0, threshold_s * 2))

        try:
            while True:
                try:
                    now = time.monotonic()
                    active_tokens = self._get_all_active_tokens()
                    if active_tokens:
                        worst_age_ms = 0.0
                        worst_token = ""
                        for token_id in active_tokens:
                            last = self._last_ws_update.get(token_id, 0.0)
                            age = now - last if last > 0 else now  # never updated = very stale
                            age_ms = age * 1000
                            if age_ms > worst_age_ms:
                                worst_age_ms = age_ms
                                worst_token = token_id

                        if worst_age_ms > WS_RECONNECT_STALE_MS:
                            logger.warning(
                                "WsFeedPoller: token %s stale %.0fms (>%dms) — forcing WS reconnect",
                                worst_token[:16], worst_age_ms, WS_RECONNECT_STALE_MS,
                            )
                            await self._feed.force_reconnect(
                                reason=f"token {worst_token[:16]} stale {worst_age_ms:.0f}ms",
                            )
                            # After reconnect, wait a bit before checking again
                            await asyncio.sleep(3.0)
                            continue
                except Exception as exc:
                    logger.error("WsFeedPoller reconnect monitor error: %s", exc)

                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
