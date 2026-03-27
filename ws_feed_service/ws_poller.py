"""
WsFeedPoller — WebSocket-based orderbook source wrapping PolymarketFeed.

Drop-in alternative to RestPoller: receives real-time orderbook events via
WebSocket instead of polling REST every 200ms.  Same integration points:
RedisWriter for price/depth publishing, SessionManager for matching.

Maintains an in-memory SnapshotStore (same mechanism as demo_ws_snapshot.py)
that incrementally builds full-depth orderbooks from all WS event types:
  - ``book``: full snapshot reset
  - ``price_change``: incremental delta updates
  - ``best_bid_ask``: BBO update
  - ``last_trade_price``: trade execution

After each event the full book is synced to Redis (with fresh ``updated_at``)
and applied to the matching engine.

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
from services.snapshot_store import SnapshotStore, set_store
from services.volume_tracker import record_trade_volume
from config.timing import WS_STALE_THRESHOLD_S, WS_RECONNECT_STALE_MS, REST_POLL_TIMEOUT_S

logger = logging.getLogger(__name__)


class WsFeedPoller:
    """
    Wraps PolymarketFeed with SnapshotStore-based orderbook maintenance.

    All WS events are routed through a SnapshotStore instance that maintains
    full-depth orderbooks per token.  After each update the current book state
    is synced to Redis (always with a fresh ``updated_at``) and applied to the
    matching engine for order matching / bracket monitoring.
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
        self._store = SnapshotStore()  # in-memory full-depth orderbook per token
        set_store(self._store)  # register as global singleton for API fill path
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

    def remove_tokens(self, token_ids: list[str]) -> None:
        """Remove expired token IDs: unsubscribe from WS, evict from SnapshotStore."""
        if not token_ids:
            return
        # 1. Stop receiving WS events for these tokens
        self._feed.remove_tokens(token_ids)
        # 2. Evict from SnapshotStore (free memory)
        with self._store._lock:
            for t in token_ids:
                self._store._store.pop(t, None)
        # 3. Remove staleness tracking
        for t in token_ids:
            self._last_ws_update.pop(t, None)
        logger.info("Removed %d expired token(s) from SnapshotStore + WS subscription", len(token_ids))

    # ── Event handling ────────────────────────────────────────────────────

    def _on_event(self, event: dict) -> None:
        """
        Dispatch a Polymarket WebSocket event.

        Every event is first processed by the SnapshotStore (which maintains
        the full-depth orderbook in memory, exactly like demo_ws_snapshot.py).
        Then the affected token's book is synced to Redis + matching engine.
        """
        event_type = event.get("event_type")

        # 1. Update SnapshotStore — handles all 7 event types
        self._store.handle_event(event)

        # 2. Sync affected tokens to Redis + ME
        if event_type == "book":
            asset_id = event.get("asset_id")
            if asset_id:
                self._touch_token(asset_id)
                self._event_count += 1
                self._sync_snapshot(asset_id)
                if self._event_count % 200 == 0:
                    logger.info(
                        "WsFeedPoller: %d book events processed so far",
                        self._event_count,
                    )

        elif event_type == "price_change":
            seen: set[str] = set()
            for change in event.get("price_changes", []):
                asset_id = change.get("asset_id")
                if asset_id and asset_id not in seen:
                    seen.add(asset_id)
                    self._touch_token(asset_id)
                    self._sync_snapshot(asset_id)

        elif event_type == "best_bid_ask":
            asset_id = event.get("asset_id")
            if asset_id:
                self._touch_token(asset_id)
                self._sync_snapshot(asset_id)

        elif event_type == "last_trade_price":
            asset_id = event.get("asset_id")
            if asset_id:
                self._touch_token(asset_id)
                self._trade_count += 1
                self._handle_last_trade(event)
                if self._trade_count % 500 == 0:
                    logger.info(
                        "WsFeedPoller: %d trade events processed so far",
                        self._trade_count,
                    )

    def _touch_token(self, token_id: str) -> None:
        """Record that a WS event was received for this token."""
        self._last_ws_update[token_id] = time.monotonic()

    # ── Snapshot sync ──────────────────────────────────────────────────────

    def _sync_snapshot(self, token_id: str) -> None:
        """Read full book from SnapshotStore → write to Redis + apply to ME.

        This is the core of the SnapshotStore approach: the in-memory store
        is always up-to-date (maintained incrementally from all WS events).
        We read the current state and push it to both Redis and the matching
        engine on every book-changing event.
        """
        snap = self._store.get_snapshot(token_id)
        if snap is None:
            return

        # Convert SnapshotStore format → list[dict] for ME's apply_snapshot
        # SnapshotStore bids use neg-key trick: key = -price
        bids_raw = [
            {"price": str(-neg_p), "size": str(s)}
            for neg_p, s in snap.bids.items()
        ]
        asks_raw = [
            {"price": str(p), "size": str(s)}
            for p, s in snap.asks.items()
        ]

        # [DISABLED] ME matching temporarily disabled
        # sessions = self._sm.get_sessions_for_token(token_id)
        # for session in sessions:
        #     if session.state == SessionState.ARCHIVED:
        #         continue
        #     book = session.get_book_for_token(token_id)
        #     if book is not None:
        #         book.apply_snapshot(bids_raw, asks_raw)
        #         session.try_match_pending(book)

        # Write to Redis
        if self._writer is not None:
            # Convert to Decimal tuples for RedisWriter
            dec_bids = [(-neg_p, s) for neg_p, s in snap.bids.items()]
            dec_asks = list(snap.asks.items())

            asyncio.ensure_future(
                self._writer.update_orderbook(token_id, dec_bids, dec_asks),
            )

            # Update BBO price key
            best_ask = float(snap.best_ask) if snap.best_ask else None
            best_bid = float(snap.best_bid) if snap.best_bid else None
            if best_ask is not None:
                asyncio.ensure_future(
                    self._writer.update_price(token_id, best_ask, best_bid),
                )

    # ── Trade handling ─────────────────────────────────────────────────────

    def _handle_last_trade(self, event: dict) -> None:
        """Record trade in sessions, write to Redis, trigger bracket monitoring."""
        asset_id = event.get("asset_id")
        if not asset_id:
            return

        price = event.get("price", "0")
        size = event.get("size", "0")
        side = event.get("side", "")

        # Write last trade to Redis
        if self._writer is not None:
            asyncio.ensure_future(
                self._writer.update_last_trade(asset_id, price, size, side),
            )
            self._record_volume(asset_id, float(price), float(size))

        # [DISABLED] ME matching/bracket monitoring temporarily disabled
        # sessions = self._sm.get_sessions_for_token(asset_id)
        # for session in sessions:
        #     if session.state == SessionState.ARCHIVED:
        #         continue
        #     book = session.get_book_for_token(asset_id)
        #     if book is None:
        #         continue
        #     book.record_trade(price=price, size=size, side=side)
        #     book.monitor_bracket_orders()

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

    # ── Staleness guard ────────────────────────────────────────────────────

    def _get_all_active_tokens(self) -> set[str]:
        """Return token_ids from ACTIVE sessions only (current candle).

        PREFETCH sessions (future candles) are excluded because Polymarket
        does not stream WS data for markets that haven't opened yet.
        Including them would cause perpetual staleness and reconnect storms.
        """
        tokens: set[str] = set()
        for engine in self._sm.list_sessions():
            if engine.state == SessionState.ACTIVE:
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

                # Feed into SnapshotStore as a synthetic book event
                self._store.handle_event({
                    "event_type": "book",
                    "asset_id": token_id,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": int(time.time() * 1000),
                })

                # Sync to Redis + ME
                self._sync_snapshot(token_id)

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

    # ── Stale guard + SnapshotStore restart (>1s no event) ───────────────

    _RECONNECT_COOLDOWN_S = 5.0   # min seconds between forced reconnects

    async def _reconnect_monitor_loop(self) -> None:
        """
        Check every 1s: if ANY known token is stale >WS_RECONNECT_STALE_MS (1s),
        clear its stale snapshots and force-reconnect ALL 3 WS connections.

        Reconnect causes Polymarket to re-send full ``book`` events for all
        subscribed tokens → SnapshotStore rebuilds from scratch.

        Tokens that have NEVER received a WS event are excluded (handled by
        the 15s REST fallback instead).
        """
        threshold_s = WS_RECONNECT_STALE_MS / 1000.0
        # Give WS time to connect and deliver first events before monitoring
        await asyncio.sleep(max(5.0, threshold_s * 5))

        last_reconnect = 0.0

        try:
            while True:
                try:
                    now = time.monotonic()
                    active_tokens = self._get_all_active_tokens()

                    stale_tokens: list[str] = []
                    known_count = 0
                    worst_age_ms = 0.0
                    worst_token = ""

                    for token_id in active_tokens:
                        last = self._last_ws_update.get(token_id, 0.0)
                        if last == 0.0:
                            continue  # never received — REST fallback handles it
                        known_count += 1
                        age_ms = (now - last) * 1000
                        if age_ms > WS_RECONNECT_STALE_MS:
                            stale_tokens.append(token_id)
                            if age_ms > worst_age_ms:
                                worst_age_ms = age_ms
                                worst_token = token_id

                    cooldown_ok = (now - last_reconnect) >= self._RECONNECT_COOLDOWN_S

                    if stale_tokens and cooldown_ok:
                        logger.warning(
                            "WsFeedPoller: %d/%d token(s) stale >%dms (worst=%s %.0fms) "
                            "— clearing snapshots + restarting all %d WS streams",
                            len(stale_tokens), known_count, WS_RECONNECT_STALE_MS,
                            worst_token[:16], worst_age_ms, self._pool_size,
                        )

                        # Clear stale snapshots so fills don't use outdated data
                        # while reconnect is in progress
                        with self._store._lock:
                            for token_id in stale_tokens:
                                snap = self._store._store.get(token_id)
                                if snap is not None:
                                    snap.bids.clear()
                                    snap.asks.clear()
                                    snap.best_bid = None
                                    snap.best_ask = None
                                    snap.spread = None
                                    snap.last_updated_ts = 0.0

                        # Force all 3 WS connections to reconnect
                        # Polymarket will re-send book events → SnapshotStore rebuilds
                        await self._feed.force_reconnect(
                            reason=(
                                f"{len(stale_tokens)}/{known_count} tokens stale, "
                                f"worst {worst_token[:16]} {worst_age_ms:.0f}ms"
                            ),
                        )
                        last_reconnect = now
                        # Wait before resuming checks to let reconnect complete
                        await asyncio.sleep(3.0)
                        continue

                except Exception as exc:
                    logger.error("WsFeedPoller reconnect monitor error: %s", exc)

                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
