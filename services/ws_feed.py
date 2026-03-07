"""
Polymarket WebSocket feed — connects to the CLOB Market Channel and streams
orderbook events to a caller-supplied callback.

Usage (standalone test):
    python -m services.ws_feed

Integration (WS Feed Service):
    from services.ws_feed import PolymarketFeed
    feed = PolymarketFeed(token_ids, on_event=my_callback)
    await feed.start()
    ...
    await feed.stop()

The feed automatically:
  - Subscribes to the given asset (token) IDs
  - Sends PING heartbeats every 10s to keep the connection alive
  - Tracks PONG replies; forces reconnect if no PONG within 30s
  - Reconnects on disconnect with exponential back-off (1s → 2s → … → max 10s)
  - Routes all events through the ``on_event`` callback
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from config.timing import WS_PING_INTERVAL_S, WS_PONG_TIMEOUT_S, WS_CLOSE_TIMEOUT_S

logger = logging.getLogger(__name__)

_WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_PING_INTERVAL = WS_PING_INTERVAL_S
_RECONNECT_BASE = 1  # seconds
_RECONNECT_MAX = 10  # seconds (was 60 — faster recovery reduces data gaps)


class PolymarketFeed:
    """Async WebSocket client that streams Polymarket CLOB events."""

    def __init__(
        self,
        token_ids: list[str],
        on_event: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self.token_ids = token_ids
        self._on_event = on_event
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_delay = _RECONNECT_BASE
        self._last_pong: float = 0.0  # monotonic timestamp of last PONG

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the feed in a background asyncio task."""
        if self._running:
            logger.warning("Feed already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        logger.info(
            "Polymarket feed started — tracking %d token(s)", len(self.token_ids)
        )

    async def stop(self) -> None:
        """Gracefully shut down the feed."""
        self._running = False
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self._ws:
            await self._ws.close()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Polymarket feed stopped")

    def add_tokens(self, token_ids: list[str]) -> None:
        """
        Add token IDs to track.

        If the WebSocket connection is currently active, re-subscribes
        with the FULL token list (not just new ones) because Polymarket
        treats each subscription message as a full replacement — sending
        only new tokens causes "INVALID OPERATION" and breaks the feed.
        """
        new = set(token_ids) - set(self.token_ids)
        if not new:
            return
        self.token_ids.extend(new)
        logger.info("Registering %d new token(s) to feed (total: %d)", len(new), len(self.token_ids))

        # Re-subscribe with FULL token list on the live connection
        if self._ws is not None and self._running:
            asyncio.create_task(self._resubscribe(self.token_ids))

    async def _resubscribe(self, token_ids: list[str]) -> None:
        """
        Send a full subscription message on the live WS connection.

        Polymarket WS treats each subscription as a REPLACEMENT, so we must
        always send the complete list of token IDs, not just new ones.
        """
        if self._ws is None:
            return
        try:
            payload = {
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }
            await self._ws.send(json.dumps(payload))
            logger.info(
                "Re-subscribed with full token list (%d token(s))",
                len(token_ids),
            )
        except Exception as exc:
            logger.warning(
                "Re-subscribe failed (%d token(s)): %s — will retry on reconnect",
                len(token_ids), exc,
            )

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _run_forever(self) -> None:
        """Reconnect loop with exponential back-off (max 10s)."""
        while self._running:
            try:
                await self._connect_and_listen()
                # Clean disconnect — reset delay
                self._reconnect_delay = _RECONNECT_BASE
            except ConnectionClosed as e:
                logger.warning("WebSocket closed: %s — reconnecting quickly", e)
                # Reset delay on clean close for faster recovery
                self._reconnect_delay = _RECONNECT_BASE
            except Exception as e:
                logger.error("WebSocket error: %s", e, exc_info=True)

            if not self._running:
                break

            logger.info(
                "Reconnecting in %.0fs...", self._reconnect_delay
            )
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2, _RECONNECT_MAX
            )

    async def _connect_and_listen(self) -> None:
        """Single connection lifecycle: connect → subscribe → listen."""
        logger.info("Connecting to %s", _WS_URI)

        async with websockets.connect(
            _WS_URI,
            ping_interval=None,  # we handle pings manually
            close_timeout=WS_CLOSE_TIMEOUT_S,
        ) as ws:
            self._ws = ws
            self._last_pong = time.monotonic()
            logger.info("Connected to Polymarket Market Channel")

            # Subscribe
            payload = {
                "assets_ids": self.token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(payload))
            logger.info(
                "Subscribed to %d asset(s)", len(self.token_ids)
            )

            # Start heartbeat
            self._ping_task = asyncio.create_task(self._heartbeat(ws))

            # Listen for events
            try:
                async for raw_msg in ws:
                    if not self._running:
                        break
                    if raw_msg == "PONG":
                        self._last_pong = time.monotonic()
                        continue
                    self._handle_message(raw_msg)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()

    async def _heartbeat(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Send PING every N seconds; close connection if PONG not received within timeout."""
        try:
            while self._running:
                await asyncio.sleep(_PING_INTERVAL)
                # Check pong timeout before sending next ping
                elapsed = time.monotonic() - self._last_pong
                if elapsed > WS_PONG_TIMEOUT_S:
                    logger.warning(
                        "No PONG received for %.0fs (timeout=%ds) — forcing reconnect",
                        elapsed, WS_PONG_TIMEOUT_S,
                    )
                    await ws.close(1000, "Pong timeout")
                    return
                await ws.send("PING")
        except (ConnectionClosed, asyncio.CancelledError):
            pass

    def _handle_message(self, raw: str) -> None:
        """Parse and dispatch a WebSocket message via the on_event callback."""
        if self._on_event is None:
            return

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Non-JSON message: %s", raw[:100])
            return

        # Polymarket can send a single event or an array of events
        events = data if isinstance(data, list) else [data]
        for event in events:
            if not isinstance(event, dict):
                continue
            try:
                self._on_event(event)
            except Exception as e:
                logger.error(
                    "Error dispatching event %s: %s",
                    event.get("event_type", "?"), e,
                    exc_info=True,
                )


# ── Module-level singleton ───────────────────────────────────────────────────

_feed: Optional[PolymarketFeed] = None


async def start_feed(
    token_ids: list[str],
    on_event: Optional[Callable[[dict], None]] = None,
) -> PolymarketFeed:
    """Start the global feed singleton. Safe to call multiple times."""
    global _feed
    if _feed is not None and _feed._running:
        _feed.add_tokens(token_ids)
        return _feed
    _feed = PolymarketFeed(token_ids, on_event=on_event)
    await _feed.start()
    return _feed


async def stop_feed() -> None:
    """Stop the global feed if running."""
    global _feed
    if _feed is not None:
        await _feed.stop()
        _feed = None


def get_feed() -> Optional[PolymarketFeed]:
    """Return the current feed instance (or None)."""
    return _feed
