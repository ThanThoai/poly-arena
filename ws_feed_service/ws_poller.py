"""
WsFeedPoller — WebSocket-based orderbook source wrapping PolymarketFeed.

Drop-in alternative to RestPoller: receives real-time orderbook events via
WebSocket instead of polling REST every 200ms.  Same integration points:
RedisWriter for price/depth publishing, SessionManager for matching.

Usage:
    poller = WsFeedPoller(session_manager, writer, token_ids)
    await poller.start()
    ...
    await poller.stop()
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Optional

from services.ws_feed import PolymarketFeed
from services.session_manager import SessionManager
from services.session_engine import SessionState
from services.volume_tracker import record_trade_volume

logger = logging.getLogger(__name__)


class WsFeedPoller:
    """
    Wraps PolymarketFeed with the same integration as RestPoller:
    applies orderbook snapshots to sessions and writes prices to Redis.
    """

    def __init__(
        self,
        session_manager: SessionManager,
        writer=None,
        token_ids: Optional[list[str]] = None,
    ) -> None:
        self._sm = session_manager
        self._writer = writer
        self._feed = PolymarketFeed(
            token_ids=token_ids or [],
            on_event=self._on_event,
        )
        self._event_count = 0
        self._trade_count = 0

    async def start(self) -> None:
        """Start the underlying WebSocket feed."""
        logger.info(
            "WsFeedPoller starting — tracking %d token(s)",
            len(self._feed.token_ids),
        )
        await self._feed.start()

    async def stop(self) -> None:
        """Stop the underlying WebSocket feed."""
        await self._feed.stop()
        logger.info(
            "WsFeedPoller stopped after %d event(s)", self._event_count,
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

    def _handle_book(self, event: dict) -> None:
        """Handle a full orderbook snapshot from WebSocket."""
        asset_id = event.get("asset_id")
        if not asset_id:
            return

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
            loop = asyncio.get_event_loop()
            asyncio.ensure_future(
                self._writer.update_orderbook(asset_id, dec_bids, dec_asks),
                loop=loop,
            )
            best_ask = float(dec_asks[0][0]) if dec_asks else None
            best_bid = float(dec_bids[0][0]) if dec_bids else None
            if best_ask is not None:
                asyncio.ensure_future(
                    self._writer.update_price(asset_id, best_ask, best_bid),
                    loop=loop,
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

            best_bid = float(change["best_bid"]) if "best_bid" in change else None
            best_ask = float(change["best_ask"]) if "best_ask" in change else None

            if best_ask is not None or best_bid is not None:
                loop = asyncio.get_event_loop()
                asyncio.ensure_future(
                    self._writer.update_price(asset_id, best_ask, best_bid),
                    loop=loop,
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

        self._trade_count += 1
        price = event.get("price", "0")
        size = event.get("size", "0")
        side = event.get("side", "")

        # Write last trade to Redis
        if self._writer is not None:
            loop = asyncio.get_event_loop()
            asyncio.ensure_future(
                self._writer.update_last_trade(asset_id, price, size, side),
                loop=loop,
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
