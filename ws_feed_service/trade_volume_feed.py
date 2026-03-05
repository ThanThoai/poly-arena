"""
Lightweight Polymarket WebSocket listener for trade volume tracking.

Used in REST polling mode to capture `last_trade_price` events from Polymarket
and accumulate per-minute volume data in Redis.

In WebSocket mode, volume is captured directly by WsFeedPoller._handle_last_trade().
"""

import asyncio
import logging

from services.ws_feed import PolymarketFeed
from services.volume_tracker import record_trade_volume
from ws_feed_service.redis_writer import RedisWriter

logger = logging.getLogger(__name__)


class TradeVolumeFeed:
    """
    Connects to Polymarket WS and records trade volume per session.

    Only processes `last_trade_price` events — ignores book/price_change.
    Token → session mapping is read from RedisWriter's internal maps.
    """

    def __init__(self, writer: RedisWriter, token_ids: list[str] | None = None) -> None:
        self._writer = writer
        self._feed = PolymarketFeed(
            token_ids=token_ids or [],
            on_event=self._on_event,
        )
        self._trade_count = 0

    async def start(self) -> None:
        logger.info(
            "TradeVolumeFeed starting — tracking %d token(s)",
            len(self._feed.token_ids),
        )
        await self._feed.start()

    async def stop(self) -> None:
        await self._feed.stop()
        logger.info(
            "TradeVolumeFeed stopped after %d trade event(s)",
            self._trade_count,
        )

    def add_tokens(self, token_ids: list[str]) -> None:
        """Add new tokens to the WS subscription."""
        self._feed.add_tokens(token_ids)

    def _on_event(self, event: dict) -> None:
        """Only process last_trade_price events."""
        if event.get("event_type") != "last_trade_price":
            return

        asset_id = event.get("asset_id")
        if not asset_id:
            return

        self._trade_count += 1
        price = float(event.get("price", 0))
        size = float(event.get("size", 0))
        if price <= 0 or size <= 0:
            return

        # Resolve sessions from writer's token maps
        combos = self._writer._session_token_map.get(asset_id)
        if not combos:
            legacy = self._writer._token_map.get(asset_id)
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

        if self._trade_count % 500 == 0:
            logger.info(
                "TradeVolumeFeed: %d trade events processed", self._trade_count,
            )
