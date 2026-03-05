"""Binance Futures WebSocket client — streams mark prices for supported symbols.

Connects to Binance's combined stream endpoint for mark price updates.
Writes prices to Redis for consumption by the futures engine and API.

Redis keys:
    futures:price:{SYM}  →  hash { price, updated_at }
    futures:prices:all    →  hash of all symbol prices (for batch reads)
"""

import asyncio
import json
import logging
import time
from typing import Callable

import websockets

log = logging.getLogger(__name__)

# Symbol mapping: internal short name → Binance futures symbol
FUTURES_SYMBOLS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}

BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="


def build_ws_url() -> str:
    """Build combined stream URL for all symbols' mark prices."""
    streams = [f"{sym}@markPrice@1s" for sym in FUTURES_SYMBOLS.values()]
    return BINANCE_WS_BASE + "/".join(streams)


class BinanceFuturesWs:
    """Manages WebSocket connection to Binance Futures mark price stream."""

    def __init__(
        self,
        on_price: Callable[[str, float, float], None] | None = None,
    ):
        """
        Args:
            on_price: callback(symbol, mark_price, timestamp) called on each update.
        """
        self._on_price = on_price
        self._running = False
        self._prices: dict[str, float] = {}
        self._ws = None
        # Reverse map: btcusdt → BTC
        self._reverse_map = {v: k for k, v in FUTURES_SYMBOLS.items()}

    @property
    def prices(self) -> dict[str, float]:
        return dict(self._prices)

    async def start(self) -> None:
        """Connect and stream prices. Auto-reconnects on failure."""
        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as exc:
                if self._running:
                    log.warning("Binance WS disconnected: %s — reconnecting in 3s", exc)
                    await asyncio.sleep(3)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect(self) -> None:
        url = build_ws_url()
        log.info("Connecting to Binance Futures WS: %d symbols", len(FUTURES_SYMBOLS))

        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self._ws = ws
            log.info("Binance Futures WS connected")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    data = msg.get("data", {})
                    stream_sym = data.get("s", "").lower()
                    internal_sym = self._reverse_map.get(stream_sym)
                    if not internal_sym:
                        continue

                    mark_price = float(data.get("p", 0))
                    event_time = data.get("E", 0) / 1000  # ms → s

                    self._prices[internal_sym] = mark_price

                    if self._on_price:
                        self._on_price(internal_sym, mark_price, event_time)

                except (KeyError, ValueError, TypeError) as exc:
                    log.debug("Binance WS parse error: %s", exc)
