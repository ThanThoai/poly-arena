"""
BaseBot — abstract template for timeframe-driven trading bots.

Subclass this and implement `decide(symbol, candles)` to return
'GREEN', 'RED', or None (skip trade).

    from base_bot import BaseBot, Candle

    class MyBot(BaseBot):
        def decide(self, symbol: str, candles: list[Candle]) -> str | None:
            # candles[-1] is the most recently CLOSED candle
            last = candles[-1]
            return 'GREEN' if last.close > last.open else 'RED'

    bot = MyBot(
        name      = 'MyBot',
        timeframe = 'M5',          # M5 | M15 | H1
        api_key   = '<your-key>',
        symbols   = ['BTC', 'ETH', 'SOL', 'XRP'],
        amount    = 100.0,
    )
    bot.run()
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

# ── Constants ──────────────────────────────────────────────────────────────────

API_BASE = "https://aiavatar.torilab.ai/poly-arena"
BINANCE_URL = "https://api.binance.com/api/v3/klines"

_TF_INTERVAL: dict[str, int] = {
    "M5": 5 * 60,
    "M15": 15 * 60,
}
_TF_BINANCE: dict[str, str] = {
    "M5": "5m",
    "M15": "15m",
}

# Number of closed candles fetched before each decision
DEFAULT_CANDLE_LIMIT = 50

# Seconds to wait after candle close before fetching (Binance propagation lag)
FETCH_DELAY = 5


# ── Indicator helpers ──────────────────────────────────────────────────────────


def _sma(values: list[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def _rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    return 100 - 100 / (1 + avg_g / avg_l)


# ── Data class ─────────────────────────────────────────────────────────────────


@dataclass
class Candle:
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def range(self) -> float:
        return self.high - self.low


# ── Base bot ───────────────────────────────────────────────────────────────────


class BaseBot(ABC):
    """
    Abstract base for a single-timeframe binary-option bot.

    Lifecycle per candle:
        1. sleep until next candle boundary + FETCH_DELAY seconds
        2. for each symbol → fetch_candles() → decide() → place_trade()
    """

    def __init__(
        self,
        *,
        name: str,
        timeframe: str,  # "M5" | "M15"
        api_key: str,
        symbols: list[str] | None = None,
        amount: float = 100.0,
        api_base: str = API_BASE,
        candle_limit: int = DEFAULT_CANDLE_LIMIT,
    ) -> None:
        if timeframe not in _TF_INTERVAL:
            raise ValueError(f"timeframe must be one of {list(_TF_INTERVAL)}")

        self.name = name
        self.timeframe = timeframe
        self.api_key = api_key
        self.symbols = symbols or ["BTC", "ETH"]
        self.amount = amount
        self.api_base = api_base.rstrip("/")
        self.candle_limit = candle_limit
        self.interval_s = _TF_INTERVAL[timeframe]
        self.binance_tf = _TF_BINANCE[timeframe]
        self.log = logging.getLogger(name)

    # ── Abstract ───────────────────────────────────────────────────────────────

    @abstractmethod
    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        """
        Return 'GREEN', 'RED', or None to skip this symbol.

        `candles` is a list of CLOSED candles in ascending order.
        candles[-1] is the most recently closed candle.
        """

    # ── Helpers ────────────────────────────────────────────────────────────────

    def fetch_candles(self, symbol: str) -> list[Candle]:
        """Fetch the last `candle_limit` closed candles from Binance."""
        binance_sym = symbol.upper() + "USDT"
        try:
            resp = httpx.get(
                BINANCE_URL,
                params={
                    "symbol": binance_sym,
                    "interval": self.binance_tf,
                    "limit": self.candle_limit + 1,  # +1 to drop in-progress candle
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            raw = resp.json()
            # Drop the last element — it's the still-open current candle
            candles = [
                Candle(
                    open_time=datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc),
                    close_time=datetime.fromtimestamp(r[6] / 1000, tz=timezone.utc),
                    open=float(r[1]),
                    high=float(r[2]),
                    low=float(r[3]),
                    close=float(r[4]),
                    volume=float(r[5]),
                )
                for r in raw[:-1]
            ]
            if candles:
                self.log.debug(
                    "fetch_candles(%s): %d candles  latest=%s  close=%.4f",
                    symbol,
                    len(candles),
                    candles[-1].open_time.strftime("%Y-%m-%d %H:%M"),
                    candles[-1].close,
                )
            return candles
        except Exception as exc:
            self.log.error("fetch_candles(%s): %s", symbol, exc)
            return []

    def place_trade(
        self, symbol: str, forecast: str, amount: Optional[float] = None
    ) -> bool:
        """POST a new binary option via the PolyArena API.

        Args:
            amount: Override the default self.amount for this specific trade.
                    Useful for confidence-scaled position sizing.
        """
        trade_amount = amount if amount is not None else self.amount
        try:
            resp = httpx.post(
                f"{self.api_base}/binary-options/",
                json={
                    "symbol": symbol,
                    "timeframe": self.timeframe,
                    "forecast": forecast,
                    "amount": trade_amount,
                },
                headers={"x-api-key": self.api_key},
                timeout=10.0,
            )
            resp.raise_for_status()
            bo = resp.json()
            self.log.info(
                "Placed  %-4s %-4s %-6s  id=%-5s  settle=%s",
                symbol,
                self.timeframe,
                forecast,
                bo["id"],
                bo.get("settlement_at", "?"),
            )
            return True
        except httpx.HTTPStatusError as exc:
            self.log.error(
                "HTTP %s placing %s: %s",
                exc.response.status_code,
                symbol,
                exc.response.text[:120],
            )
        except Exception as exc:
            self.log.error("place_trade(%s): %s", symbol, exc)
        return False

    # ── Candle hook ────────────────────────────────────────────────────────────

    def on_candle(self) -> None:
        """Called once per candle after FETCH_DELAY. Override to customise."""
        for symbol in self.symbols:
            candles = self.fetch_candles(symbol)
            if not candles:
                self.log.warning("No candles for %s, skipping.", symbol)
                continue

            forecast = self.decide(symbol, candles)
            if forecast in ("GREEN", "RED"):
                self.place_trade(symbol, forecast)
            elif forecast is not None:
                self.log.warning(
                    "decide() returned invalid value %r — skipping.", forecast
                )

    # ── Timing ─────────────────────────────────────────────────────────────────

    def _seconds_to_next_candle(self) -> float:
        now = time.time()
        return self.interval_s - (now % self.interval_s)

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Block forever: fetch → decide → place → sleep → repeat."""
        self.log.info(
            "Started  name=%s  tf=%s  symbols=%s  amount=%.2f  api=%s",
            self.name,
            self.timeframe,
            self.symbols,
            self.amount,
            self.api_base,
        )
        cycle = 0
        while True:
            cycle += 1
            try:
                self.on_candle()
            except Exception as exc:
                self.log.error(
                    "on_candle error (cycle #%d): %s", cycle, exc, exc_info=True
                )

            wait = self._seconds_to_next_candle() + FETCH_DELAY
            self.log.info(
                "Sleeping %.1fs → next %s candle (+%ds lag)  [cycle #%d]",
                wait,
                self.timeframe,
                FETCH_DELAY,
                cycle,
            )
            time.sleep(wait)
