"""
Example strategy bots built on BaseBot.

Run any of them directly:
    python bots/example_strategies.py --bot sma --tf M5 --key <api-key>

Strategies included:
  - LastCandleBot  : GREEN if last candle is bullish, RED if bearish
                     Skip: doji (body < min_body_ratio × range)
  - SMABot         : GREEN if close > SMA(n), RED otherwise
                     Skip: price within threshold_pct of SMA (ambiguous zone)
  - SMACrossBot    : fast SMA crosses above slow SMA → GREEN, below → RED
                     Skip: no crossover this candle
  - RSIBot         : oversold (<30) → GREEN, overbought (>70) → RED
                     Skip: RSI in neutral zone
  - BollingerBot   : price below lower band → GREEN, above upper → RED
                     Skip: price inside bands
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from base_bot import BaseBot, Candle, _sma, _rsi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _bollinger(closes: list[float], n: int = 20, k: float = 2.0):
    if len(closes) < n:
        return None, None, None
    window = closes[-n:]
    mid = sum(window) / n
    std = (sum((x - mid) ** 2 for x in window) / n) ** 0.5
    return mid - k * std, mid, mid + k * std


# ── Strategies ─────────────────────────────────────────────────────────────────

class LastCandleBot(BaseBot):
    """
    Simplest possible strategy:
      - Last closed candle is green → forecast GREEN (trend continuation)
      - Last closed candle is red   → forecast RED
      - Doji candle (body < min_body_ratio × range) → None (skip)
    """

    def __init__(self, *, min_body_ratio: float = 0.20, **kwargs):
        super().__init__(**kwargs)
        self.min_body_ratio = min_body_ratio

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        last = candles[-1]
        if last.range > 0 and last.body / last.range < self.min_body_ratio:
            self.log.debug(
                "%s SKIP doji — body=%.5f range=%.5f ratio=%.2f",
                symbol, last.body, last.range, last.body / last.range,
            )
            return None
        return "GREEN" if last.is_bullish else "RED"


class SMABot(BaseBot):
    """
    Price vs Simple Moving Average:
      - close > SMA(period) + threshold → GREEN (clearly above average)
      - close < SMA(period) - threshold → RED   (clearly below average)
      - price within threshold_pct of SMA       → None (ambiguous, skip)

    threshold_pct = 0.0 (default) means always trade (no neutral zone).
    Example: threshold_pct=0.002 skips when price is within 0.2% of SMA.
    """

    def __init__(self, *, period: int = 14, threshold_pct: float = 0.0, **kwargs):
        super().__init__(**kwargs)
        self.period        = period
        self.threshold_pct = threshold_pct

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        closes = [c.close for c in candles]
        sma = _sma(closes, self.period)
        if sma is None:
            return None
        last_close = closes[-1]
        band = sma * self.threshold_pct
        if last_close > sma + band:
            return "GREEN"
        if last_close < sma - band:
            return "RED"
        self.log.debug(
            "%s SKIP — price %.4f within %.4f%% of SMA %.4f",
            symbol, last_close, self.threshold_pct * 100, sma,
        )
        return None


class SMACrossBot(BaseBot):
    """
    Fast SMA / Slow SMA crossover:
      - fast crosses above slow → GREEN
      - fast crosses below slow → RED
      - no cross (same side as previous bar) → None (skip)
    """

    def __init__(self, *, fast: int = 5, slow: int = 20, **kwargs):
        super().__init__(**kwargs)
        self.fast = fast
        self.slow = slow

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        closes = [c.close for c in candles]
        f_now  = _sma(closes, self.fast)
        s_now  = _sma(closes, self.slow)
        f_prev = _sma(closes[:-1], self.fast)
        s_prev = _sma(closes[:-1], self.slow)

        if any(v is None for v in [f_now, s_now, f_prev, s_prev]):
            return None

        cross_up   = f_prev <= s_prev and f_now > s_now
        cross_down = f_prev >= s_prev and f_now < s_now

        if cross_up:
            return "GREEN"
        if cross_down:
            return "RED"
        return None  # no signal — skip


class RSIBot(BaseBot):
    """
    RSI mean-reversion:
      - RSI < oversold  → GREEN (bounce expected)
      - RSI > overbought → RED (pullback expected)
      - neutral zone → None (skip)
    """

    def __init__(self, *, period: int = 14, oversold: float = 30.0, overbought: float = 70.0, **kwargs):
        super().__init__(**kwargs)
        self.period     = period
        self.oversold   = oversold
        self.overbought = overbought

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        closes = [c.close for c in candles]
        rsi    = _rsi(closes, self.period)
        if rsi is None:
            return None
        self.log.debug("%s RSI=%.1f", symbol, rsi)
        if rsi < self.oversold:
            return "GREEN"
        if rsi > self.overbought:
            return "RED"
        return None


class BollingerBot(BaseBot):
    """
    Bollinger Band mean-reversion:
      - close < lower band → GREEN (oversold, expect bounce)
      - close > upper band → RED   (overbought, expect pullback)
      - inside bands → None (skip)
    """

    def __init__(self, *, period: int = 20, std_dev: float = 2.0, **kwargs):
        super().__init__(**kwargs)
        self.period  = period
        self.std_dev = std_dev

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        closes = [c.close for c in candles]
        lower, _, upper = _bollinger(closes, self.period, self.std_dev)
        if lower is None:
            return None
        last = closes[-1]
        self.log.debug("%s close=%.4f  lower=%.4f  upper=%.4f", symbol, last, lower, upper)
        if last < lower:
            return "GREEN"
        if last > upper:
            return "RED"
        return None


# ── CLI entry ──────────────────────────────────────────────────────────────────

STRATEGIES = {
    "last":      LastCandleBot,
    "sma":       SMABot,
    "sma-cross": SMACrossBot,
    "rsi":       RSIBot,
    "bollinger": BollingerBot,
}

TF_CHOICES = ["M5", "M15"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an example strategy bot")
    parser.add_argument("--bot",  choices=list(STRATEGIES), default="sma-cross",
                        help="Strategy to use")
    parser.add_argument("--tf",   choices=TF_CHOICES, default="M5",
                        help="Timeframe (default: M5)")
    parser.add_argument("--key",  required=True, help="Bot API key")
    parser.add_argument("--name", default=None,  help="Bot name (defaults to strategy name)")
    parser.add_argument("--api",  default="https://aiavatar.torilab.ai/poly-arena", help="API base URL")
    parser.add_argument("--amount", type=float, default=100.0, help="Trade amount USD")
    parser.add_argument("--symbols", nargs="+",
                        default=["BTC", "ETH"],
                        help="Symbols to trade")
    args = parser.parse_args()

    cls    = STRATEGIES[args.bot]
    name   = args.name or f"{cls.__name__}-{args.tf}"
    kwargs = dict(
        name      = name,
        timeframe = args.tf,
        api_key   = args.key,
        api_base  = args.api,
        amount    = args.amount,
        symbols   = args.symbols,
    )

    bot = cls(**kwargs)
    bot.run()


if __name__ == "__main__":
    main()
