"""
LLM-powered trading strategy bot.

Sends recent OHLCV candle data + basic indicators to an LLM and asks it
to predict whether the next candle will be GREEN (bullish) or RED (bearish),
along with a confidence score that drives dynamic position sizing.

Supports two providers:
  • Anthropic Claude  (pip install anthropic)
  • Google Gemini     (pip install google-genai)   ← structured output

Quick start:

    from llm_strategy import make_bot

    bot = make_bot(
        provider  = "gemini",           # "gemini" | "anthropic"
        llm_key   = "AIza...",
        bot_name  = "LLM-M5",
        timeframe = "M5",
        api_key   = "<polyarena-api-key>",
        amount    = 1000.0,             # maximum trade size (at 100% confidence)
    )
    bot.run()

Position sizing (amount_tiers):
    confidence ≥ 0.90  →  100% of max amount
    confidence ≥ 0.80  →   75%
    confidence ≥ 0.70  →   50%
    confidence ≥ 0.60  →   25%
    confidence <  0.60  →  skip (no trade)

No-trade conditions (returns None / skips):
  - LLM call fails or times out
  - Insufficient candle data (< 20 candles)
  - High volatility / choppy market (≥ 4 direction flips in last 5 candles)
  - Doji candle: last candle body < 20% of its range
  - [Gemini]    direction == "NEUTRAL" in structured output
  - [Gemini]    confidence < min_confidence (default 0.60)
  - [Anthropic] DECISION: NEUTRAL in response text
  - [Anthropic] CONFIDENCE below min_confidence
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Literal, Optional

from pydantic import BaseModel

from bots.base_bot import BaseBot, Candle, _rsi, _sma

# ── Decision result ─────────────────────────────────────────────────────────────


@dataclass
class _Decision:
    direction: Optional[str]  # "GREEN" | "RED" | None (skip)
    confidence: float  # 0.0 – 1.0


# ── Position sizing ─────────────────────────────────────────────────────────────

# List of (min_confidence, fraction_of_max_amount) sorted descending by min_conf.
AmountTiers = list[tuple[float, float]]

DEFAULT_AMOUNT_TIERS: AmountTiers = [
    (0.90, 1.00),  # very high confidence → full amount
    (0.80, 0.75),  # high confidence      → 75 %
    (0.70, 0.50),  # medium confidence    → 50 %
    (0.60, 0.25),  # low confidence       → 25 %
]


def _confidence_to_amount(
    confidence: float, base_amount: float, tiers: AmountTiers
) -> float:
    """
    Map a confidence score to a trade amount using the tier table.
    Returns 0.0 if confidence falls below every tier (trade will be skipped).
    """
    for min_conf, fraction in sorted(tiers, key=lambda x: x[0], reverse=True):
        if confidence >= min_conf:
            return round(base_amount * fraction, 2)
    return 0.0


# ── System prompt ──────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a professional cryptocurrency technical analyst specializing in short-term \
binary option predictions. Your task is to analyze candlestick data and predict \
whether the NEXT candle will close HIGHER (GREEN) or LOWER (RED) than it opens.

Rules:
- Analyze the provided OHLCV data and indicators carefully.
- Consider price action, trend, momentum, and volume.
- Assign a confidence score between 0.0 and 1.0 to your prediction.
  A score of 1.0 means absolute certainty; 0.5 means essentially a coin flip.
- If the market is choppy, sideways, or the signal is truly unclear, output NEUTRAL.
- Only commit to GREEN or RED when there is a reasonably clear directional bias \
  (confidence ≥ 0.60).
- End your response with EXACTLY these two lines (no exceptions):
    DECISION: GREEN   CONFIDENCE: 0.82
    DECISION: RED     CONFIDENCE: 0.71
    DECISION: NEUTRAL CONFIDENCE: 0.50
"""

# ── Candle quality filters ─────────────────────────────────────────────────────

_MIN_CANDLES = 20
_DOJI_RATIO = 0.20  # body / range < 20 % → doji
_CHOP_FLIPS = 4  # ≥ N direction flips in last 5 candles → choppy


def _is_doji(candle: Candle) -> bool:
    if candle.range == 0:
        return True
    return candle.body / candle.range < _DOJI_RATIO


def _is_choppy(candles: list[Candle], lookback: int = 5) -> bool:
    recent = candles[-lookback:]
    if len(recent) < lookback:
        return False
    flips = sum(
        1
        for i in range(1, len(recent))
        if recent[i].is_bullish != recent[i - 1].is_bullish
    )
    return flips >= _CHOP_FLIPS


def _pre_filter(symbol: str, candles: list[Candle], log: logging.Logger) -> bool:
    """Return True (= skip trade) if candle quality is insufficient."""
    if len(candles) < _MIN_CANDLES:
        log.info(
            "SKIP %s — not enough candles (%d < %d)", symbol, len(candles), _MIN_CANDLES
        )
        return True
    if _is_doji(candles[-1]):
        log.info(
            "SKIP %s — doji (body=%.5f range=%.5f)",
            symbol,
            candles[-1].body,
            candles[-1].range,
        )
        return True
    if _is_choppy(candles):
        log.info("SKIP %s — choppy market (too many recent direction flips)", symbol)
        return True
    return False


# ── Prompt builder ─────────────────────────────────────────────────────────────


def _build_prompt(symbol: str, timeframe: str, candles: list[Candle]) -> str:
    closes = [c.close for c in candles]
    sma5 = _sma(closes, 5)
    sma20 = _sma(closes, 20)
    rsi14 = _rsi(closes, 14)

    rows = []
    for c in candles[-15:]:
        rows.append(
            f"  {c.open_time.strftime('%Y-%m-%d %H:%M')}  "
            f"O={c.open:.4f}  H={c.high:.4f}  L={c.low:.4f}  C={c.close:.4f}  "
            f"V={c.volume:.1f}  {'▲' if c.is_bullish else '▼'}"
        )

    indicators = []
    if sma5 is not None:
        indicators.append(f"  SMA(5)  = {sma5:.4f}")
    if sma20 is not None:
        indicators.append(f"  SMA(20) = {sma20:.4f}")
    if rsi14 is not None:
        indicators.append(f"  RSI(14) = {rsi14:.1f}")
    last = candles[-1]
    indicators.append(f"  Last close = {last.close:.4f}  (open {last.open:.4f})")
    if sma5 and sma20:
        bias = "BULLISH" if sma5 > sma20 else "BEARISH"
        indicators.append(f"  SMA bias   = {bias}")

    return (
        f"Symbol: {symbol}USDT  |  Timeframe: {timeframe}\n\n"
        f"Recent candles (newest at bottom):\n"
        + "\n".join(rows)
        + f"\n\nIndicators:\n"
        + "\n".join(indicators)
        + f"\n\nPredict the direction of the NEXT {timeframe} candle for {symbol}USDT.\n"
        f"Reply with DECISION and CONFIDENCE as instructed."
    )


# ── LLM base ───────────────────────────────────────────────────────────────────


class LLMBot(BaseBot):
    """
    Abstract base for LLM-driven bots.

    Subclasses implement `_decide_full()` which returns a `_Decision`
    (direction + confidence).  The confidence is mapped to a trade amount
    via `amount_tiers`; `self.amount` is the *maximum* possible trade size.
    """

    def __init__(
        self,
        *,
        llm_key: str,
        model: str,
        min_confidence: float = 0.60,
        amount_tiers: AmountTiers = DEFAULT_AMOUNT_TIERS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.llm_key = llm_key
        self.model = model
        self.min_confidence = min_confidence
        self.amount_tiers = amount_tiers

    # -- core method subclasses must implement ---------------------------------

    @abstractmethod
    def _decide_full(self, symbol: str, candles: list[Candle]) -> _Decision: ...

    # -- BaseBot interface ------------------------------------------------------

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        """Thin wrapper — used when a caller only needs the direction."""
        return self._decide_full(symbol, candles).direction

    def on_candle(self) -> None:
        """
        Override BaseBot.on_candle to apply confidence-based position sizing.

        Flow per symbol:
          1. _decide_full() → (_Decision.direction, _Decision.confidence)
          2. confidence → trade_amount via amount_tiers
          3. place_trade(symbol, direction, amount=trade_amount)
        """
        placed = skipped = 0
        self.log.info(
            "── Candle cycle start  tf=%s  symbols=%s ──",
            self.timeframe, self.symbols,
        )

        for symbol in self.symbols:
            candles = self.fetch_candles(symbol)
            if not candles:
                self.log.warning("No candles for %s, skipping.", symbol)
                skipped += 1
                continue

            last = candles[-1]
            self.log.debug(
                "%s  last_candle=%s  O=%.4f H=%.4f L=%.4f C=%.4f  body=%.5f range=%.5f",
                symbol,
                last.open_time.strftime("%Y-%m-%d %H:%M"),
                last.open, last.high, last.low, last.close,
                last.body, last.range,
            )

            result = self._decide_full(symbol, candles)

            if result.direction not in ("GREEN", "RED"):
                skipped += 1
                continue

            trade_amount = _confidence_to_amount(
                result.confidence, self.amount, self.amount_tiers
            )
            if trade_amount <= 0:
                self.log.info(
                    "SKIP %s — confidence %.2f too low for any tier",
                    symbol, result.confidence,
                )
                skipped += 1
                continue

            self.log.info(
                "TRADE %s %s  conf=%.2f  amount=%.2f / %.2f (%.0f%%)",
                symbol, result.direction,
                result.confidence,
                trade_amount, self.amount,
                trade_amount / self.amount * 100,
            )
            if self.place_trade(symbol, result.direction, amount=trade_amount):
                placed += 1
            else:
                skipped += 1

        self.log.info(
            "── Candle cycle done   placed=%d  skipped=%d ──",
            placed, skipped,
        )


# ── Gemini structured output ───────────────────────────────────────────────────


class _GeminiDecision(BaseModel):
    analysis: str
    direction: Literal["GREEN", "RED", "NEUTRAL"]
    confidence: float  # 0.0 – 1.0


class GeminiBot(LLMBot):
    DEFAULT_MODEL = "gemini-3-flash-preview"

    def __init__(self, **kwargs):
        try:
            from google import genai
            from google.genai import types as genai_types

            self._genai = genai
            self._genai_types = genai_types
        except ImportError:
            raise ImportError("pip install google-genai")
        super().__init__(**kwargs)
        self._client = self._genai.Client(api_key=self.llm_key)

    def _decide_full(self, symbol: str, candles: list[Candle]) -> _Decision:
        if _pre_filter(symbol, candles, self.log):
            return _Decision(None, 0.0)

        prompt = _build_prompt(symbol, self.timeframe, candles)
        try:
            resp = self._client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=self._genai_types.GenerateContentConfig(
                    system_instruction=_SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=_GeminiDecision,
                ),
            )
            d: _GeminiDecision = resp.parsed
            self.log.info(
                "Gemini %s → %s  conf=%.2f",
                symbol, d.direction, d.confidence,
            )
            self.log.debug("Gemini %s analysis: %s", symbol, d.analysis)

            if d.direction == "NEUTRAL":
                self.log.info("SKIP %s — Gemini returned NEUTRAL", symbol)
                return _Decision(None, 0.0)

            if d.confidence < self.min_confidence:
                self.log.info(
                    "SKIP %s — confidence %.2f < threshold %.2f",
                    symbol,
                    d.confidence,
                    self.min_confidence,
                )
                return _Decision(None, d.confidence)

            return _Decision(d.direction, d.confidence)

        except Exception as exc:
            self.log.error("Gemini call failed for %s: %s", symbol, exc)
            return _Decision(None, 0.0)


# ── Factory ────────────────────────────────────────────────────────────────────


def make_bot(
    *,
    provider: Literal["gemini", "anthropic"],
    llm_key: str,
    bot_name: str,
    timeframe: str,
    api_key: str,
    model: Optional[str] = None,
    symbols: Optional[list[str]] = None,
    amount: float = 100.0,
    api_base: str = "https://aiavatar.torilab.ai/poly-arena",
    min_confidence: float = 0.60,
    amount_tiers: AmountTiers = DEFAULT_AMOUNT_TIERS,
) -> LLMBot:
    """
    Create and return an LLM bot ready to call .run().

    Args:
        provider:       "gemini" or "anthropic"
        llm_key:        API key for the LLM provider
        bot_name:       Display name registered in PolyArena
        timeframe:      "M5" | "M15" | "H1"
        api_key:        PolyArena x-api-key for this bot
        model:          LLM model name (uses provider default if omitted)
        symbols:        List of symbols to trade (default: BTC/ETH/SOL/XRP)
        amount:         Maximum trade size in USD (used at 100% confidence)
        api_base:       PolyArena server URL
        min_confidence: Skip trade if confidence is below this (default 0.60)
        amount_tiers:   List of (min_conf, fraction) pairs for position sizing.
                        Example: [(0.90, 1.0), (0.75, 0.5), (0.60, 0.25)]
    """
    cls_map = {"gemini": GeminiBot}
    cls = cls_map.get(provider)
    if cls is None:
        raise ValueError(f"provider must be 'gemini' or 'anthropic', got {provider!r}")

    return cls(
        name=bot_name,
        timeframe=timeframe,
        api_key=api_key,
        llm_key=llm_key,
        model=model or cls.DEFAULT_MODEL,
        symbols=symbols or ["BTC", "ETH", "SOL", "XRP"],
        amount=amount,
        api_base=api_base,
        min_confidence=min_confidence,
        amount_tiers=amount_tiers,
    )
