"""
Price Action Bot — pure technical analysis from raw OHLCV data.

Uses candlestick patterns, trend detection (swing highs/lows + SMA cross),
support/resistance zones, and cross-symbol consensus to predict next candle
direction.

Cross-symbol consensus:
  - BTC acts as market leader: altcoin signals are boosted when aligned with
    BTC trend and penalised when contradicting it.
  - Market-wide majority vote: if ≥ 60% of symbols agree on direction, all
    aligned signals get a confidence boost.

No external indicator libraries — everything computed from raw candles.

Run:
    python -m bots.price_action_bot --tf M5 --key <API_KEY> --amount 100 --symbols BTC ETH SOL XRP
"""

from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from bots.base_bot import BaseBot, Candle, _sma, FETCH_DELAY

# ── Constants ─────────────────────────────────────────────────────────────────

_MIN_CANDLES = 20
_DOJI_RATIO = 0.15  # body < 15% of range → doji
_CHOP_FLIPS = 4  # ≥4 direction flips in last 5 candles → choppy
_SPIKE_MULT = 3.0  # range > 3x avg range → spike
_SR_CLUSTER_PCT = 0.003  # 0.3% clustering for S/R zones
_SR_MIN_TOUCHES = 2
_SWING_WINDOW = 2
_TREND_LOOKBACK = 20
_SR_LOOKBACK = 40
_MIN_CONFIDENCE = 0.55

# Reduced fetch delay — Binance propagates candle data within 1-2s
_FAST_FETCH_DELAY = 2

# Volume analysis
_VOL_SMA_PERIOD = 20  # period for average volume baseline
_VOL_HIGH_THRESH = 1.5  # vol_ratio above this = high volume
_VOL_CLIMAX_THRESH = 2.5  # vol_ratio above this = climax (exhaustion risk)
_VOL_DRY_THRESH = 0.4  # vol_ratio below this = too thin, skip
_VOL_CONFIRM_BOOST = 0.07  # high volume confirms pattern
_VOL_NORMAL_BOOST = 0.03  # normal volume, slight confirmation
_VOL_DRY_PENALTY = -0.08  # thin volume, unreliable signal
_VOL_CLIMAX_PENALTY = -0.05  # climax = possible exhaustion
_VOL_TREND_BOOST = 0.04  # rising volume in pattern direction
_VOL_DIVERGENCE_PENALTY = -0.06  # price trending but volume fading

# Cross-symbol consensus
_BTC_ALIGN_BOOST = 0.08  # altcoin signal aligned with BTC trend
_BTC_AGAINST_PENALTY = -0.10  # altcoin signal against BTC trend
_MAJORITY_BOOST = 0.05  # signal aligned with market majority
_MAJORITY_THRESHOLD = 0.60  # ≥60% of symbols must agree

# Position sizing tiers: (min_confidence, fraction_of_max_amount)
AMOUNT_TIERS = [
    (0.85, 1.00),
    (0.75, 0.75),
    (0.65, 0.50),
    (0.55, 0.25),
]


# ── Data structures ──────────────────────────────────────────────────────────


class Direction(Enum):
    UP = "UP"
    DOWN = "DOWN"
    SIDEWAYS = "SIDEWAYS"


class ZoneType(Enum):
    SUPPORT = "SUPPORT"
    RESISTANCE = "RESISTANCE"


@dataclass
class PatternSignal:
    name: str
    direction: str  # "GREEN" or "RED"
    confidence: float  # base confidence 0.0–1.0


@dataclass
class TrendState:
    direction: Direction
    strength: float  # 0.0–1.0
    swing_highs: list[float] = field(default_factory=list)
    swing_lows: list[float] = field(default_factory=list)


@dataclass
class SRZone:
    level: float
    zone_type: ZoneType
    touches: int
    strength: float  # 0.0–1.0


@dataclass
class VolumeProfile:
    """Volume analysis for the current candle relative to recent history."""

    vol_ratio: float  # current volume / SMA(20) volume
    is_climax: bool  # volume > 2.5x average (exhaustion risk)
    is_dry: bool  # volume < 0.4x average (unreliable)
    vol_trend: float  # -1.0 to +1.0: rising (+) or falling (-) volume
    price_vol_divergence: bool  # price trending but volume fading


@dataclass
class SymbolAnalysis:
    """Per-symbol intermediate result before consensus adjustment."""

    symbol: str
    direction: str  # "GREEN" or "RED"
    confidence: float  # pre-consensus confidence
    trend: TrendState
    patterns: list[PatternSignal]
    volume: Optional[VolumeProfile] = None


# ── Pre-filters ──────────────────────────────────────────────────────────────


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


def _is_spike(candles: list[Candle], lookback: int = 20) -> bool:
    if len(candles) < lookback:
        return False
    avg_range = sum(c.range for c in candles[-lookback:]) / lookback
    if avg_range == 0:
        return False
    return candles[-1].range > _SPIKE_MULT * avg_range


def _pre_filter(symbol: str, candles: list[Candle], log: logging.Logger) -> bool:
    """Return True (= skip trade) if candle quality is insufficient."""
    if len(candles) < _MIN_CANDLES:
        log.info(
            "SKIP %s — not enough candles (%d < %d)", symbol, len(candles), _MIN_CANDLES
        )
        return True
    if _is_doji(candles[-1]):
        log.info("SKIP %s — doji candle", symbol)
        return True
    if _is_choppy(candles):
        log.info("SKIP %s — choppy market", symbol)
        return True
    if _is_spike(candles):
        log.info("SKIP %s — spike candle (range > %.0fx avg)", symbol, _SPIKE_MULT)
        return True
    # Dry volume filter — skip if market is too thin
    vol_profile = _analyze_volume(candles)
    if vol_profile is not None and vol_profile.is_dry:
        log.info(
            "SKIP %s — dry volume (ratio=%.2f < %.2f)",
            symbol,
            vol_profile.vol_ratio,
            _VOL_DRY_THRESH,
        )
        return True
    return False


# ── Volume analysis ─────────────────────────────────────────────────────────


def _analyze_volume(candles: list[Candle]) -> Optional[VolumeProfile]:
    """Compute volume profile for the most recent candle vs recent history."""
    if len(candles) < _VOL_SMA_PERIOD:
        return None

    volumes = [c.volume for c in candles]
    avg_vol = sum(volumes[-_VOL_SMA_PERIOD:]) / _VOL_SMA_PERIOD
    if avg_vol == 0:
        return None

    current_vol = candles[-1].volume
    vol_ratio = current_vol / avg_vol
    is_climax = vol_ratio > _VOL_CLIMAX_THRESH
    is_dry = vol_ratio < _VOL_DRY_THRESH

    # Volume trend: compare avg volume of last 5 candles vs previous 5
    if len(candles) >= 10:
        recent_5 = sum(c.volume for c in candles[-5:]) / 5
        prev_5 = sum(c.volume for c in candles[-10:-5]) / 5
        if prev_5 > 0:
            vol_trend = max(-1.0, min(1.0, (recent_5 - prev_5) / prev_5))
        else:
            vol_trend = 0.0
    else:
        vol_trend = 0.0

    # Price-volume divergence: price making new highs/lows but volume declining
    price_vol_divergence = False
    if len(candles) >= 5:
        last_5 = candles[-5:]
        closes = [c.close for c in last_5]
        vols = [c.volume for c in last_5]
        price_rising = all(closes[i] >= closes[i - 1] for i in range(1, len(closes)))
        price_falling = all(closes[i] <= closes[i - 1] for i in range(1, len(closes)))
        vol_declining = all(vols[i] <= vols[i - 1] for i in range(1, len(vols)))
        if (price_rising or price_falling) and vol_declining:
            price_vol_divergence = True

    return VolumeProfile(
        vol_ratio=vol_ratio,
        is_climax=is_climax,
        is_dry=is_dry,
        vol_trend=vol_trend,
        price_vol_divergence=price_vol_divergence,
    )


# ── Candlestick pattern detectors ────────────────────────────────────────────


def _detect_hammer(candles: list[Candle]) -> Optional[PatternSignal]:
    """Hammer: small body at top, long lower shadow (≥2x body), short upper shadow."""
    c = candles[-1]
    if c.range == 0 or c.body == 0:
        return None
    lower_shadow = min(c.open, c.close) - c.low
    upper_shadow = c.high - max(c.open, c.close)
    if lower_shadow >= 2 * c.body and upper_shadow <= c.body * 0.5:
        return PatternSignal("Hammer", "GREEN", 0.55)
    return None


def _detect_shooting_star(candles: list[Candle]) -> Optional[PatternSignal]:
    """Shooting Star: small body at bottom, long upper shadow (≥2x body)."""
    c = candles[-1]
    if c.range == 0 or c.body == 0:
        return None
    upper_shadow = c.high - max(c.open, c.close)
    lower_shadow = min(c.open, c.close) - c.low
    if upper_shadow >= 2 * c.body and lower_shadow <= c.body * 0.5:
        return PatternSignal("Shooting Star", "RED", 0.55)
    return None


def _detect_bullish_engulfing(candles: list[Candle]) -> Optional[PatternSignal]:
    """Current bullish candle's body fully engulfs previous bearish candle's body."""
    if len(candles) < 2:
        return None
    prev, curr = candles[-2], candles[-1]
    if prev.is_bullish or not curr.is_bullish:
        return None
    if curr.open <= prev.close and curr.close >= prev.open:
        return PatternSignal("Bullish Engulfing", "GREEN", 0.60)
    return None


def _detect_bearish_engulfing(candles: list[Candle]) -> Optional[PatternSignal]:
    """Current bearish candle's body fully engulfs previous bullish candle's body."""
    if len(candles) < 2:
        return None
    prev, curr = candles[-2], candles[-1]
    if not prev.is_bullish or curr.is_bullish:
        return None
    if curr.open >= prev.close and curr.close <= prev.open:
        return PatternSignal("Bearish Engulfing", "RED", 0.60)
    return None


def _detect_morning_star(candles: list[Candle]) -> Optional[PatternSignal]:
    """Three-candle reversal: bearish → small body → bullish.
    Crypto adaptation: no gap requirement."""
    if len(candles) < 3:
        return None
    first, second, third = candles[-3], candles[-2], candles[-1]
    if first.is_bullish or third.is_bullish is False:
        return None
    if first.body == 0:
        return None
    if second.body < first.body * 0.5 and third.is_bullish:
        mid_first = (first.open + first.close) / 2
        if third.close > mid_first:
            return PatternSignal("Morning Star", "GREEN", 0.65)
    return None


def _detect_evening_star(candles: list[Candle]) -> Optional[PatternSignal]:
    """Three-candle reversal: bullish → small body → bearish.
    Crypto adaptation: no gap requirement."""
    if len(candles) < 3:
        return None
    first, second, third = candles[-3], candles[-2], candles[-1]
    if not first.is_bullish or third.is_bullish:
        return None
    if first.body == 0:
        return None
    if second.body < first.body * 0.5 and not third.is_bullish:
        mid_first = (first.open + first.close) / 2
        if third.close < mid_first:
            return PatternSignal("Evening Star", "RED", 0.65)
    return None


def _detect_three_white_soldiers(candles: list[Candle]) -> Optional[PatternSignal]:
    """Three consecutive bullish candles with higher closes and decent bodies."""
    if len(candles) < 3:
        return None
    trio = candles[-3:]
    if not all(c.is_bullish for c in trio):
        return None
    if not (trio[1].close > trio[0].close and trio[2].close > trio[1].close):
        return None
    if any(c.range == 0 or c.body / c.range < 0.3 for c in trio):
        return None
    return PatternSignal("Three White Soldiers", "GREEN", 0.70)


def _detect_three_black_crows(candles: list[Candle]) -> Optional[PatternSignal]:
    """Three consecutive bearish candles with lower closes and decent bodies."""
    if len(candles) < 3:
        return None
    trio = candles[-3:]
    if any(c.is_bullish for c in trio):
        return None
    if not (trio[1].close < trio[0].close and trio[2].close < trio[1].close):
        return None
    if any(c.range == 0 or c.body / c.range < 0.3 for c in trio):
        return None
    return PatternSignal("Three Black Crows", "RED", 0.70)


_PATTERN_DETECTORS = [
    _detect_hammer,
    _detect_shooting_star,
    _detect_bullish_engulfing,
    _detect_bearish_engulfing,
    _detect_morning_star,
    _detect_evening_star,
    _detect_three_white_soldiers,
    _detect_three_black_crows,
]


def _detect_patterns(candles: list[Candle]) -> list[PatternSignal]:
    """Run all pattern detectors and return detected signals."""
    signals = []
    for detector in _PATTERN_DETECTORS:
        result = detector(candles)
        if result is not None:
            signals.append(result)
    return signals


# ── Trend detection ──────────────────────────────────────────────────────────


def _find_swing_points(
    candles: list[Candle], window: int = _SWING_WINDOW
) -> tuple[list[float], list[float]]:
    """Find swing highs and swing lows using a rolling window comparison."""
    swing_highs: list[float] = []
    swing_lows: list[float] = []
    for i in range(window, len(candles) - window):
        high = candles[i].high
        low = candles[i].low
        is_swing_high = all(
            high >= candles[i + d].high for d in range(-window, window + 1) if d != 0
        )
        is_swing_low = all(
            low <= candles[i + d].low for d in range(-window, window + 1) if d != 0
        )
        if is_swing_high:
            swing_highs.append(high)
        if is_swing_low:
            swing_lows.append(low)
    return swing_highs, swing_lows


def _detect_trend(candles: list[Candle]) -> TrendState:
    """Classify trend from swing points and SMA cross-check."""
    recent = candles[-_TREND_LOOKBACK:] if len(candles) >= _TREND_LOOKBACK else candles
    swing_highs, swing_lows = _find_swing_points(recent)

    direction = Direction.SIDEWAYS
    strength = 0.0

    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        hh_count = sum(
            1 for i in range(1, len(swing_highs)) if swing_highs[i] > swing_highs[i - 1]
        )
        ll_count = sum(
            1 for i in range(1, len(swing_lows)) if swing_lows[i] > swing_lows[i - 1]
        )
        lh_count = sum(
            1 for i in range(1, len(swing_highs)) if swing_highs[i] < swing_highs[i - 1]
        )
        hl_count = sum(
            1 for i in range(1, len(swing_lows)) if swing_lows[i] < swing_lows[i - 1]
        )

        up_score = hh_count + ll_count
        down_score = lh_count + hl_count
        total = max(up_score + down_score, 1)

        if up_score > down_score:
            direction = Direction.UP
            strength = up_score / total
        elif down_score > up_score:
            direction = Direction.DOWN
            strength = down_score / total

    # SMA cross-check for confirmation
    closes = [c.close for c in candles]
    sma5 = _sma(closes, 5)
    sma20 = _sma(closes, 20)
    if sma5 is not None and sma20 is not None:
        sma_bullish = sma5 > sma20
        if direction == Direction.UP and sma_bullish:
            strength = min(strength + 0.1, 1.0)
        elif direction == Direction.DOWN and not sma_bullish:
            strength = min(strength + 0.1, 1.0)
        elif direction == Direction.SIDEWAYS:
            direction = Direction.UP if sma_bullish else Direction.DOWN
            strength = 0.3

    return TrendState(direction, strength, swing_highs, swing_lows)


# ── Support / Resistance zones ───────────────────────────────────────────────


def _detect_sr_zones(candles: list[Candle], current_price: float) -> list[SRZone]:
    """Find S/R zones by clustering swing high/low levels."""
    recent = candles[-_SR_LOOKBACK:] if len(candles) >= _SR_LOOKBACK else candles
    swing_highs, swing_lows = _find_swing_points(recent)

    levels = swing_highs + swing_lows
    if not levels:
        return []

    levels.sort()
    clusters: list[list[float]] = []
    current_cluster: list[float] = [levels[0]]
    for lvl in levels[1:]:
        if (
            current_cluster
            and abs(lvl - current_cluster[0]) / current_cluster[0] <= _SR_CLUSTER_PCT
        ):
            current_cluster.append(lvl)
        else:
            clusters.append(current_cluster)
            current_cluster = [lvl]
    clusters.append(current_cluster)

    zones: list[SRZone] = []
    for cluster in clusters:
        if len(cluster) < _SR_MIN_TOUCHES:
            continue
        avg_level = sum(cluster) / len(cluster)
        zone_type = (
            ZoneType.SUPPORT if avg_level < current_price else ZoneType.RESISTANCE
        )
        strength = min(len(cluster) / 5.0, 1.0)
        zones.append(SRZone(avg_level, zone_type, len(cluster), strength))

    return zones


def _price_near_sr(
    price: float, zones: list[SRZone], threshold_pct: float = 0.005
) -> Optional[SRZone]:
    """Return the nearest S/R zone if price is within threshold_pct of it."""
    for zone in zones:
        if abs(price - zone.level) / zone.level <= threshold_pct:
            return zone
    return None


# ── Signal scoring (per-symbol, before consensus) ───────────────────────────


def _score_signals(
    patterns: list[PatternSignal],
    trend: TrendState,
    sr_zones: list[SRZone],
    current_price: float,
    log: logging.Logger,
    volume: Optional[VolumeProfile] = None,
) -> Optional[tuple[str, float]]:
    """
    Combine pattern signals with trend, S/R, and volume context.
    Returns (direction, confidence) or None if conflicting/weak signals.
    """
    if not patterns:
        return None

    green_patterns = [p for p in patterns if p.direction == "GREEN"]
    red_patterns = [p for p in patterns if p.direction == "RED"]

    if green_patterns and red_patterns:
        log.info(
            "SKIP — conflicting patterns: %s vs %s",
            [p.name for p in green_patterns],
            [p.name for p in red_patterns],
        )
        return None

    direction = "GREEN" if green_patterns else "RED"
    active_patterns = green_patterns or red_patterns

    base_conf = sum(p.confidence for p in active_patterns) / len(active_patterns)

    # Trend alignment
    trend_adj = 0.0
    if trend.direction != Direction.SIDEWAYS:
        trend_aligned = (direction == "GREEN" and trend.direction == Direction.UP) or (
            direction == "RED" and trend.direction == Direction.DOWN
        )
        if trend_aligned:
            trend_adj = 0.10 * trend.strength
        else:
            trend_adj = -0.15 * trend.strength

    # S/R context
    sr_adj = 0.0
    near_zone = _price_near_sr(current_price, sr_zones)
    if near_zone is not None:
        is_reversal_at_support = (
            near_zone.zone_type == ZoneType.SUPPORT and direction == "GREEN"
        )
        is_reversal_at_resistance = (
            near_zone.zone_type == ZoneType.RESISTANCE and direction == "RED"
        )
        if is_reversal_at_support or is_reversal_at_resistance:
            sr_adj = 0.08 * near_zone.strength
        else:
            sr_adj = -0.05 * near_zone.strength

    # Volume context
    vol_adj = 0.0
    if volume is not None:
        # 1. Volume confirmation: high volume strengthens the pattern signal
        if volume.vol_ratio >= _VOL_HIGH_THRESH:
            vol_adj += _VOL_CONFIRM_BOOST
        elif volume.vol_ratio >= 1.0:
            vol_adj += _VOL_NORMAL_BOOST

        # 2. Volume climax: extremely high volume often precedes reversals
        #    This is a warning — reduce confidence slightly
        if volume.is_climax:
            vol_adj += _VOL_CLIMAX_PENALTY

        # 3. Volume trend: rising volume in the pattern direction = conviction
        if volume.vol_trend > 0.2:
            # Volume is increasing — check if aligned with signal direction
            trend_aligned_vol = (
                direction == "GREEN" and trend.direction == Direction.UP
            ) or (direction == "RED" and trend.direction == Direction.DOWN)
            if trend_aligned_vol:
                vol_adj += _VOL_TREND_BOOST
        elif volume.vol_trend < -0.2:
            # Volume fading — weakens conviction
            vol_adj -= 0.02

        # 4. Price-volume divergence: price trending but volume declining
        #    Strong warning of trend exhaustion
        if volume.price_vol_divergence:
            vol_adj += _VOL_DIVERGENCE_PENALTY

        log.info(
            "Volume: ratio=%.2f  trend=%+.2f  climax=%s  divergence=%s  vol_adj=%+.2f",
            volume.vol_ratio,
            volume.vol_trend,
            volume.is_climax,
            volume.price_vol_divergence,
            vol_adj,
        )

    final_conf = base_conf + trend_adj + sr_adj + vol_adj
    final_conf = max(0.0, min(1.0, final_conf))

    log.info(
        "Score: dir=%s  base=%.2f  trend=%+.2f  sr=%+.2f  vol=%+.2f  final=%.2f  "
        "patterns=[%s]  trend=%s(%.2f)",
        direction,
        base_conf,
        trend_adj,
        sr_adj,
        vol_adj,
        final_conf,
        ", ".join(p.name for p in active_patterns),
        trend.direction.value,
        trend.strength,
    )

    return direction, final_conf


# ── Cross-symbol consensus ──────────────────────────────────────────────────


def _apply_consensus(
    analyses: list[SymbolAnalysis],
    log: logging.Logger,
) -> dict[str, tuple[str, float]]:
    """
    Adjust per-symbol confidence using cross-symbol consensus.

    Two layers:
      1. BTC dominance — BTC trend biases altcoin confidence.
      2. Market majority — if ≥60% of signals agree, aligned ones get a boost.

    Returns {symbol: (direction, adjusted_confidence)}.
    """
    if not analyses:
        return {}

    # ── 1. Find BTC trend (if BTC is in the analysis set) ──
    btc_trend: Optional[Direction] = None
    btc_direction: Optional[str] = None
    for a in analyses:
        if a.symbol == "BTC":
            btc_trend = a.trend.direction
            btc_direction = a.direction
            break

    # ── 2. Compute market majority ──
    green_count = sum(1 for a in analyses if a.direction == "GREEN")
    red_count = sum(1 for a in analyses if a.direction == "RED")
    total = green_count + red_count
    majority_dir: Optional[str] = None
    if total > 0:
        if green_count / total >= _MAJORITY_THRESHOLD:
            majority_dir = "GREEN"
        elif red_count / total >= _MAJORITY_THRESHOLD:
            majority_dir = "RED"

    if majority_dir:
        log.info(
            "Consensus: market majority=%s (%d/%d symbols)",
            majority_dir,
            max(green_count, red_count),
            total,
        )

    # ── 3. Adjust each symbol ──
    results: dict[str, tuple[str, float]] = {}
    for a in analyses:
        adj = 0.0

        # BTC dominance (only for altcoins, skip BTC itself)
        if (
            btc_trend is not None
            and a.symbol != "BTC"
            and btc_trend != Direction.SIDEWAYS
        ):
            btc_agrees = (a.direction == "GREEN" and btc_trend == Direction.UP) or (
                a.direction == "RED" and btc_trend == Direction.DOWN
            )
            if btc_agrees:
                adj += _BTC_ALIGN_BOOST
                log.info(
                    "Consensus: %s %s aligned with BTC trend %s → %+.2f",
                    a.symbol,
                    a.direction,
                    btc_trend.value,
                    _BTC_ALIGN_BOOST,
                )
            else:
                adj += _BTC_AGAINST_PENALTY
                log.info(
                    "Consensus: %s %s against BTC trend %s → %+.2f",
                    a.symbol,
                    a.direction,
                    btc_trend.value,
                    _BTC_AGAINST_PENALTY,
                )

        # BTC direction consensus (altcoin signal vs BTC signal direction)
        if btc_direction is not None and a.symbol != "BTC":
            if a.direction == btc_direction:
                adj += 0.03  # small extra boost for matching BTC direction
            else:
                adj -= 0.03

        # Market majority
        if majority_dir is not None:
            if a.direction == majority_dir:
                adj += _MAJORITY_BOOST
            # No penalty for being minority — the individual signal might be right

        final_conf = max(0.0, min(1.0, a.confidence + adj))

        if adj != 0.0:
            log.info(
                "Consensus: %s  pre=%.2f  adj=%+.2f  post=%.2f",
                a.symbol,
                a.confidence,
                adj,
                final_conf,
            )

        results[a.symbol] = (a.direction, final_conf)

    return results


# ── Position sizing ──────────────────────────────────────────────────────────


def _confidence_to_amount(confidence: float, base_amount: float) -> float:
    for min_conf, fraction in sorted(AMOUNT_TIERS, key=lambda x: x[0], reverse=True):
        if confidence >= min_conf:
            return round(base_amount * fraction, 2)
    return 0.0


# ── Bot class ────────────────────────────────────────────────────────────────


class PriceActionBot(BaseBot):
    """
    Pure price-action bot with cross-symbol consensus.

    Improvements over baseline:
      - Parallel candle fetching (all symbols at once via threads)
      - BTC dominance: altcoin signals are adjusted based on BTC trend
      - Market majority: confidence boost when most symbols agree
      - Reduced fetch delay (2s instead of 5s)
    """

    def __init__(
        self,
        *,
        min_confidence: float = _MIN_CONFIDENCE,
        fetch_delay: int = _FAST_FETCH_DELAY,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.min_confidence = min_confidence
        self.fetch_delay = fetch_delay

    def decide(self, symbol: str, candles: list[Candle]) -> Optional[str]:
        """Thin wrapper for BaseBot compatibility (no position sizing)."""
        result = self._analyze_symbol(symbol, candles)
        return result.direction if result else None
        # if not result:
        #     return None
        # return "RED" if result.direction == "GREEN" else "GREEN"

    def _analyze_symbol(
        self, symbol: str, candles: list[Candle]
    ) -> Optional[SymbolAnalysis]:
        """
        Run full per-symbol analysis pipeline (before consensus).
        Returns SymbolAnalysis or None if filtered/no signal.
        """
        if _pre_filter(symbol, candles, self.log):
            return None

        patterns = _detect_patterns(candles)
        if not patterns:
            self.log.debug("SKIP %s — no patterns detected", symbol)
            return None

        self.log.info(
            "%s patterns: [%s]",
            symbol,
            ", ".join(f"{p.name}({p.direction})" for p in patterns),
        )

        trend = _detect_trend(candles)
        current_price = candles[-1].close
        sr_zones = _detect_sr_zones(candles, current_price)
        volume = _analyze_volume(candles)

        result = _score_signals(
            patterns,
            trend,
            sr_zones,
            current_price,
            self.log,
            volume=volume,
        )
        if result is None:
            return None

        direction, confidence = result
        return SymbolAnalysis(symbol, direction, confidence, trend, patterns, volume)

    def _fetch_all_candles(self) -> dict[str, list[Candle]]:
        """Fetch candles for all symbols concurrently."""
        candle_map: dict[str, list[Candle]] = {}
        with ThreadPoolExecutor(max_workers=len(self.symbols)) as pool:
            futures = {
                pool.submit(self.fetch_candles, sym): sym for sym in self.symbols
            }
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    candle_map[sym] = fut.result()
                except Exception as exc:
                    self.log.error("Parallel fetch failed for %s: %s", sym, exc)
                    candle_map[sym] = []
        return candle_map

    def on_candle(self) -> None:
        """
        Override BaseBot.on_candle with:
          1. Parallel fetch for all symbols
          2. Per-symbol analysis
          3. Cross-symbol consensus adjustment
          4. Confidence-based position sizing + trade placement
        """
        placed = skipped = 0
        self.log.info(
            "── PriceAction cycle start  tf=%s  symbols=%s ──",
            self.timeframe,
            self.symbols,
        )

        # ── 1. Parallel fetch ──
        t0 = time.monotonic()
        candle_map = self._fetch_all_candles()
        fetch_ms = (time.monotonic() - t0) * 1000
        self.log.info("Fetched %d symbols in %.0fms", len(candle_map), fetch_ms)

        # ── 2. Per-symbol analysis (pure computation, fast) ──
        analyses: list[SymbolAnalysis] = []
        skipped_symbols: set[str] = set()
        for symbol in self.symbols:
            candles = candle_map.get(symbol, [])
            if not candles:
                self.log.warning("No candles for %s, skipping.", symbol)
                skipped += 1
                skipped_symbols.add(symbol)
                continue
            analysis = self._analyze_symbol(symbol, candles)
            if analysis is not None:
                analyses.append(analysis)
            else:
                skipped += 1
                skipped_symbols.add(symbol)

        if not analyses:
            self.log.info(
                "── PriceAction cycle done   placed=0  skipped=%d  (no signals) ──",
                skipped,
            )
            return

        # ── 3. Cross-symbol consensus ──
        adjusted = _apply_consensus(analyses, self.log)

        # ── 4. Place trades ──
        for symbol, (direction, confidence) in adjusted.items():
            if confidence < self.min_confidence:
                self.log.info(
                    "SKIP %s — post-consensus confidence %.2f < threshold %.2f",
                    symbol,
                    confidence,
                    self.min_confidence,
                )
                skipped += 1
                continue

            trade_amount = _confidence_to_amount(confidence, self.amount)
            if trade_amount <= 0:
                self.log.info(
                    "SKIP %s — confidence %.2f below all tiers", symbol, confidence
                )
                skipped += 1
                continue

            self.log.info(
                "TRADE %s %s  conf=%.2f  amount=%.2f / %.2f (%.0f%%)",
                symbol,
                direction,
                confidence,
                trade_amount,
                self.amount,
                trade_amount / self.amount * 100,
            )
            if self.place_trade(symbol, direction, amount=trade_amount):
                placed += 1
            else:
                skipped += 1

        self.log.info(
            "── PriceAction cycle done   placed=%d  skipped=%d ──", placed, skipped
        )

    def run(self) -> None:
        """Override run() to use reduced fetch delay."""
        self.log.info(
            "Started  name=%s  tf=%s  symbols=%s  amount=%.2f  api=%s  fetch_delay=%ds",
            self.name,
            self.timeframe,
            self.symbols,
            self.amount,
            self.api_base,
            self.fetch_delay,
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

            wait = self._seconds_to_next_candle() + self.fetch_delay
            self.log.info(
                "Sleeping %.1fs → next %s candle (+%ds lag)  [cycle #%d]",
                wait,
                self.timeframe,
                self.fetch_delay,
                cycle,
            )
            time.sleep(wait)


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )

    bot = PriceActionBot(
        name="PriceActionBot",
        timeframe="M5",
        api_key="Ab1qAT3O2QDx1PFOCL2LhHMgRAi-NLTdMbN51f-Em6M",
        amount=300,
        symbols=["BTC", "ETH"],
        min_confidence=0.55,
        api_base="https://aiavatar.torilab.ai/poly-arena",
    )
    bot.run()


if __name__ == "__main__":
    main()
