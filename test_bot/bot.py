"""
Test bot — 5 users × 3-5 bots, mỗi bot mỗi tick chạy random vài test case.

Mỗi tick (snipe trước candle boundary):
  1. Bot gửi batch lệnh bình thường theo profile (aggressive, conservative, ...)
  2. Xen kẽ random 2-5 test case từ CASE_POOL (boundary, invalid, edge, A+1, ...)

CASE_POOL bao gồm:
  - MARKET/LIMIT cơ bản (all symbols × timeframes × forecasts)
  - Boundary prices: 0.01, 0.02, 0.49, 0.50, 0.51, 0.98, 0.99, invalid 0/1
  - Boundary amounts: $0.01, $0.10, $1, $5, full balance, over balance, 0, negative
  - LIMIT + TTL: 1s, 5s, 10s, 30s, 60s, 120s, 300s
  - LIMIT immediate (at/above ask), LIMIT deferred (below ask)
  - session_offset=0, session_offset=1 (A+1)
  - Slippage tolerance: 0.01, 0.10, 0.50, 1.0, invalid 0/1.5
  - Invalid payloads: bad symbol, bad tf, bad forecast, missing amount, bad TTL
  - Timestamp edge: current open, next boundary, far past, far future
  - Auth edge: bad API key

Usage:
    python test_bot/bot.py                                    # defaults
    NUM_USERS=5 BOTS_PER_USER=5 python test_bot/bot.py       # env overrides
    API_URL=http://host:8099/poly-arena python test_bot/bot.py
"""

import os
import random
import time
import logging
import threading
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s",
)
log = logging.getLogger("test-bot")

BASE = os.environ.get("API_URL", "http://localhost:8099/poly-arena")
NUM_USERS = int(os.environ.get("NUM_USERS", "5"))
BOTS_PER_USER = int(os.environ.get("BOTS_PER_USER", "5"))
TRADES_PER_TICK = int(os.environ.get("TRADES_PER_TICK", "15"))
CASES_PER_TICK = int(os.environ.get("CASES_PER_TICK", "4"))
PASSWORD = os.environ.get("TEST_PASSWORD", "testpass123")
SNIPE_OFFSET_S = int(os.environ.get("SNIPE_OFFSET_S", "2"))

BOT_BALANCE_MIN = int(os.environ.get("BOT_BALANCE_MIN", "2000"))
BOT_BALANCE_MAX = int(os.environ.get("BOT_BALANCE_MAX", "10000"))

USER_NAMES = os.environ.get("USER_NAMES", "").strip()
if USER_NAMES:
    USER_LIST = [n.strip() for n in USER_NAMES.split(",") if n.strip()]
else:
    USER_LIST = [f"trader-{i+1}" for i in range(NUM_USERS)]
while len(USER_LIST) < NUM_USERS:
    USER_LIST.append(f"trader-{len(USER_LIST)+1}")

SYMBOLS = ["BTC", "ETH"]
TIMEFRAMES = ["M5", "M15"]
FORECASTS = ["GREEN", "RED"]

REASONS = [
    "RSI oversold bounce expected",
    "Breaking above resistance",
    "Mean reversion play",
    "Volume spike detected",
    "EMA crossover signal",
    "Support level holding strong",
    "Bearish divergence on 15m",
    "News catalyst incoming",
    "MACD bullish crossover",
    "Bollinger squeeze breakout",
    "Fibonacci 61.8% retracement",
    "Double bottom pattern forming",
    "Head and shoulders breakdown",
    "Golden cross on 4h chart",
    "Whale accumulation detected",
]


# ---------------------------------------------------------------------------
# Bot profiles — mỗi bot có phong cách riêng
# ---------------------------------------------------------------------------

BOT_PROFILES = [
    {
        "suffix": "aggressive",
        "amount_range": (10, 80),
        "limit_pct": 0.15,
        "ttl_pct": 0.10,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "conservative",
        "amount_range": (5, 30),
        "limit_pct": 0.50,
        "ttl_pct": 0.40,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "scalper",
        "amount_range": (5, 25),
        "limit_pct": 0.30,
        "ttl_pct": 0.60,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "whale",
        "amount_range": (30, 100),
        "limit_pct": 0.40,
        "ttl_pct": 0.20,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "random-m5",
        "amount_range": (5, 50),
        "limit_pct": 0.30,
        "ttl_pct": 0.30,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
]

# A+1 profile
A1_PROFILE = {
    "suffix": "a1-sniper",
    "amount_range": (10, 50),
    "limit_pct": 1.0,
    "ttl_pct": 0,
    "preferred_symbols": ["BTC"],
    "preferred_tf": ["M5"],
}

A1_LIMIT_PRICES = [0.49, 0.50, 0.51]

# ---------------------------------------------------------------------------
# Random BTC-M5 trader profiles — simple random orders, no edge cases
# ---------------------------------------------------------------------------

RANDOM_TRADER_USER = os.environ.get("RANDOM_TRADER_USER", "random-trader")
RANDOM_TRADER_NUM_BOTS = int(os.environ.get("RANDOM_TRADER_BOTS", "4"))
RANDOM_TRADER_INTERVAL = int(os.environ.get("RANDOM_TRADER_INTERVAL", "15"))  # seconds between orders

RANDOM_BTC_PROFILES = [
    {
        "suffix": "btc-sniper",
        "amount_range": (5, 40),
        "limit_pct": 0.20,       # 20% limit, 80% market
        "ttl_range": (15, 120),
        "description": "Fast market orders, occasional limits",
    },
    {
        "suffix": "btc-limit-hunter",
        "amount_range": (8, 50),
        "limit_pct": 0.80,       # 80% limit
        "ttl_range": (30, 300),
        "description": "Mostly limit orders at various prices",
    },
    {
        "suffix": "btc-yolo",
        "amount_range": (15, 80),
        "limit_pct": 0.05,       # almost all market
        "ttl_range": (10, 60),
        "description": "Big market orders, go hard",
    },
    {
        "suffix": "btc-cautious",
        "amount_range": (3, 20),
        "limit_pct": 0.60,       # 60% limit
        "ttl_range": (60, 300),
        "description": "Small careful orders",
    },
]


# ---------------------------------------------------------------------------
# Trade builders — normal profile trades
# ---------------------------------------------------------------------------


def _base_payload(profile: dict) -> dict:
    lo, hi = profile["amount_range"]
    return {
        "symbol": random.choice(profile["preferred_symbols"]),
        "timeframe": random.choice(profile["preferred_tf"]),
        "forecast": random.choice(FORECASTS),
        "amount": round(random.uniform(lo, hi), 2),
    }


def build_trade(
    profile: dict, target_ts: int | None = None, best_ask: float | None = None
) -> dict:
    p = _base_payload(profile)

    is_limit = random.random() < profile["limit_pct"]
    has_ttl = random.random() < profile["ttl_pct"]

    if is_limit:
        p["limit_price"] = round(random.uniform(0.20, 0.80), 2)

    if has_ttl and is_limit:
        if profile["suffix"] == "scalper":
            p["ttl"] = random.choice([15, 30, 60])
        else:
            p["ttl"] = random.choice([30, 60, 120, 180, 300])

    # ~20% of MARKET orders get a ceiling_price + order_type (FAK/FOK)
    if not is_limit and random.random() < 0.20:
        ref = best_ask or 0.50
        # ceiling_price: sometimes below market, sometimes above
        p["ceiling_price"] = round(random.uniform(max(0.01, ref - 0.10), min(0.99, ref + 0.15)), 2)
        p["order_type"] = random.choice(["FAK", "FOK"])

    if target_ts is not None:
        p["timestamp"] = target_ts

    if random.random() > 0.5:
        p["reason"] = random.choice(REASONS)

    return p


def build_random_btc_trade(profile: dict) -> dict:
    """Build a single random BTC M5 trade from a random-trader profile."""
    lo, hi = profile["amount_range"]
    forecast = random.choice(FORECASTS)
    payload: dict = {
        "symbol": "BTC",
        "timeframe": "M5",
        "forecast": forecast,
        "amount": round(random.uniform(lo, hi), 2),
    }

    is_limit = random.random() < profile["limit_pct"]
    if is_limit:
        # Random limit price biased around 0.50 ± 0.25
        payload["limit_price"] = round(random.uniform(0.25, 0.75), 2)
        # Add TTL for ~70% of limit orders
        if random.random() < 0.70:
            ttl_lo, ttl_hi = profile["ttl_range"]
            payload["ttl"] = random.choice(range(ttl_lo, ttl_hi + 1, 5))

    # ~20% of MARKET orders get ceiling_price + order_type (FAK/FOK)
    if not is_limit and random.random() < 0.20:
        ref_price = 0.50  # fallback reference
        payload["ceiling_price"] = round(random.uniform(max(0.01, ref_price - 0.10), min(0.99, ref_price + 0.15)), 2)
        payload["order_type"] = random.choice(["FAK", "FOK"])

    if random.random() < 0.30:
        payload["reason"] = random.choice(REASONS)

    return payload


def random_trader_loop(bot_name: str, api_key: str, profile: dict) -> None:
    """Continuous random trading loop for BTC M5 — fires every N seconds."""
    interval = RANDOM_TRADER_INTERVAL
    log.info(
        "%s [random-trader] started: style=%s interval=%ds amount=%s",
        bot_name, profile["suffix"], interval, profile["amount_range"],
    )

    # Initial stagger
    jitter = random.uniform(0, 8)
    time.sleep(jitter)

    while True:
        try:
            payload = build_random_btc_trade(profile)
            otype = "LIMIT" if payload.get("limit_price") else "MKT"
            ttl_str = f" TTL={payload['ttl']}s" if payload.get("ttl") else ""
            ceil_str = f" ceil={payload['ceiling_price']}" if payload.get("ceiling_price") else ""
            ot_str = f" {payload['order_type']}" if payload.get("order_type") else ""
            log.info(
                "%s [random] %s %s $%.2f%s%s%s%s",
                bot_name, otype, payload["forecast"],
                payload["amount"],
                f" @{payload['limit_price']}" if payload.get("limit_price") else "",
                ttl_str, ot_str, ceil_str,
            )
            place_trade(api_key, payload, bot_name)
        except Exception as e:
            log.error("%s [random] Error: %s", bot_name, e)

        # Sleep with small random jitter
        time.sleep(interval + random.uniform(-3, 3))


def build_a1_batch(target_ts: int | None = None) -> list[dict]:
    trades: list[dict] = []
    for symbol in SYMBOLS:
        for limit_price in A1_LIMIT_PRICES:
            for forecast in FORECASTS:
                amount = round(random.uniform(*A1_PROFILE["amount_range"]), 2)
                trade: dict = {
                    "symbol": symbol,
                    "timeframe": "M5",
                    "forecast": forecast,
                    "amount": amount,
                    "limit_price": limit_price,
                    "session_offset": 1,
                    "reason": f"A+1: {symbol} LIMIT {limit_price} {forecast}",
                }
                if target_ts is not None:
                    trade["timestamp"] = target_ts
                trades.append(trade)
    random.shuffle(trades)
    return trades


# ---------------------------------------------------------------------------
# Test case pool — tất cả boundary & edge case
#
# Mỗi entry: (tag, builder_fn(ref, target_ts) -> dict, expect_fail)
#   - tag: label ghi log
#   - builder_fn: nhận best_ask reference + target_ts, trả về payload
#   - expect_fail: True nếu lệnh phải bị reject
# ---------------------------------------------------------------------------


def _make_case(tag: str, payload: dict, expect_fail: bool = False):
    """Helper to register a case entry."""
    return (tag, payload, expect_fail)


def build_case_pool(ref: float, target_ts: int | None) -> list[tuple[str, dict, bool]]:
    """Build the full pool of test cases, parameterized by current best_ask.

    Returns list of (tag, payload, expect_fail).
    """
    now_ts = int(time.time())
    period = 300  # M5
    current_open = now_ts - (now_ts % period)
    next_b = current_open + period

    pool: list[tuple[str, dict, bool]] = []

    # ── 1. MARKET basics: all symbols × timeframes × forecasts ───────────
    for sym in SYMBOLS:
        for tf in TIMEFRAMES:
            for fc in FORECASTS:
                pool.append(_make_case(
                    f"MKT-{sym}-{tf}-{fc}",
                    {"symbol": sym, "timeframe": tf, "forecast": fc,
                     "amount": round(random.uniform(5, 20), 2)},
                ))

    # ── 2. Boundary prices (LIMIT) ───────────────────────────────────────
    valid_prices = [0.01, 0.02, 0.10, 0.49, 0.50, 0.51, 0.90, 0.98, 0.99]
    for p in valid_prices:
        pool.append(_make_case(
            f"price={p}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": 5.0, "limit_price": p},
        ))
    # Invalid prices
    for p in [0.0, 1.0, -0.5, 1.5]:
        pool.append(_make_case(
            f"BAD-price={p}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": 5.0, "limit_price": p},
            expect_fail=True,
        ))

    # ── 3. Boundary amounts ──────────────────────────────────────────────
    for amt in [0.01, 0.10, 0.50, 1.0, 5.0, 50.0]:
        pool.append(_make_case(
            f"amt={amt}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": amt},
        ))
    # Invalid amounts
    for amt in [0, -1, -100]:
        pool.append(_make_case(
            f"BAD-amt={amt}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": amt},
            expect_fail=True,
        ))
    # Over-balance
    pool.append(_make_case(
        "amt=OVER",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 999_999.0},
        expect_fail=True,
    ))

    # ── 4. LIMIT immediate (at/above ask) ────────────────────────────────
    for delta in [0.00, 0.01, 0.05, 0.10]:
        p = round(min(0.99, ref + delta), 2)
        pool.append(_make_case(
            f"LMT-imm={p}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": round(random.uniform(5, 15), 2), "limit_price": p},
        ))

    # ── 5. LIMIT deferred (below ask) ────────────────────────────────────
    for delta in [0.02, 0.05, 0.10, 0.20]:
        p = round(max(0.02, ref - delta), 2)
        pool.append(_make_case(
            f"LMT-defer={p}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": round(random.uniform(8, 25), 2), "limit_price": p},
        ))

    # ── 6. LIMIT + TTL combos ────────────────────────────────────────────
    for ttl in [1, 5, 10, 30, 60, 120, 300]:
        lp = round(max(0.02, ref - 0.05), 2)
        pool.append(_make_case(
            f"LMT+TTL={ttl}s",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": round(random.uniform(5, 15), 2),
             "limit_price": lp, "ttl": ttl},
        ))
    # Invalid TTL
    for ttl in [0, -10]:
        pool.append(_make_case(
            f"BAD-TTL={ttl}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": 10.0, "limit_price": 0.40, "ttl": ttl},
            expect_fail=True,
        ))

    # ── 7. session_offset ────────────────────────────────────────────────
    # offset=0 (current)
    pool.append(_make_case(
        "offset=0-MKT",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "session_offset": 0},
    ))
    pool.append(_make_case(
        "offset=0-LMT",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "limit_price": round(max(0.02, ref - 0.05), 2),
         "session_offset": 0},
    ))
    # offset=1,2,3 (A+1, A+2, A+3) — all symbols × forecasts
    for offset in [1, 2, 3]:
        for sym in SYMBOLS:
            for fc in FORECASTS:
                pool.append(_make_case(
                    f"A+{offset}-{sym}-{fc}",
                    {"symbol": sym, "timeframe": "M5", "forecast": fc,
                     "amount": round(random.uniform(5, 15), 2),
                     "limit_price": round(random.uniform(0.40, 0.60), 2),
                     "session_offset": offset},
                ))
    # Invalid offset
    for off in [4, -1, 99]:
        pool.append(_make_case(
            f"BAD-offset={off}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": 10.0, "session_offset": off},
            expect_fail=True,
        ))

    # ── 8. Slippage tolerance ────────────────────────────────────────────
    for tol in [0.01, 0.05, 0.10, 0.20, 0.50, 1.0]:
        pool.append(_make_case(
            f"slip={tol}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": 10.0, "slippage_tolerance": tol},
        ))
    for tol in [0.0, -0.1, 1.5]:
        pool.append(_make_case(
            f"BAD-slip={tol}",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
             "amount": 10.0, "slippage_tolerance": tol},
            expect_fail=True,
        ))

    # ── 9. Timestamp edge cases ──────────────────────────────────────────
    pool.append(_make_case(
        "ts=current-open",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 5.0, "timestamp": current_open},
    ))
    pool.append(_make_case(
        "ts=next-boundary",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 5.0, "timestamp": next_b},
    ))
    pool.append(_make_case(
        "ts=far-past",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 5.0, "timestamp": current_open - period * 5},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "ts=far-future",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 5.0, "timestamp": next_b + period * 5},
        expect_fail=True,
    ))

    # ── 10. Invalid payloads ─────────────────────────────────────────────
    pool.append(_make_case(
        "BAD-symbol",
        {"symbol": "DOGE", "timeframe": "M5", "forecast": "GREEN", "amount": 10},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "BAD-tf",
        {"symbol": "BTC", "timeframe": "M1", "forecast": "GREEN", "amount": 10},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "BAD-forecast",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "BLUE", "amount": 10},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "BAD-empty",
        {},
        expect_fail=True,
    ))

    # ── 11. Large MARKET (slippage/depth test) ───────────────────────────
    pool.append(_make_case(
        "LARGE-mkt",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN", "amount": 100.0},
    ))

    # ── 12. LIMIT at exact best_ask (should fill as taker) ───────────────
    pool.append(_make_case(
        "LMT-at-ask",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "limit_price": round(ref, 2)},
    ))

    # ── 13. MARKET RED (opposite direction) ──────────────────────────────
    pool.append(_make_case(
        "MKT-RED",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "RED", "amount": 12.0},
    ))

    # ── 14. Multi-symbol LIMIT sweep ─────────────────────────────────────
    for sym in SYMBOLS:
        pool.append(_make_case(
            f"multi-{sym}",
            {"symbol": sym, "timeframe": random.choice(TIMEFRAMES),
             "forecast": random.choice(FORECASTS),
             "amount": round(random.uniform(5, 20), 2),
             "limit_price": round(random.uniform(0.30, 0.70), 2)},
        ))

    # ── 15. FAK/FOK MARKET orders with ceiling_price ──────────────────────
    # FAK + ceiling_price above ask → should fill at best prices up to ceiling
    pool.append(_make_case(
        "FAK-ceil-above",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 15.0, "order_type": "FAK",
         "ceiling_price": round(min(0.99, ref + 0.10), 2)},
    ))
    # FAK + ceiling_price at ask → should fill at ask
    pool.append(_make_case(
        "FAK-ceil-at-ask",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "order_type": "FAK",
         "ceiling_price": round(ref, 2)},
    ))
    # FAK + ceiling_price below ask → should be rejected (ceiling < best_ask)
    pool.append(_make_case(
        "FAK-ceil-below",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "order_type": "FAK",
         "ceiling_price": round(max(0.01, ref - 0.05), 2)},
        expect_fail=True,
    ))
    # FOK + ceiling_price above ask, small amount → should fill
    pool.append(_make_case(
        "FOK-ceil-above",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 5.0, "order_type": "FOK",
         "ceiling_price": round(min(0.99, ref + 0.10), 2)},
    ))
    # FOK + large amount (likely insufficient liquidity) → may reject
    pool.append(_make_case(
        "FOK-ceil-large",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 500.0, "order_type": "FOK",
         "ceiling_price": round(min(0.99, ref + 0.05), 2)},
    ))
    # FAK without ceiling_price → normal MARKET fill
    pool.append(_make_case(
        "FAK-no-ceil",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "order_type": "FAK"},
    ))
    # FOK without ceiling_price → normal MARKET fill
    pool.append(_make_case(
        "FOK-no-ceil",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "RED",
         "amount": 8.0, "order_type": "FOK"},
    ))

    # ── 16. Invalid ceiling_price / order_type ───────────────────────────
    pool.append(_make_case(
        "BAD-ceil=0",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "ceiling_price": 0.0, "order_type": "FAK"},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "BAD-ceil=1",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "ceiling_price": 1.0, "order_type": "FAK"},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "BAD-ceil-neg",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "ceiling_price": -0.5, "order_type": "FAK"},
        expect_fail=True,
    ))
    pool.append(_make_case(
        "BAD-order-type",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 10.0, "order_type": "GTC"},
        expect_fail=True,
    ))

    # ── 17. Deep LIMIT + very short TTL (should expire fast) ─────────────
    pool.append(_make_case(
        "deep-LMT+1s",
        {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN",
         "amount": 8.0, "limit_price": round(max(0.02, ref - 0.20), 2), "ttl": 1},
    ))

    # Inject target_ts into valid (non-failing) cases that don't have
    # their own timestamp/session_offset set
    if target_ts is not None:
        for i, (tag, payload, ef) in enumerate(pool):
            if not ef and "timestamp" not in payload and "session_offset" not in payload:
                pool[i] = (tag, {**payload, "timestamp": target_ts}, ef)

    return pool


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def wait_for_api() -> bool:
    for attempt in range(30):
        try:
            r = requests.get(f"{BASE.rsplit('/poly-arena', 1)[0]}/health", timeout=5)
            if r.ok:
                log.info("API is ready")
                return True
        except requests.ConnectionError:
            pass
        log.info("Waiting for API... (attempt %d)", attempt + 1)
        time.sleep(2)
    log.error("API not reachable after 60s")
    return False


def register_or_login_user(username: str, password: str) -> str:
    r = requests.post(
        f"{BASE}/auth/register",
        json={"username": username, "password": password},
        timeout=10,
    )
    if r.status_code == 201:
        log.info("User registered: %s", username)
        return r.json()["access_token"]

    if r.status_code == 409:
        r = requests.post(
            f"{BASE}/auth/login",
            json={"username": username, "password": password},
            timeout=10,
        )
        r.raise_for_status()
        log.info("User logged in: %s", username)
        return r.json()["access_token"]

    r.raise_for_status()
    return ""


def fetch_my_bots(jwt_token: str) -> list[dict]:
    r = requests.get(
        f"{BASE}/bots/my",
        headers={"Authorization": f"Bearer {jwt_token}"},
        timeout=10,
    )
    r.raise_for_status()
    bots = r.json()
    log.info("Fetched %d existing bot(s) for user", len(bots))
    return bots


def create_bot(bot_name: str, jwt_token: str, initial_balance: float = 10000.0) -> str:
    log.info("Creating bot '%s' (balance=$%.0f) ...", bot_name, initial_balance)
    r = requests.post(
        f"{BASE}/bots/",
        json={"bot_name": bot_name, "initial_balance": initial_balance},
        headers={"Authorization": f"Bearer {jwt_token}"},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    log.info("Bot created: name=%s balance=$%.0f", data["bot_name"], data["balance"])
    return data["api_key"]


def place_trade(api_key: str, payload: dict, bot_name: str) -> None:
    r = requests.post(
        f"{BASE}/binary-options/",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )

    if r.ok:
        data = r.json()
        otype = "LIMIT" if payload.get("limit_price") else "MKT"
        ttl_str = f" TTL={payload['ttl']}s" if payload.get("ttl") else ""
        ts_str = f" [TS={payload['timestamp']}]" if payload.get("timestamp") else ""
        log.info(
            "%s Trade #%d: %s %s %s %s $%.2f → avg=%.4f shares=%.2f%s%s",
            bot_name,
            data["id"],
            otype,
            payload.get("symbol", "?"),
            payload.get("timeframe", "?"),
            payload.get("forecast", "?"),
            payload.get("amount", 0),
            data.get("avg_price") or 0,
            data.get("num_shares") or 0,
            ttl_str,
            ts_str,
        )
    else:
        log.warning(
            "%s Trade failed (%d): %s", bot_name, r.status_code, r.text[:200],
        )


def place_case(api_key: str, tag: str, payload: dict, bot_name: str,
               expect_fail: bool = False) -> None:
    """Place a test case order with tag logging."""
    r = requests.post(
        f"{BASE}/binary-options/",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )
    otype = "LIMIT" if payload.get("limit_price") else "MKT"
    price_str = f"@{payload['limit_price']}" if payload.get("limit_price") else ""
    offset_str = f" A+{payload['session_offset']}" if payload.get("session_offset") else ""
    ttl_str = f" TTL={payload['ttl']}s" if payload.get("ttl") else ""
    slip_str = f" slip={payload['slippage_tolerance']}" if payload.get("slippage_tolerance") else ""
    ceil_str = f" ceil={payload['ceiling_price']}" if payload.get("ceiling_price") else ""
    ot_str = f" {payload['order_type']}" if payload.get("order_type") else ""

    if r.ok:
        d = r.json()
        lvl = log.warning if expect_fail else log.info
        prefix = "UNEXPECTED OK" if expect_fail else "CASE OK"
        lvl(
            "%s [%s] %s %s %s %s $%.2f%s%s%s%s%s%s → #%d avg=%.4f",
            bot_name, prefix, tag, otype,
            payload.get("symbol", "?"), payload.get("forecast", "?"),
            payload.get("amount", 0), price_str, offset_str, ttl_str, slip_str,
            ot_str, ceil_str,
            d["id"], d.get("avg_price") or 0,
        )
    else:
        detail = ""
        try:
            detail = r.json().get("detail", "")[:100]
        except Exception:
            detail = r.text[:100]
        lvl = log.info if expect_fail else log.warning
        prefix = "CASE EXPECTED FAIL" if expect_fail else "CASE FAIL"
        lvl(
            "%s [%s] %s %s %s %s $%.2f%s%s%s%s → %d: %s",
            bot_name, prefix, tag, otype,
            payload.get("symbol", "?"), payload.get("forecast", "?"),
            payload.get("amount", 0), price_str, offset_str, ttl_str, slip_str,
            r.status_code, detail,
        )


def fetch_best_ask(symbol: str = "BTC", timeframe: str = "M5") -> float | None:
    try:
        r = requests.get(f"{BASE}/binary-options/engine/prices", timeout=10)
        if r.ok:
            for p in r.json().get("prices", []):
                if (
                    p["symbol"] == symbol
                    and p["timeframe"] == timeframe
                    and p["direction"] == "UP"
                ):
                    return p.get("best_ask")
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Batch runner — normal trades + random test cases
# ---------------------------------------------------------------------------


def run_batch(
    api_key: str, bot_name: str, profile: dict, count: int,
    target_ts: int | None = None,
) -> None:
    """Run one tick: normal trades + random test cases."""

    best_ask = fetch_best_ask()
    ref = best_ask or 0.50
    if best_ask:
        log.info("%s Best ask reference: %.4f", bot_name, best_ask)

    suffix = profile["suffix"]

    # Build normal trades
    if suffix == "a1-sniper":
        trades = build_a1_batch(target_ts=target_ts)
    else:
        trades = [
            build_trade(profile, target_ts=target_ts, best_ask=best_ask)
            for _ in range(count)
        ]

    # Send normal trades
    for idx, payload in enumerate(trades, 1):
        otype = "LIMIT" if payload.get("limit_price") else "MKT"
        has_ttl = f"TTL={payload['ttl']}s" if payload.get("ttl") else ""
        has_ts = f"TS={payload['timestamp']}" if payload.get("timestamp") else ""
        extras = "+".join(filter(None, [has_ttl, has_ts]))
        if extras:
            extras = f" +{extras}"
        log.info(
            "%s [%d/%d] %s %s %s %s $%.2f%s",
            bot_name, idx, len(trades), otype,
            payload["symbol"], payload["timeframe"], payload["forecast"],
            payload["amount"], extras,
        )
        try:
            place_trade(api_key, payload, bot_name)
        except Exception as e:
            log.error("%s Error placing trade: %s", bot_name, e)
        time.sleep(random.uniform(0.3, 1.0))

    # Pick random test cases from the pool
    case_pool = build_case_pool(ref, target_ts)
    num_cases = min(CASES_PER_TICK, len(case_pool))
    picked = random.sample(case_pool, num_cases)

    if picked:
        log.info(
            "%s ── Running %d random test case(s) ──", bot_name, len(picked),
        )
        for tag, payload, expect_fail in picked:
            try:
                place_case(api_key, tag, payload, bot_name, expect_fail)
            except Exception as e:
                log.error("%s Case '%s' error: %s", bot_name, tag, e)
            time.sleep(random.uniform(0.2, 0.8))

    # Auth edge case: bad API key (10% chance per tick)
    if random.random() < 0.10:
        log.info("%s ── Auth edge: bad API key ──", bot_name)
        place_case(
            "totally-invalid-key-xyz",
            "BAD-apikey",
            {"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN", "amount": 5.0},
            bot_name,
            expect_fail=True,
        )


# ---------------------------------------------------------------------------
# Bot thread loop
# ---------------------------------------------------------------------------


def bot_loop(bot_name: str, api_key: str, profile: dict) -> None:
    tf = profile["preferred_tf"][0]
    period = {"M5": 300, "M15": 900}.get(tf, 300)

    log.info(
        "%s started: style=%s trades=%d cases=%d period=%ds snipe=%ds",
        bot_name, profile["suffix"],
        TRADES_PER_TICK, CASES_PER_TICK, period, SNIPE_OFFSET_S,
    )

    jitter = random.uniform(0, 5)
    log.info("%s sleeping %.1fs before first cycle (stagger)...", bot_name, jitter)
    time.sleep(jitter)

    while True:
        now_ts = int(time.time())
        current_open = now_ts - (now_ts % period)
        next_boundary = current_open + period

        fire_at = next_boundary - SNIPE_OFFSET_S
        wait_s = fire_at - int(time.time())

        if wait_s > 0:
            log.info(
                "%s Next boundary=%d, fire at T-%ds, sleeping %ds...",
                bot_name, next_boundary, SNIPE_OFFSET_S, wait_s,
            )
            time.sleep(wait_s)

        log.info("=" * 60)
        log.info(
            "%s SNIPE: %d trades + %d cases at T-%ds, timestamp=%d",
            bot_name, TRADES_PER_TICK, CASES_PER_TICK,
            SNIPE_OFFSET_S, next_boundary,
        )
        run_batch(
            api_key, bot_name, profile, TRADES_PER_TICK,
            target_ts=next_boundary,
        )


# ---------------------------------------------------------------------------
# User setup
# ---------------------------------------------------------------------------


def _match_profile(bot_name: str) -> dict:
    if A1_PROFILE["suffix"] in bot_name:
        return A1_PROFILE
    for profile in BOT_PROFILES:
        if profile["suffix"] in bot_name:
            return profile
    return random.choice(BOT_PROFILES)


def setup_user(username: str, num_bots: int) -> list[tuple[str, str, dict]]:
    """Setup 1 user: register/login → reuse or create bots.

    Args:
        num_bots: number of bots for this user (3-5).
    """
    try:
        jwt_token = register_or_login_user(username, PASSWORD)
    except Exception as e:
        log.error("Failed to register/login user '%s': %s", username, e)
        return []

    existing_bots: list[dict] = []
    try:
        existing_bots = fetch_my_bots(jwt_token)
    except Exception as e:
        log.warning("[%s] Failed to fetch bots: %s", username, e)

    bot_slots: list[tuple[str, str, dict]] = []

    # Reuse active bots
    for bot_data in existing_bots:
        if len(bot_slots) >= num_bots:
            break
        if not bot_data.get("is_active", True):
            continue
        if bot_data.get("status") == "DELETED":
            continue
        name = bot_data["bot_name"]
        profile = _match_profile(name)
        bot_slots.append((name, bot_data["api_key"], profile))
        log.info(
            "[%s] Reusing bot: %s (style=%s, balance=$%.0f)",
            username, name, profile["suffix"], bot_data.get("balance", 0),
        )

    # Create new bots
    all_profiles = BOT_PROFILES + [A1_PROFILE]
    for i in range(len(bot_slots), num_bots):
        profile = all_profiles[i % len(all_profiles)]
        bot_name = f"{username}-{profile['suffix']}"
        balance = random.randint(BOT_BALANCE_MIN, BOT_BALANCE_MAX)
        balance = round(balance / 50) * 50
        balance = max(BOT_BALANCE_MIN, balance)
        try:
            api_key = create_bot(bot_name, jwt_token, initial_balance=balance)
            bot_slots.append((bot_name, api_key, profile))
        except Exception as e:
            log.error("[%s] Failed to create bot '%s': %s", username, bot_name, e)

    log.info("[%s] Setup complete: %d bot(s)", username, len(bot_slots))
    return bot_slots


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not wait_for_api():
        return

    all_bot_slots: list[tuple[str, str, dict]] = []

    for i in range(NUM_USERS):
        username = USER_LIST[i]
        num_bots = random.randint(3, min(BOTS_PER_USER, 5))
        log.info("=" * 60)
        log.info("Setting up user %d/%d: %s (%d bots)", i + 1, NUM_USERS, username, num_bots)
        slots = setup_user(username, num_bots)
        all_bot_slots.extend(slots)

    if not all_bot_slots:
        log.error("No bots available, exiting")
        return

    # ── Setup random BTC-M5 trader ────────────────────────────────────────
    random_bot_slots: list[tuple[str, str, dict]] = []
    log.info("=" * 60)
    log.info(
        "Setting up random-trader: %s (%d bots)",
        RANDOM_TRADER_USER, RANDOM_TRADER_NUM_BOTS,
    )
    try:
        rt_jwt = register_or_login_user(RANDOM_TRADER_USER, PASSWORD)
        existing_rt = []
        try:
            existing_rt = fetch_my_bots(rt_jwt)
        except Exception:
            pass

        # Reuse existing bots
        for bot_data in existing_rt:
            if len(random_bot_slots) >= RANDOM_TRADER_NUM_BOTS:
                break
            if not bot_data.get("is_active", True):
                continue
            if bot_data.get("status") == "DELETED":
                continue
            name = bot_data["bot_name"]
            # Match to a random profile
            matched = None
            for rp in RANDOM_BTC_PROFILES:
                if rp["suffix"] in name:
                    matched = rp
                    break
            if matched is None:
                matched = RANDOM_BTC_PROFILES[len(random_bot_slots) % len(RANDOM_BTC_PROFILES)]
            random_bot_slots.append((name, bot_data["api_key"], matched))
            log.info(
                "[%s] Reusing bot: %s (style=%s, balance=$%.0f)",
                RANDOM_TRADER_USER, name, matched["suffix"],
                bot_data.get("balance", 0),
            )

        # Create new bots if needed
        for i in range(len(random_bot_slots), RANDOM_TRADER_NUM_BOTS):
            profile = RANDOM_BTC_PROFILES[i % len(RANDOM_BTC_PROFILES)]
            bot_name = f"{RANDOM_TRADER_USER}-{profile['suffix']}"
            balance = random.randint(BOT_BALANCE_MIN, BOT_BALANCE_MAX)
            balance = round(balance / 50) * 50
            balance = max(BOT_BALANCE_MIN, balance)
            try:
                api_key = create_bot(bot_name, rt_jwt, initial_balance=balance)
                random_bot_slots.append((bot_name, api_key, profile))
            except Exception as e:
                log.error(
                    "[%s] Failed to create bot '%s': %s",
                    RANDOM_TRADER_USER, bot_name, e,
                )
    except Exception as e:
        log.error("Failed to setup random-trader '%s': %s", RANDOM_TRADER_USER, e)

    log.info(
        "[%s] Setup complete: %d random BTC-M5 bot(s)",
        RANDOM_TRADER_USER, len(random_bot_slots),
    )

    # Summary
    log.info("=" * 60)
    log.info(
        "Launching %d bots across %d users | trades/tick=%d cases/tick=%d",
        len(all_bot_slots), NUM_USERS, TRADES_PER_TICK, CASES_PER_TICK,
    )
    log.info(
        "  + %d random BTC-M5 bots (user=%s, interval=%ds)",
        len(random_bot_slots), RANDOM_TRADER_USER, RANDOM_TRADER_INTERVAL,
    )
    log.info("=" * 60)

    threads = []

    # Snipe-based bot threads (existing)
    for bot_name, api_key, profile in all_bot_slots:
        t = threading.Thread(
            target=bot_loop,
            args=(bot_name, api_key, profile),
            name=bot_name,
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info("Thread started for %s (%s)", bot_name, profile["suffix"])

    # Random trader threads (continuous, every N seconds)
    for bot_name, api_key, profile in random_bot_slots:
        t = threading.Thread(
            target=random_trader_loop,
            args=(bot_name, api_key, profile),
            name=bot_name,
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info(
            "Thread started for %s (%s) [random-trader, %ds interval]",
            bot_name, profile["suffix"], RANDOM_TRADER_INTERVAL,
        )

    log.info(
        "All %d bots running. Press Ctrl+C to stop.", len(threads),
    )

    try:
        while True:
            time.sleep(60)
            alive = sum(1 for t in threads if t.is_alive())
            log.info("Health check: %d/%d bot threads alive", alive, len(threads))
    except KeyboardInterrupt:
        log.info("Shutting down...")


if __name__ == "__main__":
    main()
