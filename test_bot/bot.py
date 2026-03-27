"""
Test bot — creates bots directly (no user auth), each bot runs random trades + test cases.

Mỗi tick (snipe trước candle boundary):
  1. Bot gửi batch lệnh bình thường theo profile (aggressive, conservative, ...)
  2. Xen kẽ random 2-5 test case từ CASE_POOL (boundary, invalid, edge, A+1, ...)

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
NUM_USERS = int(os.environ.get("NUM_USERS", "1"))
BOTS_PER_USER = int(os.environ.get("BOTS_PER_USER", "5"))
TRADES_PER_TICK = int(os.environ.get("TRADES_PER_TICK", "15"))
CASES_PER_TICK = int(os.environ.get("CASES_PER_TICK", "4"))
SNIPE_OFFSET_S = int(os.environ.get("SNIPE_OFFSET_S", "2"))

BOT_BALANCE_MIN = int(os.environ.get("BOT_BALANCE_MIN", "10000"))
BOT_BALANCE_MAX = int(os.environ.get("BOT_BALANCE_MAX", "10000"))

BOT_PREFIXES_STR = os.environ.get("BOT_PREFIXES", os.environ.get("USER_NAMES", "")).strip()
if BOT_PREFIXES_STR:
    BOT_PREFIX_LIST = [n.strip() for n in BOT_PREFIXES_STR.split(",") if n.strip()]
else:
    BOT_PREFIX_LIST = [f"trader-{i+1}" for i in range(NUM_USERS)]
while len(BOT_PREFIX_LIST) < NUM_USERS:
    BOT_PREFIX_LIST.append(f"trader-{len(BOT_PREFIX_LIST)+1}")

SYMBOLS = ["BTC"]
TIMEFRAMES = ["M5", "M15"]
FORECASTS = ["GREEN", "RED"]

# ── Futures config ─────────────────────────────────────────────────────────
FUTURES_ENABLED = os.environ.get("FUTURES_ENABLED", "0") == "1"
FUTURES_SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
FUTURES_SIDES = ["LONG", "SHORT"]
FUTURES_NUM_BOTS = int(os.environ.get("FUTURES_NUM_BOTS", "4"))
FUTURES_INTERVAL = int(os.environ.get("FUTURES_INTERVAL", "20"))  # seconds between cycles
FUTURES_BOT_PREFIX = os.environ.get("FUTURES_BOT_PREFIX", "futures-trader")

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
# Futures bot profiles
# ---------------------------------------------------------------------------

FUTURES_PROFILES = [
    {
        "suffix": "futures-scalper",
        "leverage_range": (10, 25),
        "margin_range": (20, 100),
        "limit_pct": 0.20,
        "close_pct": 0.40,
        "tp_sl_pct": 0.80,
        "preferred_symbols": ["BTC", "ETH"],
    },
    {
        "suffix": "futures-whale",
        "leverage_range": (3, 10),
        "margin_range": (100, 500),
        "limit_pct": 0.40,
        "close_pct": 0.20,
        "tp_sl_pct": 0.60,
        "preferred_symbols": ["BTC", "ETH", "SOL"],
    },
    {
        "suffix": "futures-degen",
        "leverage_range": (20, 50),
        "margin_range": (10, 80),
        "limit_pct": 0.10,
        "close_pct": 0.50,
        "tp_sl_pct": 0.50,
        "preferred_symbols": FUTURES_SYMBOLS,
    },
    {
        "suffix": "futures-cautious",
        "leverage_range": (2, 5),
        "margin_range": (50, 200),
        "limit_pct": 0.60,
        "close_pct": 0.15,
        "tp_sl_pct": 0.90,
        "preferred_symbols": ["BTC"],
    },
]


# ---------------------------------------------------------------------------
# Futures API helpers
# ---------------------------------------------------------------------------


def fetch_futures_prices() -> dict[str, float]:
    """Fetch current mark prices from the futures API."""
    try:
        r = requests.get(f"{BASE}/futures/prices", timeout=10)
        if r.ok:
            data = r.json().get("prices", {})
            return {sym: float(info["price"]) for sym, info in data.items()}
    except Exception as exc:
        log.debug("Futures price fetch failed: %s", exc)
    return {}


def fetch_futures_positions(api_key: str) -> list[dict]:
    """Fetch open futures positions."""
    try:
        r = requests.get(
            f"{BASE}/futures/positions?status=OPEN",
            headers={"x-api-key": api_key},
            timeout=10,
        )
        if r.ok:
            return r.json()
    except Exception:
        pass
    return []


def place_futures_order(api_key: str, payload: dict, bot_name: str) -> None:
    """Place a futures order and log the result."""
    r = requests.post(
        f"{BASE}/futures/orders",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )
    if r.ok:
        data = r.json()
        status = data.get("status", "?")
        if status == "filled":
            log.info(
                "%s [futures] FILLED %s %s %dx $%.2f → pos#%d entry=$%.2f liq=$%.2f fee=$%.4f",
                bot_name, payload["side"], payload["symbol"], payload["leverage"],
                payload["amount"], data.get("position_id", 0),
                data.get("entry_price", 0), data.get("liquidation_price", 0),
                data.get("entry_fee", 0),
            )
        else:
            log.info(
                "%s [futures] PENDING %s %s %dx $%.2f limit=$%.2f → order#%d",
                bot_name, payload["side"], payload["symbol"], payload["leverage"],
                payload["amount"], payload.get("limit_price", 0),
                data.get("order_id", 0),
            )
    else:
        log.warning(
            "%s [futures] Order failed (%d): %s",
            bot_name, r.status_code, r.text[:200],
        )


def close_futures_position(api_key: str, position_id: int, bot_name: str) -> None:
    """Close a futures position at market."""
    r = requests.post(
        f"{BASE}/futures/positions/{position_id}/close",
        headers={"x-api-key": api_key},
        timeout=15,
    )
    if r.ok:
        data = r.json()
        log.info(
            "%s [futures] CLOSED pos#%d pnl=$%.2f exit=$%.2f fee=$%.4f",
            bot_name, position_id, data.get("realized_pnl", 0),
            data.get("exit_price", 0), data.get("exit_fee", 0),
        )
    else:
        log.warning(
            "%s [futures] Close failed pos#%d (%d): %s",
            bot_name, position_id, r.status_code, r.text[:200],
        )


def update_futures_tp_sl(api_key: str, position_id: int, tp: float | None, sl: float | None, bot_name: str) -> None:
    """Update TP/SL on an open position."""
    body: dict = {}
    if tp is not None:
        body["tp_price"] = tp
    if sl is not None:
        body["sl_price"] = sl
    if not body:
        return
    try:
        r = requests.patch(
            f"{BASE}/futures/positions/{position_id}",
            json=body,
            headers={"Content-Type": "application/json", "x-api-key": api_key},
            timeout=10,
        )
        if r.ok:
            log.info("%s [futures] Updated pos#%d TP=%s SL=%s", bot_name, position_id, tp, sl)
        else:
            log.warning("%s [futures] TP/SL update failed pos#%d: %s", bot_name, position_id, r.text[:200])
    except Exception as exc:
        log.error("%s [futures] TP/SL update error: %s", bot_name, exc)


# ---------------------------------------------------------------------------
# Futures trade builders
# ---------------------------------------------------------------------------


def build_futures_trade(profile: dict, prices: dict[str, float]) -> dict | None:
    """Build a single futures order payload based on profile and current prices."""
    symbols = [s for s in profile["preferred_symbols"] if s in prices]
    if not symbols:
        return None

    sym = random.choice(symbols)
    mark = prices[sym]
    side = random.choice(FUTURES_SIDES)
    leverage = random.randint(*profile["leverage_range"])
    lo, hi = profile["margin_range"]
    margin = round(random.uniform(lo, hi), 2)

    is_limit = random.random() < profile["limit_pct"]
    order_type = "LIMIT" if is_limit else "MARKET"

    payload: dict = {
        "symbol": sym,
        "side": side,
        "amount": margin,
        "leverage": leverage,
        "order_type": order_type,
    }

    if is_limit:
        if side == "LONG":
            payload["limit_price"] = round(mark * random.uniform(0.993, 0.999), 2)
        else:
            payload["limit_price"] = round(mark * random.uniform(1.001, 1.007), 2)
        payload["ttl"] = random.choice([30, 60, 120, 300])

    # Add TP/SL
    if random.random() < profile["tp_sl_pct"]:
        ref = payload.get("limit_price", mark)
        tp_pct = random.uniform(0.005, 0.03)
        sl_pct = random.uniform(0.003, 0.02)
        if side == "LONG":
            payload["tp_price"] = round(ref * (1 + tp_pct), 2)
            payload["sl_price"] = round(ref * (1 - sl_pct), 2)
        else:
            payload["tp_price"] = round(ref * (1 - tp_pct), 2)
            payload["sl_price"] = round(ref * (1 + sl_pct), 2)

    return payload


# ---------------------------------------------------------------------------
# Futures trading loop
# ---------------------------------------------------------------------------


def futures_trader_loop(bot_name: str, api_key: str, profile: dict) -> None:
    """Continuous futures trading loop — open/close positions every N seconds."""
    interval = FUTURES_INTERVAL
    log.info(
        "%s [futures] started: style=%s interval=%ds leverage=%s margin=%s symbols=%s",
        bot_name, profile["suffix"], interval,
        profile["leverage_range"], profile["margin_range"],
        profile["preferred_symbols"],
    )

    # Initial stagger
    time.sleep(random.uniform(0, 10))

    while True:
        try:
            prices = fetch_futures_prices()
            if not prices:
                log.warning("%s [futures] No prices available, retrying...", bot_name)
                time.sleep(interval)
                continue

            # Maybe close some existing positions
            positions = fetch_futures_positions(api_key)
            for pos in positions:
                if random.random() < profile["close_pct"]:
                    close_futures_position(api_key, pos["id"], bot_name)
                    time.sleep(random.uniform(0.3, 1.0))
                elif random.random() < 0.15:
                    # Randomly adjust TP/SL on some positions
                    mark = prices.get(pos["symbol"])
                    if mark:
                        tp_pct = random.uniform(0.005, 0.04)
                        sl_pct = random.uniform(0.003, 0.025)
                        if pos["side"] == "LONG":
                            new_tp = round(mark * (1 + tp_pct), 2)
                            new_sl = round(mark * (1 - sl_pct), 2)
                        else:
                            new_tp = round(mark * (1 - tp_pct), 2)
                            new_sl = round(mark * (1 + sl_pct), 2)
                        update_futures_tp_sl(api_key, pos["id"], new_tp, new_sl, bot_name)

            # Open 1-3 new positions
            num_trades = random.randint(1, 3)
            for _ in range(num_trades):
                payload = build_futures_trade(profile, prices)
                if payload:
                    place_futures_order(api_key, payload, bot_name)
                    time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            log.error("%s [futures] Error: %s", bot_name, e)

        time.sleep(interval + random.uniform(-5, 5))


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
        "preferred_tf": ["M5", "M15"],
    },
    {
        "suffix": "conservative",
        "amount_range": (5, 30),
        "limit_pct": 0.50,
        "ttl_pct": 0.40,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5", "M15"],
    },
    {
        "suffix": "scalper",
        "amount_range": (5, 25),
        "limit_pct": 0.30,
        "ttl_pct": 0.60,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5", "M15"],
    },
    {
        "suffix": "whale",
        "amount_range": (30, 100),
        "limit_pct": 0.40,
        "ttl_pct": 0.20,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5", "M15"],
    },
    {
        "suffix": "random-m5",
        "amount_range": (5, 50),
        "limit_pct": 0.30,
        "ttl_pct": 0.30,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5", "M15"],
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
RANDOM_TRADER_NUM_BOTS = int(os.environ.get("RANDOM_TRADER_BOTS", "0"))
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


def get_or_create_bot(bot_name: str, initial_balance: float = 10000.0) -> str:
    """Create a bot or return existing one's api_key (no user auth needed)."""
    log.info("Getting/creating bot '%s' (balance=$%.0f) ...", bot_name, initial_balance)
    r = requests.post(
        f"{BASE}/bots",
        json={"bot_name": bot_name, "initial_balance": initial_balance, "get_or_create": True},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    log.info("Bot ready: name=%s balance=$%.0f", data["bot_name"], data["balance"])
    return data["api_key"]


def place_trade(api_key: str, payload: dict, bot_name: str) -> None:
    r = requests.post(
        f"{BASE}/binary-options",
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
        f"{BASE}/binary-options",
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


def setup_bots(prefix: str, num_bots: int) -> list[tuple[str, str, dict]]:
    """Setup bots directly (no user auth needed).

    Args:
        prefix: bot name prefix (e.g. "trader-1").
        num_bots: number of bots to create (3-5).
    """
    bot_slots: list[tuple[str, str, dict]] = []

    all_profiles = BOT_PROFILES + [A1_PROFILE]
    for i in range(num_bots):
        profile = all_profiles[i % len(all_profiles)]
        bot_name = f"{prefix}-{profile['suffix']}"
        balance = random.randint(BOT_BALANCE_MIN, BOT_BALANCE_MAX)
        balance = round(balance / 50) * 50
        balance = max(BOT_BALANCE_MIN, balance)
        try:
            api_key = get_or_create_bot(bot_name, initial_balance=balance)
            bot_slots.append((bot_name, api_key, profile))
        except Exception as e:
            log.error("[%s] Failed to create bot '%s': %s", prefix, bot_name, e)

    log.info("[%s] Setup complete: %d bot(s)", prefix, len(bot_slots))
    return bot_slots


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not wait_for_api():
        return

    all_bot_slots: list[tuple[str, str, dict]] = []

    for i in range(NUM_USERS):
        prefix = BOT_PREFIX_LIST[i]
        num_bots = random.randint(3, min(BOTS_PER_USER, 5))
        log.info("=" * 60)
        log.info("Setting up group %d/%d: %s (%d bots)", i + 1, NUM_USERS, prefix, num_bots)
        slots = setup_bots(prefix, num_bots)
        all_bot_slots.extend(slots)

    if not all_bot_slots:
        log.error("No bots available, exiting")
        return

    # ── Setup random BTC-M5 trader bots ───────────────────────────────────
    random_bot_slots: list[tuple[str, str, dict]] = []
    log.info("=" * 60)
    log.info(
        "Setting up random-trader bots: prefix=%s (%d bots)",
        RANDOM_TRADER_USER, RANDOM_TRADER_NUM_BOTS,
    )
    for i in range(RANDOM_TRADER_NUM_BOTS):
        profile = RANDOM_BTC_PROFILES[i % len(RANDOM_BTC_PROFILES)]
        bot_name = f"{RANDOM_TRADER_USER}-{profile['suffix']}"
        balance = random.randint(BOT_BALANCE_MIN, BOT_BALANCE_MAX)
        balance = round(balance / 50) * 50
        balance = max(BOT_BALANCE_MIN, balance)
        try:
            api_key = get_or_create_bot(bot_name, initial_balance=balance)
            random_bot_slots.append((bot_name, api_key, profile))
        except Exception as e:
            log.error(
                "[%s] Failed to create bot '%s': %s",
                RANDOM_TRADER_USER, bot_name, e,
            )

    log.info(
        "[%s] Setup complete: %d random BTC-M5 bot(s)",
        RANDOM_TRADER_USER, len(random_bot_slots),
    )

    # ── Setup futures trader bots ──────────────────────────────────────────
    futures_bot_slots: list[tuple[str, str, dict]] = []
    if FUTURES_ENABLED:
        log.info("=" * 60)
        log.info(
            "Setting up futures trader bots: prefix=%s (%d bots)",
            FUTURES_BOT_PREFIX, FUTURES_NUM_BOTS,
        )
        for i in range(FUTURES_NUM_BOTS):
            profile = FUTURES_PROFILES[i % len(FUTURES_PROFILES)]
            bot_name = f"{FUTURES_BOT_PREFIX}-{profile['suffix']}"
            balance = random.randint(BOT_BALANCE_MIN, BOT_BALANCE_MAX)
            balance = round(balance / 50) * 50
            balance = max(BOT_BALANCE_MIN, balance)
            try:
                api_key = get_or_create_bot(bot_name, initial_balance=balance)
                futures_bot_slots.append((bot_name, api_key, profile))
            except Exception as e:
                log.error(
                    "[%s] Failed to create futures bot '%s': %s",
                    FUTURES_BOT_PREFIX, bot_name, e,
                )
        log.info(
            "[%s] Setup complete: %d futures bot(s)",
            FUTURES_BOT_PREFIX, len(futures_bot_slots),
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
    if FUTURES_ENABLED:
        log.info(
            "  + %d futures bots (prefix=%s, interval=%ds)",
            len(futures_bot_slots), FUTURES_BOT_PREFIX, FUTURES_INTERVAL,
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

    # Futures trader threads (continuous, every N seconds)
    for bot_name, api_key, profile in futures_bot_slots:
        t = threading.Thread(
            target=futures_trader_loop,
            args=(bot_name, api_key, profile),
            name=bot_name,
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info(
            "Thread started for %s (%s) [futures, %ds interval]",
            bot_name, profile["suffix"], FUTURES_INTERVAL,
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
