"""
Test bot — chạy đồng thời nhiều bot, mỗi bot tạo random trades mỗi 5 phút.
Khi khởi động sẽ tạo N bot (mặc định 4), mỗi bot chạy trong 1 thread riêng.
Mỗi bot có phong cách giao dịch riêng (aggressive, conservative, scalper, mixed).
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
NUM_BOTS = int(os.environ.get("NUM_BOTS", "5"))
INTERVAL = int(os.environ.get("INTERVAL_SEC", "300"))  # 5 phút
TRADES_PER_TICK = int(os.environ.get("TRADES_PER_TICK", "15"))
TEST_USER = os.environ.get("TEST_USER", "test-trader")
TEST_PASSWORD = os.environ.get("TEST_PASSWORD", "testpass123")

SYMBOLS = ["BTC"]
TIMEFRAMES = ["M5"]
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
        "amount_range": (50, 500),
        "limit_pct": 0.15,       # ít dùng limit
        "bracket_pct": 0.70,     # hay dùng bracket
        "ttl_pct": 0.10,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "conservative",
        "amount_range": (5, 100),
        "limit_pct": 0.50,       # hay dùng limit
        "bracket_pct": 0.80,     # luôn có bracket
        "ttl_pct": 0.40,         # hay set TTL
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "scalper",
        "amount_range": (10, 150),
        "limit_pct": 0.30,
        "bracket_pct": 0.50,
        "ttl_pct": 0.60,         # scalper hay dùng TTL ngắn
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "whale",
        "amount_range": (200, 800),
        "limit_pct": 0.40,
        "bracket_pct": 0.60,
        "ttl_pct": 0.20,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "random-m5",
        "amount_range": (10, 300),
        "limit_pct": 0.30,
        "bracket_pct": 0.40,
        "ttl_pct": 0.30,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
]


# ---------------------------------------------------------------------------
# Trade builders — tạo lệnh theo profile
# ---------------------------------------------------------------------------


def _base_payload(profile: dict) -> dict:
    """Random symbol/timeframe/forecast/amount theo profile."""
    lo, hi = profile["amount_range"]
    return {
        "symbol": random.choice(profile["preferred_symbols"]),
        "timeframe": random.choice(profile["preferred_tf"]),
        "forecast": random.choice(FORECASTS),
        "amount": round(random.uniform(lo, hi), 2),
    }


def build_trade(profile: dict) -> dict:
    """Tạo 1 trade ngẫu nhiên theo phong cách của bot."""
    p = _base_payload(profile)

    is_limit = random.random() < profile["limit_pct"]
    has_bracket = random.random() < profile["bracket_pct"]
    has_ttl = random.random() < profile["ttl_pct"]

    if is_limit:
        p["limit_price"] = round(random.uniform(0.20, 0.80), 2)

    if has_bracket:
        entry = p.get("limit_price", 0.50)
        # ~70% chance TP
        if random.random() < 0.7:
            p["tp_price"] = round(min(0.95, entry + random.uniform(0.10, 0.30)), 2)
        # ~70% chance SL
        if random.random() < 0.7:
            p["sl_price"] = round(max(0.05, entry - random.uniform(0.10, 0.25)), 2)

    if has_ttl and is_limit:
        if profile["suffix"] == "scalper":
            p["ttl"] = random.choice([15, 30, 60])
        else:
            p["ttl"] = random.choice([30, 60, 120, 180, 300])

    # ~50% chance reason
    if random.random() > 0.5:
        p["reason"] = random.choice(REASONS)

    return p


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def wait_for_api() -> bool:
    """Đợi API ready, trả về True nếu OK."""
    for attempt in range(30):
        try:
            r = requests.get(
                f"{BASE.rsplit('/poly-arena', 1)[0]}/health", timeout=5
            )
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
    """Register a new user or login if already exists. Returns JWT token."""
    # Try register first
    r = requests.post(
        f"{BASE}/auth/register",
        json={"username": username, "password": password},
        timeout=10,
    )
    if r.status_code == 201:
        token = r.json()["access_token"]
        log.info("User registered: %s", username)
        return token

    if r.status_code == 409:
        # User already exists — login instead
        r = requests.post(
            f"{BASE}/auth/login",
            json={"username": username, "password": password},
            timeout=10,
        )
        r.raise_for_status()
        token = r.json()["access_token"]
        log.info("User logged in: %s", username)
        return token

    r.raise_for_status()
    return ""  # unreachable


def create_bot(bot_name: str, jwt_token: str) -> str:
    """Tạo bot (authenticated via JWT), trả về api_key."""
    log.info("Creating bot '%s' ...", bot_name)
    r = requests.post(
        f"{BASE}/bots/",
        json={"bot_name": bot_name},
        headers={"Authorization": f"Bearer {jwt_token}"},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    api_key = data["api_key"]
    log.info("Bot created: name=%s balance=$%.0f", data["bot_name"], data["balance"])
    return api_key


def place_trade(api_key: str, payload: dict, bot_name: str) -> None:
    """Gửi 1 trade lên API."""
    r = requests.post(
        f"{BASE}/binary-options/",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )

    if r.ok:
        data = r.json()
        otype = "LIMIT" if payload.get("limit_price") else "MKT"
        bracket = ""
        if payload.get("tp_price") or payload.get("sl_price"):
            bracket = f" [TP={payload.get('tp_price', '-')} SL={payload.get('sl_price', '-')}]"
        ttl_str = f" TTL={payload['ttl']}s" if payload.get("ttl") else ""
        log.info(
            "%s Trade #%d: %s %s %s %s $%.2f → avg=%.4f shares=%.2f%s%s",
            bot_name,
            data["id"],
            otype,
            payload["symbol"],
            payload["timeframe"],
            payload["forecast"],
            payload["amount"],
            data.get("avg_price") or 0,
            data.get("num_shares") or 0,
            bracket,
            ttl_str,
        )
    else:
        log.warning("%s Trade failed (%d): %s", bot_name, r.status_code, r.text[:200])


def run_batch(api_key: str, bot_name: str, profile: dict, count: int) -> None:
    """Tạo 1 batch trades cho 1 bot."""
    trades = [build_trade(profile) for _ in range(count)]

    for idx, payload in enumerate(trades, 1):
        otype = "LIMIT" if payload.get("limit_price") else "MKT"
        has_tp = "TP" if payload.get("tp_price") else ""
        has_sl = "SL" if payload.get("sl_price") else ""
        has_ttl = f"TTL={payload['ttl']}s" if payload.get("ttl") else ""
        extras = "+".join(filter(None, [has_tp, has_sl, has_ttl]))
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
        time.sleep(random.uniform(0.3, 1.5))


# ---------------------------------------------------------------------------
# Bot thread loop
# ---------------------------------------------------------------------------


def bot_loop(bot_name: str, api_key: str, profile: dict) -> None:
    """Main loop cho 1 bot — chạy trong thread riêng."""
    log.info(
        "%s started: style=%s trades=%d interval=%ds symbols=%s tf=%s",
        bot_name, profile["suffix"], TRADES_PER_TICK, INTERVAL,
        profile["preferred_symbols"], profile["preferred_tf"],
    )

    # Stagger start: mỗi bot bắt đầu lệch nhau vài giây
    jitter = random.uniform(0, 10)
    log.info("%s sleeping %.1fs before first batch (stagger)...", bot_name, jitter)
    time.sleep(jitter)

    while True:
        log.info("=" * 60)
        log.info("%s Starting batch of %d trades...", bot_name, TRADES_PER_TICK)
        run_batch(api_key, bot_name, profile, TRADES_PER_TICK)
        # Mỗi bot có interval hơi khác nhau để tránh burst cùng lúc
        sleep_time = INTERVAL + random.randint(-30, 30)
        log.info("%s Batch complete. Sleeping %ds...", bot_name, sleep_time)
        time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not wait_for_api():
        return

    # Step 1: Register or login test user
    try:
        jwt_token = register_or_login_user(TEST_USER, TEST_PASSWORD)
    except Exception as e:
        log.error("Failed to register/login user '%s': %s", TEST_USER, e)
        return

    threads = []

    # Step 2: Create bots under the authenticated user
    for i in range(NUM_BOTS):
        profile = BOT_PROFILES[i % len(BOT_PROFILES)]
        bot_name = f"bot-{profile['suffix']}-{random.randint(1000, 9999)}"

        try:
            api_key = create_bot(bot_name, jwt_token)
        except Exception as e:
            log.error("Failed to create bot '%s': %s", bot_name, e)
            continue

        t = threading.Thread(
            target=bot_loop,
            args=(bot_name, api_key, profile),
            name=bot_name,
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info("Thread started for %s (%s)", bot_name, profile["suffix"])

    if not threads:
        log.error("No bots created, exiting")
        return

    log.info(
        "All %d bots running. Press Ctrl+C to stop.",
        len(threads),
    )

    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
            alive = sum(1 for t in threads if t.is_alive())
            log.info("Health check: %d/%d bot threads alive", alive, len(threads))
    except KeyboardInterrupt:
        log.info("Shutting down...")


if __name__ == "__main__":
    main()
