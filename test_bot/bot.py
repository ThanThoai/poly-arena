"""
Test bot — chạy đồng thời nhiều user, mỗi user có nhiều bot với balance khác nhau.
Mỗi bot chạy trong 1 thread riêng, tạo random trades mỗi 5 phút.
Mỗi bot có phong cách giao dịch riêng (aggressive, conservative, scalper, whale, random).

Khi khởi động:
  1. Tạo/login NUM_USERS user (mặc định 2)
  2. Mỗi user có BOTS_PER_USER bot (mặc định 5) với balance random
  3. Nếu user đã tồn tại → login lại, reuse bot cũ

v2 changes:
  - Single Condition Policy: only TP or SL, not both
  - TP/SL must pass pre-validation: TP > best_ask, SL < best_ask
  - Bot creation requires JWT auth (register/login first)

v3 changes:
  - Multi-user support: NUM_USERS users, each with BOTS_PER_USER bots
  - Random initial_balance per bot (configurable range)
  - Reuse existing users and bots on restart
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
NUM_USERS = int(os.environ.get("NUM_USERS", "2"))
BOTS_PER_USER = int(os.environ.get("BOTS_PER_USER", "5"))
INTERVAL = int(os.environ.get("INTERVAL_SEC", "300"))  # 5 phút
TRADES_PER_TICK = int(os.environ.get("TRADES_PER_TICK", "15"))
PASSWORD = os.environ.get("TEST_PASSWORD", "testpass123")

# Balance range for new bots (random between these values)
BOT_BALANCE_MIN = int(os.environ.get("BOT_BALANCE_MIN", "200"))
BOT_BALANCE_MAX = int(os.environ.get("BOT_BALANCE_MAX", "2000"))

# User names — generate from env or use defaults
USER_NAMES = os.environ.get("USER_NAMES", "").strip()
if USER_NAMES:
    USER_LIST = [n.strip() for n in USER_NAMES.split(",") if n.strip()]
else:
    USER_LIST = [f"trader-{i+1}" for i in range(NUM_USERS)]
# Ensure we have enough names
while len(USER_LIST) < NUM_USERS:
    USER_LIST.append(f"trader-{len(USER_LIST)+1}")

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
        "amount_range": (10, 80),
        "limit_pct": 0.15,       # ít dùng limit
        "bracket_pct": 0.70,     # hay dùng bracket
        "ttl_pct": 0.10,
        "next_session_pct": 0.40,  # 40% lệnh đặt cho phiên A+1
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "conservative",
        "amount_range": (5, 30),
        "limit_pct": 0.50,       # hay dùng limit
        "bracket_pct": 0.80,     # luôn có bracket
        "ttl_pct": 0.40,         # hay set TTL
        "next_session_pct": 0.50,  # conservative hay đặt trước
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "scalper",
        "amount_range": (5, 25),
        "limit_pct": 0.30,
        "bracket_pct": 0.50,
        "ttl_pct": 0.60,         # scalper hay dùng TTL ngắn
        "next_session_pct": 0.30,  # scalper đặt A+1 vừa phải
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "whale",
        "amount_range": (30, 100),
        "limit_pct": 0.40,
        "bracket_pct": 0.60,
        "ttl_pct": 0.20,
        "next_session_pct": 0.45,
        "preferred_symbols": ["BTC"],
        "preferred_tf": ["M5"],
    },
    {
        "suffix": "random-m5",
        "amount_range": (5, 50),
        "limit_pct": 0.30,
        "bracket_pct": 0.40,
        "ttl_pct": 0.30,
        "next_session_pct": 0.35,
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


def build_trade(profile: dict, best_ask: float | None = None) -> dict:
    """Tạo 1 trade ngẫu nhiên theo phong cách của bot.

    v2: Single Condition Policy — only TP or SL, never both.
    TP/SL prices are generated relative to best_ask to pass pre-validation:
      - TP must be > best_ask (take profit above current price)
      - SL must be < best_ask (stop loss below current price)
    """
    p = _base_payload(profile)

    is_limit = random.random() < profile["limit_pct"]
    has_bracket = random.random() < profile["bracket_pct"]
    has_ttl = random.random() < profile["ttl_pct"]

    if is_limit:
        p["limit_price"] = round(random.uniform(0.20, 0.80), 2)

    if has_bracket:
        # Use best_ask as reference for realistic pricing.
        # Fallback to estimated entry if best_ask unavailable.
        ref_price = best_ask or p.get("limit_price") or 0.50

        # Single Condition Policy: choose ONE of TP or SL
        if random.random() < 0.6:
            # TP: must be > best_ask (pre-validation requirement)
            tp = ref_price + random.uniform(0.05, 0.30)
            p["tp_price"] = round(min(0.95, tp), 2)
        else:
            # SL: must be < best_ask (pre-validation requirement)
            sl = ref_price - random.uniform(0.05, 0.25)
            p["sl_price"] = round(max(0.05, sl), 2)

    if has_ttl and is_limit:
        if profile["suffix"] == "scalper":
            p["ttl"] = random.choice([15, 30, 60])
        else:
            p["ttl"] = random.choice([30, 60, 120, 180, 300])

    # Next-session order (A+1) — use timestamp to target the next candle boundary
    if random.random() < profile.get("next_session_pct", 0):
        tf = p["timeframe"]
        period = {"M5": 300, "M15": 900, "H1": 3600}.get(tf, 300)
        now_ts = int(time.time())
        current_open = now_ts - (now_ts % period)
        next_open = current_open + period
        # Use timestamp (next candle boundary) instead of session_offset
        # e.g. at 14:29:54, timestamp=14:30:00 → order effective in 14:30 session
        if random.random() < 0.5:
            p["timestamp"] = next_open
        else:
            p["session_offset"] = 1

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


def fetch_my_bots(jwt_token: str) -> list[dict]:
    """Lấy danh sách bot của user hiện tại (GET /bots/my)."""
    r = requests.get(
        f"{BASE}/bots/my",
        headers={"Authorization": f"Bearer {jwt_token}"},
        timeout=10,
    )
    r.raise_for_status()
    bots = r.json()
    log.info("Fetched %d existing bot(s) for user", len(bots))
    return bots


def create_bot(bot_name: str, jwt_token: str, initial_balance: float = 1000.0) -> str:
    """Tạo bot (authenticated via JWT), trả về api_key."""
    log.info("Creating bot '%s' (balance=$%.0f) ...", bot_name, initial_balance)
    r = requests.post(
        f"{BASE}/bots/",
        json={"bot_name": bot_name, "initial_balance": initial_balance},
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
        condition = ""
        if payload.get("tp_price"):
            condition = f" [TP={payload['tp_price']}]"
        elif payload.get("sl_price"):
            condition = f" [SL={payload['sl_price']}]"
        ttl_str = f" TTL={payload['ttl']}s" if payload.get("ttl") else ""
        session_str = " [A+1]" if payload.get("session_offset") else (f" [TS={payload['timestamp']}]" if payload.get("timestamp") else "")
        log.info(
            "%s Trade #%d: %s %s %s %s $%.2f → avg=%.4f shares=%.2f%s%s%s",
            bot_name,
            data["id"],
            otype,
            payload["symbol"],
            payload["timeframe"],
            payload["forecast"],
            payload["amount"],
            data.get("avg_price") or 0,
            data.get("num_shares") or 0,
            condition,
            ttl_str,
            session_str,
        )
    else:
        log.warning("%s Trade failed (%d): %s", bot_name, r.status_code, r.text[:200])


def fetch_best_ask(symbol: str = "BTC", timeframe: str = "M5") -> float | None:
    """Fetch current best_ask from the engine/prices endpoint for pre-validation reference."""
    try:
        r = requests.get(
            f"{BASE}/binary-options/engine/prices",
            timeout=10,
        )
        if r.ok:
            for p in r.json().get("prices", []):
                if p["symbol"] == symbol and p["timeframe"] == timeframe and p["direction"] == "UP":
                    return p.get("best_ask")
    except Exception:
        pass
    return None


def run_batch(api_key: str, bot_name: str, profile: dict, count: int) -> None:
    """Tạo 1 batch trades cho 1 bot."""
    # Fetch best_ask once per batch for realistic TP/SL pricing
    best_ask = fetch_best_ask()
    if best_ask:
        log.info("%s Best ask reference: %.4f", bot_name, best_ask)

    trades = [build_trade(profile, best_ask=best_ask) for _ in range(count)]

    for idx, payload in enumerate(trades, 1):
        otype = "LIMIT" if payload.get("limit_price") else "MKT"
        has_tp = "TP" if payload.get("tp_price") else ""
        has_sl = "SL" if payload.get("sl_price") else ""
        has_ttl = f"TTL={payload['ttl']}s" if payload.get("ttl") else ""
        has_a1 = "A+1" if payload.get("session_offset") else ("TS=" + str(payload["timestamp"]) if payload.get("timestamp") else "")
        extras = "+".join(filter(None, [has_tp, has_sl, has_ttl, has_a1]))
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
# User setup — tạo/login user và setup bots
# ---------------------------------------------------------------------------


def _match_profile(bot_name: str) -> dict:
    """Guess bot profile from its name suffix, fallback to random profile."""
    for profile in BOT_PROFILES:
        if profile["suffix"] in bot_name:
            return profile
    return random.choice(BOT_PROFILES)


def setup_user(username: str) -> list[tuple[str, str, dict]]:
    """
    Setup 1 user: register/login → fetch existing bots → create new bots if needed.

    Returns list of (bot_name, api_key, profile) tuples ready for bot_loop.
    """
    # Step 1: Register or login
    try:
        jwt_token = register_or_login_user(username, PASSWORD)
    except Exception as e:
        log.error("Failed to register/login user '%s': %s", username, e)
        return []

    # Step 2: Fetch existing bots
    existing_bots: list[dict] = []
    try:
        existing_bots = fetch_my_bots(jwt_token)
    except Exception as e:
        log.warning("[%s] Failed to fetch existing bots: %s — will create new ones", username, e)

    # Step 3: Reuse existing active bots (up to BOTS_PER_USER)
    bot_slots: list[tuple[str, str, dict]] = []

    for bot_data in existing_bots:
        if len(bot_slots) >= BOTS_PER_USER:
            break
        if not bot_data.get("is_active", True):
            continue
        name = bot_data["bot_name"]
        api_key = bot_data["api_key"]
        profile = _match_profile(name)
        bot_slots.append((name, api_key, profile))
        log.info("[%s] Reusing bot: %s (style=%s, balance=$%.0f)",
                 username, name, profile["suffix"], bot_data.get("balance", 0))

    # Step 4: Create additional bots with random balances
    for i in range(len(bot_slots), BOTS_PER_USER):
        profile = BOT_PROFILES[i % len(BOT_PROFILES)]
        bot_name = f"{username}-{profile['suffix']}"
        balance = random.randint(BOT_BALANCE_MIN, BOT_BALANCE_MAX)
        # Round to nice numbers (nearest 50)
        balance = round(balance / 50) * 50
        balance = max(BOT_BALANCE_MIN, balance)
        try:
            api_key = create_bot(bot_name, jwt_token, initial_balance=balance)
            bot_slots.append((bot_name, api_key, profile))
        except Exception as e:
            log.error("[%s] Failed to create bot '%s': %s", username, bot_name, e)

    reused = min(len(existing_bots), BOTS_PER_USER)
    created = len(bot_slots) - reused
    log.info(
        "[%s] Setup complete: %d bot(s) (%d reused, %d new)",
        username, len(bot_slots), reused, created,
    )

    return bot_slots


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not wait_for_api():
        return

    # Setup all users and collect bot slots
    all_bot_slots: list[tuple[str, str, dict]] = []

    for i in range(NUM_USERS):
        username = USER_LIST[i]
        log.info("=" * 60)
        log.info("Setting up user %d/%d: %s", i + 1, NUM_USERS, username)
        slots = setup_user(username)
        all_bot_slots.extend(slots)

    if not all_bot_slots:
        log.error("No bots available across all users, exiting")
        return

    # Launch bot threads
    threads = []
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

    log.info(
        "All %d bots across %d users running. Press Ctrl+C to stop.",
        len(threads), NUM_USERS,
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
