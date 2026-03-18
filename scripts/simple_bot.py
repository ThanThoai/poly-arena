#!/usr/bin/env python3
"""
Simple single-bot trading script with cron-style scheduling.

Uses the same test-trader account as the test_bot service.
Places one trade at a time so you can observe the full lifecycle.

Usage:
    from scripts.simple_bot import run

    # Simple loop (trade every 30s)
    run()

    # Cron-style: trade every 5 minutes at :10s
    run(cron="*/5 * * * *", cron_second=10)

    # Cron-style: trade at the top of every hour
    run(symbol="ETH", timeframe="M15", cron="*/15 * * * *")

    # One-shot (no schedule)
    run(count=1)
"""

import os
import random
import time
from datetime import datetime, timezone

import requests

# ── Config (same defaults as test_bot/bot.py) ────────────────────────────────

BASE = os.environ.get("API_URL", "http://localhost:8099/poly-arena")
TEST_USER = os.environ.get("TEST_USER", "test-trader")
TEST_PASSWORD = os.environ.get("TEST_PASSWORD", "testpass123")
BOT_NAME = os.environ.get("BOT_NAME", "bot-random-m5-9816")


# ── Auth helpers ──────────────────────────────────────────────────────────────


def register_or_login(username: str, password: str) -> str:
    """Register or login, return JWT token."""
    r = requests.post(
        f"{BASE}/auth/register",
        json={"username": username, "password": password},
        timeout=10,
    )
    if r.status_code == 201:
        print(f"[+] Registered user: {username}")
        return r.json()["access_token"]

    if r.status_code == 409:
        r = requests.post(
            f"{BASE}/auth/login",
            json={"username": username, "password": password},
            timeout=10,
        )
        r.raise_for_status()
        print(f"[+] Logged in as: {username}")
        return r.json()["access_token"]

    r.raise_for_status()
    return ""


def get_or_create_bot(jwt: str, bot_name: str) -> str:
    """Return api_key for bot_name, creating it if needed."""
    r = requests.get(
        f"{BASE}/bots/my",
        headers={"Authorization": f"Bearer {jwt}"},
        timeout=10,
    )
    r.raise_for_status()
    for bot in r.json():
        if bot["bot_name"] == bot_name:
            print(f"[+] Reusing bot: {bot_name} (balance=${bot['balance']:.2f})")
            return bot["api_key"]

    r = requests.post(
        f"{BASE}/bots",
        json={"bot_name": bot_name},
        headers={"Authorization": f"Bearer {jwt}"},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    print(f"[+] Created bot: {bot_name} (balance=${data['balance']:.2f})")
    return data["api_key"]


# ── Trading ───────────────────────────────────────────────────────────────────


def place_trade(api_key: str, payload: dict) -> dict | None:
    """Place a single trade, return response dict or None on failure."""
    r = requests.post(
        f"{BASE}/binary-options",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )
    if r.ok:
        return r.json()
    print(f"[-] Trade failed ({r.status_code}): {r.text[:200]}")
    return None


def fetch_prices(symbol: str, timeframe: str) -> dict | None:
    """Fetch best_ask/best_bid from engine for a symbol+tf+UP."""
    try:
        r = requests.get(f"{BASE}/binary-options/engine/prices", timeout=10)
        if r.ok:
            for p in r.json().get("prices", []):
                if (
                    p["symbol"] == symbol
                    and p["timeframe"] == timeframe
                    and p["direction"] == "UP"
                ):
                    return p
    except Exception:
        pass
    return None


def run_once(api_key: str, symbol: str, timeframe: str) -> dict | None:
    """Build and place one random trade. Returns API response or None."""
    # forecast = random.choice(["GREEN", "RED"])
    forecast = "GREEN"
    amount = round(random.uniform(10, 100), 2)

    payload: dict = {
        "symbol": symbol,
        "timeframe": timeframe,
        "forecast": forecast,
        "amount": amount,
        "limit_price": 0.01,
    }

    # Optionally add a bracket order (50% chance)
    prices = fetch_prices(symbol, timeframe)
    best_ask = prices.get("best_ask") if prices else None
    if best_ask and random.random() < 0.5:
        ref = float(best_ask)
        if random.random() < 0.6:
            payload["tp_price"] = round(min(0.95, ref + random.uniform(0.05, 0.20)), 2)
        else:
            payload["sl_price"] = round(max(0.05, ref - random.uniform(0.05, 0.20)), 2)

    otype = "LIMIT" if payload.get("limit_price") else "MKT"
    bracket = ""
    if payload.get("tp_price"):
        bracket = f" TP={payload['tp_price']}"
    elif payload.get("sl_price"):
        bracket = f" SL={payload['sl_price']}"

    print(f"\n>>> {otype} {symbol} {timeframe} {forecast} ${amount}{bracket}")
    result = place_trade(api_key, payload)
    if result:
        avg = result.get("avg_price") or 0
        shares = result.get("num_shares") or 0
        status = result.get("me_order_status", "?")
        print(
            f"<<< Order #{result['id']}  avg={avg:.4f}  shares={shares:.2f}  status={status}"
        )
    return result


# ── Cron scheduler ────────────────────────────────────────────────────────────


def _parse_cron_field(field: str, min_val: int, max_val: int) -> set[int]:
    """Parse a single cron field into a set of matching integers."""
    values: set[int] = set()
    for part in field.split(","):
        if "/" in part:
            base, step = part.split("/", 1)
            step = int(step)
            if base == "*":
                start = min_val
            else:
                start = int(base)
            values.update(range(start, max_val + 1, step))
        elif part == "*":
            values.update(range(min_val, max_val + 1))
        elif "-" in part:
            lo, hi = part.split("-", 1)
            values.update(range(int(lo), int(hi) + 1))
        else:
            values.add(int(part))
    return values


def _cron_matches(cron: str, dt: datetime) -> bool:
    """Check if a datetime matches a 5-field cron expression (min hour dom month dow)."""
    fields = cron.strip().split()
    if len(fields) != 5:
        raise ValueError(
            f"Cron expression must have 5 fields, got {len(fields)}: {cron!r}"
        )

    minute, hour, dom, month, dow = fields
    return (
        dt.minute in _parse_cron_field(minute, 0, 59)
        and dt.hour in _parse_cron_field(hour, 0, 23)
        and dt.day in _parse_cron_field(dom, 1, 31)
        and dt.month in _parse_cron_field(month, 1, 12)
        and dt.weekday() in _parse_cron_field(dow, 0, 6)  # 0=Mon in Python
    )


def _wait_for_cron(cron: str, second: int = 0) -> None:
    """Sleep until the next minute matching the cron expression + offset second."""
    while True:
        now = datetime.now(timezone.utc)
        if _cron_matches(cron, now) and now.second >= second:
            return
        time.sleep(1)


# ── Main ──────────────────────────────────────────────────────────────────────


def run(
    symbol: str = "BTC",
    timeframe: str = "M5",
    interval: int = 30,
    count: int = 0,
    bot_name: str = BOT_NAME,
    username: str = TEST_USER,
    password: str = TEST_PASSWORD,
    cron: str | None = None,
    cron_second: int = 0,
) -> None:
    """
    Run the simple bot.

    Args:
        symbol:      BTC | ETH | SOL | XRP
        timeframe:   M5 | M15 | H1
        interval:    seconds between trades (used when cron is None)
        count:       number of trades (0 = infinite)
        bot_name:    bot display name
        username:    test-trader account username
        password:    test-trader account password
        cron:        cron expression, e.g. "*/5 * * * *" (every 5 min).
                     When set, `interval` is ignored.
        cron_second: second offset within the matching minute (0-59)
    """
    mode = f"cron={cron} +{cron_second}s" if cron else f"interval={interval}s"

    print(f"=== Simple Bot ===")
    print(f"API:      {BASE}")
    print(f"User:     {username}")
    print(f"Bot:      {bot_name}")
    print(f"Symbol:   {symbol}  TF: {timeframe}")
    print(f"Schedule: {mode}")
    print()

    jwt = register_or_login(username, password)
    api_key = get_or_create_bot(jwt, bot_name)

    i = 0
    last_trigger_min: int | None = None  # prevent double-fire within same minute

    try:
        while True:
            if cron:
                _wait_for_cron(cron, cron_second)
                now = datetime.now(timezone.utc)
                current_min = now.hour * 60 + now.minute
                if current_min == last_trigger_min:
                    time.sleep(1)
                    continue
                last_trigger_min = current_min
                print(f"[cron] Triggered at {now.strftime('%H:%M:%S')} UTC")
            for s in ["BTC", "ETH"]:
                run_once(api_key, s, timeframe)
            i += 1

            if count and i >= count:
                print(f"\nDone — placed {i} trade(s).")
                break

            if not cron:
                time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {i} trade(s).")


if __name__ == "__main__":
    run(cron="*/5 * * * *", cron_second=0)
