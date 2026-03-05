"""
Simple aggressive bot — MARKET-heavy, big amounts, BTC M5 only.

Usage:
    python test_bot/aggressive_bot.py
    API_URL=http://host:8099/poly-arena python test_bot/aggressive_bot.py

Env vars:
    API_URL          — API base (default: http://localhost:8099/poly-arena)
    BOT_USER         — username (default: aggressive-user)
    BOT_PASSWORD     — password (default: testpass123)
    BOT_NAME         — bot name (default: aggressive-bot)
    BOT_BALANCE      — initial balance (default: 5000)
    TRADES_PER_TICK  — trades per M5 tick (default: 15)
    SNIPE_OFFSET_S   — seconds before candle boundary to fire (default: 2)
"""

import os
import random
import time
import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger("aggressive-bot")

BASE = os.environ.get("API_URL", "http://localhost:8099/poly-arena")
BOT_NAME = os.environ.get("BOT_NAME", "aggressive-bot")
BOT_BALANCE = float(os.environ.get("BOT_BALANCE", "10000"))
TRADES_PER_TICK = int(os.environ.get("TRADES_PER_TICK", "15"))
SNIPE_OFFSET_S = int(os.environ.get("SNIPE_OFFSET_S", "2"))

FORECASTS = ["GREEN", "RED"]
REASONS = [
    "RSI oversold bounce expected",
    "Breaking above resistance",
    "Volume spike detected",
    "EMA crossover signal",
    "MACD bullish crossover",
    "Bollinger squeeze breakout",
    "Whale accumulation detected",
]

# Aggressive profile: big amounts, mostly MARKET, rarely LIMIT/TTL
AMOUNT_MIN, AMOUNT_MAX = 10, 80
LIMIT_PCT = 0.15   # 15% chance of LIMIT order
TTL_PCT = 0.10     # 10% chance of TTL on LIMIT


def fetch_best_ask() -> float | None:
    try:
        r = requests.get(f"{BASE}/binary-options/engine/prices", timeout=10)
        if r.ok:
            for p in r.json().get("prices", []):
                if p["symbol"] == "BTC" and p["timeframe"] == "M5" and p["direction"] == "UP":
                    return p.get("best_ask")
    except Exception:
        pass
    return None


def build_trade(target_ts: int | None = None, best_ask: float | None = None) -> dict:
    payload: dict = {
        "symbol": "BTC",
        "timeframe": "M5",
        "forecast": random.choice(FORECASTS),
        "amount": round(random.uniform(AMOUNT_MIN, AMOUNT_MAX), 2),
    }

    is_limit = random.random() < LIMIT_PCT
    if is_limit:
        payload["limit_price"] = round(random.uniform(0.20, 0.80), 2)
        if random.random() < TTL_PCT:
            payload["ttl"] = random.choice([30, 60, 120, 180, 300])

    # ~20% of MARKET orders get ceiling_price + FAK/FOK
    if not is_limit and random.random() < 0.20:
        ref = best_ask or 0.50
        payload["ceiling_price"] = round(
            random.uniform(max(0.01, ref - 0.10), min(0.99, ref + 0.15)), 2
        )
        payload["order_type"] = random.choice(["FAK", "FOK"])

    if target_ts is not None:
        payload["timestamp"] = target_ts

    if random.random() > 0.5:
        payload["reason"] = random.choice(REASONS)

    return payload


def place_trade(api_key: str, payload: dict) -> None:
    r = requests.post(
        f"{BASE}/binary-options",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )
    if r.ok:
        d = r.json()
        otype = "LIMIT" if payload.get("limit_price") else "MKT"
        log.info(
            "Trade #%d: %s %s $%.2f → avg=%.4f shares=%.2f",
            d["id"], otype, payload["forecast"],
            payload["amount"], d.get("avg_price") or 0, d.get("num_shares") or 0,
        )
    else:
        log.warning("Trade failed (%d): %s", r.status_code, r.text[:200])


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


def get_or_create_bot() -> str:
    """Create bot or return existing one's api_key (no user auth needed)."""
    log.info("Getting/creating bot '%s' (balance=$%.0f)", BOT_NAME, BOT_BALANCE)
    r = requests.post(
        f"{BASE}/bots",
        json={"bot_name": BOT_NAME, "initial_balance": BOT_BALANCE, "get_or_create": True},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    log.info("Bot ready: %s balance=$%.0f", data["bot_name"], data["balance"])
    return data["api_key"]


def main():
    if not wait_for_api():
        return

    api_key = get_or_create_bot()

    period = 300  # M5

    log.info("Aggressive bot running: trades/tick=%d snipe=%ds", TRADES_PER_TICK, SNIPE_OFFSET_S)

    while True:
        now_ts = int(time.time())
        current_open = now_ts - (now_ts % period)
        next_boundary = current_open + period
        fire_at = next_boundary - SNIPE_OFFSET_S
        wait_s = fire_at - int(time.time())

        if wait_s > 0:
            log.info("Next boundary=%d, sleeping %ds...", next_boundary, wait_s)
            time.sleep(wait_s)

        best_ask = fetch_best_ask()
        if best_ask:
            log.info("Best ask: %.4f", best_ask)

        log.info("=" * 50)
        log.info("FIRE: %d trades at T-%ds, timestamp=%d", TRADES_PER_TICK, SNIPE_OFFSET_S, next_boundary)

        for i in range(TRADES_PER_TICK):
            payload = build_trade(target_ts=next_boundary, best_ask=best_ask)
            try:
                place_trade(api_key, payload)
            except Exception as e:
                log.error("Error: %s", e)
            time.sleep(random.uniform(0.3, 1.0))


if __name__ == "__main__":
    main()
