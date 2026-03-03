#!/usr/bin/env python3
"""
3 baseline random bots — one per timeframe (M5, M15, H1).

Each bot:
  • Registers itself once via POST /api/bots, caches api_key to bots_config.json
  • Waits until the next clock-aligned candle open for its timeframe
  • At candle open: places 1 random trade per symbol (GREEN or RED)
  • Repeats forever

Usage:
    python bots/baseline_bots.py
    python bots/baseline_bots.py --api http://localhost:8010
"""

import argparse
import json
import logging
import random
import time
import threading
from pathlib import Path

import httpx

# ── Config ─────────────────────────────────────────────────────────────────────

API_BASE = "https://aiavatar.torilab.ai/poly-arena"
SYMBOLS = ["BTC", "ETH"]
FORECASTS = ["GREEN", "RED"]
AMOUNT = 100.0  # fixed amount per trade (USD)

BOTS = [
    {"name": "Baseline-M5", "timeframe": "M5", "interval_s": 5 * 60},
    {"name": "Baseline-M15", "timeframe": "M15", "interval_s": 15 * 60},
]

CONFIG_FILE = Path(__file__).parent / "bots_config.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
log = logging.getLogger("baseline")


# ── Helpers ────────────────────────────────────────────────────────────────────


def seconds_to_next_candle(interval_s: int) -> float:
    """Return seconds until the next clock-aligned candle boundary."""
    import time as _t

    now = _t.time()
    return interval_s - (now % interval_s)


def load_config() -> dict:
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text())
    return {}


def save_config(cfg: dict) -> None:
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))


# ── Bot thread ─────────────────────────────────────────────────────────────────


def run_bot(bot_cfg: dict, api_base: str, api_key: str) -> None:
    name = bot_cfg["name"]
    timeframe = bot_cfg["timeframe"]
    interval = bot_cfg["interval_s"]
    logger = logging.getLogger(name)
    rnd = random.Random()  # unseeded — truly random each run

    logger.info("Started  tf=%-4s  interval=%ds", timeframe, interval)

    while True:
        wait = seconds_to_next_candle(interval)
        logger.info("Sleeping %.1fs → next %s candle", wait, timeframe)
        time.sleep(wait)

        # Small jitter to avoid all bots hitting the API simultaneously
        time.sleep(rnd.uniform(0.2, 1.0))

        for symbol in SYMBOLS:
            forecast = rnd.choice(FORECASTS)
            try:
                resp = httpx.post(
                    f"{api_base}/binary-options/",
                    json={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "forecast": forecast,
                        "amount": AMOUNT,
                        "reason": f"Baseline bot {name} placed a trade for {symbol} {timeframe} {forecast}",
                    },
                    headers={"x-api-key": api_key},
                    timeout=10.0,
                )
                resp.raise_for_status()
                bo = resp.json()
                logger.info(
                    "Placed  %-4s %-4s %-6s  id=%-5s  settle=%s",
                    symbol,
                    timeframe,
                    forecast,
                    bo["id"],
                    bo.get("settlement_at", "?"),
                )
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "HTTP %s placing %s %s: %s",
                    exc.response.status_code,
                    symbol,
                    timeframe,
                    exc.response.text[:120],
                )
            except Exception as exc:
                logger.error("Error placing %s %s: %s", symbol, timeframe, exc)


# ── Registration ───────────────────────────────────────────────────────────────


DEFAULT_USER = "baseline-trader"
DEFAULT_PASS = "baseline123456"


def _get_jwt_token(api_base: str, username: str, password: str) -> str:
    """Register or login to get a JWT token for bot creation."""
    # Try register first
    resp = httpx.post(
        f"{api_base}/auth/register",
        json={"username": username, "password": password},
        timeout=10.0,
    )
    if resp.status_code == 201:
        log.info("User registered: %s", username)
        return resp.json()["access_token"]

    if resp.status_code == 409:
        # User already exists — login
        resp = httpx.post(
            f"{api_base}/auth/login",
            json={"username": username, "password": password},
            timeout=10.0,
        )
        resp.raise_for_status()
        log.info("User logged in: %s", username)
        return resp.json()["access_token"]

    resp.raise_for_status()
    return ""  # unreachable


def ensure_bots(api_base: str) -> dict:
    """
    Create bots that don't exist yet, return {name: api_key}.
    Caches api_keys locally so restarts don't re-register.
    Uses JWT auth for bot creation (v2 requirement).
    """
    keys = load_config()

    # Check if any bots need creating
    need_create = [cfg for cfg in BOTS if cfg["name"] not in keys]
    if not need_create:
        for cfg in BOTS:
            log.info("Bot '%-14s' already registered (cached)", cfg["name"])
        return keys

    # Get JWT token for bot creation
    try:
        jwt_token = _get_jwt_token(api_base, DEFAULT_USER, DEFAULT_PASS)
    except Exception as exc:
        log.error("Failed to get JWT token: %s", exc)
        return keys

    for cfg in need_create:
        name = cfg["name"]
        try:
            resp = httpx.post(
                f"{api_base}/bots/",
                json={"bot_name": name},
                headers={"Authorization": f"Bearer {jwt_token}"},
                timeout=10.0,
            )
            if resp.status_code == 409:
                log.warning(
                    "Bot '%s' exists on server but api_key is not cached. "
                    "Delete %s or re-create the bot manually.",
                    name,
                    CONFIG_FILE,
                )
                continue
            resp.raise_for_status()
            data = resp.json()
            keys[name] = data["api_key"]
            log.info("Created bot '%-14s'  api_key=%s…", name, data["api_key"][:12])

        except Exception as exc:
            log.error("Could not register bot '%s': %s", name, exc)

    save_config(keys)
    return keys


# ── Entry ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run 3 baseline random bots (M5 / M15 / H1)"
    )
    parser.add_argument(
        "--api",
        default=API_BASE,
        help=f"PolyArena API base URL (default: {API_BASE})",
    )
    args = parser.parse_args()

    log.info("API base: %s", args.api)
    keys = ensure_bots(args.api)

    threads = []
    for cfg in BOTS:
        name = cfg["name"]
        if name not in keys:
            log.warning("Skipping '%s' — no api_key available", name)
            continue
        t = threading.Thread(
            target=run_bot,
            args=(cfg, args.api, keys[name]),
            name=name,
            daemon=True,
        )
        threads.append(t)
        t.start()

    if not threads:
        log.error("No bot threads started — check errors above.")
        return

    log.info("%d bot thread(s) running.  Press Ctrl+C to stop.", len(threads))
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
