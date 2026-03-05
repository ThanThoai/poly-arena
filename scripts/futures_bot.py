#!/usr/bin/env python3
"""
Futures trading test bot — places random LONG/SHORT trades with TP/SL.

Connects to the same API as the prediction market bot but uses the
/futures/ endpoints. Supports both MARKET and LIMIT orders.

Usage:
    # Run with defaults (trade every 30s, all symbols)
    python scripts/futures_bot.py

    # Custom settings
    python scripts/futures_bot.py --interval 60 --symbols BTC,ETH --leverage 20

    # One-shot mode
    python scripts/futures_bot.py --count 3

    # As module
    from scripts.futures_bot import run
    run(interval=15, symbols=["BTC", "ETH"])
"""

import argparse
import os
import random
import time
from datetime import datetime, timezone

import requests

# ── Config ───────────────────────────────────────────────────────────────────

BASE = os.environ.get("API_URL", "http://localhost:8099/poly-arena")
BOT_NAME = os.environ.get("FUTURES_BOT_NAME", "futures-bot-1")

SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
SIDES = ["LONG", "SHORT"]


# ── Bot helpers ──────────────────────────────────────────────────────────────


def get_or_create_bot(bot_name: str) -> str:
    """Create bot or return existing one's api_key (no user auth needed)."""
    r = requests.post(
        f"{BASE}/bots/",
        json={"bot_name": bot_name, "get_or_create": True},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    print(f"[+] Bot ready: {bot_name} (balance=${data['balance']:.2f})")
    return data["api_key"]


# ── Price helpers ────────────────────────────────────────────────────────────


def fetch_prices() -> dict[str, float]:
    """Fetch current mark prices from the futures API."""
    try:
        r = requests.get(f"{BASE}/futures/prices", timeout=10)
        if r.ok:
            data = r.json().get("prices", {})
            return {sym: float(info["price"]) for sym, info in data.items()}
    except Exception as exc:
        print(f"[-] Price fetch failed: {exc}")
    return {}


def fetch_open_positions(api_key: str) -> list[dict]:
    """Fetch open positions."""
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


# ── Trading ──────────────────────────────────────────────────────────────────


def place_futures_order(api_key: str, payload: dict) -> dict | None:
    """Place a futures order, return response or None."""
    r = requests.post(
        f"{BASE}/futures/orders",
        json=payload,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        timeout=15,
    )
    if r.ok:
        return r.json()
    print(f"[-] Order failed ({r.status_code}): {r.text[:300]}")
    return None


def close_position(api_key: str, position_id: int) -> dict | None:
    """Close an open position at market."""
    r = requests.post(
        f"{BASE}/futures/positions/{position_id}/close",
        headers={"x-api-key": api_key},
        timeout=15,
    )
    if r.ok:
        return r.json()
    print(f"[-] Close failed ({r.status_code}): {r.text[:200]}")
    return None


def run_once(
    api_key: str,
    symbols: list[str],
    leverage_range: tuple[int, int],
    margin_range: tuple[float, float],
    close_probability: float,
) -> None:
    """Execute one trading cycle: maybe close old positions, open new ones."""
    prices = fetch_prices()
    if not prices:
        print("[-] No prices available, skipping cycle")
        return

    # Maybe close some existing positions
    open_positions = fetch_open_positions(api_key)
    for pos in open_positions:
        if random.random() < close_probability:
            mark = prices.get(pos["symbol"])
            if mark:
                pnl = pos.get("unrealized_pnl", 0)
                print(f"  [x] Closing position #{pos['id']} {pos['side']} {pos['symbol']} "
                      f"(uPnL: {'+'if pnl >= 0 else ''}{pnl:.2f})")
                result = close_position(api_key, pos["id"])
                if result:
                    print(f"      Closed: PnL={result['realized_pnl']:.2f} "
                          f"exit=${result['exit_price']:.2f} fee=${result['exit_fee']:.4f}")

    # Open new positions on random symbols
    num_trades = random.randint(1, min(3, len(symbols)))
    chosen = random.sample(symbols, num_trades)

    for sym in chosen:
        mark = prices.get(sym)
        if not mark:
            print(f"  [-] No price for {sym}, skipping")
            continue

        side = random.choice(SIDES)
        leverage = random.randint(*leverage_range)
        margin = round(random.uniform(*margin_range), 2)
        order_type = random.choices(["MARKET", "LIMIT"], weights=[0.7, 0.3])[0]

        payload: dict = {
            "symbol": sym,
            "side": side,
            "amount": margin,
            "leverage": leverage,
            "order_type": order_type,
        }

        # LIMIT: set limit_price near mark
        if order_type == "LIMIT":
            if side == "LONG":
                # Buy limit below mark
                payload["limit_price"] = round(mark * random.uniform(0.995, 0.999), 2)
            else:
                # Sell limit above mark
                payload["limit_price"] = round(mark * random.uniform(1.001, 1.005), 2)
            payload["ttl"] = random.choice([60, 120, 300])

        # Randomly add TP and/or SL (60% chance)
        if random.random() < 0.6:
            tp_pct = random.uniform(0.005, 0.03)  # 0.5% - 3%
            sl_pct = random.uniform(0.003, 0.02)  # 0.3% - 2%

            if side == "LONG":
                payload["tp_price"] = round(mark * (1 + tp_pct), 2)
                payload["sl_price"] = round(mark * (1 - sl_pct), 2)
            else:
                payload["tp_price"] = round(mark * (1 - tp_pct), 2)
                payload["sl_price"] = round(mark * (1 + sl_pct), 2)

        # Format log
        tp_str = f" TP=${payload['tp_price']}" if payload.get('tp_price') else ""
        sl_str = f" SL=${payload['sl_price']}" if payload.get('sl_price') else ""
        lim_str = f" @${payload['limit_price']}" if order_type == "LIMIT" else f" @${mark:.2f}"

        print(f"\n>>> {order_type} {side} {sym} {leverage}x ${margin}{lim_str}{tp_str}{sl_str}")

        result = place_futures_order(api_key, payload)
        if result:
            status = result.get("status", "?")
            if status == "filled":
                print(f"<<< Position #{result['position_id']}  "
                      f"size={result['size']:.6f}  entry=${result['entry_price']:.2f}  "
                      f"fee=${result['entry_fee']:.4f}  liq=${result.get('liquidation_price', 0):.2f}")
            else:
                print(f"<<< Order #{result.get('order_id')}  "
                      f"size={result['size']:.6f}  limit=${result['limit_price']:.2f}  "
                      f"status={status}")
            print(f"    Balance: ${result['balance']:.2f}")


# ── Main ─────────────────────────────────────────────────────────────────────


def run(
    symbols: list[str] | None = None,
    interval: int = 30,
    count: int = 0,
    leverage_range: tuple[int, int] = (5, 20),
    margin_range: tuple[float, float] = (50, 500),
    close_probability: float = 0.3,
    bot_name: str = BOT_NAME,
) -> None:
    """
    Run the futures test bot.

    Args:
        symbols:           List of symbols to trade (default: all)
        interval:          Seconds between cycles
        count:             Number of cycles (0 = infinite)
        leverage_range:    (min, max) leverage
        margin_range:      (min, max) USD margin per trade
        close_probability: Probability of closing each open position per cycle
        bot_name:          Bot name
    """
    if symbols is None:
        symbols = SYMBOLS

    print("=== Futures Test Bot ===")
    print(f"API:      {BASE}")
    print(f"Bot:      {bot_name}")
    print(f"Symbols:  {', '.join(symbols)}")
    print(f"Leverage: {leverage_range[0]}-{leverage_range[1]}x")
    print(f"Margin:   ${margin_range[0]}-${margin_range[1]}")
    print(f"Interval: {interval}s")
    print(f"Close %:  {close_probability*100:.0f}%")
    print()

    api_key = get_or_create_bot(bot_name)

    i = 0
    try:
        while True:
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n{'='*50}")
            print(f"Cycle {i+1} at {now} UTC")
            print(f"{'='*50}")

            run_once(api_key, symbols, leverage_range, margin_range, close_probability)

            i += 1
            if count and i >= count:
                print(f"\nDone — completed {i} cycle(s).")
                break

            print(f"\n--- sleeping {interval}s ---")
            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\nStopped after {i} cycle(s).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Futures trading test bot")
    parser.add_argument("--symbols", default="BTC,ETH,SOL,XRP", help="Comma-separated symbols")
    parser.add_argument("--interval", type=int, default=30, help="Seconds between cycles")
    parser.add_argument("--count", type=int, default=0, help="Number of cycles (0=infinite)")
    parser.add_argument("--leverage-min", type=int, default=5, help="Min leverage")
    parser.add_argument("--leverage-max", type=int, default=20, help="Max leverage")
    parser.add_argument("--margin-min", type=float, default=50, help="Min margin USD")
    parser.add_argument("--margin-max", type=float, default=500, help="Max margin USD")
    parser.add_argument("--close-prob", type=float, default=0.3, help="Close probability per position")
    parser.add_argument("--bot-name", default=BOT_NAME, help="Bot name")
    args = parser.parse_args()

    run(
        symbols=args.symbols.split(","),
        interval=args.interval,
        count=args.count,
        leverage_range=(args.leverage_min, args.leverage_max),
        margin_range=(args.margin_min, args.margin_max),
        close_probability=args.close_prob,
        bot_name=args.bot_name,
    )
