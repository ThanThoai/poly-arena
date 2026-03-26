#!/usr/bin/env python3
"""
Demo: WebSocket Orderbook Snapshot Builder

Connects to Polymarket WebSocket, subscribes to BTC M5 + M15 tokens,
and displays a live-updating orderbook snapshot in the terminal.

Usage:
    python scripts/demo_ws_snapshot.py
    python scripts/demo_ws_snapshot.py --depth 10
    python scripts/demo_ws_snapshot.py --symbol ETH --tf M5
"""

from __future__ import annotations

import asyncio
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.polymarket import PolymarketClient
from services.ws_feed import PolymarketFeed
from services.snapshot_store import SnapshotStore

# ── ANSI Colors ──────────────────────────────────────────────────────────────

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
CLEAR_SCREEN = "\033[2J\033[H"


def _resolve_tokens(
    symbol: str, timeframes: list[str],
) -> dict[str, dict]:
    """
    Discover current token_ids for the given symbol and timeframes.

    Returns: {token_id: {"symbol", "timeframe", "direction", "candle_open"}}
    """
    pm = PolymarketClient()
    token_map: dict[str, dict] = {}

    try:
        for tf in timeframes:
            tf_lower = tf.lower() if tf[0].isdigit() else tf
            # Resolve candle_open for current candle
            from config.timing import TF_SECONDS
            tf_key = tf.upper() if not tf[0].isdigit() else f"M{tf.replace('m','')}"
            period = TF_SECONDS[tf_key]
            now = int(time.time())
            candle_open = now - (now % period)

            for direction in ("UP", "DOWN"):
                try:
                    token_id = pm.get_token_id(symbol, tf, direction)
                    token_map[token_id] = {
                        "symbol": symbol.upper(),
                        "timeframe": tf_key,
                        "direction": direction,
                        "candle_open": candle_open,
                    }
                    print(
                        f"  {GREEN}OK{RESET}  {symbol.upper()} {tf_key} {direction:<4} "
                        f"token={token_id[:16]}..."
                    )
                except Exception as e:
                    print(
                        f"  {RED}ERR{RESET} {symbol.upper()} {tf_key} {direction:<4} {e}"
                    )
    finally:
        pm.close()

    return token_map


def _render(
    store: SnapshotStore,
    token_map: dict[str, dict],
    depth: int,
    start_time: float,
) -> str:
    """Render the current snapshot state as a terminal string."""
    lines: list[str] = []
    now = time.time()
    uptime = now - start_time

    lines.append(CLEAR_SCREEN)
    lines.append(
        f"{BOLD}{'=' * 72}{RESET}"
    )
    lines.append(
        f"{BOLD}  Polymarket WebSocket Snapshot Demo{RESET}"
        f"  {DIM}uptime: {uptime:.0f}s  events: {store.total_events}{RESET}"
    )
    lines.append(
        f"{BOLD}{'=' * 72}{RESET}"
    )

    # Group tokens by (symbol, timeframe)
    groups: dict[str, list[str]] = {}
    for token_id, info in token_map.items():
        key = f"{info['symbol']} {info['timeframe']}"
        groups.setdefault(key, []).append(token_id)

    for group_label, token_ids in sorted(groups.items()):
        lines.append("")
        lines.append(f"{BOLD}{CYAN}  {group_label}{RESET}")
        lines.append(f"  {'-' * 68}")

        for token_id in sorted(token_ids, key=lambda t: token_map[t]["direction"]):
            info = token_map[token_id]
            direction = info["direction"]
            dir_color = GREEN if direction == "UP" else RED

            snap = store.get_snapshot(token_id)

            if snap is None:
                lines.append(
                    f"  {dir_color}{direction:<4}{RESET}  "
                    f"{DIM}waiting for data...{RESET}"
                )
                continue

            # Header line with BBO
            age_s = (now * 1000 - snap.last_updated) / 1000 if snap.last_updated else 0
            age_str = f"{age_s:.1f}s ago" if age_s < 999 else "n/a"

            mid = snap.midpoint
            mid_str = f"{mid:.4f}" if mid else "---"
            spread_str = f"{snap.spread:.4f}" if snap.spread else "---"

            lines.append(
                f"  {dir_color}{BOLD}{direction:<4}{RESET}  "
                f"bid={GREEN}{snap.best_bid or '---':>7}{RESET}  "
                f"ask={RED}{snap.best_ask or '---':>7}{RESET}  "
                f"mid={YELLOW}{mid_str:>7}{RESET}  "
                f"spread={spread_str:>6}  "
                f"{DIM}{age_str}{RESET}"
            )

            # Stats line
            stats = (
                f"         books={snap.book_event_count}  "
                f"deltas={snap.price_change_count}  "
                f"trades={snap.trade_count}"
            )
            if snap.last_trade:
                t = snap.last_trade
                stats += f"  last_trade={t.price}x{t.size}({t.side})"
            lines.append(f"  {DIM}{stats}{RESET}")

            # Orderbook depth
            bids = snap.get_bids(depth)
            asks = snap.get_asks(depth)
            max_rows = max(len(bids), len(asks))

            if max_rows > 0:
                lines.append("")
                lines.append(
                    f"    {GREEN}{'BIDS':^30}{RESET}  |  {RED}{'ASKS':^30}{RESET}"
                )
                lines.append(
                    f"    {'Price':>12}  {'Size':>14}    |  {'Price':>12}  {'Size':>14}"
                )
                lines.append(f"    {'-' * 30}  |  {'-' * 30}")

                for i in range(min(max_rows, depth)):
                    bid_str = (
                        f"{GREEN}{bids[i][0]:>12.4f}  {bids[i][1]:>14.2f}{RESET}"
                        if i < len(bids)
                        else f"{'':>30}"
                    )
                    ask_str = (
                        f"{RED}{asks[i][0]:>12.4f}  {asks[i][1]:>14.2f}{RESET}"
                        if i < len(asks)
                        else f"{'':>30}"
                    )
                    lines.append(f"    {bid_str}  |  {ask_str}")

            lines.append("")

    # Footer
    lines.append(f"  {DIM}Ctrl+C to exit{RESET}")
    return "\n".join(lines)


async def main():
    parser = argparse.ArgumentParser(description="WebSocket Orderbook Snapshot Demo")
    parser.add_argument("--symbol", default="BTC", help="Symbol (default: BTC)")
    parser.add_argument(
        "--tf", nargs="*", default=["M5", "M15"],
        help="Timeframe(s) (default: M5 M15)",
    )
    parser.add_argument("--depth", type=int, default=5, help="Orderbook depth (default: 5)")
    parser.add_argument(
        "--refresh", type=float, default=1.0,
        help="Display refresh interval in seconds (default: 1.0)",
    )
    args = parser.parse_args()

    # ── Step 1: Discover tokens ──────────────────────────────────────────
    print(f"\n{BOLD}Discovering tokens for {args.symbol} {args.tf}...{RESET}\n")
    token_map = _resolve_tokens(args.symbol, args.tf)

    if not token_map:
        print(f"\n{RED}No tokens found. Check symbol/timeframe.{RESET}")
        return

    token_ids = list(token_map.keys())
    print(f"\n{GREEN}Found {len(token_ids)} token(s). Connecting to WebSocket...{RESET}\n")

    # ── Step 2: Create snapshot store ────────────────────────────────────
    store = SnapshotStore()

    # ── Step 3: Connect WebSocket feed ───────────────────────────────────
    feed = PolymarketFeed(
        token_ids=token_ids,
        on_event=store.handle_event,
    )
    await feed.start()

    # Wait a moment for initial book events
    print(f"{DIM}Waiting for initial book snapshots...{RESET}")
    await asyncio.sleep(2)

    # ── Step 4: Live display loop ────────────────────────────────────────
    start_time = time.time()
    try:
        while True:
            output = _render(store, token_map, args.depth, start_time)
            print(output, flush=True)
            await asyncio.sleep(args.refresh)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print(f"\n{YELLOW}Shutting down...{RESET}")
        await feed.stop()

        # Print final summary
        print(f"\n{BOLD}Final Summary{RESET}")
        print(f"  Total events: {store.total_events}")
        for token_id, info in token_map.items():
            snap = store.get_snapshot(token_id)
            if snap:
                print(
                    f"  {info['symbol']} {info['timeframe']} {info['direction']}: "
                    f"books={snap.book_event_count} "
                    f"deltas={snap.price_change_count} "
                    f"trades={snap.trade_count} "
                    f"bid={snap.best_bid} ask={snap.best_ask}"
                )
        print()


if __name__ == "__main__":
    asyncio.run(main())
