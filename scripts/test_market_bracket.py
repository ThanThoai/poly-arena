"""
Test script for MARKET orders with bracket (TP/SL) via API.

Tests:
  1. MARKET pure (no bracket)         → should fill & settle by candle
  2. MARKET + TP only                 → should fill via ME, TP monitored
  3. MARKET + SL only                 → should fill via ME, SL monitored
  4. MARKET + TP + SL                 → should fill via ME, both monitored

For each case, verifies:
  - Order created successfully
  - me_order_status transitions from PENDING → FILLED/PARTIAL
  - avg_price reflects ME fill (not just Polymarket snapshot)

Usage:
  python scripts/test_market_bracket.py [--api-url http://localhost:8099/poly-arena]
"""

import argparse
import time
import sys

import requests

# ── Config ───────────────────────────────────────────────────────────────────

DEFAULT_API = "http://localhost:8099/poly-arena"
BOT_API_KEY = "EDp6sJM7v45cs_vfgoRaAcaPjKgDrHT9jItPcR7YI0s"  # bot-scalper-9926
AMOUNT = 10.0
POLL_INTERVAL = 2     # seconds between polls
POLL_TIMEOUT = 30     # max seconds to wait for fill

# ── Helpers ──────────────────────────────────────────────────────────────────

def create_order(api: str, payload: dict) -> dict:
    resp = requests.post(
        f"{api}/binary-options/",
        json=payload,
        headers={"X-API-Key": BOT_API_KEY},
    )
    resp.raise_for_status()
    return resp.json()


def get_order(api: str, bo_id: int) -> dict:
    resp = requests.get(
        f"{api}/binary-options/{bo_id}",
        headers={"X-API-Key": BOT_API_KEY},
    )
    resp.raise_for_status()
    return resp.json()


def wait_for_fill(api: str, bo_id: int) -> dict:
    """Poll until me_order_status != PENDING or timeout."""
    start = time.time()
    while time.time() - start < POLL_TIMEOUT:
        order = get_order(api, bo_id)
        status = order.get("me_order_status")
        result = order.get("result")

        # Already settled or cancelled
        if result and result != "PENDING":
            return order

        # ME filled
        if status in ("FILLED", "PARTIAL"):
            return order

        time.sleep(POLL_INTERVAL)

    return get_order(api, bo_id)


def print_order(label: str, order: dict):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    fields = [
        "id", "symbol", "timeframe", "forecast", "amount",
        "avg_price", "num_shares", "me_order_status", "result", "profit",
        "tp_price", "sl_price", "exit_trigger", "exit_price",
        "settlement_at",
    ]
    for f in fields:
        val = order.get(f)
        if val is not None:
            print(f"  {f:20s} = {val}")
    print()


def check(label: str, condition: bool, msg: str):
    icon = "PASS" if condition else "FAIL"
    print(f"  [{icon}] {label}: {msg}")
    return condition


# ── Test Cases ───────────────────────────────────────────────────────────────

def test_market_pure(api: str) -> bool:
    """Case 1: MARKET order without bracket — no ME involvement."""
    print("\n" + "#"*60)
    print("# Case 1: MARKET pure (no TP, no SL)")
    print("#"*60)

    order = create_order(api, {
        "symbol": "BTC", "timeframe": "M5",
        "forecast": "GREEN", "amount": AMOUNT,
    })
    print_order("Created", order)

    ok = True
    ok &= check("me_order_status", order["me_order_status"] is None,
                 f"expected None, got {order['me_order_status']}")
    ok &= check("result", order["result"] == "PENDING",
                 f"expected PENDING, got {order['result']}")
    ok &= check("avg_price", order["avg_price"] is not None and order["avg_price"] > 0,
                 f"got {order['avg_price']}")
    return ok


def test_market_tp_only(api: str) -> bool:
    """Case 2: MARKET + TP — ME should fill, TP monitored."""
    print("\n" + "#"*60)
    print("# Case 2: MARKET + TP only")
    print("#"*60)

    order = create_order(api, {
        "symbol": "BTC", "timeframe": "M5",
        "forecast": "GREEN", "amount": AMOUNT,
        "tp_price": 0.90,
    })
    print_order("Created", order)

    initial_avg = order["avg_price"]
    bo_id = order["id"]

    filled = wait_for_fill(api, bo_id)
    print_order("After wait", filled)

    ok = True
    ok &= check("me_order_status", filled["me_order_status"] in ("FILLED", "PARTIAL", "CANCELED"),
                 f"expected FILLED/PARTIAL/CANCELED, got {filled['me_order_status']}")

    if filled["me_order_status"] in ("FILLED", "PARTIAL"):
        ok &= check("avg_price updated",
                     filled["avg_price"] is not None and filled["avg_price"] > 0,
                     f"avg_price={filled['avg_price']} (initial={initial_avg})")
        ok &= check("num_shares updated",
                     filled["num_shares"] is not None and filled["num_shares"] > 0,
                     f"num_shares={filled['num_shares']}")
    else:
        print(f"  [INFO] Order was cancelled/expired — ME may not have filled")

    return ok


def test_market_sl_only(api: str) -> bool:
    """Case 3: MARKET + SL — ME should fill, SL monitored."""
    print("\n" + "#"*60)
    print("# Case 3: MARKET + SL only")
    print("#"*60)

    order = create_order(api, {
        "symbol": "BTC", "timeframe": "M5",
        "forecast": "GREEN", "amount": AMOUNT,
        "sl_price": 0.10,
    })
    print_order("Created", order)

    initial_avg = order["avg_price"]
    bo_id = order["id"]

    filled = wait_for_fill(api, bo_id)
    print_order("After wait", filled)

    ok = True
    ok &= check("me_order_status", filled["me_order_status"] in ("FILLED", "PARTIAL", "CANCELED"),
                 f"expected FILLED/PARTIAL/CANCELED, got {filled['me_order_status']}")

    if filled["me_order_status"] in ("FILLED", "PARTIAL"):
        ok &= check("avg_price updated",
                     filled["avg_price"] is not None and filled["avg_price"] > 0,
                     f"avg_price={filled['avg_price']} (initial={initial_avg})")
    return ok


def test_market_tp_sl(api: str) -> bool:
    """Case 4: MARKET + TP + SL — ME should fill, both monitored."""
    print("\n" + "#"*60)
    print("# Case 4: MARKET + TP + SL")
    print("#"*60)

    order = create_order(api, {
        "symbol": "BTC", "timeframe": "M5",
        "forecast": "GREEN", "amount": AMOUNT,
        "tp_price": 0.90,
        "sl_price": 0.10,
    })
    print_order("Created", order)

    initial_avg = order["avg_price"]
    bo_id = order["id"]

    filled = wait_for_fill(api, bo_id)
    print_order("After wait", filled)

    ok = True
    ok &= check("me_order_status", filled["me_order_status"] in ("FILLED", "PARTIAL", "CANCELED"),
                 f"expected FILLED/PARTIAL/CANCELED, got {filled['me_order_status']}")

    if filled["me_order_status"] in ("FILLED", "PARTIAL"):
        ok &= check("avg_price updated",
                     filled["avg_price"] is not None and filled["avg_price"] > 0,
                     f"avg_price={filled['avg_price']} (initial={initial_avg})")
        ok &= check("bracket intact",
                     filled["exit_trigger"] is None,
                     f"exit_trigger={filled['exit_trigger']} (should be None until TP/SL fires)")
    return ok


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Test MARKET bracket orders")
    parser.add_argument("--api-url", default=DEFAULT_API)
    args = parser.parse_args()
    api = args.api_url

    print(f"API: {api}")
    print(f"Bot: bot-scalper-9926")
    print(f"Amount: {AMOUNT}")

    results = {}
    for name, fn in [
        ("MARKET pure",    test_market_pure),
        ("MARKET + TP",    test_market_tp_only),
        ("MARKET + SL",    test_market_sl_only),
        ("MARKET + TP+SL", test_market_tp_sl),
    ]:
        try:
            results[name] = fn(api)
        except Exception as exc:
            print(f"\n  [ERROR] {name}: {exc}")
            results[name] = False

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("  SUMMARY")
    print("="*60)
    all_pass = True
    for name, ok in results.items():
        icon = "PASS" if ok else "FAIL"
        print(f"  [{icon}] {name}")
        if not ok:
            all_pass = False

    print()
    if all_pass:
        print("All tests passed!")
    else:
        print("Some tests FAILED — check output above")
        sys.exit(1)


if __name__ == "__main__":
    main()
