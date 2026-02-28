"""
REST-based Auto-Exit utility.

Shared logic for simulating bracket exits via Polymarket REST orderbook.
Used by:
  - routers/binary_options.py (immediate auto-exit at entry)
  - ws_feed_service/order_consumer.py (runtime monitoring exit)
"""

import logging
from decimal import Decimal
from typing import Optional, Tuple

import httpx

from config.timing import HTTP_TIMEOUT

logger = logging.getLogger(__name__)


def fetch_best_bid_from_rest(token_id: str) -> Tuple[Optional[float], list]:
    """
    Fetch the current best_bid and bid levels from Polymarket REST API.
    Returns (best_bid, bid_levels) where bid_levels is [(Decimal_price, Decimal_size), ...].
    Returns (None, []) on failure.
    """
    try:
        resp = httpx.get(
            "https://clob.polymarket.com/book",
            params={"token_id": token_id},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        book = resp.json()
    except Exception as e:
        logger.warning("Failed to fetch bid levels from REST: %s", e)
        return None, []

    bids = sorted(
        [
            (Decimal(str(level["price"])), Decimal(str(level["size"])))
            for level in book.get("bids", [])
            if float(level["size"]) > 0
        ],
        key=lambda x: x[0],
        reverse=True,
    )
    if not bids:
        return None, []

    return float(bids[0][0]), bids


def simulate_bracket_exit_from_rest(
    num_shares: float, bid_levels: list,
) -> Tuple[float, float, list]:
    """
    Simulate selling num_shares against REST bid levels (walk bids descending).
    Returns (avg_exit_price, qty_exited, exit_walk_levels).
    """
    qty_remaining = Decimal(str(num_shares))
    total_value = Decimal("0")
    qty_exited = Decimal("0")
    exit_walk: list = []

    for bid_price, bid_size in bid_levels:
        if qty_remaining <= 0:
            break
        fill_qty = min(qty_remaining, bid_size)
        total_value += fill_qty * bid_price
        qty_exited += fill_qty
        qty_remaining -= fill_qty
        exit_walk.append({
            "price": float(bid_price),
            "qty": float(fill_qty),
            "cost": round(float(fill_qty * bid_price), 8),
        })

    avg_exit = float(total_value / qty_exited) if qty_exited > 0 else 0.0
    return avg_exit, float(qty_exited), exit_walk
