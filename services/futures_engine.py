"""Futures trading engine — position management, order matching, TP/SL/liquidation.

This engine runs in the WS feed service process. It:
1. Receives mark prices from Binance WS
2. Fills pending limit orders when price crosses
3. Monitors TP/SL and liquidation for open positions
4. Publishes events to Redis streams for the API to consume

Redis streams (consumed by FastAPI):
    stream:futures:fills        — limit order filled
    stream:futures:closes       — position closed (TP/SL/LIQ/manual)
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from config.futures_fees import (
    calc_taker_fee,
    calc_maker_fee,
    calc_initial_margin,
    calc_liquidation_price,
    MAINTENANCE_MARGIN_RATE,
)

log = logging.getLogger(__name__)


class FuturesEngine:
    """Thread-safe futures order/position engine.

    Maintains in-memory state of pending orders and open positions,
    synchronized with DB via the API layer.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._prices: dict[str, float] = {}  # symbol → mark price
        self._pending_orders: dict[int, dict] = {}  # order_id → order dict
        self._open_positions: dict[int, dict] = {}  # position_id → position dict
        self._events: list[dict] = []  # collected events to publish

    def update_price(self, symbol: str, price: float) -> list[dict]:
        """Update mark price and check all orders/positions.

        Returns list of events (fills, closes, liquidations).
        """
        events = []
        with self._lock:
            self._prices[symbol] = price
            self._events.clear()

            # Check pending limit orders
            self._check_limit_orders(symbol, price)

            # Check TP/SL/liquidation for open positions
            self._check_positions(symbol, price)

            events = list(self._events)
            self._events.clear()

        return events

    def get_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

    def get_all_prices(self) -> dict[str, float]:
        return dict(self._prices)

    def register_order(self, order: dict) -> None:
        """Register a pending limit order for monitoring."""
        with self._lock:
            self._pending_orders[order["id"]] = order
            log.info("Registered futures limit order #%d: %s %s %s @ %.2f",
                     order["id"], order["side"], order["size"], order["symbol"], order["limit_price"])

    def remove_order(self, order_id: int) -> None:
        with self._lock:
            self._pending_orders.pop(order_id, None)

    def register_position(self, pos: dict) -> None:
        """Register an open position for TP/SL/liquidation monitoring."""
        with self._lock:
            self._open_positions[pos["id"]] = pos
            log.info("Registered futures position #%d: %s %s %.4f @ %.2f (lev=%dx)",
                     pos["id"], pos["side"], pos["symbol"], pos["size"],
                     pos["entry_price"], pos["leverage"])

    def remove_position(self, position_id: int) -> None:
        with self._lock:
            self._open_positions.pop(position_id, None)

    def update_position_tp_sl(self, position_id: int, tp_price: float | None, sl_price: float | None) -> None:
        with self._lock:
            pos = self._open_positions.get(position_id)
            if pos:
                if tp_price is not None:
                    pos["tp_price"] = tp_price
                if sl_price is not None:
                    pos["sl_price"] = sl_price

    def _check_limit_orders(self, symbol: str, price: float) -> None:
        """Fill limit orders if price crosses their limit. Expire TTL orders."""
        now = time.time()
        to_remove = []
        for oid, order in self._pending_orders.items():
            # Check TTL expiry (for all symbols)
            expires_at = order.get("expires_at")
            if expires_at and now >= expires_at:
                self._events.append({
                    "type": "order_expire",
                    "order_id": oid,
                    "bot_name": order["bot_name"],
                    "symbol": order["symbol"],
                    "size": order["size"],
                    "limit_price": order["limit_price"],
                    "leverage": order["leverage"],
                })
                to_remove.append(oid)
                continue

            if order["symbol"] != symbol:
                continue

            filled = False
            if order["side"] == "LONG" and price <= order["limit_price"]:
                filled = True
            elif order["side"] == "SHORT" and price >= order["limit_price"]:
                filled = True

            if filled:
                self._events.append({
                    "type": "order_fill",
                    "order_id": oid,
                    "symbol": symbol,
                    "side": order["side"],
                    "size": order["size"],
                    "fill_price": price,
                    "leverage": order["leverage"],
                    "tp_price": order.get("tp_price"),
                    "sl_price": order.get("sl_price"),
                    "bot_name": order["bot_name"],
                    "exchange": order.get("exchange", "binance"),
                })
                to_remove.append(oid)

        for oid in to_remove:
            del self._pending_orders[oid]

    def _check_positions(self, symbol: str, price: float) -> None:
        """Check TP/SL/liquidation for open positions."""
        to_remove = []
        for pid, pos in self._open_positions.items():
            if pos["symbol"] != symbol:
                continue

            # Update unrealized PnL
            if pos["side"] == "LONG":
                pnl = (price - pos["entry_price"]) * pos["size"]
            else:
                pnl = (pos["entry_price"] - price) * pos["size"]
            pos["unrealized_pnl"] = round(pnl, 8)
            pos["mark_price"] = price

            trigger = None

            # Check liquidation
            liq_price = pos.get("liquidation_price")
            if liq_price:
                if pos["side"] == "LONG" and price <= liq_price:
                    trigger = "LIQ"
                elif pos["side"] == "SHORT" and price >= liq_price:
                    trigger = "LIQ"

            # Check SL (before TP — SL takes priority)
            if not trigger and pos.get("sl_price"):
                if pos["side"] == "LONG" and price <= pos["sl_price"]:
                    trigger = "SL"
                elif pos["side"] == "SHORT" and price >= pos["sl_price"]:
                    trigger = "SL"

            # Check TP
            if not trigger and pos.get("tp_price"):
                if pos["side"] == "LONG" and price >= pos["tp_price"]:
                    trigger = "TP"
                elif pos["side"] == "SHORT" and price <= pos["tp_price"]:
                    trigger = "TP"

            if trigger:
                # Calculate realized PnL
                exit_fee = calc_taker_fee(pos["size"], price, pos.get("exchange", "binance"))
                if trigger == "LIQ":
                    # Liquidation: lose entire margin
                    realized_pnl = -pos["margin"]
                else:
                    realized_pnl = round(pnl - exit_fee, 8)

                self._events.append({
                    "type": "position_close",
                    "position_id": pid,
                    "symbol": symbol,
                    "side": pos["side"],
                    "size": pos["size"],
                    "entry_price": pos["entry_price"],
                    "exit_price": price,
                    "exit_fee": exit_fee,
                    "realized_pnl": realized_pnl,
                    "trigger": trigger,
                    "bot_name": pos["bot_name"],
                    "leverage": pos["leverage"],
                })
                to_remove.append(pid)

        for pid in to_remove:
            del self._open_positions[pid]


# Singleton
futures_engine = FuturesEngine()
