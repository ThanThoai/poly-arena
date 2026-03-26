"""
Polymarket Market Snapshot Store — maintains real-time orderbook snapshots
built from WebSocket events.

Follows the design in docs/snapshot_service.md:
  - ``book`` events → full snapshot reset
  - ``price_change`` events → incremental updates
  - ``last_trade_price`` → last trade info
  - ``tick_size_change`` → tick size update
  - ``best_bid_ask`` → BBO shortcut
  - ``new_market`` → skeleton snapshot creation
  - ``market_resolved`` → final price, book cleared

Usage:
    store = SnapshotStore()
    store.handle_event(event_dict)          # dispatch any WS event
    snap = store.get_snapshot(token_id)     # read current state
    ob = store.get_orderbook(token_id, 10)  # top-10 bids/asks
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional

from sortedcontainers import SortedDict

logger = logging.getLogger(__name__)


# ── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class TradeInfo:
    price: Decimal
    size: Decimal
    side: str  # "BUY" | "SELL"
    fee_rate_bps: str = "0"
    timestamp: int = 0


@dataclass
class MarketSnapshot:
    token_id: str
    market_id: str = ""

    # Orderbook: bids descending, asks ascending
    # SortedDict with neg-key trick for bids (descending iteration)
    bids: SortedDict = field(default_factory=SortedDict)  # neg_price → size
    asks: SortedDict = field(default_factory=SortedDict)  # price → size

    last_trade: Optional[TradeInfo] = None
    tick_size: Decimal = Decimal("0.01")

    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    spread: Optional[Decimal] = None
    book_hash: Optional[str] = None

    last_updated: int = 0  # timestamp ms
    is_resolved: bool = False

    # Stats
    book_event_count: int = 0
    price_change_count: int = 0
    trade_count: int = 0

    @property
    def midpoint(self) -> Optional[Decimal]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return None

    @property
    def display_price(self) -> Optional[Decimal]:
        if self.spread is not None and self.spread > Decimal("0.04"):
            if self.last_trade:
                return self.last_trade.price
        return self.midpoint

    def get_bids(self, depth: int = 20) -> list[tuple[Decimal, Decimal]]:
        """Return top N bids as [(price, size), ...] highest-first."""
        result = []
        for neg_price, size in self.bids.items():
            if len(result) >= depth:
                break
            result.append((-neg_price, size))
        return result

    def get_asks(self, depth: int = 20) -> list[tuple[Decimal, Decimal]]:
        """Return top N asks as [(price, size), ...] lowest-first."""
        result = []
        for price, size in self.asks.items():
            if len(result) >= depth:
                break
            result.append((price, size))
        return result

    def _update_bbo(self) -> None:
        """Recalculate best bid/ask/spread from book."""
        self.best_bid = -self.bids.keys()[0] if self.bids else None
        self.best_ask = self.asks.keys()[0] if self.asks else None
        if self.best_bid is not None and self.best_ask is not None:
            self.spread = self.best_ask - self.best_bid
        else:
            self.spread = None


def _dec(value) -> Decimal:
    """Safely convert to Decimal."""
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


# ── Snapshot Store ───────────────────────────────────────────────────────────

class SnapshotStore:
    """
    Thread-safe store mapping token_id → MarketSnapshot.

    Processes Polymarket WebSocket events to maintain up-to-date orderbook
    snapshots.  All 7 event types from the Polymarket Market Channel are
    supported.
    """

    def __init__(self) -> None:
        self._store: dict[str, MarketSnapshot] = {}
        self._lock = threading.Lock()
        self._event_count = 0
        self._callbacks: list = []  # optional listeners

    def on_update(self, callback) -> None:
        """Register a callback(token_id, event_type) for snapshot updates."""
        self._callbacks.append(callback)

    def _emit(self, token_id: str, event_type: str) -> None:
        for cb in self._callbacks:
            try:
                cb(token_id, event_type)
            except Exception:
                pass

    # ── Event Dispatcher ─────────────────────────────────────────────────

    def handle_event(self, event: dict) -> None:
        """Dispatch a Polymarket WebSocket event to the appropriate handler."""
        event_type = event.get("event_type", "")
        self._event_count += 1

        if event_type == "book":
            self._handle_book(event)
        elif event_type == "price_change":
            self._handle_price_change(event)
        elif event_type == "last_trade_price":
            self._handle_last_trade_price(event)
        elif event_type == "tick_size_change":
            self._handle_tick_size_change(event)
        elif event_type == "best_bid_ask":
            self._handle_best_bid_ask(event)
        elif event_type == "new_market":
            self._handle_new_market(event)
        elif event_type == "market_resolved":
            self._handle_market_resolved(event)

    # ── 4.1 book — full snapshot reset ───────────────────────────────────

    def _handle_book(self, event: dict) -> None:
        token_id = event.get("asset_id", "")
        market_id = event.get("market", "")
        timestamp = int(event.get("timestamp", 0))

        with self._lock:
            snap = self._get_or_create(token_id, market_id)

            # Stale check (but book is always authoritative — accept if close)
            if snap.last_updated > 0 and timestamp < snap.last_updated - 1000:
                return

            # Clear & rebuild
            snap.bids.clear()
            snap.asks.clear()

            for level in event.get("bids", []):
                price = _dec(level.get("price", 0))
                size = _dec(level.get("size", 0))
                if size > 0:
                    snap.bids[-price] = size  # neg-key for descending

            for level in event.get("asks", []):
                price = _dec(level.get("price", 0))
                size = _dec(level.get("size", 0))
                if size > 0:
                    snap.asks[price] = size

            snap.book_hash = event.get("hash")
            snap.last_updated = timestamp
            snap.book_event_count += 1
            snap._update_bbo()

        self._emit(token_id, "book")

    # ── 4.2 price_change — incremental update ───────────────────────────

    def _handle_price_change(self, event: dict) -> None:
        market_id = event.get("market", "")
        timestamp = int(event.get("timestamp", 0))

        for change in event.get("changes", []):
            # change format: {"asset_id", "price", "size", "side"} or list [side, price, size]
            if isinstance(change, dict):
                token_id = change.get("asset_id", "")
                price = _dec(change.get("price", 0))
                size = _dec(change.get("size", 0))
                side = change.get("side", "")
            elif isinstance(change, list) and len(change) >= 3:
                # Alternate format: [side, price, size]
                side = str(change[0])
                price = _dec(change[1])
                size = _dec(change[2])
                token_id = event.get("asset_id", "")
            else:
                continue

            if not token_id:
                continue

            with self._lock:
                snap = self._store.get(token_id)
                if snap is None:
                    continue

                if snap.is_resolved:
                    continue

                # Apply change
                if side.upper() in ("BUY", "BID"):
                    if size == 0:
                        snap.bids.pop(-price, None)
                    else:
                        snap.bids[-price] = size
                elif side.upper() in ("SELL", "ASK"):
                    if size == 0:
                        snap.asks.pop(price, None)
                    else:
                        snap.asks[price] = size

                snap.last_updated = timestamp
                snap.price_change_count += 1
                snap._update_bbo()

            self._emit(token_id, "price_change")

    # ── 4.3 last_trade_price ─────────────────────────────────────────────

    def _handle_last_trade_price(self, event: dict) -> None:
        token_id = event.get("asset_id", "")
        timestamp = int(event.get("timestamp", 0))

        with self._lock:
            snap = self._store.get(token_id)
            if snap is None:
                return

            snap.last_trade = TradeInfo(
                price=_dec(event.get("price", 0)),
                size=_dec(event.get("size", 0)),
                side=event.get("side", ""),
                fee_rate_bps=event.get("fee_rate_bps", "0"),
                timestamp=timestamp,
            )
            snap.last_updated = timestamp
            snap.trade_count += 1

        self._emit(token_id, "last_trade_price")

    # ── 4.4 tick_size_change ─────────────────────────────────────────────

    def _handle_tick_size_change(self, event: dict) -> None:
        token_id = event.get("asset_id", "")
        with self._lock:
            snap = self._store.get(token_id)
            if snap is None:
                return
            snap.tick_size = _dec(event.get("new_tick_size", "0.01"))
            snap.last_updated = int(event.get("timestamp", 0))

        self._emit(token_id, "tick_size_change")

    # ── 4.5 best_bid_ask ────────────────────────────────────────────────

    def _handle_best_bid_ask(self, event: dict) -> None:
        token_id = event.get("asset_id", "")
        with self._lock:
            snap = self._store.get(token_id)
            if snap is None:
                return
            snap.best_bid = _dec(event.get("best_bid", 0))
            snap.best_ask = _dec(event.get("best_ask", 0))
            snap.spread = _dec(event.get("spread", 0))
            snap.last_updated = int(event.get("timestamp", 0))

        self._emit(token_id, "best_bid_ask")

    # ── 4.6 new_market ──────────────────────────────────────────────────

    def _handle_new_market(self, event: dict) -> None:
        market_id = event.get("market", "")
        assets = event.get("assets_ids", [])
        tick_size = _dec(event.get("order_price_min_tick_size", "0.01"))

        with self._lock:
            for token_id in assets:
                if token_id not in self._store:
                    self._store[token_id] = MarketSnapshot(
                        token_id=token_id,
                        market_id=market_id,
                        tick_size=tick_size,
                    )

        for token_id in assets:
            self._emit(token_id, "new_market")

    # ── 4.7 market_resolved ─────────────────────────────────────────────

    def _handle_market_resolved(self, event: dict) -> None:
        winning_token = event.get("winning_asset_id", "")
        all_tokens = event.get("assets_ids", [])
        timestamp = int(event.get("timestamp", 0))

        with self._lock:
            for token_id in all_tokens:
                snap = self._store.get(token_id)
                if snap is None:
                    continue

                snap.is_resolved = True
                snap.bids.clear()
                snap.asks.clear()

                if token_id == winning_token:
                    snap.best_bid = Decimal("1.00")
                    snap.best_ask = Decimal("1.00")
                else:
                    snap.best_bid = Decimal("0.00")
                    snap.best_ask = Decimal("0.00")

                snap.spread = Decimal("0")
                snap.last_updated = timestamp

        for token_id in all_tokens:
            self._emit(token_id, "market_resolved")

    # ── Query API ────────────────────────────────────────────────────────

    def get_snapshot(self, token_id: str) -> Optional[MarketSnapshot]:
        with self._lock:
            return self._store.get(token_id)

    def get_orderbook(
        self, token_id: str, depth: int = 10,
    ) -> Optional[dict]:
        with self._lock:
            snap = self._store.get(token_id)
            if snap is None:
                return None
            return {
                "bids": snap.get_bids(depth),
                "asks": snap.get_asks(depth),
                "best_bid": snap.best_bid,
                "best_ask": snap.best_ask,
                "spread": snap.spread,
                "last_updated": snap.last_updated,
            }

    def get_midpoint(self, token_id: str) -> Optional[Decimal]:
        with self._lock:
            snap = self._store.get(token_id)
            if snap is None:
                return None
            return snap.midpoint

    def get_display_price(self, token_id: str) -> Optional[Decimal]:
        with self._lock:
            snap = self._store.get(token_id)
            if snap is None:
                return None
            return snap.display_price

    def list_tokens(self) -> list[str]:
        with self._lock:
            return list(self._store.keys())

    @property
    def total_events(self) -> int:
        return self._event_count

    # ── Internal ─────────────────────────────────────────────────────────

    def _get_or_create(self, token_id: str, market_id: str = "") -> MarketSnapshot:
        """Get or create a snapshot — caller must hold self._lock."""
        if token_id not in self._store:
            self._store[token_id] = MarketSnapshot(
                token_id=token_id,
                market_id=market_id,
            )
        snap = self._store[token_id]
        if market_id and not snap.market_id:
            snap.market_id = market_id
        return snap
