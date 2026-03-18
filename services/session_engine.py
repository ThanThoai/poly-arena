"""
SessionEngine — Per-session isolated unit for the multi-session matching architecture.

Each candle session (e.g. "BTC:M5:1709313000") becomes an isolated unit with:
  - Its own lifecycle state (PREFETCH → ACTIVE → SETTLING → ARCHIVED)
  - Grouped orderbooks (UP + DOWN tokens)
  - Its own Redis queue key

SessionEngine delegates matching to the existing ShadowOrderbook — no matching
logic is duplicated here.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional

from services.matching_engine import (
    ShadowOrderbook,
    SimulatedOrder,
    BracketFillResult,
    OrderSide,
    OrderStatus,
    OrderStateChangeEvent,
)

logger = logging.getLogger(__name__)


class SessionState(str, Enum):
    PREFETCH = "PREFETCH"   # Token resolved, book created, pre-populated
    ACTIVE   = "ACTIVE"     # Accepting orders, matching active
    SETTLING = "SETTLING"   # No new orders, settlement running
    ARCHIVED = "ARCHIVED"   # Books destroyed, memory freed


class SessionEngine:
    """
    Per-session isolated unit grouping UP + DOWN orderbooks.

    Thread-safety: mutations to lifecycle state are protected by ``_lock``.
    Book-level operations delegate to ShadowOrderbook which has its own lock.
    """

    def __init__(self, session_id: str, tokens: dict[str, str]) -> None:
        """
        Args:
            session_id: e.g. "BTC:M5:1709313000"
            tokens: {"UP": "0xabc...", "DOWN": "0xdef..."}
        """
        self.session_id = session_id
        self._lock = threading.Lock()
        self.state = SessionState.PREFETCH
        self.tokens = dict(tokens)                               # direction → token_id
        self._token_to_dir = {v: k for k, v in tokens.items()}   # token_id → direction
        self.books: dict[str, ShadowOrderbook] = {}              # direction → book

        # Create a ShadowOrderbook per direction
        for direction, token_id in tokens.items():
            self.books[direction] = ShadowOrderbook(token_id)

        self.queue_key = f"queue:orders:{session_id}"

        # Parse session_id → symbol, timeframe, candle_open
        parts = session_id.split(":")
        self.symbol = parts[0] if len(parts) > 0 else ""
        self.timeframe = parts[1] if len(parts) > 1 else ""
        self.candle_open = int(parts[2]) if len(parts) > 2 else 0

        # When False, WS events update book data but skip matching/bracket monitoring.
        # REST poller handles matching instead.
        self.ws_matching_enabled = True

        # Lifecycle timestamps
        self.created_at = datetime.now(timezone.utc)
        self.activated_at: Optional[datetime] = None
        self.settling_at: Optional[datetime] = None
        self.archived_at: Optional[datetime] = None

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def token_ids(self) -> list[str]:
        return list(self.tokens.values())

    def has_token(self, token_id: str) -> bool:
        return token_id in self._token_to_dir

    def get_book_for_token(self, token_id: str) -> Optional[ShadowOrderbook]:
        direction = self._token_to_dir.get(token_id)
        if direction is None:
            return None
        return self.books.get(direction)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def transition(self, new_state: SessionState) -> None:
        with self._lock:
            old_state = self.state
            if old_state == SessionState.ARCHIVED:
                logger.warning(
                    "Session %s: cannot transition from ARCHIVED to %s",
                    self.session_id, new_state,
                )
                return

            self.state = new_state

            if new_state == SessionState.ACTIVE:
                self.activated_at = datetime.now(timezone.utc)
                logger.info("Session %s: %s → ACTIVE", self.session_id, old_state)

            elif new_state == SessionState.SETTLING:
                self.settling_at = datetime.now(timezone.utc)
                # Expire all books — cancels pending orders
                for direction, book in self.books.items():
                    n = book.expire_book()
                    if n:
                        logger.info(
                            "Session %s: expired %d order(s) in %s book on SETTLING",
                            self.session_id, n, direction,
                        )
                logger.info("Session %s: %s → SETTLING", self.session_id, old_state)

            elif new_state == SessionState.ARCHIVED:
                self.archived_at = datetime.now(timezone.utc)
                self.books.clear()
                self.tokens.clear()
                self._token_to_dir.clear()
                logger.info("Session %s: %s → ARCHIVED (memory freed)", self.session_id, old_state)

            else:
                logger.info("Session %s: %s → %s", self.session_id, old_state, new_state)

    # ── WS Event dispatch ─────────────────────────────────────────────────────

    def dispatch_ws_event(self, event: dict) -> None:
        """
        Route a WS event to the correct book within this session.

        Handles: book, price_change, best_bid_ask, last_trade_price.
        Rejects events if ARCHIVED.
        """
        if self.state == SessionState.ARCHIVED:
            return

        asset_id = event.get("asset_id", "")
        book = self.get_book_for_token(asset_id)
        if book is None:
            return

        etype = event.get("event_type", "")

        if etype == "book":
            book.apply_snapshot(event.get("bids", []), event.get("asks", []))
            if self.ws_matching_enabled:
                book.run_matching()
                book.monitor_bracket_orders()

        elif etype == "price_change":
            book.apply_changes(event.get("changes", []))
            if self.ws_matching_enabled:
                book.run_matching()
                book.monitor_bracket_orders()

        elif etype == "best_bid_ask":
            self._apply_best_bid_ask(book, asset_id, event)

        elif etype == "last_trade_price":
            book.record_trade(
                price=event.get("price", "0"),
                size=event.get("size", "0"),
                side=event.get("side", ""),
            )
            if self.ws_matching_enabled:
                book.monitor_bracket_orders()

    def _apply_best_bid_ask(self, book: ShadowOrderbook, asset_id: str, event: dict) -> None:
        """
        Handle best_bid_ask event with bid/ask size inference.

        Mirrors MatchingEngine._handle_best_bid_ask logic.
        """
        raw_bid = event.get("bid")
        raw_ask = event.get("ask")
        if raw_bid or raw_ask:
            changes = []
            if raw_bid and event.get("bid_size"):
                changes.append({"side": "bid", "price": raw_bid, "size": event["bid_size"]})
            elif raw_bid:
                with book._lock:
                    if book.bids:
                        best_bid_price, best_bid_size = book.bids.peekitem(-1)
                        new_bid = Decimal(str(raw_bid))
                        if new_bid > best_bid_price and best_bid_size > 0:
                            changes.append({
                                "side": "bid",
                                "price": raw_bid,
                                "size": str(best_bid_size),
                            })

            if raw_ask and event.get("ask_size"):
                changes.append({"side": "ask", "price": raw_ask, "size": event["ask_size"]})
            elif raw_ask:
                with book._lock:
                    if book.asks:
                        best_ask_price, best_ask_size = book.asks.peekitem(0)
                        new_ask = Decimal(str(raw_ask))
                        if new_ask < best_ask_price and best_ask_size > 0:
                            changes.append({
                                "side": "ask",
                                "price": raw_ask,
                                "size": str(best_ask_size),
                            })

            if changes:
                book.apply_changes(changes)
                if self.ws_matching_enabled:
                    book.run_matching()
            else:
                with book._lock:
                    book.last_update = datetime.now(timezone.utc)

        # Trigger bracket monitoring
        if self.ws_matching_enabled:
            book.monitor_bracket_orders()

    # ── REST poller helpers ─────────────────────────────────────────────────────

    def try_match_pending(self, book: ShadowOrderbook) -> None:
        """Run matching + bracket monitoring on a specific book (called by REST poller)."""
        book.run_matching()
        book.monitor_bracket_orders()

    def get_token_id(self, direction: str) -> Optional[str]:
        """Return the token_id for a given direction (UP/DOWN)."""
        return self.tokens.get(direction)

    # ── Order placement ───────────────────────────────────────────────────────

    def place_virtual_order(
        self,
        token_id: str,
        side: OrderSide,
        price: Decimal,
        quantity: Decimal,
        tp_price: Optional[Decimal] = None,
        sl_price: Optional[Decimal] = None,
        timeframe: Optional[str] = None,
        ttl_seconds: Optional[float] = None,
        on_bracket_exit: Optional[Callable] = None,
        order_type: str = "LIMIT",
        max_slippage: Optional[Decimal] = None,
        max_cost: Optional[Decimal] = None,
        order_queued_at: Optional[datetime] = None,
    ) -> tuple[SimulatedOrder, list[BracketFillResult]]:
        """Place a virtual order — delegates to the correct ShadowOrderbook."""
        if self.state in (SessionState.SETTLING, SessionState.ARCHIVED):
            raise ValueError(
                f"Cannot place order on session {self.session_id} in state {self.state}"
            )
        book = self.get_book_for_token(token_id)
        if book is None:
            raise ValueError(f"Token {token_id[:16]} not in session {self.session_id}")
        if book._expired:
            raise ValueError(f"Book expired for token {token_id[:16]} in session {self.session_id}")
        return book.place_virtual_order(
            side, price, quantity, tp_price, sl_price, timeframe, ttl_seconds,
            on_bracket_exit, order_type, max_slippage, max_cost, order_queued_at,
        )

    def place_prefilled_bracket_order(
        self,
        token_id: str,
        side: OrderSide,
        avg_entry_price: Decimal,
        filled: Decimal,
        tp_price: Optional[Decimal] = None,
        sl_price: Optional[Decimal] = None,
        on_bracket_exit: Optional[Callable] = None,
        expire_at: Optional[datetime] = None,
    ) -> tuple[SimulatedOrder, list[BracketFillResult]]:
        """Register a pre-filled bracket order for monitoring."""
        if self.state == SessionState.ARCHIVED:
            raise ValueError(
                f"Cannot place prefilled order on ARCHIVED session {self.session_id}"
            )
        book = self.get_book_for_token(token_id)
        if book is None:
            raise ValueError(f"Token {token_id[:16]} not in session {self.session_id}")
        if book._expired:
            raise ValueError(f"Book expired for token {token_id[:16]} in session {self.session_id}")
        return book.place_prefilled_bracket_order(
            side, avg_entry_price, filled, tp_price, sl_price, on_bracket_exit,
            expire_at,
        )

    # ── Query ─────────────────────────────────────────────────────────────────

    def best_ask(self, token_id: str) -> Optional[float]:
        book = self.get_book_for_token(token_id)
        if book is None or book.is_stale():
            return None
        ask = book.best_ask()
        return float(ask) if ask is not None else None

    def best_bid(self, token_id: str) -> Optional[float]:
        book = self.get_book_for_token(token_id)
        if book is None or book.is_stale():
            return None
        bid = book.best_bid()
        return float(bid) if bid is not None else None

    def expire_all_pending(self) -> int:
        """Expire TTL-elapsed orders across all books in this session."""
        total = 0
        for book in self.books.values():
            state_events: list[OrderStateChangeEvent] = []
            with book._lock:
                expired = book._expire_pending_orders()
                if expired:
                    total += len(expired)
                    state_events = book.collect_state_changes()
            book._fire_state_change_callbacks(state_events)
        return total

    def __repr__(self) -> str:
        return (
            f"SessionEngine(id={self.session_id!r}, state={self.state.value}, "
            f"tokens={len(self.tokens)})"
        )
