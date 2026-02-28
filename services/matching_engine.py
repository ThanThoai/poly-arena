"""
Shadow Matching Engine for Polymarket binary-options orderbooks.

Implements the v2 specification from docs/maching_engine_v2.md including:
  - Single Condition Policy: each order has at most one condition (TP or SL)
  - Section 2: SimulatedOrder with Bracket Order (TP/SL) fields
  - Section 3: WebSocket event handlers (book, price_change, best_bid_ask,
               last_trade_price, market_resolved)
  - Section 4: Core matching algorithm with shadow liquidity deduction
  - Section 5: Bracket Order TP/SL monitoring with single-condition logic
               and realistic slippage through multiple bid levels
  - Section 6: Decimal precision, partial-fill awareness, slippage handling

Architecture
────────────
    WS events ──► MatchingEngine.dispatch_event()
                       │
                       ├── book / price_change ──► ShadowOrderbook snapshot/delta
                       │                           └── run_matching() [Sec 4]
                       │
                       ├── best_bid_ask         ──► _monitor_bracket_orders() [Sec 5]
                       └── last_trade_price     ──► record_trade()
                                                    └── _monitor_bracket_orders() [Sec 5]

    Router ──► engine.best_ask(token_id)   (µs lookup, no lock contention)
    Router ──► engine.place_virtual_order() (with optional TP/SL)

Thread safety
─────────────
    Each ShadowOrderbook has its own threading.Lock.
    MatchingEngine registry uses a separate lock.
    FastAPI sync routes and asyncio WS feed are both safe.
"""

from __future__ import annotations

import logging
import math
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional

from config.timing import (
    ME_DUST_THRESHOLD as _DUST_THRESHOLD,
    ME_DEFAULT_SLIPPAGE as _DEFAULT_SLIPPAGE,
    ME_BOOK_STALE_MAX_S,
    ME_BOOK_STALE_DEFAULT_S,
    ME_CLEANUP_INTERVAL,
    TF_SECONDS as _TF_SECONDS,
)

logger = logging.getLogger(__name__)


# ── Timeframe helpers ─────────────────────────────────────────────────────────


def candle_expire_at(timeframe: str, now: Optional[datetime] = None) -> datetime:
    """
    Return the UTC datetime of the **next candle close** for the given timeframe.

    The expiry is always aligned to the candle grid, not offset from the current
    moment.  Examples (UTC):

        timeframe=M5,  now=12:12:00  →  expire_at=12:15:00
        timeframe=M5,  now=12:12:45  →  expire_at=12:15:00
        timeframe=M5,  now=12:15:00  →  expire_at=12:20:00  (already on boundary)
        timeframe=M1,  now=12:12:30  →  expire_at=12:13:00
        timeframe=H1,  now=12:12:00  →  expire_at=13:00:00

    Args:
        timeframe: One of M1, M5, M15, M30, H1, H4, D1.
        now:       Reference time (UTC).  Defaults to datetime.now(timezone.utc).

    Raises:
        ValueError: If timeframe is not recognised.
    """
    tf = timeframe.upper()
    period_s = _TF_SECONDS.get(tf)
    if period_s is None:
        raise ValueError(
            f"Unknown timeframe '{timeframe}'. Supported: {list(_TF_SECONDS)}"
        )
    if now is None:
        now = datetime.now(timezone.utc)

    now_ts = now.timestamp()
    # Floor to the start of the current period, then add one period
    period_start_ts = math.floor(now_ts / period_s) * period_s
    expire_ts       = period_start_ts + period_s
    return datetime.fromtimestamp(expire_ts, tz=timezone.utc)


# ── Enums ────────────────────────────────────────────────────────────────────


class OrderSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    PENDING  = "PENDING"
    PARTIAL  = "PARTIAL"
    FILLED   = "FILLED"
    CANCELED = "CANCELED"


# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class SimulatedOrder:
    """
    Virtual limit order placed against the shadow orderbook.
    Supports Bracket Order (TP/SL) parameters per spec Section 2.1.
    """
    order_id:  str
    side:      OrderSide
    price:     Decimal          # limit price (ignored for MARKET)
    quantity:  Decimal          # total requested size
    order_type: str             = "LIMIT"   # "MARKET" or "LIMIT"
    max_slippage: Optional[Decimal] = None  # None = _DEFAULT_SLIPPAGE for MARKET
    max_cost:  Optional[Decimal] = None   # cost cap for MARKET BUY (amount from user)
    filled:    Decimal          = field(default_factory=lambda: Decimal("0"))
    status:    OrderStatus      = OrderStatus.PENDING
    created_at: datetime        = field(default_factory=lambda: datetime.now(timezone.utc))
    expire_at:  Optional[datetime] = None   # None = never expires

    # ── Entry tracking (weighted avg across multiple fill levels) ────────────
    _entry_cost: Decimal        = field(default_factory=lambda: Decimal("0"))
    # Slippage reference price — locked in at first match when book has data
    _slippage_ref_price: Optional[Decimal] = field(default=None, compare=False, repr=False)
    # Per-level fill details: list of (price, qty) tuples accumulated during matching
    _fill_levels: list          = field(default_factory=list, compare=False, repr=False)
    # cumulative cost = Σ(fill_qty × fill_price) — avg_entry_price = _entry_cost / filled

    # ── Bracket Order fields (Section 2.1) ───────────────────────────────────
    tp_price:        Optional[Decimal] = None   # Take Profit trigger
    sl_price:        Optional[Decimal] = None   # Stop Loss trigger
    position_closed: bool              = False  # True once TP or SL fires

    # ── TP/SL execution record ───────────────────────────────────────────────
    exit_price:    Optional[Decimal] = None  # avg fill price on exit
    exit_trigger:  Optional[str]     = None  # "TP" | "SL"
    exit_filled:   Optional[Decimal] = None  # shares actually exited

    # ── Write-back callback ──────────────────────────────────────────────────
    # Called (outside the book lock) after every bracket exit.
    # Signature: callback(result: BracketFillResult) -> None
    _on_bracket_exit: Optional[Callable] = field(
        default=None, compare=False, repr=False,
    )

    @property
    def remaining_qty(self) -> Decimal:
        return self.quantity - self.filled

    @property
    def avg_entry_price(self) -> Optional[Decimal]:
        """Weighted average entry price across all fill levels."""
        if self.filled > 0:
            return self._entry_cost / self.filled
        return None

    @property
    def has_bracket(self) -> bool:
        return self.tp_price is not None or self.sl_price is not None

    @property
    def is_eligible_for_bracket(self) -> bool:
        """True when this order has TP/SL set, has been at least partially
        filled, and the position has not yet been closed.

        Includes CANCELED orders (MARKET IOC remainder or LIMIT TTL expiry)
        that still have a filled portion — spec Workflow E monitors any
        order with filled > 0, regardless of status.
        """
        return (
            self.side == OrderSide.BUY
            and self.has_bracket
            and self.filled > 0
            and not self.position_closed
            and self.status in (OrderStatus.FILLED, OrderStatus.PARTIAL, OrderStatus.CANCELED)
        )

    @property
    def is_expired(self) -> bool:
        """True if expire_at is set and current time has passed it."""
        return self.expire_at is not None and datetime.now(timezone.utc) >= self.expire_at

    def _update_status(self) -> None:
        if self.filled >= self.quantity:
            self.status = OrderStatus.FILLED
        elif self.filled > 0:
            self.status = OrderStatus.PARTIAL

    def calculate_profit(self) -> Optional[Decimal]:
        """
        Calculate realized P&L based on actual entry and exit prices.

        For bracket exits (TP/SL):
            profit = (exit_filled × avg_exit_price) - (exit_filled × avg_entry_price)

        Returns None if no exit has been executed or no fills recorded.
        Use calculate_unrealized_pnl() for open positions without SL.
        """
        if self.exit_filled is None or self.exit_filled <= 0:
            return None
        avg_entry = self.avg_entry_price
        if avg_entry is None or self.exit_price is None:
            return None
        return (self.exit_filled * self.exit_price) - (self.exit_filled * avg_entry)

    def calculate_unrealized_pnl(self, current_bid: Decimal) -> Optional[Decimal]:
        """
        Calculate unrealized P&L for an open position at current market bid.

        Used when:
        - No SL is set and TP has not fired yet (position still open)
        - Partially exited via TP/SL, remainder still open
        - Binary option expires without TP/SL trigger

        Formula:
            unrealized = open_qty × current_bid - open_qty × avg_entry_price

        Where open_qty = filled - (exit_filled or 0)
        """
        if self.filled <= 0:
            return None
        avg_entry = self.avg_entry_price
        if avg_entry is None:
            return None
        already_exited = self.exit_filled or Decimal("0")
        open_qty = self.filled - already_exited
        if open_qty <= 0:
            return None
        return (open_qty * current_bid) - (open_qty * avg_entry)

    def total_pnl(self, current_bid: Optional[Decimal] = None) -> Optional[Decimal]:
        """
        Total P&L combining realized (TP/SL exits) + unrealized (open remainder).

        - If position fully closed: returns realized profit only.
        - If position partially closed: realized + unrealized on remainder.
        - If position never closed and no SL: unrealized at current_bid (LOSS if bid < entry).
        - Returns None if order has no fills or current_bid required but not provided.
        """
        realized   = self.calculate_profit() or Decimal("0")
        unrealized = Decimal("0")

        if not self.position_closed and self.filled > 0:
            if current_bid is None:
                return None  # can't compute open portion without market price
            unreal = self.calculate_unrealized_pnl(current_bid)
            unrealized = unreal if unreal is not None else Decimal("0")

        total = realized + unrealized
        # Return None only if there's truly nothing computed
        if self.exit_filled is None and unrealized == 0:
            return None
        return total


@dataclass
class LastTrade:
    price:     Decimal
    size:      Decimal
    side:      str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class OrderStateChangeEvent:
    """Emitted when a virtual order's fill or status changes."""
    order_id: str
    event_type: str                    # "FILL" | "CANCEL"
    filled: Decimal
    avg_entry_price: Optional[Decimal]
    status: OrderStatus
    cancel_reason: Optional[str] = None
    fill_levels: list = field(default_factory=list)  # [(price, qty)] new fills since last report


@dataclass
class BracketFillResult:
    """Records the outcome of a TP or SL execution."""
    order_id:     str
    trigger:      str            # "TP" | "SL"
    trigger_price: Decimal       # the TP/SL level that was hit
    market_bid:   Decimal        # best bid at time of trigger
    qty_to_close: Decimal        # shares we wanted to exit
    qty_exited:   Decimal        # shares actually filled (slippage)
    avg_exit_price: Decimal      # weighted avg across bid levels consumed
    levels_consumed: int         # how many bid price levels were eaten
    fill_levels:  list = field(default_factory=list)  # [(price, qty)] per-level exit fills
    timestamp:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ── Shadow Orderbook ─────────────────────────────────────────────────────────


class ShadowOrderbook:
    """
    Thread-safe shadow orderbook for a single Polymarket token.

    Maintains the full bids/asks dict updated by WebSocket events,
    runs the matching algorithm for virtual orders, and monitors
    bracket (TP/SL) conditions on every price tick.

    Thread-safety model
    ───────────────────
    A single per-book ``threading.Lock`` (``self._lock``) serializes **all**
    mutations to bids, asks, and virtual orders.  Per-order locks are not
    needed because every order mutation (matching, expiry, bracket exit)
    occurs within the same book-level critical section.

    Callbacks (``_on_bracket_exit``, state-change callbacks) are fired
    **outside** the lock to avoid blocking I/O operations (Redis publish,
    DB writes) while holding the lock.
    """

    def __init__(self, token_id: str) -> None:
        self.token_id = token_id
        self._lock    = threading.Lock()
        self.bids:  dict[Decimal, Decimal] = {}  # price → size, highest first
        self.asks:  dict[Decimal, Decimal] = {}  # price → size, lowest first
        self.last_trade:  Optional[LastTrade]        = None
        self.last_update: Optional[datetime]         = None
        self._virtual_orders: list[SimulatedOrder]   = []
        self._bracket_log:    list[BracketFillResult] = []
        self._cleanup_counter: int = 0
        self._expired = False  # set True when token rotates — blocks new orders & matching
        # ── Centralized state-change tracking ─────────────────────────────
        self._last_reported: dict[str, tuple[Decimal, OrderStatus]] = {}
        self._state_change_callbacks: list[Callable] = []

    # ── Snapshot / delta handlers ────────────────────────────────────────────

    def apply_snapshot(self, bids: list[dict], asks: list[dict]) -> None:
        """Handle a `book` event — full orderbook replacement (spec 3.1)."""
        with self._lock:
            self.bids.clear()
            self.asks.clear()
            for entry in bids:
                price = Decimal(str(entry["price"]))
                size  = Decimal(str(entry["size"]))
                if size > 0:
                    self.bids[price] = size
            for entry in asks:
                price = Decimal(str(entry["price"]))
                size  = Decimal(str(entry["size"]))
                if size > 0:
                    self.asks[price] = size
            self.last_update = datetime.now(timezone.utc)
        logger.debug(
            "Snapshot %s: %d bids, %d asks",
            self.token_id[:16], len(self.bids), len(self.asks),
        )

    def apply_changes(self, changes: list[dict]) -> None:
        """Handle a `price_change` event — delta updates (spec 3.2)."""
        with self._lock:
            for ch in changes:
                side   = ch.get("side", "").lower()
                price  = Decimal(str(ch["price"]))
                size   = Decimal(str(ch["size"]))
                target = self.bids if side == "bid" else self.asks
                if size <= 0:
                    target.pop(price, None)
                else:
                    target[price] = size
            self.last_update = datetime.now(timezone.utc)

    def record_trade(self, price: str, size: str, side: str) -> None:
        """Handle a `last_trade_price` event — record + trigger bracket check."""
        with self._lock:
            self.last_trade = LastTrade(
                price=Decimal(str(price)),
                size=Decimal(str(size)),
                side=side,
            )

    # ── Price queries ────────────────────────────────────────────────────────

    def best_ask(self) -> Optional[Decimal]:
        with self._lock:
            return min(self.asks.keys()) if self.asks else None

    def best_bid(self) -> Optional[Decimal]:
        with self._lock:
            return max(self.bids.keys()) if self.bids else None

    def spread(self) -> Optional[Decimal]:
        ba = self.best_ask()
        bb = self.best_bid()
        return (ba - bb) if ba is not None and bb is not None else None

    def depth(self, side: str = "ask", levels: int = 5) -> list[tuple[Decimal, Decimal]]:
        """Top N [(price, size)] levels."""
        with self._lock:
            book    = self.asks if side == "ask" else self.bids
            reverse = side == "bid"
            return sorted(book.items(), key=lambda x: x[0], reverse=reverse)[:levels]

    def total_liquidity(self, side: str = "ask", within_pct: float = 0.05) -> Decimal:
        """Total size within `within_pct` of best price."""
        with self._lock:
            book = self.asks if side == "ask" else self.bids
            if not book:
                return Decimal("0")
            if side == "ask":
                best = min(book.keys())
                threshold = best * (1 + Decimal(str(within_pct)))
                return sum(sz for p, sz in book.items() if p <= threshold)
            else:
                best = max(book.keys())
                threshold = best * (1 - Decimal(str(within_pct)))
                return sum(sz for p, sz in book.items() if p >= threshold)

    def is_stale(self, max_age_s: float = ME_BOOK_STALE_DEFAULT_S) -> bool:
        if self.last_update is None:
            return True
        return (datetime.now(timezone.utc) - self.last_update).total_seconds() > max_age_s

    def expire_book(self) -> int:
        """Mark this book as expired (token rotated).

        Cancels all pending LIMIT orders and prevents new orders from being
        placed or matched.  Fires state-change callbacks so the API can
        refund balances.  Returns the number of orders cancelled.
        """
        cancelled = 0
        state_events: list[OrderStateChangeEvent] = []
        with self._lock:
            self._expired = True
            for order in self._virtual_orders:
                if order.status not in (OrderStatus.FILLED, OrderStatus.CANCELED):
                    order.status = OrderStatus.CANCELED
                    cancelled += 1
            self.bids.clear()
            self.asks.clear()
            state_events = self.collect_state_changes()
        # Fire callbacks outside lock
        self._fire_state_change_callbacks(state_events)
        logger.info(
            "Book EXPIRED: token=%s, cancelled %d pending order(s)",
            self.token_id[:16], cancelled,
        )
        return cancelled

    # ── Virtual order placement ──────────────────────────────────────────────

    def place_virtual_order(
        self,
        side:              OrderSide,
        price:             Decimal,
        quantity:          Decimal,
        tp_price:          Optional[Decimal]  = None,
        sl_price:          Optional[Decimal]  = None,
        timeframe:         Optional[str]      = None,
        ttl_seconds:       Optional[float]    = None,
        on_bracket_exit:   Optional[Callable] = None,
        order_type:        str                = "LIMIT",
        max_slippage:      Optional[Decimal]  = None,
        max_cost:          Optional[Decimal]  = None,
    ) -> tuple[SimulatedOrder, list[BracketFillResult]]:
        """
        Create a virtual order, immediately try to match it,
        and attach optional TP/SL bracket parameters (spec 2.1).

        MARKET orders use IOC (Immediate-Or-Cancel) semantics: any remaining
        quantity after sweeping the available book is immediately canceled.
        LIMIT orders remain active until filled or expired.

        Returns:
            Tuple of (order, bracket_results).  ``bracket_results`` contains any
            TP/SL exits that fired immediately after fill.  The bracket exit
            **callbacks are NOT fired** — the caller is responsible for invoking
            them AFTER publishing the fill event so that the DB consumer always
            sees the fill before the bracket exit.

        Expiry (one of, in priority order):
          - ``ttl_seconds`` — raw offset from now (user-specified TTL).
          - ``timeframe``   — align to next candle close on the grid.
                              e.g. timeframe='M5', now=12:12 → expire_at=12:15
          - both None       — Good-Till-Canceled, never expires automatically.

        Args:
            order_type:       "MARKET" or "LIMIT" (default).
            timeframe:        Candle timeframe string (M5/M15/H1).
                              expire_at is aligned to the candle grid.
            ttl_seconds:      Raw seconds from now. Takes priority over timeframe.
            on_bracket_exit:  Optional callback fired (outside lock) after every
                              TP/SL/FORCE_CLOSE exit.
                              Signature: callback(result: BracketFillResult) -> None
            max_slippage:     Maximum slippage for MARKET orders as a fraction
                              (e.g. 0.05 = 5%). None = _DEFAULT_SLIPPAGE (10%).
                              Ignored for LIMIT orders.
        """
        if ttl_seconds is not None:
            expire_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        elif timeframe is not None:
            expire_at = candle_expire_at(timeframe)
        else:
            expire_at = None

        order = SimulatedOrder(
            order_id=str(uuid.uuid4()),
            side=side,
            price=price,
            quantity=quantity,
            order_type=order_type,
            max_slippage=max_slippage,
            max_cost=max_cost,
            tp_price=tp_price,
            sl_price=sl_price,
            expire_at=expire_at,
            _on_bracket_exit=on_bracket_exit,
        )
        immediate_bracket_results: list[BracketFillResult] = []
        with self._lock:
            self._virtual_orders.append(order)
            # Snapshot book state BEFORE matching so we know if
            # liquidity existed on the relevant side.
            had_liquidity = (
                bool(self.asks) if order.side == OrderSide.BUY else bool(self.bids)
            )
            self._match_order(order)

            # MARKET = IOC: cancel remainder if not fully filled (spec 3.1).
            # Only apply IOC when the book had liquidity (or the order
            # actually got fills).  If the book was empty (WS feed hasn't
            # arrived yet), leave the order PENDING so run_matching() can
            # retry once the book is populated.
            if order_type == "MARKET" and order.status != OrderStatus.FILLED:
                if had_liquidity or order.filled > 0:
                    order.status = OrderStatus.CANCELED
                    logger.info(
                        "MARKET IOC cancel: id=%s filled=%s/%s avg=%s "
                        "had_liquidity=%s",
                        order.order_id[:12], order.filled, order.quantity,
                        order.avg_entry_price, had_liquidity,
                    )
                else:
                    logger.info(
                        "MARKET order PENDING (empty book): id=%s qty=%s "
                        "ask_levels=%d bid_levels=%d",
                        order.order_id[:12], order.quantity,
                        len(self.asks), len(self.bids),
                    )
            # If order filled immediately and has bracket, check TP/SL right away
            # instead of waiting for next WS event (#2).
            # Bracket results are returned to the caller — callbacks are NOT
            # fired here so the caller can publish fill events FIRST.
            # Single condition: check whichever is set (TP or SL, never both)
            if order.is_eligible_for_bracket and self.bids:
                current_best_bid = max(self.bids.keys())
                if order.tp_price is not None and current_best_bid >= order.tp_price:
                    result = self._execute_bracket_exit(order, current_best_bid, "TP")
                    self._bracket_log.append(result)
                    immediate_bracket_results.append(result)
                elif order.sl_price is not None and current_best_bid <= order.sl_price:
                    result = self._execute_bracket_exit(order, current_best_bid, "SL")
                    self._bracket_log.append(result)
                    immediate_bracket_results.append(result)
            # Collect state changes before releasing the lock
            state_events = self.collect_state_changes()
        # Fire state-change callbacks outside lock (for other orders on same book)
        self._fire_state_change_callbacks(state_events)
        return order, immediate_bracket_results

    # ── Pre-filled bracket order (for MARKET orders filled via REST) ────────

    def place_prefilled_bracket_order(
        self,
        side:              OrderSide,
        avg_entry_price:   Decimal,
        filled:            Decimal,
        tp_price:          Optional[Decimal] = None,
        sl_price:          Optional[Decimal] = None,
        on_bracket_exit:   Optional[Callable] = None,
    ) -> tuple[SimulatedOrder, list[BracketFillResult]]:
        """
        Register an already-filled order for bracket (TP/SL) monitoring only.
        No matching is attempted — the order is injected as FILLED.

        Used for MARKET orders that were filled via Polymarket REST API.
        The ME only monitors TP/SL conditions and fires bracket exit callbacks.

        Returns:
            Tuple of (order, bracket_results).  Bracket exit callbacks are NOT
            fired — the caller must invoke them after ensuring DB state is ready.
        """
        order = SimulatedOrder(
            order_id=str(uuid.uuid4()),
            side=side,
            price=avg_entry_price,
            quantity=filled,
            order_type="MARKET",
            filled=filled,
            status=OrderStatus.FILLED,
            _entry_cost=avg_entry_price * filled,
            tp_price=tp_price,
            sl_price=sl_price,
            _on_bracket_exit=on_bracket_exit,
        )

        immediate_bracket_results: list[BracketFillResult] = []
        with self._lock:
            self._virtual_orders.append(order)

            # Single condition: check whichever is set (TP or SL, never both)
            if order.is_eligible_for_bracket and self.bids:
                current_best_bid = max(self.bids.keys())
                if order.tp_price is not None and current_best_bid >= order.tp_price:
                    result = self._execute_bracket_exit(order, current_best_bid, "TP")
                    self._bracket_log.append(result)
                    immediate_bracket_results.append(result)
                elif order.sl_price is not None and current_best_bid <= order.sl_price:
                    result = self._execute_bracket_exit(order, current_best_bid, "SL")
                    self._bracket_log.append(result)
                    immediate_bracket_results.append(result)

        logger.info(
            "Prefilled bracket order registered: id=%s filled=%s avg=%s tp=%s sl=%s",
            order.order_id[:12], order.filled, order.avg_entry_price,
            tp_price, sl_price,
        )
        return order, immediate_bracket_results

    # ── State-change callback registration ───────────────────────────────────

    def register_state_change_callback(self, callback: Callable) -> None:
        """Register a callback for order state changes (FILL/CANCEL).

        Callbacks are invoked **outside** the book lock with a list of
        ``OrderStateChangeEvent`` objects.

        Signature: ``callback(events: list[OrderStateChangeEvent]) -> None``
        """
        with self._lock:
            self._state_change_callbacks.append(callback)

    def seed_last_reported(self, order_id: str, filled: Decimal, status: OrderStatus) -> None:
        """Seed the last-reported state for an order to suppress duplicate events.

        Called right after ``place_virtual_order()`` when the caller has already
        published the immediate fill, so ``collect_state_changes()`` won't
        re-emit that same fill.
        """
        with self._lock:
            self._last_reported[order_id] = (filled, status)

    def collect_state_changes(self) -> list[OrderStateChangeEvent]:
        """Compare every order vs ``_last_reported`` and emit change events.

        Must be called while holding ``self._lock``.

        Emits:
          - FILL when ``filled`` increased since last report.
          - CANCEL when status changed to CANCELED.
        Also cleans up tracking for pruned orders.
        """
        events: list[OrderStateChangeEvent] = []
        live_ids: set[str] = set()

        for order in self._virtual_orders:
            live_ids.add(order.order_id)
            prev = self._last_reported.get(order.order_id)
            prev_filled = prev[0] if prev else Decimal("0")
            prev_status = prev[1] if prev else OrderStatus.PENDING

            # FILL event: filled increased
            if order.filled > prev_filled:
                # Extract only new fill levels since last report
                new_fill_levels = list(order._fill_levels)
                order._fill_levels = []  # reset for next report
                events.append(OrderStateChangeEvent(
                    order_id=order.order_id,
                    event_type="FILL",
                    filled=order.filled,
                    avg_entry_price=order.avg_entry_price,
                    status=order.status,
                    fill_levels=new_fill_levels,
                ))

            # CANCEL event: status transitioned to CANCELED
            if order.status == OrderStatus.CANCELED and prev_status != OrderStatus.CANCELED:
                events.append(OrderStateChangeEvent(
                    order_id=order.order_id,
                    event_type="CANCEL",
                    filled=order.filled,
                    avg_entry_price=order.avg_entry_price,
                    status=order.status,
                    cancel_reason="TTL_EXPIRED",
                ))

            # Update tracking
            self._last_reported[order.order_id] = (order.filled, order.status)

        # Cleanup tracking for pruned orders
        stale_ids = set(self._last_reported.keys()) - live_ids
        for oid in stale_ids:
            del self._last_reported[oid]

        return events

    def _fire_state_change_callbacks(self, events: list[OrderStateChangeEvent]) -> None:
        """Fire registered callbacks outside the lock."""
        if not events:
            return
        for cb in self._state_change_callbacks:
            try:
                cb(events)
            except Exception as exc:
                logger.error("state-change callback error: %s", exc, exc_info=True)

    # ── Matching algorithm (Section 4) ───────────────────────────────────────

    _BRACKET_LOG_MAX = 500
    _CLEANUP_INTERVAL = ME_CLEANUP_INTERVAL

    def run_matching(self) -> None:
        """Re-run matching for all active virtual orders (after book updates).
        Also expires stale PENDING orders whose TTL has elapsed.

        Skips matching entirely when the book is stale (>120s without
        updates) to prevent fills against outdated price data.
        """
        state_events: list[OrderStateChangeEvent] = []
        with self._lock:
            self._expire_pending_orders()

            # Guard: do not match against expired or stale books.
            _skip_matching = self._expired or self.is_stale(max_age_s=ME_BOOK_STALE_MAX_S)
            if _skip_matching:
                logger.warning(
                    "run_matching SKIPPED on %s — book stale (last_update=%s)",
                    self.token_id[:16],
                    self.last_update.isoformat() if self.last_update else "never",
                )

            for order in self._virtual_orders:
                if _skip_matching:
                    break
                if order.status in (OrderStatus.FILLED, OrderStatus.CANCELED):
                    continue
                had_liquidity = (
                    bool(self.asks) if order.side == OrderSide.BUY else bool(self.bids)
                )
                self._match_order(order)

                # MARKET IOC: after matching against a populated book,
                # cancel any unfilled remainder.  This handles the case
                # where the order was placed before the book had data.
                if order.order_type == "MARKET" and order.status != OrderStatus.FILLED:
                    if had_liquidity or order.filled > 0:
                        order.status = OrderStatus.CANCELED
                        logger.info(
                            "MARKET IOC cancel (rematch): id=%s filled=%s/%s avg=%s",
                            order.order_id[:12], order.filled, order.quantity,
                            order.avg_entry_price,
                        )
            # Collect state changes before releasing the lock
            state_events = self.collect_state_changes()
            # Periodic cleanup of terminal orders to prevent memory leak
            self._cleanup_counter += 1
            if self._cleanup_counter >= self._CLEANUP_INTERVAL:
                self._cleanup_counter = 0
                self._prune_terminal_orders()
        # Fire callbacks outside lock
        self._fire_state_change_callbacks(state_events)

    _PRUNE_GRACE_S = 5  # keep terminal orders for 5s (event-driven callbacks don't need long grace)

    def _prune_terminal_orders(self) -> None:
        """Remove fully terminal orders from _virtual_orders.
        Must be called while holding self._lock.

        An order is terminal when:
          - CANCELED (TTL expired, no further matching or bracket possible)
          - FILLED + position_closed (bracket exit completed)
        Orders that are FILLED but still have an active bracket are kept.

        Terminal orders are kept for _PRUNE_GRACE_S seconds after their
        expire_at (or last update) so monitor threads have time to read
        the terminal status and publish events before the order disappears.
        """
        now = datetime.now(timezone.utc)
        grace = timedelta(seconds=self._PRUNE_GRACE_S)
        before = len(self._virtual_orders)

        def _is_prunable(o: SimulatedOrder) -> bool:
            if o.status == OrderStatus.CANCELED:
                # Don't prune CANCELED orders that still have active brackets
                # (e.g. MARKET IOC partial fill → CANCELED but filled > 0 with TP/SL)
                if o.is_eligible_for_bracket:
                    return False
                ref = o.expire_at or o.created_at
                return (now - ref) > grace
            if o.status == OrderStatus.FILLED and o.position_closed:
                return True  # bracket exit done, safe to remove immediately
            return False

        self._virtual_orders = [
            o for o in self._virtual_orders if not _is_prunable(o)
        ]
        pruned = before - len(self._virtual_orders)
        if pruned:
            logger.info("Pruned %d terminal orders (was %d, now %d)",
                        pruned, before, len(self._virtual_orders))
        # Cap bracket log
        if len(self._bracket_log) > self._BRACKET_LOG_MAX:
            self._bracket_log = self._bracket_log[-self._BRACKET_LOG_MAX:]

    def expire_pending_orders(self) -> list[SimulatedOrder]:
        """
        Public method: cancel expired orders whose expire_at has passed.

        Behaviour by status:
          - PENDING (filled=0): status → CANCELED. No fill, no P&L.
          - PARTIAL (filled>0): quantity is clamped to filled so remaining
            unfilled qty can no longer be matched. Status stays PARTIAL
            (the filled portion proceeds to settlement). Any future
            run_matching() calls will skip it because remaining_qty == 0.

        Returns all orders that were acted on (both PENDING→CANCELED and
        PARTIAL that had their quantity clamped).
        """
        with self._lock:
            return self._expire_pending_orders()

    def _expire_pending_orders(self) -> list[SimulatedOrder]:
        """
        Internal expiry check — must be called while holding self._lock.
        """
        now = datetime.now(timezone.utc)
        expired = []
        for order in self._virtual_orders:
            if order.expire_at is None or now < order.expire_at:
                continue
            if order.status == OrderStatus.PENDING:
                # Zero fill — nothing to settle, just cancel
                order.status = OrderStatus.CANCELED
                expired.append(order)
                logger.info(
                    "Order expired (PENDING→CANCELED): id=%s price=%s qty=%s",
                    order.order_id[:12], order.price, order.quantity,
                )
            elif order.status == OrderStatus.PARTIAL:
                # Has partial fill — clamp quantity to filled so remaining
                # unfilled qty is closed out and no further matching occurs.
                # Set status to CANCELED so the order monitor can detect it
                # and publish the cancel event with partial fill data.
                unfilled = order.remaining_qty
                order.quantity = order.filled  # remaining_qty now == 0
                order.status = OrderStatus.CANCELED
                expired.append(order)
                logger.info(
                    "Order expired (PARTIAL→CANCELED): id=%s filled=%s unfilled=%s",
                    order.order_id[:12], order.filled, unfilled,
                )
        return expired

    def _match_order(self, order: SimulatedOrder) -> None:
        """
        Core matching algorithm — spec Section 4.
        Must be called while holding self._lock.

        MARKET orders: sweep all available levels (no price check).
        LIMIT orders: match only at limit price or better.
        """
        is_market = order.order_type == "MARKET"
        filled_before = order.filled

        # ── Slippage bounds for MARKET orders ─────────────────────────────
        # Lock in the reference price on first match with book data so
        # subsequent re-matches (after book repopulates) don't lose the
        # original slippage protection.
        slippage_limit_buy: Optional[Decimal] = None
        slippage_limit_sell: Optional[Decimal] = None
        if is_market:
            slippage = order.max_slippage if order.max_slippage is not None else _DEFAULT_SLIPPAGE
            if order.side == OrderSide.BUY and self.asks:
                if order._slippage_ref_price is None:
                    order._slippage_ref_price = min(self.asks.keys())
                slippage_limit_buy = order._slippage_ref_price * (1 + slippage)
            elif order.side == OrderSide.SELL and self.bids:
                if order._slippage_ref_price is None:
                    order._slippage_ref_price = max(self.bids.keys())
                slippage_limit_sell = order._slippage_ref_price * (1 - slippage)

        # ── Pre-match book snapshot for debug ─────────────────────────────
        if order.side == OrderSide.BUY:
            top_asks = sorted(self.asks.items())[:5]
            logger.info(
                "MATCH_START %s BUY %s: id=%s price=%s qty=%s "
                "remaining=%s slippage_limit=%s "
                "ask_levels=%d top_asks=%s",
                order.order_type, self.token_id[:16],
                order.order_id[:12], order.price, order.quantity,
                order.remaining_qty, slippage_limit_buy,
                len(self.asks),
                [(str(p), str(s)) for p, s in top_asks],
            )
        else:
            top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:5]
            logger.info(
                "MATCH_START %s SELL %s: id=%s price=%s qty=%s "
                "remaining=%s slippage_limit=%s "
                "bid_levels=%d top_bids=%s",
                order.order_type, self.token_id[:16],
                order.order_id[:12], order.price, order.quantity,
                order.remaining_qty, slippage_limit_sell,
                len(self.bids),
                [(str(p), str(s)) for p, s in top_bids],
            )

        if order.side == OrderSide.BUY:
            # Iterate sorted asks ascending — stop when no more price matches
            for ask_price in sorted(self.asks.keys()):
                ask_size = self.asks.get(ask_price, Decimal("0"))
                if ask_size <= 0:
                    continue
                # MARKET: sweep asks within slippage bound
                # LIMIT: only match if order.price >= ask_price
                if is_market:
                    if slippage_limit_buy is not None and ask_price > slippage_limit_buy:
                        logger.info(
                            "MATCH_SLIPPAGE_STOP BUY %s: ask=%s > limit=%s, stopping",
                            order.order_id[:12], ask_price, slippage_limit_buy,
                        )
                        break
                elif order.price < ask_price:
                    break  # LIMIT ascending — no further matches possible

                match_qty = min(order.remaining_qty, ask_size)

                # ── Cost cap for MARKET BUY: don't exceed max_cost ─────
                if order.max_cost is not None:
                    budget_remaining = order.max_cost - order._entry_cost
                    if budget_remaining <= 0:
                        logger.info(
                            "MATCH_COST_CAP BUY %s: budget exhausted "
                            "(cost=%s >= max_cost=%s), stopping",
                            order.order_id[:12], order._entry_cost,
                            order.max_cost,
                        )
                        break
                    affordable_qty = budget_remaining / ask_price
                    if affordable_qty < match_qty:
                        match_qty = affordable_qty
                        logger.info(
                            "MATCH_COST_CAP BUY %s: capping qty to %s "
                            "(budget=%s / price=%s)",
                            order.order_id[:12], match_qty,
                            budget_remaining, ask_price,
                        )

                if match_qty < _DUST_THRESHOLD:
                    break

                order.filled          += match_qty
                order._entry_cost     += match_qty * ask_price  # track weighted entry cost
                order._fill_levels.append((ask_price, match_qty))
                self.asks[ask_price]  -= match_qty
                if self.asks[ask_price] < _DUST_THRESHOLD:
                    del self.asks[ask_price]
                order._update_status()
                logger.info(
                    "MATCH_FILL BUY %s: %s @ %s (level_remain=%s) "
                    "filled=%s/%s avg_entry=%s cost=%s",
                    order.order_id[:12], match_qty, ask_price,
                    self.asks.get(ask_price, "consumed"),
                    order.filled, order.quantity, order.avg_entry_price,
                    order._entry_cost,
                )
                if order.status == OrderStatus.FILLED:
                    break
                # Check cost cap again after fill
                if order.max_cost is not None and order._entry_cost >= order.max_cost:
                    logger.info(
                        "MATCH_COST_CAP BUY %s: cost=%s reached max_cost=%s, stopping",
                        order.order_id[:12], order._entry_cost, order.max_cost,
                    )
                    break
        else:
            # Iterate sorted bids descending
            for bid_price in sorted(self.bids.keys(), reverse=True):
                bid_size = self.bids.get(bid_price, Decimal("0"))
                if bid_size <= 0:
                    continue
                # MARKET: sweep bids within slippage bound
                # LIMIT: only match if order.price <= bid_price
                if is_market:
                    if slippage_limit_sell is not None and bid_price < slippage_limit_sell:
                        logger.info(
                            "MATCH_SLIPPAGE_STOP SELL %s: bid=%s < limit=%s, stopping",
                            order.order_id[:12], bid_price, slippage_limit_sell,
                        )
                        break
                elif order.price > bid_price:
                    break  # LIMIT descending — no further matches possible

                match_qty = min(order.remaining_qty, bid_size)
                order.filled          += match_qty
                order._entry_cost     += match_qty * bid_price  # track weighted entry cost
                order._fill_levels.append((bid_price, match_qty))
                self.bids[bid_price]  -= match_qty
                if self.bids[bid_price] < _DUST_THRESHOLD:
                    del self.bids[bid_price]
                order._update_status()
                logger.info(
                    "MATCH_FILL SELL %s: %s @ %s (level_remain=%s) "
                    "filled=%s/%s avg_entry=%s",
                    order.order_id[:12], match_qty, bid_price,
                    self.bids.get(bid_price, "consumed"),
                    order.filled, order.quantity, order.avg_entry_price,
                )
                if order.status == OrderStatus.FILLED:
                    break

        # ── Post-match summary ────────────────────────────────────────────
        new_fills = order.filled - filled_before
        if new_fills > 0 or is_market:
            logger.info(
                "MATCH_DONE %s %s %s: id=%s new_fills=%s total_filled=%s/%s "
                "avg_entry=%s status=%s remaining_asks=%d remaining_bids=%d",
                order.order_type, order.side.value, self.token_id[:16],
                order.order_id[:12], new_fills,
                order.filled, order.quantity, order.avg_entry_price,
                order.status.value, len(self.asks), len(self.bids),
            )

    # ── Workflow E: Bracket Order TP/SL monitoring (Section 5) ───────────────

    def monitor_bracket_orders(self) -> list[BracketFillResult]:
        """
        Evaluate all active bracket orders against current best bid.

        Single condition policy (v2 spec): each order has at most one condition
        (TP or SL), so we simply check whichever is set.

        Liquidation executes through standard matching algo against shadow
        bids — realistic multi-level slippage.  Only the `filled` portion
        is liquidated.

        Returns list of BracketFillResult for all exits executed this tick.
        """
        results: list[BracketFillResult] = []
        # Callbacks to fire AFTER lock is released to avoid DB ops inside lock
        pending_callbacks: list[tuple[Callable, BracketFillResult]] = []

        with self._lock:
            if not self.bids:
                return results
            current_best_bid = max(self.bids.keys())

            for order in self._virtual_orders:
                if not order.is_eligible_for_bracket:
                    continue

                # ── Take Profit check ─────────────────────────────────────────
                if order.tp_price is not None and current_best_bid >= order.tp_price:
                    logger.info(
                        "Take Profit triggered for Order %s at price %s "
                        "(tp_price=%s, qty_to_close=%s)",
                        order.order_id[:12], current_best_bid,
                        order.tp_price, order.filled,
                    )
                    result = self._execute_bracket_exit(
                        order, current_best_bid, "TP",
                    )
                    results.append(result)
                    self._bracket_log.append(result)
                    if order._on_bracket_exit is not None:
                        pending_callbacks.append((order._on_bracket_exit, result))

                # ── Stop Loss check ───────────────────────────────────────────
                elif order.sl_price is not None and current_best_bid <= order.sl_price:
                    logger.info(
                        "Stop Loss triggered for Order %s at price %s "
                        "(sl_price=%s, qty_to_close=%s)",
                        order.order_id[:12], current_best_bid,
                        order.sl_price, order.filled,
                    )
                    result = self._execute_bracket_exit(
                        order, current_best_bid, "SL",
                    )
                    results.append(result)
                    self._bracket_log.append(result)
                    if order._on_bracket_exit is not None:
                        pending_callbacks.append((order._on_bracket_exit, result))

        # Fire write-back callbacks outside the lock
        for cb, res in pending_callbacks:
            try:
                cb(res)
            except Exception as exc:
                logger.error("bracket exit callback error: %s", exc, exc_info=True)

        return results

    def _execute_bracket_exit(
        self,
        order:         SimulatedOrder,
        market_bid:    Decimal,
        trigger:       str,
    ) -> BracketFillResult:
        """
        Execute a taker SELL for `order.filled` shares against shadow bids.

        Passes through the standard matching algorithm so large exits consume
        multiple bid levels and suffer realistic slippage (spec Section 6.2).

        Must be called while holding self._lock.
        """
        # If a previous partial exit occurred, only close the remaining portion
        already_exited = order.exit_filled or Decimal("0")
        qty_to_close = order.filled - already_exited
        qty_exited   = Decimal("0")
        total_value  = Decimal("0")
        levels_hit   = 0
        exit_levels: list = []

        # Walk bids descending — consume as much as needed (with slippage)
        for bid_price in sorted(self.bids.keys(), reverse=True):
            bid_size = self.bids.get(bid_price, Decimal("0"))
            if bid_size <= 0:
                continue

            fill_qty = min(qty_to_close - qty_exited, bid_size)
            qty_exited          += fill_qty
            total_value         += fill_qty * bid_price
            exit_levels.append((bid_price, fill_qty))
            self.bids[bid_price] -= fill_qty
            if self.bids[bid_price] < _DUST_THRESHOLD:
                del self.bids[bid_price]
            levels_hit += 1

            logger.debug(
                "%s exit %s: sold %s @ %s (cumulative=%s/%s)",
                trigger, order.order_id[:8],
                fill_qty, bid_price, qty_exited, qty_to_close,
            )

            if qty_exited >= qty_to_close:
                break

        avg_exit = (total_value / qty_exited) if qty_exited > 0 else market_bid

        # Accumulate exit_filled across partial exits
        total_exited = already_exited + qty_exited
        # Compute cumulative avg exit price across all partial exits
        if order.exit_price is not None and already_exited > 0:
            prev_value = order.exit_price * already_exited
            cumulative_avg = (prev_value + total_value) / total_exited if total_exited > 0 else avg_exit
        else:
            cumulative_avg = avg_exit

        # Only mark position fully closed if all shares were exited.
        # If bids were exhausted (partial exit), keep position open for
        # the remaining shares so TP/SL can fire again on next tick.
        order.position_closed = (total_exited >= order.filled)
        order.exit_price      = cumulative_avg
        order.exit_trigger    = trigger
        order.exit_filled     = total_exited

        result = BracketFillResult(
            order_id        = order.order_id,
            trigger         = trigger,
            trigger_price   = order.tp_price if trigger == "TP" else order.sl_price,
            market_bid      = market_bid,
            qty_to_close    = qty_to_close,
            qty_exited      = qty_exited,
            avg_exit_price  = avg_exit,
            levels_consumed = levels_hit,
            fill_levels     = exit_levels,
        )

        logger.info(
            "%s exit complete: order=%s qty=%s/%s avg_price=%s levels=%d",
            trigger, order.order_id[:12],
            qty_exited, qty_to_close,
            avg_exit, levels_hit,
        )
        return result

    # ── Utilities ────────────────────────────────────────────────────────────

    def force_close_at_market(self, order_id: str) -> Optional[BracketFillResult]:
        """
        Force-close an open position at the current best market bid.

        Used when:
        - No SL is set and the position must be liquidated (e.g. expiry, manual close)
        - Position is partially exited and the remainder needs closing

        Executes through the standard slippage algorithm (walks bids descending).
        Marks position_closed=True and records exit so calculate_profit() works.
        Returns None if order not found, already closed, or no bids available.
        """
        callback = None
        with self._lock:
            order = next(
                (o for o in self._virtual_orders if o.order_id == order_id),
                None,
            )
            if order is None or order.filled <= 0 or order.position_closed:
                return None
            if not self.bids:
                return None
            result = self._execute_bracket_exit(order, max(self.bids.keys()), "FORCE_CLOSE")
            self._bracket_log.append(result)
            logger.info(
                "Force-close order %s: qty=%s avg_exit=%s profit=%s",
                order_id[:12], result.qty_exited, result.avg_exit_price,
                order.calculate_profit(),
            )
            callback = order._on_bracket_exit

        if callback is not None:
            try:
                callback(result)
            except Exception as exc:
                logger.error("force-close callback error: %s", exc, exc_info=True)

        return result

    def cancel_order(self, order_id: str) -> Optional[SimulatedOrder]:
        """
        Cancel a single virtual order by ID.
        Returns the order if found and canceled, None otherwise.
        Partially filled orders retain their fill data for P&L calculation.
        """
        with self._lock:
            for order in self._virtual_orders:
                if order.order_id == order_id:
                    if order.status in (OrderStatus.PENDING, OrderStatus.PARTIAL):
                        order.status = OrderStatus.CANCELED
                        logger.info(
                            "Canceled order %s (filled=%s/%s, avg_entry=%s)",
                            order_id[:12], order.filled, order.quantity,
                            order.avg_entry_price,
                        )
                        return order
                    return None  # already FILLED or CANCELED
            return None

    def cancel_all_virtual(self) -> int:
        """Cancel all pending/partial virtual orders. Returns count canceled."""
        with self._lock:
            count = 0
            for order in self._virtual_orders:
                if order.status in (OrderStatus.PENDING, OrderStatus.PARTIAL):
                    order.status = OrderStatus.CANCELED
                    count += 1
            return count

    @property
    def active_orders(self) -> list[SimulatedOrder]:
        with self._lock:
            return [
                o for o in self._virtual_orders
                if o.status not in (OrderStatus.FILLED, OrderStatus.CANCELED)
            ]

    @property
    def bracket_log(self) -> list[BracketFillResult]:
        with self._lock:
            return list(self._bracket_log)


# ── Matching Engine ──────────────────────────────────────────────────────────


class MatchingEngine:
    """
    Registry of ShadowOrderbooks keyed by token_id.

    Dispatches all WebSocket events (book, price_change, best_bid_ask,
    last_trade_price, market_resolved) to the correct book and triggers
    TP/SL bracket monitoring on every price tick.
    """

    def __init__(self) -> None:
        self._books:   dict[str, ShadowOrderbook] = {}
        self._lock     = threading.Lock()
        self._running  = True
        self._on_market_resolved: Optional[Callable] = None  # callback(asset_id, orders)
        # Set of token_ids known to be current (updated by register_valid_tokens / invalidate_books)
        self._valid_token_ids: set[str] = set()

    def get_or_create_book(self, token_id: str) -> ShadowOrderbook:
        with self._lock:
            if token_id not in self._books:
                self._books[token_id] = ShadowOrderbook(token_id)
                logger.info("Created shadow orderbook: token=%s", token_id[:24])
            return self._books[token_id]

    def get_book(self, token_id: str) -> Optional[ShadowOrderbook]:
        with self._lock:
            return self._books.get(token_id)

    def register_valid_tokens(self, token_ids: list[str]) -> None:
        """Replace the set of valid (current-session) token_ids.

        Called at startup and on every token rotation so that
        place_virtual_order() can reject orders targeting stale tokens.
        """
        with self._lock:
            self._valid_token_ids = set(token_ids)
        logger.info(
            "Registered %d valid token(s)", len(token_ids),
        )

    def is_valid_token(self, token_id: str) -> bool:
        """Check if a token_id belongs to the current candle session."""
        with self._lock:
            # If no tokens registered yet (startup), allow all
            if not self._valid_token_ids:
                return True
            return token_id in self._valid_token_ids

    def invalidate_books(self, token_ids: list[str]) -> int:
        """Expire books for rotated token_ids.

        Cancels all pending orders and prevents new orders from matching
        against stale book data from the previous candle session.
        Also removes them from the valid token set.

        Returns total number of cancelled orders.
        """
        total_cancelled = 0
        with self._lock:
            self._valid_token_ids -= set(token_ids)
        for tid in token_ids:
            book = self.get_book(tid)
            if book is not None:
                total_cancelled += book.expire_book()
        if total_cancelled:
            logger.info(
                "invalidate_books: expired %d book(s), cancelled %d order(s)",
                len(token_ids), total_cancelled,
            )
        return total_cancelled

    # ── Event dispatch ───────────────────────────────────────────────────────

    def dispatch_event(self, event: dict) -> None:
        """Route a Polymarket WebSocket event to the appropriate handler."""
        if not self._running:
            return
        etype    = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if etype == "book":
            self._handle_book(asset_id, event)
        elif etype == "price_change":
            self._handle_price_change(asset_id, event)
        elif etype == "best_bid_ask":
            self._handle_best_bid_ask(asset_id, event)
        elif etype == "last_trade_price":
            self._handle_last_trade(asset_id, event)
        elif etype == "market_resolved":
            self._handle_market_resolved(asset_id)

    def _handle_book(self, asset_id: str, event: dict) -> None:
        """Full snapshot — replace orderbook and run matching (spec 3.1)."""
        book = self.get_or_create_book(asset_id)
        bids = event.get("bids", [])
        asks = event.get("asks", [])
        book.apply_snapshot(bids, asks)
        book.run_matching()
        logger.info(
            "Book %s: %d bids, %d asks | bid=%s ask=%s",
            asset_id[:16], len(bids), len(asks),
            book.best_bid(), book.best_ask(),
        )
        # Trigger bracket monitoring after book update (TP/SL may fire)
        exits = book.monitor_bracket_orders()
        if exits:
            logger.info(
                "book snapshot on %s triggered %d bracket exit(s)",
                asset_id[:16], len(exits),
            )

    def _handle_price_change(self, asset_id: str, event: dict) -> None:
        """Delta update — apply changes and run matching (spec 3.2)."""
        book = self.get_or_create_book(asset_id)
        book.apply_changes(event.get("changes", []))
        book.run_matching()
        # Trigger bracket monitoring after price change (TP/SL may fire)
        exits = book.monitor_bracket_orders()
        if exits:
            logger.info(
                "price_change on %s triggered %d bracket exit(s)",
                asset_id[:16], len(exits),
            )

    def _handle_best_bid_ask(self, asset_id: str, event: dict) -> None:
        """
        Top-of-book spread update (spec 3.3).

        Primary trigger for Bracket Order monitoring (spec 3.6):
        'best_bid_ask updates should invoke the Conditional Monitoring
        Algorithm (Workflow E).'
        """
        book = self.get_or_create_book(asset_id)

        # Sync best bid/ask into shadow book if it's fresher than last delta
        # (lightweight upsert — does not replace the full book).
        # Always refresh last_update so the book is not marked stale between
        # full price_change snapshots — best_bid_ask events confirm the feed
        # is alive even when top-of-book prices haven't moved.
        raw_bid = event.get("bid")
        raw_ask = event.get("ask")
        if raw_bid or raw_ask:
            changes = []
            # Only include a side if its size is present and non-zero.
            # best_bid_ask events may omit *_size fields — treating
            # missing size as "0" would incorrectly DELETE the price
            # level from the shadow book, causing stale-data fills.
            if raw_bid and event.get("bid_size"):
                changes.append({"side": "bid", "price": raw_bid, "size": event["bid_size"]})
            if raw_ask and event.get("ask_size"):
                changes.append({"side": "ask", "price": raw_ask, "size": event["ask_size"]})
            if changes:
                book.apply_changes(changes)
                # Run matching so pending LIMIT orders can fill when
                # the best ask drops to or below their limit price.
                book.run_matching()
            else:
                # Prices unchanged but feed is alive — touch last_update to
                # prevent the book from becoming stale
                with book._lock:
                    book.last_update = datetime.now(timezone.utc)

        # Trigger Workflow E
        exits = book.monitor_bracket_orders()
        if exits:
            logger.info(
                "best_bid_ask on %s triggered %d bracket exit(s)",
                asset_id[:16], len(exits),
            )

    def _handle_last_trade(self, asset_id: str, event: dict) -> None:
        """
        Real market execution event (spec 3.4).

        Records the trade and triggers Bracket Order monitoring (spec 3.6):
        'last_trade_price updates should invoke Workflow E.'
        """
        book = self.get_or_create_book(asset_id)
        book.record_trade(
            price=event.get("price", "0"),
            size=event.get("size", "0"),
            side=event.get("side", ""),
        )
        # Trigger Workflow E after market price update
        exits = book.monitor_bracket_orders()
        if exits:
            logger.info(
                "last_trade_price on %s triggered %d bracket exit(s)",
                asset_id[:16], len(exits),
            )

    def _handle_market_resolved(self, asset_id: str) -> None:
        """
        Market finalized — cancel TP/SL monitoring and mark positions closed (v2 spec Section 5).

        Sets position_closed=True and clears tp/sl on all affected orders so
        bracket monitoring stops.  Also cancels any unfilled orders.
        """
        book = self.get_book(asset_id)
        if book is None:
            return

        resolved_orders: list[SimulatedOrder] = []
        with book._lock:
            for order in book._virtual_orders:
                if order.position_closed:
                    continue
                # Cancel unfilled orders
                if order.status in (OrderStatus.PENDING, OrderStatus.PARTIAL):
                    order.status = OrderStatus.CANCELED
                # Clear TP/SL conditions and mark position closed
                order.tp_price = None
                order.sl_price = None
                order.position_closed = True
                resolved_orders.append(order)

        if resolved_orders:
            logger.info(
                "Market resolved %s: marked %d order(s) as position_closed, "
                "cleared all TP/SL conditions",
                asset_id[:16], len(resolved_orders),
            )

        # Publish resolution event via callback if registered
        if self._on_market_resolved is not None:
            try:
                self._on_market_resolved(asset_id, resolved_orders)
            except Exception as exc:
                logger.error("market_resolved callback error: %s", exc, exc_info=True)

    # ── Public API ───────────────────────────────────────────────────────────

    def best_ask(self, token_id: str) -> Optional[float]:
        """Best ask as float, or None if book unavailable/stale."""
        book = self.get_book(token_id)
        if book is None or book.is_stale():
            return None
        ask = book.best_ask()
        return float(ask) if ask is not None else None

    def best_bid(self, token_id: str) -> Optional[float]:
        """Best bid as float, or None if book unavailable/stale."""
        book = self.get_book(token_id)
        if book is None or book.is_stale():
            return None
        bid = book.best_bid()
        return float(bid) if bid is not None else None

    def place_virtual_order(
        self,
        token_id:          str,
        side:              OrderSide,
        price:             Decimal,
        quantity:          Decimal,
        tp_price:          Optional[Decimal]  = None,
        sl_price:          Optional[Decimal]  = None,
        timeframe:         Optional[str]      = None,
        ttl_seconds:       Optional[float]    = None,
        on_bracket_exit:   Optional[Callable] = None,
        order_type:        str                = "LIMIT",
        max_slippage:      Optional[Decimal]  = None,
        max_cost:          Optional[Decimal]  = None,
    ) -> tuple[SimulatedOrder, list[BracketFillResult]]:
        """
        Convenience wrapper: get/create book and place a virtual order.

        order_type:       "MARKET" or "LIMIT" (default).
        timeframe:        Candle timeframe (M5/M15/H1).
                          expire_at aligned to next candle close on the grid.
                          e.g. timeframe='M5', now=12:12 → expire_at=12:15
        ttl_seconds:      Raw seconds from now. Takes priority over timeframe.
        on_bracket_exit:  Callback fired after every TP/SL/FORCE_CLOSE exit.
                          Signature: callback(result: BracketFillResult) -> None
        max_slippage:     Maximum slippage for MARKET orders (fraction).
                          None = 10% default. Ignored for LIMIT.
        max_cost:         Cost cap for MARKET BUY orders (user's dollar amount).
                          Matching stops when cumulative cost reaches this limit.
        """
        # Validate token_id is still current (prevents filling against
        # stale books from previous candle sessions)
        if not self.is_valid_token(token_id):
            raise ValueError(
                f"Stale token_id (not in current session): {token_id[:16]}"
            )
        book = self.get_or_create_book(token_id)
        if book._expired:
            raise ValueError(
                f"Cannot place order on expired book (token rotated): {token_id[:16]}"
            )
        return book.place_virtual_order(
            side, price, quantity, tp_price, sl_price, timeframe, ttl_seconds,
            on_bracket_exit, order_type, max_slippage, max_cost,
        )

    def place_prefilled_bracket_order(
        self,
        token_id:          str,
        side:              OrderSide,
        avg_entry_price:   Decimal,
        filled:            Decimal,
        tp_price:          Optional[Decimal] = None,
        sl_price:          Optional[Decimal] = None,
        on_bracket_exit:   Optional[Callable] = None,
    ) -> tuple[SimulatedOrder, list[BracketFillResult]]:
        """
        Convenience wrapper: get/create book and register a pre-filled bracket order.
        Used for MARKET orders filled via REST that need TP/SL monitoring.
        """
        if not self.is_valid_token(token_id):
            raise ValueError(
                f"Stale token_id (not in current session): {token_id[:16]}"
            )
        book = self.get_or_create_book(token_id)
        if book._expired:
            raise ValueError(
                f"Cannot place order on expired book (token rotated): {token_id[:16]}"
            )
        return book.place_prefilled_bracket_order(
            side, avg_entry_price, filled, tp_price, sl_price, on_bracket_exit,
        )

    def cancel_order(self, token_id: str, order_id: str) -> Optional[SimulatedOrder]:
        """Cancel a single virtual order. Returns the order if canceled."""
        book = self.get_book(token_id)
        if book is None:
            return None
        return book.cancel_order(order_id)

    def force_close_at_market(self, token_id: str, order_id: str) -> Optional[BracketFillResult]:
        """Force-close an open position at current market bid (no SL scenario)."""
        book = self.get_book(token_id)
        if book is None:
            return None
        return book.force_close_at_market(order_id)

    def expire_pending_orders(self, token_id: str) -> list[SimulatedOrder]:
        """Cancel all PENDING expired orders for a token. Returns canceled orders."""
        book = self.get_book(token_id)
        if book is None:
            return []
        return book.expire_pending_orders()

    def expire_all_pending_orders(self) -> dict[str, list[SimulatedOrder]]:
        """Scan all books and cancel expired PENDING orders. Returns {token_id: [canceled]}."""
        with self._lock:
            token_ids = list(self._books.keys())
        result = {}
        for tid in token_ids:
            book = self.get_book(tid)
            if book:
                canceled = book.expire_pending_orders()
                if canceled:
                    result[tid] = canceled
        return result

    def book_summary(self, token_id: str) -> Optional[dict]:
        """Summary dict for debugging/monitoring."""
        book = self.get_book(token_id)
        if book is None:
            return None
        with book._lock:
            active = [
                o for o in book._virtual_orders
                if o.status not in (OrderStatus.FILLED, OrderStatus.CANCELED)
            ]
            bracket_active = [o for o in active if o.has_bracket and not o.position_closed]
            exits = list(book._bracket_log)

        return {
            "token_id":    token_id,
            "best_bid":    str(book.best_bid()),
            "best_ask":    str(book.best_ask()),
            "spread":      str(book.spread()),
            "bid_levels":  len(book.bids),
            "ask_levels":  len(book.asks),
            "stale":       book.is_stale(),
            "last_update": book.last_update.isoformat() if book.last_update else None,
            "last_trade":  {
                "price": str(book.last_trade.price),
                "size":  str(book.last_trade.size),
                "side":  book.last_trade.side,
            } if book.last_trade else None,
            "active_virtual_orders":  len(active),
            "active_bracket_orders":  len(bracket_active),
            "bracket_exits_total":    len(exits),
            "recent_bracket_exits": [
                {
                    "order_id":       e.order_id[:16],
                    "trigger":        e.trigger,
                    "trigger_price":  str(e.trigger_price),
                    "market_bid":     str(e.market_bid),
                    "qty_exited":     str(e.qty_exited),
                    "avg_exit_price": str(e.avg_exit_price),
                    "levels":         e.levels_consumed,
                    "ts":             e.timestamp.isoformat(),
                }
                for e in exits[-5:]
            ],
        }

    def all_books_summary(self) -> list[dict]:
        with self._lock:
            token_ids = list(self._books.keys())
        return [s for tid in token_ids if (s := self.book_summary(tid)) is not None]

    def shutdown(self) -> None:
        """Stop accepting events and cancel all virtual orders."""
        self._running = False
        with self._lock:
            for book in self._books.values():
                book.cancel_all_virtual()
        logger.info("Matching engine shut down (%d books)", len(self._books))


# ── Module-level singleton ───────────────────────────────────────────────────

_engine: Optional[MatchingEngine] = None


def get_engine() -> MatchingEngine:
    """Return the global MatchingEngine singleton (create on first call)."""
    global _engine
    if _engine is None:
        _engine = MatchingEngine()
    return _engine
