"""
OrderConsumer — BRPOP loop that reads virtual orders from Redis queue
and places them in the matching engine.

Runs in a daemon thread so it doesn't block the asyncio event loop.

Uses event-driven centralized monitoring:
  - State-change callbacks (FILL / CANCEL) fire from the matching engine
    after run_matching() and place_virtual_order(), replacing per-order
    polling threads.
  - Bracket exit callbacks fire after TP/SL executions.
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import redis

from services.matching_engine import (
    MatchingEngine, OrderSide, OrderStatus, OrderStateChangeEvent,
    BracketFillResult,
)
from services.session_manager import SessionManager
from ws_feed_service.config import BRPOP_TIMEOUT_S
from ws_feed_service.redis_writer import RedisWriter

logger = logging.getLogger(__name__)

# ── Limit order fill file logger ─────────────────────────────────────────

_FILL_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
_FILL_LOG_PATH = os.path.join(_FILL_LOG_DIR, "limit_fills.log")

_fill_logger = logging.getLogger("limit_fill_log")
_fill_logger.setLevel(logging.INFO)
_fill_logger.propagate = False

try:
    os.makedirs(_FILL_LOG_DIR, exist_ok=True)
    _fill_fh = logging.FileHandler(_FILL_LOG_PATH, encoding="utf-8")
    _fill_fh.setFormatter(logging.Formatter("%(message)s"))
    _fill_logger.addHandler(_fill_fh)
except Exception as _exc:
    logger.warning("Cannot create limit fill log at %s: %s", _FILL_LOG_PATH, _exc)


def _log_limit_fill(
    bo_id: int,
    order_id: str,
    filled: float,
    avg_entry_price: float,
    status: str,
    fill_levels: list,
    bids_snapshot: list,
    asks_snapshot: list,
    token_id: str = "",
) -> None:
    """Write a structured JSON line to the limit fill log file."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bo_id": bo_id,
        "order_id": order_id,
        "filled": filled,
        "avg_entry_price": avg_entry_price,
        "status": status,
        "fill_levels": [
            {"price": float(p), "qty": float(q)} for p, q in fill_levels
        ] if fill_levels else [],
        "orderbook_bids": [
            {"price": float(p), "size": float(s)} for p, s in bids_snapshot
        ],
        "orderbook_asks": [
            {"price": float(p), "size": float(s)} for p, s in asks_snapshot
        ],
        "token_id": token_id,
    }
    _fill_logger.info(json.dumps(record, ensure_ascii=False))


def _serialize_fill_levels(levels: list) -> str:
    """Serialize fill levels [(price, qty)] to JSON string of [{price, qty, cost}]."""
    if not levels:
        return ""
    import json
    return json.dumps([
        {
            "price": float(p),
            "qty": float(q),
            "cost": round(float(p) * float(q), 8),
        }
        for p, q in levels
    ])


class OrderConsumer:
    """
    Daemon thread that pops virtual orders from Redis and places them
    in the matching engine.

    Order monitoring is event-driven: a single state-change callback
    registered per book replaces the old per-order polling threads.
    """

    def __init__(
        self,
        sync_redis: redis.Redis,
        session_manager: SessionManager,
        redis_writer: RedisWriter,
        loop: asyncio.AbstractEventLoop,
        registry=None,
    ) -> None:
        self._r = sync_redis
        self._session_manager = session_manager
        self._writer = redis_writer
        self._loop = loop
        self._running = False
        self._thread: Optional[threading.Thread] = None
        # ── Centralized monitoring state ──────────────────────────────────
        self._order_to_bo: dict[str, int] = {}       # order_id → bo_id
        self._order_to_token: dict[str, str] = {}    # order_id → token_id
        self._registered_books: set[str] = set()      # token_ids with callback

    def start(self) -> None:
        """Start the consumer daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="order-consumer", daemon=True,
        )
        self._thread.start()
        logger.info("OrderConsumer started")

    def stop(self) -> None:
        """Signal the consumer to stop (blocks until thread exits)."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=BRPOP_TIMEOUT_S + 2)
        logger.info("OrderConsumer stopped")

    def _run(self) -> None:
        """Main multi-key BRPOP loop — dynamically polls active session queues."""
        while self._running:
            try:
                keys = self._session_manager.active_queue_keys()
                if not keys:
                    import time
                    time.sleep(BRPOP_TIMEOUT_S)
                    continue
                result = self._r.brpop(keys, timeout=BRPOP_TIMEOUT_S)
                if result is None:
                    continue  # timeout, check _running flag
                queue_key, raw = result
                # Extract session_id: "queue:orders:BTC:M5:1709313000" → "BTC:M5:1709313000"
                key_str = queue_key.decode() if isinstance(queue_key, bytes) else queue_key
                session_id = key_str.split(":", 2)[2] if key_str.count(":") >= 2 else None
                self._process_order(raw, session_id)
            except Exception as exc:
                if self._running:
                    logger.error("OrderConsumer BRPOP error: %s", exc)

    def _process_order(self, raw: str, session_id: Optional[str] = None) -> None:
        """Parse JSON and place virtual order in the matching engine."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("OrderConsumer: invalid JSON: %s", exc)
            return

        bo_id = data.get("bo_id")
        token_id = data.get("token_id")
        is_prefilled = data.get("prefilled", False)

        if token_id is None:
            logger.warning(
                "OrderConsumer: no token_id in payload (bo_id=%s) — rejecting",
                bo_id,
            )
            if bo_id is not None:
                self._publish_async(
                    self._writer.publish_order_cancel(
                        bo_id=bo_id, order_id="",
                        reason="NO_TOKEN_ID",
                        filled=0.0, avg_entry_price=0.0,
                    )
                )
            return

        # Resolve session from queue key or payload
        if session_id is None:
            session_id = data.get("session_id")

        # Validate session exists
        session = self._session_manager.get_session(session_id) if session_id else None
        if session is None:
            logger.warning(
                "OrderConsumer: session %s not found for bo_id=%s — rejecting",
                session_id, bo_id,
            )
            if bo_id is not None:
                self._publish_async(
                    self._writer.publish_order_cancel(
                        bo_id=bo_id, order_id="",
                        reason="SESSION_NOT_FOUND",
                        filled=0.0, avg_entry_price=0.0,
                    )
                )
            return

        # Ensure session owns this token
        self._session_manager.add_valid_token(token_id)

        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")
        has_bracket = tp_price is not None or sl_price is not None

        on_bracket_exit = None
        if has_bracket and bo_id is not None:
            on_bracket_exit = self._make_bracket_callback(bo_id)

        if is_prefilled:
            # ── Pre-filled MARKET order: register for bracket monitoring only ──
            self._process_prefilled_order(data, bo_id, token_id, on_bracket_exit, session)
        else:
            # ── Standard order flow (LIMIT or legacy MARKET) ──────────────────
            self._process_standard_order(data, bo_id, token_id, on_bracket_exit, session)

    def _process_prefilled_order(
        self, data: dict, bo_id: Optional[int],
        token_id: Optional[str], on_bracket_exit,
        session=None,
    ) -> None:
        """Handle a pre-filled MARKET order — register for bracket monitoring only."""
        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")

        # Compute expire_at from settlement_at so bracket-monitoring orders
        # are auto-cleaned when the session settles (prevents zombie orders).
        expire_at = None
        settlement_at_str = data.get("settlement_at")
        if settlement_at_str:
            expire_at = datetime.fromisoformat(settlement_at_str)

        try:
            order, bracket_results = session.place_prefilled_bracket_order(
                token_id=token_id,
                side=OrderSide.BUY,
                avg_entry_price=Decimal(str(data["prefilled_avg_price"])),
                filled=Decimal(str(data["prefilled_filled"])),
                tp_price=Decimal(str(tp_price)) if tp_price else None,
                sl_price=Decimal(str(sl_price)) if sl_price else None,
                on_bracket_exit=on_bracket_exit,
                expire_at=expire_at,
            )

            logger.info(
                "Prefilled bracket order registered from queue: bo_id=%s "
                "me_order=%s filled=%s avg=%s tp=%s sl=%s session=%s",
                bo_id, order.order_id[:12],
                order.filled, order.avg_entry_price,
                tp_price, sl_price, session.session_id,
            )

            # Register for centralized monitoring (bracket exits)
            if bo_id is not None:
                self._order_to_bo[order.order_id] = bo_id
                self._order_to_token[order.order_id] = token_id
                book = self._session_manager.get_book(token_id)
                if book is not None:
                    # Seed as FILLED so no duplicate fill events are emitted
                    book.seed_last_reported(order.order_id, order.filled, order.status)
                    if token_id not in self._registered_books:
                        book.register_state_change_callback(self._on_state_changes)
                        self._registered_books.add(token_id)
                        logger.info(
                            "Registered state-change callback on book %s",
                            token_id[:16],
                        )

            # Fire bracket exit callbacks AFTER registration is complete.
            # For prefilled orders, DB already has avg_price/num_shares from
            # order creation, so bracket exit consumer can safely read them.
            for br in bracket_results:
                if on_bracket_exit is not None:
                    on_bracket_exit(br)
        except ValueError as exc:
            # Book expired (token rotated) — publish cancel
            logger.warning(
                "Prefilled order rejected (expired book) for bo_id=%s: %s",
                bo_id, exc,
            )
            if bo_id is not None:
                self._publish_async(
                    self._writer.publish_order_cancel(
                        bo_id=bo_id,
                        order_id="",
                        reason="TOKEN_ROTATED",
                        filled=0.0,
                        avg_entry_price=0.0,
                    )
                )
        except Exception as exc:
            logger.error(
                "Failed to place prefilled bracket order for bo_id=%s: %s",
                bo_id, exc,
            )

    def _process_standard_order(
        self, data: dict, bo_id: Optional[int],
        token_id: Optional[str], on_bracket_exit,
        session=None,
    ) -> None:
        """Handle standard LIMIT/MARKET order flow via matching engine."""
        side = OrderSide(data.get("side", "BUY"))
        price = Decimal(str(data["price"]))
        quantity = Decimal(str(data["quantity"]))
        amount = data.get("amount")
        limit_price = data.get("limit_price")
        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")
        timeframe = data.get("timeframe")
        ttl = data.get("ttl")
        slippage_tolerance = data.get("slippage_tolerance")
        session_offset = data.get("session_offset", 0)
        settlement_at_str = data.get("settlement_at")

        is_market = limit_price is None

        # TTL: pass as ttl_seconds so matching engine uses raw offset.
        # For future sessions (offset >= 1), ensure the order lives at least
        # until settlement_at so it doesn't expire before the target candle.
        settlement_dt = None
        if settlement_at_str:
            settlement_dt = datetime.fromisoformat(settlement_at_str)

        if ttl is not None:
            ttl_seconds = float(ttl)
            # Clamp: user TTL on future sessions must not expire before
            # the target session settles — otherwise the order dies before
            # the candle even opens.
            if session_offset >= 1 and settlement_dt:
                min_ttl = max((settlement_dt - datetime.now(timezone.utc)).total_seconds(), 1.0)
                if ttl_seconds < min_ttl:
                    logger.info(
                        "TTL clamped for future session (offset=%d): "
                        "user_ttl=%.0fs < min_ttl=%.0fs → using min_ttl",
                        session_offset, ttl_seconds, min_ttl,
                    )
                    ttl_seconds = min_ttl
        elif session_offset >= 1 and settlement_dt:
            ttl_seconds = max((settlement_dt - datetime.now(timezone.utc)).total_seconds(), 1.0)
        else:
            ttl_seconds = None

        # MARKET order: recalculate quantity from ME's fresh best_ask so
        # cost ≈ amount.  Price is irrelevant for MARKET (engine skips
        # price check and sweeps all available levels).
        if is_market:
            if amount is not None and token_id is not None:
                me_best_ask = self._session_manager.best_ask(token_id)
                if me_best_ask is not None and me_best_ask > 0:
                    orig_qty = quantity
                    quantity = Decimal(str(amount)) / Decimal(str(me_best_ask))
                    logger.info(
                        "MARKET order bo_id=%s: qty=%s (amount=%s / best_ask=%s) "
                        "orig_qty=%s slippage=%s",
                        bo_id, quantity, amount, me_best_ask,
                        orig_qty, slippage_tolerance,
                    )
                else:
                    logger.warning(
                        "MARKET order bo_id=%s: best_ask unavailable "
                        "(best_ask=%s), using original qty=%s",
                        bo_id, me_best_ask, quantity,
                    )

        try:
            # For MARKET BUY, pass amount as max_cost so matching engine
            # caps cumulative cost instead of overshooting when sweeping
            # multiple ask levels above best_ask.
            cost_cap = None
            if is_market and side == OrderSide.BUY and amount is not None:
                cost_cap = Decimal(str(amount))

            order, bracket_results = session.place_virtual_order(
                token_id=token_id,
                side=side,
                price=Decimal(str(limit_price)) if limit_price is not None else price,
                quantity=quantity,
                tp_price=Decimal(str(tp_price)) if tp_price else None,
                sl_price=Decimal(str(sl_price)) if sl_price else None,
                timeframe=timeframe,
                ttl_seconds=ttl_seconds,
                on_bracket_exit=on_bracket_exit,
                order_type="MARKET" if is_market else "LIMIT",
                max_slippage=Decimal(str(slippage_tolerance)) if slippage_tolerance is not None else None,
                max_cost=cost_cap,
            )

            logger.info(
                "Virtual order placed from queue: bo_id=%s me_order=%s "
                "filled=%s avg=%s status=%s tp=%s sl=%s ttl=%s",
                bo_id, order.order_id[:12],
                order.filled, order.avg_entry_price, order.status.value,
                tp_price, sl_price, ttl,
            )

            # Step 1: Publish fill event FIRST so DB has avg_price/num_shares
            # before any bracket exit arrives.
            if bo_id is not None and order.filled > 0:
                fill_levels_snapshot = list(order._fill_levels)
                wp = _serialize_fill_levels(order._fill_levels)
                order._fill_levels = []  # reset after serialization
                self._publish_async(
                    self._writer.publish_order_fill(
                        bo_id=bo_id,
                        order_id=order.order_id,
                        filled=float(order.filled),
                        avg_entry_price=float(order.avg_entry_price) if order.avg_entry_price else 0.0,
                        status=order.status.value,
                        walk_prices=wp,
                    )
                )
                logger.info(
                    "Immediate fill published: bo_id=%s filled=%s avg=%s status=%s",
                    bo_id, order.filled, order.avg_entry_price, order.status.value,
                )

                # Log limit order fill to file with orderbook snapshot
                if not is_market:
                    book = self._session_manager.get_book(token_id)
                    bids_snap = book.depth(side="bid", levels=10) if book else []
                    asks_snap = book.depth(side="ask", levels=10) if book else []
                    _log_limit_fill(
                        bo_id=bo_id,
                        order_id=order.order_id,
                        filled=float(order.filled),
                        avg_entry_price=float(order.avg_entry_price) if order.avg_entry_price else 0.0,
                        status=order.status.value,
                        fill_levels=fill_levels_snapshot,
                        bids_snapshot=bids_snap,
                        asks_snapshot=asks_snap,
                        token_id=token_id,
                    )

            # Step 2: Register for centralized monitoring
            if bo_id is not None:
                self._order_to_bo[order.order_id] = bo_id
                self._order_to_token[order.order_id] = token_id
                # Seed last_reported so callback won't duplicate the immediate fill
                book = self._session_manager.get_book(token_id)
                if book is not None:
                    book.seed_last_reported(order.order_id, order.filled, order.status)
                    # Register callback once per book
                    if token_id not in self._registered_books:
                        book.register_state_change_callback(self._on_state_changes)
                        self._registered_books.add(token_id)
                        logger.info(
                            "Registered state-change callback on book %s",
                            token_id[:16],
                        )

                # BUG FIX: If the order was already CANCELED during
                # place_virtual_order (e.g. MARKET IOC with no/partial fill),
                # the state-change callback fired BEFORE _order_to_bo was
                # registered, so the cancel event was silently dropped.
                # Publish the cancel explicitly now.
                if order.status == OrderStatus.CANCELED:
                    self._publish_async(
                        self._writer.publish_order_cancel(
                            bo_id=bo_id,
                            order_id=order.order_id,
                            reason="MARKET_IOC_CANCEL",
                            filled=float(order.filled),
                            avg_entry_price=float(order.avg_entry_price) if order.avg_entry_price else 0.0,
                        )
                    )
                    logger.info(
                        "Immediate cancel published (post-registration): "
                        "bo_id=%s order=%s filled=%s",
                        bo_id, order.order_id[:12], order.filled,
                    )
                    # Cleanup tracking for terminal state
                    self._order_to_bo.pop(order.order_id, None)
                    self._order_to_token.pop(order.order_id, None)

            # Step 3: Fire bracket exit callbacks AFTER fill is published.
            # This ensures _handle_bracket_exit in API always finds
            # avg_price/num_shares already written by _handle_order_fill.
            for br in bracket_results:
                if on_bracket_exit is not None:
                    on_bracket_exit(br)
        except ValueError as exc:
            # Book expired (token rotated) — publish cancel so API marks order CANCELLED
            logger.warning(
                "Order rejected (expired book) for bo_id=%s: %s", bo_id, exc,
            )
            if bo_id is not None:
                self._publish_async(
                    self._writer.publish_order_cancel(
                        bo_id=bo_id,
                        order_id="",
                        reason="TOKEN_ROTATED",
                        filled=0.0,
                        avg_entry_price=0.0,
                    )
                )
        except Exception as exc:
            logger.error(
                "Failed to place virtual order for bo_id=%s: %s", bo_id, exc,
            )

    # ── Centralized state-change handler ───────────────────────────────────

    def _on_state_changes(self, events: list[OrderStateChangeEvent]) -> None:
        """
        Handle state-change events from the matching engine.

        Called by the engine after run_matching() / place_virtual_order()
        with a batch of FILL and CANCEL events.
        """
        for event in events:
            bo_id = self._order_to_bo.get(event.order_id)
            if bo_id is None:
                continue  # not our order

            if event.event_type == "FILL":
                wp = _serialize_fill_levels(event.fill_levels)
                self._publish_async(
                    self._writer.publish_order_fill(
                        bo_id=bo_id,
                        order_id=event.order_id,
                        filled=float(event.filled),
                        avg_entry_price=float(event.avg_entry_price) if event.avg_entry_price else 0.0,
                        status=event.status.value,
                        walk_prices=wp,
                    )
                )
                logger.info(
                    "State-change FILL: bo_id=%d order=%s filled=%s status=%s",
                    bo_id, event.order_id[:12], event.filled, event.status.value,
                )

                # Log limit order fill to file with orderbook snapshot
                token_id = self._order_to_token.get(event.order_id, "")
                if token_id:
                    book = self._session_manager.get_book(token_id)
                    bids_snap = book.depth(side="bid", levels=10) if book else []
                    asks_snap = book.depth(side="ask", levels=10) if book else []
                    _log_limit_fill(
                        bo_id=bo_id,
                        order_id=event.order_id,
                        filled=float(event.filled),
                        avg_entry_price=float(event.avg_entry_price) if event.avg_entry_price else 0.0,
                        status=event.status.value,
                        fill_levels=event.fill_levels,
                        bids_snapshot=bids_snap,
                        asks_snapshot=asks_snap,
                        token_id=token_id,
                    )

            elif event.event_type == "CANCEL":
                self._publish_async(
                    self._writer.publish_order_cancel(
                        bo_id=bo_id,
                        order_id=event.order_id,
                        reason=event.cancel_reason or "TTL_EXPIRED",
                        filled=float(event.filled),
                        avg_entry_price=float(event.avg_entry_price) if event.avg_entry_price else 0.0,
                    )
                )
                logger.info(
                    "State-change CANCEL: bo_id=%d order=%s filled=%s reason=%s",
                    bo_id, event.order_id[:12], event.filled, event.cancel_reason,
                )

            # Cleanup tracking for terminal states
            if event.status in (OrderStatus.FILLED, OrderStatus.CANCELED):
                self._order_to_bo.pop(event.order_id, None)
                self._order_to_token.pop(event.order_id, None)

    def _publish_async(self, coro) -> None:
        """Bridge an async coroutine to the event loop from a sync thread."""
        try:
            asyncio.run_coroutine_threadsafe(coro, self._loop)
        except Exception as exc:
            logger.error("Failed to schedule async publish: %s", exc)

    # ── Bracket exit callback ─────────────────────────────────────────────

    def _make_bracket_callback(self, bo_id: int):
        """
        Return a callback that publishes bracket exit data to Redis stream.

        The callback runs in the matching engine's thread context, so we use
        asyncio.run_coroutine_threadsafe to bridge to the async RedisWriter.
        """
        def callback(result: BracketFillResult) -> None:
            from datetime import datetime, timezone
            wp = _serialize_fill_levels(result.fill_levels)
            self._publish_async(
                self._writer.publish_bracket_exit(
                    bo_id=bo_id,
                    trigger=result.trigger,
                    exit_price=float(result.avg_exit_price),
                    exit_filled=float(result.qty_exited),
                    order_id=result.order_id,
                    exit_at=datetime.now(timezone.utc).isoformat(),
                    walk_prices=wp,
                )
            )
        return callback
