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
import threading
from decimal import Decimal
from typing import Optional

import redis

from services.matching_engine import (
    MatchingEngine, OrderSide, OrderStatus, OrderStateChangeEvent,
    BracketFillResult,
)
from ws_feed_service.config import QUEUE_ORDERS_NEW, BRPOP_TIMEOUT_S
from ws_feed_service.redis_writer import RedisWriter

logger = logging.getLogger(__name__)


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
        engine: MatchingEngine,
        redis_writer: RedisWriter,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._r = sync_redis
        self._engine = engine
        self._writer = redis_writer
        self._loop = loop
        self._running = False
        self._thread: Optional[threading.Thread] = None
        # ── Centralized monitoring state ──────────────────────────────────
        self._order_to_bo: dict[str, int] = {}       # order_id → bo_id
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
        """Main BRPOP loop."""
        while self._running:
            try:
                result = self._r.brpop(QUEUE_ORDERS_NEW, timeout=BRPOP_TIMEOUT_S)
                if result is None:
                    continue  # timeout, check _running flag
                _key, raw = result
                self._process_order(raw)
            except Exception as exc:
                if self._running:
                    logger.error("OrderConsumer BRPOP error: %s", exc)

    def _process_order(self, raw: str) -> None:
        """Parse JSON and place virtual order in the matching engine."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("OrderConsumer: invalid JSON: %s", exc)
            return

        bo_id = data.get("bo_id")
        token_id = data.get("token_id")
        is_prefilled = data.get("prefilled", False)

        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")
        has_bracket = tp_price is not None or sl_price is not None

        on_bracket_exit = None
        if has_bracket and bo_id is not None:
            on_bracket_exit = self._make_bracket_callback(bo_id)

        if is_prefilled:
            # ── Pre-filled MARKET order: register for bracket monitoring only ──
            self._process_prefilled_order(data, bo_id, token_id, on_bracket_exit)
        else:
            # ── Standard order flow (LIMIT or legacy MARKET) ──────────────────
            self._process_standard_order(data, bo_id, token_id, on_bracket_exit)

    def _process_prefilled_order(
        self, data: dict, bo_id: Optional[int],
        token_id: Optional[str], on_bracket_exit,
    ) -> None:
        """Handle a pre-filled MARKET order — register for bracket monitoring only."""
        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")

        try:
            order, bracket_results = self._engine.place_prefilled_bracket_order(
                token_id=token_id,
                side=OrderSide.BUY,
                avg_entry_price=Decimal(str(data["prefilled_avg_price"])),
                filled=Decimal(str(data["prefilled_filled"])),
                tp_price=Decimal(str(tp_price)) if tp_price else None,
                sl_price=Decimal(str(sl_price)) if sl_price else None,
                on_bracket_exit=on_bracket_exit,
            )

            logger.info(
                "Prefilled bracket order registered from queue: bo_id=%s "
                "me_order=%s filled=%s avg=%s tp=%s sl=%s",
                bo_id, order.order_id[:12],
                order.filled, order.avg_entry_price,
                tp_price, sl_price,
            )

            # Register for centralized monitoring (bracket exits)
            if bo_id is not None:
                self._order_to_bo[order.order_id] = bo_id
                book = self._engine.get_book(token_id)
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
        except Exception as exc:
            logger.error(
                "Failed to place prefilled bracket order for bo_id=%s: %s",
                bo_id, exc,
            )

    def _process_standard_order(
        self, data: dict, bo_id: Optional[int],
        token_id: Optional[str], on_bracket_exit,
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

        is_market = limit_price is None

        # TTL: pass as ttl_seconds so matching engine uses raw offset
        ttl_seconds = float(ttl) if ttl is not None else None

        # MARKET order: recalculate quantity from ME's fresh best_ask so
        # cost ≈ amount.  Price is irrelevant for MARKET (engine skips
        # price check and sweeps all available levels).
        if is_market:
            if amount is not None and token_id is not None:
                me_best_ask = self._engine.best_ask(token_id)
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

            order, bracket_results = self._engine.place_virtual_order(
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

            # Step 2: Register for centralized monitoring
            if bo_id is not None:
                self._order_to_bo[order.order_id] = bo_id
                # Seed last_reported so callback won't duplicate the immediate fill
                book = self._engine.get_book(token_id)
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

            # Step 3: Fire bracket exit callbacks AFTER fill is published.
            # This ensures _handle_bracket_exit in API always finds
            # avg_price/num_shares already written by _handle_order_fill.
            for br in bracket_results:
                if on_bracket_exit is not None:
                    on_bracket_exit(br)
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
