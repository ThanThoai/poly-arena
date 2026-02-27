"""
OrderConsumer — BRPOP loop that reads virtual orders from Redis queue
and places them in the matching engine.

Runs in a daemon thread so it doesn't block the asyncio event loop.

Monitors each order for:
  - Fill updates (PARTIAL / FILLED) → publish to stream:order:fills
  - TTL expiry (CANCELED) → publish to stream:order:cancels
"""

import asyncio
import json
import logging
import threading
from decimal import Decimal
from typing import Optional

import redis

from services.matching_engine import (
    MatchingEngine, OrderSide, OrderStatus, BracketFillResult,
)
from ws_feed_service.config import QUEUE_ORDERS_NEW, BRPOP_TIMEOUT_S
from ws_feed_service.redis_writer import RedisWriter

logger = logging.getLogger(__name__)


class OrderConsumer:
    """
    Daemon thread that pops virtual orders from Redis and places them
    in the matching engine.
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
        side = OrderSide(data.get("side", "BUY"))
        price = Decimal(str(data["price"]))
        quantity = Decimal(str(data["quantity"]))
        amount = data.get("amount")  # original dollar amount from API
        limit_price = data.get("limit_price")
        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")
        timeframe = data.get("timeframe")
        ttl = data.get("ttl")  # TTL in seconds (None = use candle expiry)

        has_bracket = tp_price is not None or sl_price is not None
        is_market = limit_price is None

        on_bracket_exit = None
        if has_bracket and bo_id is not None:
            on_bracket_exit = self._make_bracket_callback(bo_id)

        # TTL: pass as ttl_seconds so matching engine uses raw offset
        ttl_seconds = float(ttl) if ttl is not None else None

        # MARKET order: use price=1.0 to guarantee fill at best ask.
        # Recalculate quantity based on ME's best ask so cost matches amount.
        if is_market:
            price = Decimal("1")
            if amount is not None and token_id is not None:
                me_best_ask = self._engine.best_ask(token_id)
                if me_best_ask is not None and me_best_ask > 0:
                    quantity = Decimal(str(amount)) / Decimal(str(me_best_ask))
                    logger.info(
                        "MARKET order bo_id=%s: qty=%s (amount=%s / best_ask=%s)",
                        bo_id, quantity, amount, me_best_ask,
                    )

        try:
            order = self._engine.place_virtual_order(
                token_id=token_id,
                side=side,
                price=Decimal(str(limit_price)) if limit_price is not None else price,
                quantity=quantity,
                tp_price=Decimal(str(tp_price)) if tp_price else None,
                sl_price=Decimal(str(sl_price)) if sl_price else None,
                timeframe=timeframe,
                ttl_seconds=ttl_seconds,
                on_bracket_exit=on_bracket_exit,
            )

            logger.info(
                "Virtual order placed from queue: bo_id=%s me_order=%s "
                "filled=%s avg=%s status=%s tp=%s sl=%s ttl=%s",
                bo_id, order.order_id[:12],
                order.filled, order.avg_entry_price, order.status.value,
                tp_price, sl_price, ttl,
            )

            # If order filled immediately, publish fill event right away
            # so _handle_order_fill can run bracket instant settle.
            # Without this, the monitor thread would delay 2s before detecting.
            if bo_id is not None and order.filled > 0:
                self._publish_async(
                    self._writer.publish_order_fill(
                        bo_id=bo_id,
                        order_id=order.order_id,
                        filled=float(order.filled),
                        avg_entry_price=float(order.avg_entry_price) if order.avg_entry_price else 0.0,
                        status=order.status.value,
                    )
                )
                logger.info(
                    "Immediate fill published: bo_id=%s filled=%s avg=%s status=%s",
                    bo_id, order.filled, order.avg_entry_price, order.status.value,
                )

            # Start order monitor thread — tracks further fills, expiry, cancels
            if bo_id is not None:
                self._start_order_monitor(bo_id, token_id, order.order_id)
        except Exception as exc:
            logger.error(
                "Failed to place virtual order for bo_id=%s: %s", bo_id, exc,
            )

    # ── Order monitor ─────────────────────────────────────────────────────

    def _start_order_monitor(
        self, bo_id: int, token_id: str, order_id: str,
    ) -> None:
        """
        Start a daemon thread that monitors the matching engine order and
        publishes fill/cancel events to Redis streams.

        Tracks:
          - PARTIAL fills → publish_order_fill (so DB updates num_shares)
          - FILLED → publish_order_fill then stop
          - CANCELED (TTL expiry) → publish_order_cancel with fill data then stop
        """
        def _monitor():
            import time as _time
            last_filled = Decimal("0")
            consecutive_errors = 0
            MAX_ERRORS = 5

            while True:
                _time.sleep(2)  # check every 2s
                try:
                    book = self._engine.get_book(token_id)
                    if book is None:
                        return

                    # Read order state under the book lock to avoid data races
                    # with matching engine threads modifying the same fields.
                    with book._lock:
                        order = None
                        for o in book._virtual_orders:
                            if o.order_id == order_id:
                                order = o
                                break
                        if order is None:
                            return  # order pruned from book

                        # Snapshot fields while locked
                        snap_filled = order.filled
                        snap_status = order.status
                        snap_avg = order.avg_entry_price

                    consecutive_errors = 0  # reset on success

                    # ── Check for new fills ──────────────────────────
                    if snap_filled > last_filled:
                        last_filled = snap_filled
                        self._publish_async(
                            self._writer.publish_order_fill(
                                bo_id=bo_id,
                                order_id=order_id,
                                filled=float(snap_filled),
                                avg_entry_price=float(snap_avg) if snap_avg else 0.0,
                                status=snap_status.value,
                            )
                        )

                    # ── Terminal states — stop monitoring ─────────────
                    if snap_status == OrderStatus.FILLED:
                        return  # fully filled, bracket monitor takes over

                    if snap_status == OrderStatus.CANCELED:
                        # Publish cancel with partial fill info
                        filled_f = float(snap_filled)
                        avg_f = float(snap_avg) if snap_avg else 0.0
                        self._publish_async(
                            self._writer.publish_order_cancel(
                                bo_id=bo_id,
                                order_id=order_id,
                                reason="TTL_EXPIRED",
                                filled=filled_f,
                                avg_entry_price=avg_f,
                            )
                        )
                        return

                except Exception as exc:
                    consecutive_errors += 1
                    logger.error(
                        "Order monitor error for bo_id=%d (%d/%d): %s",
                        bo_id, consecutive_errors, MAX_ERRORS, exc,
                    )
                    if consecutive_errors >= MAX_ERRORS:
                        logger.error(
                            "Order monitor giving up for bo_id=%d after %d errors",
                            bo_id, MAX_ERRORS,
                        )
                        return
                    # Retry on next cycle instead of exiting immediately

        t = threading.Thread(
            target=_monitor, name=f"order-mon-{bo_id}", daemon=True,
        )
        t.start()

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
            self._publish_async(
                self._writer.publish_bracket_exit(
                    bo_id=bo_id,
                    trigger=result.trigger,
                    exit_price=float(result.avg_exit_price),
                    exit_filled=float(result.qty_exited),
                    order_id=result.order_id,
                    exit_at=datetime.now(timezone.utc).isoformat(),
                )
            )
        return callback
