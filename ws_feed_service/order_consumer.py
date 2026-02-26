"""
OrderConsumer — BRPOP loop that reads virtual orders from Redis queue
and places them in the matching engine.

Runs in a daemon thread so it doesn't block the asyncio event loop.
"""

import asyncio
import json
import logging
import threading
from decimal import Decimal
from typing import Optional

import redis

from services.matching_engine import MatchingEngine, OrderSide, BracketFillResult
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
        limit_price = data.get("limit_price")
        tp_price = data.get("tp_price")
        sl_price = data.get("sl_price")
        timeframe = data.get("timeframe")

        has_bracket = tp_price is not None or sl_price is not None

        on_bracket_exit = None
        if has_bracket and bo_id is not None:
            on_bracket_exit = self._make_bracket_callback(bo_id)

        try:
            order = self._engine.place_virtual_order(
                token_id=token_id,
                side=side,
                price=Decimal(str(limit_price)) if limit_price is not None else price,
                quantity=quantity,
                tp_price=Decimal(str(tp_price)) if tp_price else None,
                sl_price=Decimal(str(sl_price)) if sl_price else None,
                timeframe=timeframe,
                on_bracket_exit=on_bracket_exit,
            )
            logger.info(
                "Virtual order placed from queue: bo_id=%s me_order=%s "
                "tp=%s sl=%s",
                bo_id, order.order_id[:12], tp_price, sl_price,
            )
        except Exception as exc:
            logger.error(
                "Failed to place virtual order for bo_id=%s: %s", bo_id, exc,
            )

    def _make_bracket_callback(self, bo_id: int):
        """
        Return a callback that publishes bracket exit data to Redis stream.

        The callback runs in the matching engine's thread context, so we use
        asyncio.run_coroutine_threadsafe to bridge to the async RedisWriter.
        """
        def callback(result: BracketFillResult) -> None:
            coro = self._writer.publish_bracket_exit(
                bo_id=bo_id,
                trigger=result.trigger,
                exit_price=float(result.avg_exit_price),
                exit_filled=float(result.qty_exited),
                order_id=result.order_id,
            )
            try:
                asyncio.run_coroutine_threadsafe(coro, self._loop)
            except Exception as exc:
                logger.error(
                    "Failed to schedule bracket exit publish for bo_id=%d: %s",
                    bo_id, exc,
                )
        return callback
