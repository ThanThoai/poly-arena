"""
WS Feed Service — standalone entry point.

Runs the Polymarket WebSocket feed, matching engine, and token registry
as an independent process. Communicates with the FastAPI process via Redis.

Usage:
    python -m ws_feed_service.main
"""

import asyncio
import logging
import os
import signal
import sys

# Ensure project root is on sys.path so we can import services.*
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.redis_client import get_async_redis, get_sync_redis, close_async_redis, close_sync_redis
from services.matching_engine import MatchingEngine, get_engine
from services.token_registry import TokenRegistry
from services.ws_feed import PolymarketFeed
from ws_feed_service.config import QUEUE_ORDERS_NEW
from ws_feed_service.redis_writer import RedisWriter
from ws_feed_service.order_consumer import OrderConsumer

from database import SessionLocal
from models import BinaryOption, BOResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
log = logging.getLogger("ws_feed_service")


async def _recover_pending_orders(sync_redis, engine: MatchingEngine) -> int:
    """
    Re-push PENDING BOs that have bracket orders (tp_price or sl_price set)
    back to the Redis queue so the OrderConsumer can re-register them
    in the matching engine after a restart.

    Returns the number of orders re-pushed.
    """
    import json
    db = SessionLocal()
    count = 0
    try:
        pending = (
            db.query(BinaryOption)
            .filter(
                BinaryOption.result == BOResult.PENDING,
                BinaryOption.exit_trigger.is_(None),
            )
            .all()
        )
        for bo in pending:
            has_bracket = bo.tp_price is not None or bo.sl_price is not None
            is_limit = bo.limit_price is not None
            if not (has_bracket or is_limit):
                continue

            order_data = json.dumps({
                "bo_id": bo.id,
                "token_id": None,  # will need token discovery
                "side": "BUY",
                "price": bo.avg_price,
                "quantity": bo.num_shares,
                "limit_price": bo.limit_price,
                "tp_price": bo.tp_price,
                "sl_price": bo.sl_price,
                "timeframe": bo.timeframe.value if hasattr(bo.timeframe, 'value') else bo.timeframe,
            })
            sync_redis.lpush(QUEUE_ORDERS_NEW, order_data)
            count += 1
    except Exception as exc:
        log.error("Recovery failed: %s", exc)
    finally:
        db.close()
    return count


def _patch_dispatch_event(engine: MatchingEngine, writer: RedisWriter, loop: asyncio.AbstractEventLoop):
    """
    Monkey-patch engine.dispatch_event to also write prices to Redis
    after book/price_change/best_bid_ask events.

    This avoids modifying matching_engine.py or ws_feed.py.
    """
    original_dispatch = engine.dispatch_event

    def patched_dispatch(event: dict) -> None:
        original_dispatch(event)

        etype = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if etype in ("book", "price_change", "best_bid_ask") and asset_id:
            book = engine.get_book(asset_id)
            if book is not None:
                best_ask = book.best_ask()
                if best_ask is not None:
                    coro = writer.update_price(asset_id, float(best_ask))
                    try:
                        asyncio.run_coroutine_threadsafe(coro, loop)
                    except Exception:
                        pass

    engine.dispatch_event = patched_dispatch
    log.info("Patched engine.dispatch_event to write prices to Redis")


async def main():
    log.info("WS Feed Service starting...")

    # 1. Connect Redis
    async_redis = get_async_redis()
    sync_redis = get_sync_redis()

    # Verify connectivity
    try:
        await async_redis.ping()
        log.info("Redis connected: %s", os.getenv("REDIS_URL", "redis://localhost:6379"))
    except Exception as exc:
        log.error("Cannot connect to Redis: %s", exc)
        sys.exit(1)

    # 2. Init MatchingEngine + RedisWriter
    engine = get_engine()
    writer = RedisWriter(async_redis)

    # 3. Token discovery (blocking REST)
    registry = TokenRegistry()
    token_ids = registry.discover_all()

    if not token_ids:
        log.warning("No token IDs discovered — running without WS feed")

    # 4. Register token mapping in RedisWriter
    writer.register_token_mapping(registry._mapping)

    # 5. Patch engine.dispatch_event to write Redis
    loop = asyncio.get_event_loop()
    _patch_dispatch_event(engine, writer, loop)

    # 6. Start PolymarketFeed
    feed = None
    if token_ids:
        feed = PolymarketFeed(token_ids)
        await feed.start()
        log.info("PolymarketFeed started with %d token(s)", len(token_ids))

    # 7. Start TokenRegistry refresh loop
    def on_new_tokens(ids: list[str]) -> None:
        if feed is not None:
            feed.add_tokens(ids)
        # Re-register mapping so new tokens get price writes
        writer.register_token_mapping(registry._mapping)
        log.info("TokenRegistry pushed %d new token(s)", len(ids))

    registry._on_new_tokens = on_new_tokens
    await registry.start()

    # 8. Start OrderConsumer daemon thread
    consumer = OrderConsumer(sync_redis, engine, writer, loop)
    consumer.start()

    # 9. Recovery: re-push PENDING BOs
    recovered = await _recover_pending_orders(sync_redis, engine)
    if recovered:
        log.info("Recovery: re-pushed %d pending order(s) to queue", recovered)

    # 10. Wait for shutdown signal
    shutdown_event = asyncio.Event()

    def _signal_handler():
        log.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    log.info("WS Feed Service running — press Ctrl+C to stop")
    await shutdown_event.wait()

    # ── Cleanup ──────────────────────────────────────────────────────────────
    log.info("Shutting down...")
    consumer.stop()
    await registry.stop()
    if feed is not None:
        await feed.stop()
    engine.shutdown()
    await close_async_redis()
    close_sync_redis()
    log.info("WS Feed Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
