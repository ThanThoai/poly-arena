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
from ws_feed_service.config import QUEUE_ORDERS_NEW, ORDERBOOK_DEPTH_LEVELS
from ws_feed_service.redis_writer import RedisWriter
from ws_feed_service.order_consumer import OrderConsumer

from database import SessionLocal
from models import BinaryOption, BOResult

_log_level = logging.DEBUG if os.getenv("DEBUG", "").strip() in ("1", "true", "yes") else logging.INFO

logging.basicConfig(
    level=_log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
log = logging.getLogger("ws_feed_service")


async def _recover_pending_orders(sync_redis, engine: MatchingEngine, registry: TokenRegistry) -> int:
    """
    Re-push PENDING BOs that have bracket orders (tp_price or sl_price set)
    back to the Redis queue so the OrderConsumer can re-register them
    in the matching engine after a restart.

    Returns the number of orders re-pushed.
    """
    import json

    # GREEN → UP, RED → DOWN
    _FORECAST_TO_DIR = {"GREEN": "UP", "RED": "DOWN"}

    db = SessionLocal()
    count = 0
    skipped = 0
    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)

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
            if not (has_bracket or is_limit) and bo.me_order_status != "PENDING":
                continue

            # Skip orders already processed by matching engine (FILLED/CANCELED)
            if bo.me_order_status in ("CANCELED", "FILLED"):
                log.info(
                    "Recovery: skipping BO #%d — already %s",
                    bo.id, bo.me_order_status,
                )
                continue

            # Unfilled LIMIT orders: avg_price/num_shares are NULL by design.
            # Re-push them with limit_price as price and amount/limit_price as quantity
            # so the matching engine can attempt to fill them.
            is_unfilled = bo.avg_price is None or bo.num_shares is None
            if is_unfilled and not is_limit:
                # Non-LIMIT order with NULL fill data — truly corrupt, skip
                skipped += 1
                log.warning(
                    "Recovery: skipping BO #%d — avg_price/num_shares is NULL (non-LIMIT)",
                    bo.id,
                )
                continue

            # Calculate remaining TTL based on original creation time
            # instead of re-using the full original TTL value
            ttl_remaining = None
            if bo.ttl is not None and bo.created_at is not None:
                elapsed = (now - bo.created_at).total_seconds()
                ttl_remaining = max(1, bo.ttl - elapsed)
                if ttl_remaining <= 1:
                    # TTL already expired — skip recovery, settlement will handle it
                    log.info(
                        "Recovery: skipping BO #%d — TTL expired (elapsed=%.0fs, ttl=%ds)",
                        bo.id, elapsed, bo.ttl,
                    )
                    continue

            # Resolve token_id from registry instead of using None
            tf_val = bo.timeframe.value if hasattr(bo.timeframe, 'value') else bo.timeframe
            sym_val = bo.symbol.value if hasattr(bo.symbol, 'value') else bo.symbol
            forecast_val = bo.forecast.value if hasattr(bo.forecast, 'value') else bo.forecast
            direction = _FORECAST_TO_DIR.get(forecast_val, "UP")
            token_id = registry.get_token_id(sym_val, tf_val, direction)
            if token_id is None:
                skipped += 1
                log.warning(
                    "Recovery: skipping BO #%d — no token_id for %s/%s/%s",
                    bo.id, sym_val, tf_val, direction,
                )
                continue

            # Determine if this is an already-filled order (MARKET+bracket)
            # that should be registered as prefilled (monitoring only)
            is_already_filled = (
                not is_unfilled
                and not is_limit
                and has_bracket
            )

            if is_unfilled:
                # Unfilled LIMIT: use limit_price as price, amount/limit_price as quantity
                rec_price = bo.limit_price
                rec_quantity = round(bo.amount / bo.limit_price, 8)
            else:
                rec_price = bo.avg_price
                rec_quantity = bo.num_shares

            payload = {
                "bo_id": bo.id,
                "token_id": token_id,
                "side": "BUY",
                "price": rec_price,
                "quantity": rec_quantity,
                "limit_price": bo.limit_price,
                "tp_price": bo.tp_price,
                "sl_price": bo.sl_price,
                "timeframe": tf_val,
                "ttl": ttl_remaining,
            }

            # Already-filled MARKET+bracket: set prefilled so OrderConsumer
            # registers for bracket monitoring instead of re-matching
            if is_already_filled:
                payload["prefilled"] = True
                payload["prefilled_avg_price"] = bo.avg_price
                payload["prefilled_filled"] = bo.num_shares

            order_data = json.dumps(payload)
            sync_redis.lpush(QUEUE_ORDERS_NEW, order_data)
            count += 1
    except Exception as exc:
        log.error("Recovery failed: %s", exc)
    finally:
        db.close()
    if skipped:
        log.warning("Recovery: skipped %d orders (no token_id)", skipped)
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
                best_bid = book.best_bid()
                if best_ask is not None:
                    coro = writer.update_price(
                        asset_id,
                        float(best_ask),
                        float(best_bid) if best_bid is not None else None,
                    )
                    try:
                        asyncio.ensure_future(coro, loop=loop)
                    except Exception:
                        pass

                # Publish orderbook depth (raw Polymarket data, not shadow)
                try:
                    bid_depth = book.raw_depth("bid", ORDERBOOK_DEPTH_LEVELS)
                    ask_depth = book.raw_depth("ask", ORDERBOOK_DEPTH_LEVELS)
                    depth_coro = writer.update_orderbook(
                        asset_id, bid_depth, ask_depth,
                    )
                    asyncio.ensure_future(depth_coro, loop=loop)
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

    # 4. Register token mapping in RedisWriter + valid tokens in engine
    writer.register_token_mapping(registry._mapping)
    # Register session-aware token mapping for future session orderbook writes
    current_sessions = {tf: registry.get_current_candle_open(tf) for tf in ["M5", "M15", "H1"]}
    writer.register_session_tokens(registry.get_all_token_mapping(), current_sessions)
    engine.register_valid_tokens(list(registry._mapping.values()))

    # Seed Redis with initial orderbook depth from REST discovery
    initial_books = registry.pop_initial_books()
    if initial_books:
        from decimal import Decimal
        for token_id, (bids, asks) in initial_books.items():
            dec_bids = [(Decimal(str(p)), Decimal(str(s))) for p, s in bids]
            dec_asks = [(Decimal(str(p)), Decimal(str(s))) for p, s in asks]
            await writer.update_orderbook(token_id, dec_bids, dec_asks)
            best_ask = min((p for p, _ in asks), default=None)
            best_bid = max((p for p, _ in bids), default=None)
            if best_ask is not None:
                await writer.update_price(token_id, best_ask, best_bid)
        log.info("Seeded Redis with %d initial orderbook(s) from REST", len(initial_books))

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
        # Re-register mapping so new tokens get price writes.
        # Returns combos whose token_id rotated + the old token_ids.
        rotated, old_token_ids = writer.register_token_mapping(registry._mapping)
        # Re-register session tokens with fresh data
        fresh_sessions = {tf: registry.get_current_candle_open(tf) for tf in ["M5", "M15", "H1"]}
        writer.register_session_tokens(registry.get_all_token_mapping(), fresh_sessions)
        if rotated:
            # Clear stale Redis price keys immediately so the UI shows
            # "no data" instead of old prices while waiting for first
            # WS tick from the new session's token.
            asyncio.ensure_future(writer.clear_price_keys(rotated))
        # Update valid token set: add new, remove old
        engine.register_valid_tokens(list(registry._mapping.values()))
        if old_token_ids:
            # Expire old books so pending LIMIT orders don't fill against
            # stale asks from the previous candle (e.g. $0.01 after RED).
            engine.invalidate_books(old_token_ids)

        # Seed Redis with initial orderbook depth from REST fetch.
        # Critical for M5: tokens rotate every 5 minutes and Polymarket WS
        # may not send events for new tokens immediately, leaving Redis empty.
        initial_books = registry.pop_initial_books()
        if initial_books:
            from decimal import Decimal
            async def _seed():
                for token_id, (bids, asks) in initial_books.items():
                    dec_bids = [(Decimal(str(p)), Decimal(str(s))) for p, s in bids]
                    dec_asks = [(Decimal(str(p)), Decimal(str(s))) for p, s in asks]
                    await writer.update_orderbook(token_id, dec_bids, dec_asks)
                    best_ask = min((p for p, _ in asks), default=None)
                    best_bid = max((p for p, _ in bids), default=None)
                    if best_ask is not None:
                        await writer.update_price(token_id, best_ask, best_bid)
            asyncio.ensure_future(_seed())

        log.info(
            "TokenRegistry pushed %d new token(s)%s%s, seeded %d book(s)",
            len(ids),
            f", cleared {len(rotated)} stale price key(s)" if rotated else "",
            f", expired {len(old_token_ids)} old book(s)" if old_token_ids else "",
            len(initial_books),
        )

    registry._on_new_tokens = on_new_tokens
    await registry.start()

    # 8. Start OrderConsumer daemon thread
    consumer = OrderConsumer(sync_redis, engine, writer, loop, registry=registry)
    consumer.start()

    # 9. Recovery: re-push PENDING BOs
    recovered = await _recover_pending_orders(sync_redis, engine, registry)
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
