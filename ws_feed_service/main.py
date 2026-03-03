"""
WS Feed Service — standalone entry point.

Runs the Polymarket WebSocket feed, session manager, and token registry
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
from services.session_manager import SessionManager, get_session_manager
from services.session_engine import SessionState
from services.token_registry import TokenRegistry
from services.ws_feed import PolymarketFeed
from ws_feed_service.config import ORDERBOOK_DEPTH_LEVELS
from ws_feed_service.redis_writer import RedisWriter
from ws_feed_service.order_consumer import OrderConsumer

from config.timing import TF_SECONDS as _TF_SECONDS, SESSION_LIFECYCLE_TICK_S
from ws_feed_service.session_lifecycle import ensure_future_sessions, cleanup_expired_sessions
from database import SessionLocal
from models import BinaryOption, BOResult

_log_level = logging.DEBUG if os.getenv("DEBUG", "").strip() in ("1", "true", "yes") else logging.INFO

logging.basicConfig(
    level=_log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
log = logging.getLogger("ws_feed_service")


# ── Session creation from registry ────────────────────────────────────────────


def _create_sessions_from_registry(sm: SessionManager, registry: TokenRegistry) -> None:
    """
    Create initial SessionEngine instances from TokenRegistry's current + future mappings.

    Groups UP+DOWN tokens for the same (sym, tf, candle_ts) into one SessionEngine.
    """
    import time

    # Group tokens by (sym, tf, candle_ts) → {"UP": token_id, "DOWN": token_id}
    session_tokens: dict[str, dict[str, str]] = {}  # session_id → {dir: token_id}

    # Current candle tokens → ACTIVE
    for (sym, tf, direction), token_id in registry._mapping.items():
        period_s = _TF_SECONDS[tf]
        now_ts = int(time.time())
        candle_open = now_ts - (now_ts % period_s)
        session_id = f"{sym}:{tf}:{candle_open}"
        session_tokens.setdefault(session_id, {})[direction] = token_id

    for session_id, tokens in session_tokens.items():
        sm.create_session(session_id, tokens, initial_state=SessionState.ACTIVE)

    # Future candle tokens → PREFETCH
    future_session_tokens: dict[str, dict[str, str]] = {}
    for (sym, tf, direction), future_ids in registry._future_mapping.items():
        for token_id in future_ids:
            candle_ts = registry._token_sessions.get(token_id, 0)
            if candle_ts == 0:
                continue
            session_id = f"{sym}:{tf}:{candle_ts}"
            future_session_tokens.setdefault(session_id, {})[direction] = token_id

    for session_id, tokens in future_session_tokens.items():
        sm.create_session(session_id, tokens, initial_state=SessionState.PREFETCH)

    log.info(
        "Created %d ACTIVE + %d PREFETCH session(s) from registry",
        len(session_tokens), len(future_session_tokens),
    )


def _update_sessions_from_registry(sm: SessionManager, registry: TokenRegistry) -> None:
    """
    Called on token rotation. Creates new sessions for newly discovered tokens
    and transitions old sessions: detect which (sym, tf) rotated → old ACTIVE → SETTLING.
    """
    import time

    # Detect candle boundaries for each timeframe
    for tf in ["M5", "M15"]:
        period_s = _TF_SECONDS[tf]
        now_ts = int(time.time())
        new_candle_ts = now_ts - (now_ts % period_s)

        for sym in ["BTC", "ETH"]:
            # Check if we have a new current session for this (sym, tf)
            up_token = registry.get_token_id(sym, tf, "UP")
            down_token = registry.get_token_id(sym, tf, "DOWN")
            if not up_token and not down_token:
                continue

            new_session_id = f"{sym}:{tf}:{new_candle_ts}"
            existing = sm.get_session(new_session_id)

            if existing is None:
                # New session — create as ACTIVE
                tokens = {}
                if up_token:
                    tokens["UP"] = up_token
                if down_token:
                    tokens["DOWN"] = down_token
                sm.create_session(new_session_id, tokens, initial_state=SessionState.ACTIVE)
            elif existing.state == SessionState.PREFETCH:
                # Promote PREFETCH → ACTIVE
                sm.transition_session(new_session_id, SessionState.ACTIVE)

            # Transition old ACTIVE sessions for this (sym, tf) to SETTLING
            sm.on_candle_boundary(sym, tf, new_candle_ts)

    # Create PREFETCH sessions for future tokens
    future_session_tokens: dict[str, dict[str, str]] = {}
    for (sym, tf, direction), future_ids in registry._future_mapping.items():
        for token_id in future_ids:
            candle_ts = registry._token_sessions.get(token_id, 0)
            if candle_ts == 0:
                continue
            session_id = f"{sym}:{tf}:{candle_ts}"
            future_session_tokens.setdefault(session_id, {})[direction] = token_id

    for session_id, tokens in future_session_tokens.items():
        if sm.get_session(session_id) is None:
            sm.create_session(session_id, tokens, initial_state=SessionState.PREFETCH)


# ── Recovery ──────────────────────────────────────────────────────────────────


async def _recover_pending_orders(sync_redis, session_manager: SessionManager, registry: TokenRegistry) -> int:
    """
    Re-push PENDING BOs that have bracket orders (tp_price or sl_price set)
    back to the per-session Redis queue so the OrderConsumer can re-register them
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

            # Compute session_id from settlement_at
            period_s = _TF_SECONDS.get(tf_val, 300)
            if bo.settlement_at:
                candle_open = int(bo.settlement_at.timestamp()) - period_s
            else:
                now_ts = int(now.timestamp())
                candle_open = now_ts - (now_ts % period_s)
            session_id = f"{sym_val}:{tf_val}:{candle_open}"

            # Ensure session exists for recovery
            session = session_manager.get_session(session_id)
            if session is None:
                # Create session if missing (e.g. orders from previous candle)
                up_token = registry.get_token_id(sym_val, tf_val, "UP")
                down_token = registry.get_token_id(sym_val, tf_val, "DOWN")
                tokens = {}
                if up_token:
                    tokens["UP"] = up_token
                if down_token:
                    tokens["DOWN"] = down_token
                if tokens:
                    session_manager.create_session(session_id, tokens, initial_state=SessionState.ACTIVE)
                    log.info("Recovery: created session %s for BO #%d", session_id, bo.id)

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
                "session_id": session_id,
            }

            # Already-filled MARKET+bracket: set prefilled so OrderConsumer
            # registers for bracket monitoring instead of re-matching
            if is_already_filled:
                payload["prefilled"] = True
                payload["prefilled_avg_price"] = bo.avg_price
                payload["prefilled_filled"] = bo.num_shares

            order_data = json.dumps(payload)
            queue_key = f"queue:orders:{session_id}"
            sync_redis.lpush(queue_key, order_data)
            count += 1
    except Exception as exc:
        log.error("Recovery failed: %s", exc)
    finally:
        db.close()
    if skipped:
        log.warning("Recovery: skipped %d orders (no token_id)", skipped)
    return count


# ── Event dispatcher ──────────────────────────────────────────────────────────


def _make_event_dispatcher(
    session_manager: SessionManager,
    writer: RedisWriter,
    loop: asyncio.AbstractEventLoop,
):
    """
    Build a single callback for PolymarketFeed's ``on_event`` parameter.

    The callback:
      1. Dispatches every WS event to ``session_manager.dispatch_event()``
         (fan-out to per-session books, bracket monitoring, matching).
      2. After dispatch, writes best prices + orderbook depth to Redis
         for price-change events.
    """

    def event_dispatcher(event: dict) -> None:
        # 1. Fan-out to sessions (the sole matching path)
        session_manager.dispatch_event(event)

        # 2. Write prices + depth to Redis for price-affecting events
        etype = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if etype in ("book", "price_change", "best_bid_ask") and asset_id:
            book = session_manager.get_book(asset_id)
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

    return event_dispatcher


# ── Main ──────────────────────────────────────────────────────────────────────


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

    # 2. Init SessionManager + RedisWriter
    session_manager = get_session_manager()
    writer = RedisWriter(async_redis)

    # 3. Token discovery (blocking REST)
    registry = TokenRegistry()
    token_ids = registry.discover_all()

    if not token_ids:
        log.warning("No token IDs discovered — running without WS feed")

    # 4. Register token mapping in RedisWriter + create sessions
    writer.register_token_mapping(registry._mapping)
    # Register session-aware token mapping for future session orderbook writes
    current_sessions = {tf: registry.get_current_candle_open(tf) for tf in ["M5", "M15"]}
    writer.register_session_tokens(registry.get_all_token_mapping(), current_sessions)
    await writer.publish_token_mapping()

    # Create initial sessions from discovered tokens
    _create_sessions_from_registry(session_manager, registry)

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

    # 5. Build event dispatcher + start PolymarketFeed
    loop = asyncio.get_event_loop()
    event_dispatcher = _make_event_dispatcher(session_manager, writer, loop)

    feed = None
    if token_ids:
        feed = PolymarketFeed(token_ids, on_event=event_dispatcher)
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
        fresh_sessions = {tf: registry.get_current_candle_open(tf) for tf in ["M5", "M15"]}
        writer.register_session_tokens(registry.get_all_token_mapping(), fresh_sessions)
        asyncio.ensure_future(writer.publish_token_mapping())
        if rotated:
            # Clear stale Redis price keys immediately so the UI shows
            # "no data" instead of old prices while waiting for first
            # WS tick from the new session's token.
            asyncio.ensure_future(writer.clear_price_keys(rotated))

        # Update sessions from registry (create new, transition old)
        _update_sessions_from_registry(session_manager, registry)

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
            "TokenRegistry pushed %d new token(s)%s, seeded %d book(s)",
            len(ids),
            f", cleared {len(rotated)} stale price key(s)" if rotated else "",
            len(initial_books),
        )

    registry._on_new_tokens = on_new_tokens
    await registry.start()

    # 8. Start OrderConsumer daemon thread
    consumer = OrderConsumer(sync_redis, session_manager, writer, loop, registry=registry)
    consumer.start()

    # 8b. Wire up market_resolved callback on SessionManager
    # When Polymarket resolves a market early, publish affected bo_ids to Redis
    # so the FastAPI consumer can process only the correct orders.
    def _on_market_resolved(asset_id: str, resolved_orders: list) -> None:
        bo_ids = []
        for order in resolved_orders:
            bo_id = consumer._order_to_bo.get(order.order_id)
            if bo_id is not None:
                bo_ids.append(bo_id)
        if bo_ids:
            asyncio.ensure_future(
                writer.publish_market_resolved(
                    asset_id=asset_id,
                    winning_outcome="",  # ME doesn't know outcome, Polymarket WS provides it
                    bo_ids=bo_ids,
                ),
                loop=loop,
            )
            log.info(
                "Market resolved callback: asset_id=%s bo_ids=%s",
                asset_id[:16], bo_ids,
            )

    session_manager._on_market_resolved = _on_market_resolved

    # 9. Recovery: re-push PENDING BOs to per-session queues
    recovered = await _recover_pending_orders(sync_redis, session_manager, registry)
    if recovered:
        log.info("Recovery: re-pushed %d pending order(s) to queue", recovered)

    # 10. Periodic TTL expiry tick — ensures expired orders are detected
    # even when no WebSocket events arrive (idle market).
    async def _expiry_tick():
        while not shutdown_event.is_set():
            try:
                n = session_manager.expire_all_pending()
                if n:
                    log.info("Expiry tick: expired %d order(s)", n)
            except Exception as exc:
                log.error("Expiry tick error: %s", exc)
            await asyncio.sleep(5)

    # 10b. Session lifecycle tick — ensure 4 sessions per (sym, tf) + cleanup expired
    # ensure_future_sessions uses Polymarket REST (blocking) for token resolution.
    # Run in executor to avoid blocking the event loop (prevents WS PING timeout).
    # feed.add_tokens() needs asyncio context → called on main thread after executor.
    async def _session_lifecycle_tick():
        while not shutdown_event.is_set():
            try:
                _loop = asyncio.get_event_loop()
                created, new_token_ids = await _loop.run_in_executor(
                    None,
                    ensure_future_sessions,
                    session_manager, None, writer, registry,  # feed=None inside executor
                )
                # Subscribe new tokens on main thread (needs asyncio for WS resubscribe)
                if feed is not None and new_token_ids:
                    feed.add_tokens(new_token_ids)
                cleanup_expired_sessions(session_manager, sync_redis)
            except Exception as exc:
                log.error("Session lifecycle tick error: %s", exc)
            await asyncio.sleep(SESSION_LIFECYCLE_TICK_S)

    shutdown_event = asyncio.Event()
    asyncio.ensure_future(_expiry_tick())
    asyncio.ensure_future(_session_lifecycle_tick())

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
    session_manager.shutdown()
    await close_async_redis()
    close_sync_redis()
    log.info("WS Feed Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
