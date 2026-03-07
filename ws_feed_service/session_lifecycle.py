"""
Session Lifecycle — ensure 3 active sessions per (sym, tf) + cleanup expired ones.

Called every SESSION_LIFECYCLE_TICK_S seconds from the WS Feed Service main loop.

Responsibilities:
  - ensure_future_sessions(): maintain current + 2 future sessions per (sym, tf)
  - cleanup_expired_sessions(): archive expired sessions (keeps ARCHIVED in registry)
  - purge_archived_sessions(): remove ARCHIVED sessions after ARCHIVED_RETENTION_S
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config.timing import (
    TF_SECONDS,
    REQUIRED_FUTURE_SESSIONS,
    SESSION_PRE_CREATE_BUFFER_S,
    SESSION_CLEANUP_DELAY_S,
    ARCHIVED_RETENTION_S,
)
from services.session_engine import SessionState
from ws_feed_service.config import SYMBOLS, TIMEFRAMES, ORDERBOOK_KEY_PREFIX

if TYPE_CHECKING:
    from services.session_manager import SessionManager
    from services.token_registry import TokenRegistry
    from services.ws_feed import PolymarketFeed
    from ws_feed_service.redis_writer import RedisWriter

logger = logging.getLogger(__name__)


def _expected_candle_opens(
    timeframe: str,
    now_ts: int,
    num_future: int = REQUIRED_FUTURE_SESSIONS,
) -> list[int]:
    """
    Return expected candle_open timestamps: current + num_future future sessions.

    If we are within SESSION_PRE_CREATE_BUFFER_S of the next boundary,
    add one extra future session (pre-create) to keep the invariant
    when the current session expires.
    """
    period_s = TF_SECONDS[timeframe]
    current_open = now_ts - (now_ts % period_s)
    next_boundary = current_open + period_s
    time_to_boundary = next_boundary - now_ts

    opens = [current_open + i * period_s for i in range(num_future + 1)]

    # Pre-create: if <20s to boundary, add one extra future
    if time_to_boundary <= SESSION_PRE_CREATE_BUFFER_S:
        extra = current_open + (num_future + 1) * period_s
        if extra not in opens:
            opens.append(extra)

    return opens


def ensure_future_sessions(
    sm: SessionManager,
    feed: PolymarketFeed | None,
    writer: RedisWriter,
    registry: TokenRegistry,
) -> tuple[int, list[str]]:
    """
    For each (sym, tf), ensure we have sessions for current + 2 future candles.

    Creates missing sessions by resolving tokens via Polymarket REST API directly
    (no TokenRegistry dependency for token resolution — avoids stale cache issues).

    Returns (created_count, new_token_ids).
    Note: Does NOT call feed.add_tokens() — caller is responsible for WS subscription
    (needed when running in executor thread where asyncio context is unavailable).

    Side effects: updates registry._future_mapping, registry._token_sessions,
    and registry._initial_books so that _register_writer() and the caller's
    initial-book seeding logic include the newly resolved tokens.
    """
    now_ts = int(time.time())
    created = 0
    all_new_token_ids: list[str] = []

    # Build set of existing session_ids for fast lookup
    existing_sessions = {s.session_id for s in sm.list_sessions()}

    # Lazy-init: reuse a single HTTP client for all REST calls in this tick
    _pm = None

    try:
        for sym in SYMBOLS:
            for tf in TIMEFRAMES:
                expected_opens = _expected_candle_opens(tf, now_ts)

                for candle_open in expected_opens:
                    session_id = f"{sym}:{tf}:{candle_open}"
                    if session_id in existing_sessions:
                        continue

                    # Create shared client on first actual resolve
                    if _pm is None:
                        try:
                            from services.polymarket import PolymarketClient
                            from config.timing import HTTP_TIMEOUT
                            _pm = PolymarketClient(timeout=HTTP_TIMEOUT)
                        except Exception:
                            _pm = None  # Fall through — _resolve_tokens creates its own

                    # Resolve UP+DOWN tokens + fetch initial orderbook depth
                    tokens, initial_books = _resolve_tokens(sym, tf, candle_open, pm=_pm)
                    if not tokens:
                        # Polymarket hasn't published this market yet — retry next tick
                        continue

                    # Determine initial state
                    period_s = TF_SECONDS[tf]
                    current_open = now_ts - (now_ts % period_s)
                    initial_state = SessionState.ACTIVE if candle_open == current_open else SessionState.PREFETCH

                    # Create session
                    sm.create_session(session_id, tokens, initial_state=initial_state)
                    created += 1

                    # Collect new token IDs for batch WS subscription
                    all_new_token_ids.extend(tokens.values())

                    # ── Update registry so _register_writer includes these tokens ──
                    for direction, token_id in tokens.items():
                        key = (sym, tf, direction)
                        if candle_open == current_open:
                            # Current candle: update _mapping directly
                            registry._mapping[key] = token_id
                        else:
                            # Future candle: append to _future_mapping
                            if key not in registry._future_mapping:
                                registry._future_mapping[key] = []
                            if token_id not in registry._future_mapping[key]:
                                registry._future_mapping[key].append(token_id)
                        # Track session timestamp for this token
                        registry._token_sessions[token_id] = candle_open

                    # Store initial books for Redis seeding by caller
                    registry._initial_books.update(initial_books)

                    logger.info(
                        "Session lifecycle: created %s (state=%s, tokens=%d, books=%d)",
                        session_id, initial_state.value, len(tokens), len(initial_books),
                    )
    finally:
        if _pm is not None:
            try:
                _pm.close()
            except Exception:
                pass

    # Batch: re-register writer once after all sessions created
    if created:
        _register_writer(writer, registry)
        logger.info("Session lifecycle: created %d new session(s)", created)

    return created, all_new_token_ids


def _resolve_tokens(
    sym: str,
    tf: str,
    candle_open: int,
    pm=None,
) -> tuple[dict[str, str], dict[str, tuple[list, list]]]:
    """
    Resolve UP+DOWN token_ids and fetch initial orderbook depth.

    Returns (tokens, initial_books) where:
      tokens: {"UP": token_id, "DOWN": token_id}
      initial_books: {token_id: (bids, asks)}

    Accepts an optional PolymarketClient to reuse across calls.
    """
    tokens: dict[str, str] = {}
    initial_books: dict[str, tuple[list, list]] = {}

    try:
        from services.polymarket import PolymarketClient
        from config.timing import HTTP_TIMEOUT

        should_close = pm is None
        if pm is None:
            pm = PolymarketClient(timeout=HTTP_TIMEOUT)

        try:
            for direction in ("UP", "DOWN"):
                token_id = pm.get_token_id_at(sym, tf, direction, candle_open)
                if token_id:
                    tokens[direction] = token_id
                    # Fetch initial orderbook depth for Redis seeding
                    try:
                        _, _, bids, asks = pm._fetch_book_depth(token_id)
                        if bids or asks:
                            initial_books[token_id] = (bids, asks)
                    except Exception:
                        pass  # Non-critical: WS will populate eventually
        finally:
            if should_close:
                pm.close()
    except Exception as exc:
        logger.warning(
            "Session lifecycle: REST resolve failed %s %s ts=%d — %s",
            sym, tf, candle_open, exc,
        )

    return tokens, initial_books


def _register_writer(writer: RedisWriter, registry: TokenRegistry) -> None:
    """Re-register token mappings in RedisWriter after session creation."""
    writer.register_token_mapping(registry._mapping)
    fresh_sessions = {tf: registry.get_current_candle_open(tf) for tf in TIMEFRAMES}
    writer.register_session_tokens(registry.get_all_token_mapping(), fresh_sessions)


def check_orderbook_keys(
    sm: SessionManager,
    sync_redis,
    registry: TokenRegistry,
) -> tuple[int, int]:
    """
    Periodic health check (every 5s): verify every non-ARCHIVED session has
    its 2 expected orderbook Redis keys (UP + DOWN per session).

    Missing keys are re-seeded from Polymarket REST API.

    Returns (checked_sessions, repaired_keys).
    """
    checked = 0
    repaired = 0
    _pm = None

    try:
        for session in sm.list_sessions():
            if session.state == SessionState.ARCHIVED:
                continue

            sym = session.symbol
            tf = session.timeframe
            candle_open = session.candle_open
            checked += 1

            missing_dirs: list[str] = []
            for direction in ("UP", "DOWN"):
                key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}:{candle_open}"
                try:
                    if not sync_redis.exists(key):
                        missing_dirs.append(direction)
                except Exception as exc:
                    logger.warning("OB health: Redis check failed %s: %s", key, exc)

            if not missing_dirs:
                continue

            logger.warning(
                "OB health: session %s missing %d key(s): %s",
                session.session_id, len(missing_dirs), missing_dirs,
            )

            # Lazy-init REST client
            if _pm is None:
                try:
                    from services.polymarket import PolymarketClient
                    from config.timing import HTTP_TIMEOUT
                    _pm = PolymarketClient(timeout=HTTP_TIMEOUT)
                except Exception as exc:
                    logger.error("OB health: cannot create PolymarketClient: %s", exc)
                    break

            for direction in missing_dirs:
                token_id = session.tokens.get(direction)
                if not token_id:
                    token_id = registry.get_token_id(sym, tf, direction)
                if not token_id:
                    logger.warning("OB health: no token_id for %s:%s:%s:%d", sym, tf, direction, candle_open)
                    continue

                try:
                    _, _, bids, asks = _pm._fetch_book_depth(token_id)
                    if not bids and not asks:
                        continue

                    import json as _json
                    bids_json = _json.dumps([[float(p), float(s)] for p, s in bids])
                    asks_json = _json.dumps([[float(p), float(s)] for p, s in asks])
                    ob_mapping = {
                        "bids": bids_json,
                        "asks": asks_json,
                        "updated_at": str(time.time()),
                    }
                    ob_key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}:{candle_open}"
                    sync_redis.hset(ob_key, mapping=ob_mapping)
                    sync_redis.expire(ob_key, 120)

                    # Also write legacy key for current session
                    period_s = TF_SECONDS[tf]
                    now_ts = int(time.time())
                    current_open = now_ts - (now_ts % period_s)
                    if candle_open == current_open:
                        legacy_key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}"
                        sync_redis.hset(legacy_key, mapping=ob_mapping)
                        sync_redis.expire(legacy_key, 120)

                    repaired += 1
                    logger.info(
                        "OB health: repaired %s:%s:%s:%d (%d bids, %d asks)",
                        sym, tf, direction, candle_open, len(bids), len(asks),
                    )
                except Exception as exc:
                    logger.error("OB health: REST fetch failed %s:%s:%s:%d — %s", sym, tf, direction, candle_open, exc)
    finally:
        if _pm is not None:
            try:
                _pm.close()
            except Exception:
                pass

    if repaired:
        logger.info("OB health: checked %d session(s), repaired %d key(s)", checked, repaired)
    return checked, repaired


def cleanup_expired_sessions(
    sm: SessionManager,
    sync_redis,
) -> int:
    """
    Archive sessions that have expired (candle ended + delay).

    For each non-ARCHIVED session where now > candle_open + period + CLEANUP_DELAY:
      1. Transition ACTIVE → SETTLING → ARCHIVED (or SETTLING → ARCHIVED)
      2. RPOP orphaned orders from queue → cancel info logged

    ARCHIVED sessions remain in the registry for ARCHIVED_RETENTION_S (60s)
    after settling_at. Use purge_archived_sessions() to remove them later.

    Returns the number of sessions archived.
    """
    now_ts = int(time.time())
    cleaned = 0

    for session in sm.list_sessions():
        if session.state == SessionState.ARCHIVED:
            continue

        period_s = TF_SECONDS.get(session.timeframe, 300)
        session_end_ts = session.candle_open + period_s
        cleanup_threshold = session_end_ts + SESSION_CLEANUP_DELAY_S

        if now_ts <= cleanup_threshold:
            continue

        session_id = session.session_id

        # Transition through states
        if session.state == SessionState.ACTIVE:
            sm.transition_session(session_id, SessionState.SETTLING)
        if session.state == SessionState.SETTLING or session.state == SessionState.PREFETCH:
            sm.transition_session(session_id, SessionState.ARCHIVED)

        # RPOP orphaned orders from queue
        queue_key = f"queue:orders:{session_id}"
        orphaned = 0
        while True:
            data = sync_redis.rpop(queue_key)
            if data is None:
                break
            orphaned += 1
            try:
                payload = json.loads(data)
                bo_id = payload.get("bo_id", "?")
                logger.warning(
                    "Session lifecycle: orphaned order bo_id=%s in expired queue %s",
                    bo_id, session_id,
                )
            except Exception:
                logger.warning(
                    "Session lifecycle: malformed orphaned order in queue %s",
                    session_id,
                )

        cleaned += 1
        logger.info(
            "Session lifecycle: archived %s (orphaned=%d)",
            session_id, orphaned,
        )

    if cleaned:
        logger.info("Session lifecycle: archived %d expired session(s)", cleaned)
    return cleaned


def purge_archived_sessions(
    sm: SessionManager,
    sync_redis,
) -> int:
    """
    Permanently remove ARCHIVED sessions that have been settled for > ARCHIVED_RETENTION_S.

    For each ARCHIVED session where now > settling_at + ARCHIVED_RETENTION_S:
      1. Delete Redis keys (queue + orderbook snapshots)
      2. Purge from SessionManager registry

    Returns the number of sessions purged.
    """
    now = datetime.now(timezone.utc)
    purged = 0

    for session in sm.list_archived_sessions():
        # Use settling_at if available, fall back to archived_at
        reference_at = session.settling_at or session.archived_at
        if reference_at is None:
            continue

        elapsed = (now - reference_at).total_seconds()
        if elapsed <= ARCHIVED_RETENTION_S:
            continue

        session_id = session.session_id
        sym = session.symbol
        tf = session.timeframe
        candle_open = session.candle_open

        # Delete Redis keys
        keys_to_delete = [
            f"queue:orders:{session_id}",
            f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:UP:{candle_open}",
            f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:DOWN:{candle_open}",
        ]
        for key in keys_to_delete:
            try:
                sync_redis.delete(key)
            except Exception as exc:
                logger.error("Session lifecycle: failed to delete %s: %s", key, exc)

        sm.purge_session(session_id)
        purged += 1
        logger.info(
            "Session lifecycle: purged %s (deleted %d keys, retained %.0fs after settling)",
            session_id, len(keys_to_delete), elapsed,
        )

    if purged:
        logger.info("Session lifecycle: purged %d archived session(s)", purged)
    return purged
