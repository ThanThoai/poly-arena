"""
Session Lifecycle — ensure 4 active sessions per (sym, tf) + cleanup expired ones.

Called every SESSION_LIFECYCLE_TICK_S seconds from the WS Feed Service main loop.

Responsibilities:
  - ensure_future_sessions(): maintain current + 3 future sessions per (sym, tf)
  - cleanup_expired_sessions(): archive + delete Redis keys for expired sessions
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING

from config.timing import (
    TF_SECONDS,
    REQUIRED_FUTURE_SESSIONS,
    SESSION_PRE_CREATE_BUFFER_S,
    SESSION_CLEANUP_DELAY_S,
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
    For each (sym, tf), ensure we have sessions for current + 3 future candles.

    Creates missing sessions by resolving tokens via Polymarket REST API directly
    (no TokenRegistry dependency for token resolution — avoids stale cache issues).

    Returns (created_count, new_token_ids).
    Note: Does NOT call feed.add_tokens() — caller is responsible for WS subscription
    (needed when running in executor thread where asyncio context is unavailable).
    """
    now_ts = int(time.time())
    created = 0
    all_new_token_ids: list[str] = []

    # Build set of existing session_ids for fast lookup
    existing_sessions = {s.session_id for s in sm.list_sessions()}

    for sym in SYMBOLS:
        for tf in TIMEFRAMES:
            expected_opens = _expected_candle_opens(tf, now_ts)

            for candle_open in expected_opens:
                session_id = f"{sym}:{tf}:{candle_open}"
                if session_id in existing_sessions:
                    continue

                # Resolve UP+DOWN tokens via REST API directly
                tokens = _resolve_tokens(sym, tf, candle_open)
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

                logger.info(
                    "Session lifecycle: created %s (state=%s, tokens=%d)",
                    session_id, initial_state.value, len(tokens),
                )

    # Batch: re-register writer once after all sessions created
    if created:
        _register_writer(writer, registry)
        logger.info("Session lifecycle: created %d new session(s)", created)

    return created, all_new_token_ids


def _resolve_tokens(
    sym: str,
    tf: str,
    candle_open: int,
) -> dict[str, str]:
    """
    Resolve UP+DOWN token_ids using Polymarket REST API directly.
    No TokenRegistry dependency — avoids stale cache issues.

    get_token_id_at() uses an internal slug cache (TTL=300s) so most calls
    are cache hits with no HTTP request. UP+DOWN share the same slug so
    only 1 actual HTTP call per (sym, tf, candle_open) in the worst case.
    """
    tokens: dict[str, str] = {}

    try:
        from services.polymarket import PolymarketClient
        from config.timing import HTTP_TIMEOUT

        with PolymarketClient(timeout=HTTP_TIMEOUT) as pm:
            for direction in ("UP", "DOWN"):
                token_id = pm.get_token_id_at(sym, tf, direction, candle_open)
                if token_id:
                    tokens[direction] = token_id
    except Exception as exc:
        logger.warning(
            "Session lifecycle: REST resolve failed %s %s ts=%d — %s",
            sym, tf, candle_open, exc,
        )

    return tokens


def _register_writer(writer: RedisWriter, registry: TokenRegistry) -> None:
    """Re-register token mappings in RedisWriter after session creation."""
    writer.register_token_mapping(registry._mapping)
    fresh_sessions = {tf: registry.get_current_candle_open(tf) for tf in TIMEFRAMES}
    writer.register_session_tokens(registry.get_all_token_mapping(), fresh_sessions)


def cleanup_expired_sessions(
    sm: SessionManager,
    sync_redis,
) -> int:
    """
    Archive and clean up sessions that have expired (candle ended + delay).

    For each non-ARCHIVED session where now > candle_open + period + CLEANUP_DELAY:
      1. Transition ACTIVE → SETTLING → ARCHIVED (or SETTLING → ARCHIVED)
      2. RPOP orphaned orders from queue → cancel info logged
      3. Delete Redis keys (queue + orderbook snapshots)

    Returns the number of sessions cleaned up.
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
        sym = session.symbol
        tf = session.timeframe
        candle_open = session.candle_open

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

        # Delete Redis keys
        keys_to_delete = [
            queue_key,
            f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:UP:{candle_open}",
            f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:DOWN:{candle_open}",
        ]
        for key in keys_to_delete:
            try:
                sync_redis.delete(key)
            except Exception as exc:
                logger.error("Session lifecycle: failed to delete %s: %s", key, exc)

        cleaned += 1
        logger.info(
            "Session lifecycle: cleaned up %s (orphaned=%d, deleted %d keys)",
            session_id, orphaned, len(keys_to_delete),
        )

    if cleaned:
        logger.info("Session lifecycle: cleaned up %d expired session(s)", cleaned)
    return cleaned
