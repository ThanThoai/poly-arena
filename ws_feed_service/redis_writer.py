"""
RedisWriter — writes price data and bracket exit events to Redis.

Used by the WS Feed Service to publish data that FastAPI consumes.
"""

import json
import logging
import time
from decimal import Decimal
from typing import Optional

import redis.asyncio as aioredis

from ws_feed_service.config import (
    PRICE_KEY_PREFIX,
    PRICE_CACHE_TTL_S,
    ORDERBOOK_KEY_PREFIX,
    STREAM_BRACKET_EXITS,
    STREAM_ORDER_CANCELS,
    STREAM_ORDER_FILLS,
    STREAM_MARKET_RESOLVED,
    STREAM_MAXLEN,
    UI_FUTURE_SESSIONS,
    UI_PAST_SESSIONS,
)

logger = logging.getLogger(__name__)


class RedisWriter:
    """Writes price cache entries and bracket exit stream events to Redis."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._r = redis_client
        # token_id → [(symbol, timeframe, direction)]
        self._token_map: dict[str, list[tuple[str, str, str]]] = {}
        # Session-aware token map: token_id → [(sym, tf, dir, candle_ts)]
        self._session_token_map: dict[str, list[tuple[str, str, str, int]]] = {}
        # Current session timestamps: tf → candle_ts
        self._current_sessions: dict[str, int] = {}
        # Dedup cache: key → (bids_json, asks_json) to skip redundant writes
        self._last_ob_json: dict[str, tuple[str, str]] = {}

    def register_token_mapping(
        self, mapping: dict[tuple[str, str, str], str]
    ) -> tuple[list[tuple[str, str, str]], list[str]]:
        """
        Build reverse map from TokenRegistry._mapping.

        mapping: {(symbol, timeframe, direction): token_id}

        Returns:
          - rotated_combos: list of (sym, tf, dir) whose token_id changed
          - old_token_ids: list of old token_ids that were replaced (for book invalidation)
        """
        # Build OLD forward map (combo → old token_id) before clearing
        old_forward: dict[tuple[str, str, str], str] = {
            combo: token_id
            for token_id, combos in self._token_map.items()
            for combo in combos
        }

        self._token_map.clear()
        self._last_ob_json.clear()
        for (sym, tf, direction), token_id in mapping.items():
            self._token_map.setdefault(token_id, []).append((sym, tf, direction))

        # Any combo whose token_id changed → its Redis price key is now stale
        rotated_combos: list[tuple[str, str, str]] = []
        old_token_ids_set: set[str] = set()
        for combo, new_token_id in mapping.items():
            old_id = old_forward.get(combo)
            if old_id is not None and old_id != new_token_id:
                rotated_combos.append(combo)
                old_token_ids_set.add(old_id)

        logger.info(
            "RedisWriter: registered %d token(s) → %d price key(s)%s",
            len(self._token_map),
            sum(len(v) for v in self._token_map.values()),
            f", {len(rotated_combos)} key(s) rotated" if rotated_combos else "",
        )
        return rotated_combos, list(old_token_ids_set)

    def register_session_tokens(
        self,
        all_mapping: dict[str, list[tuple[str, str, str, int]]],
        current_sessions: dict[str, int],
        max_future: int = UI_FUTURE_SESSIONS,
        max_past: int = UI_PAST_SESSIONS,
    ) -> None:
        """
        Build session-aware token map from TokenRegistry.get_all_token_mapping().

        Keeps up to max_past past sessions + current + up to max_future future sessions.

        all_mapping: token_id → [(sym, tf, dir, candle_ts), ...]
        current_sessions: tf → current candle_ts
        """
        from config.timing import TF_SECONDS

        self._current_sessions = dict(current_sessions)
        self._session_token_map.clear()

        for token_id, combos in all_mapping.items():
            filtered: list[tuple[str, str, str, int]] = []
            for sym, tf, direction, candle_ts in combos:
                current_ts = current_sessions.get(tf, 0)
                period = TF_SECONDS.get(tf, 300)
                steps = (candle_ts - current_ts) // period if current_ts else 0
                if steps < -max_past or steps > max_future:
                    continue
                filtered.append((sym, tf, direction, candle_ts))
            if filtered:
                self._session_token_map[token_id] = filtered

        total_tokens = len(self._session_token_map)
        total_combos = sum(len(v) for v in self._session_token_map.values())
        logger.info(
            "RedisWriter: session tokens registered — %d token(s) → %d combo(s) "
            "(%d past + current + %d future)",
            total_tokens, total_combos, max_past, max_future,
        )

    async def update_price(
        self,
        token_id: str,
        best_ask: Optional[float],
        best_bid: Optional[float] = None,
    ) -> None:
        """
        Write best_ask and best_bid for all (sym, tf, dir) combos mapped to this token_id.

        Key format: price:{SYM}:{TF}:{DIR}
        Hash fields: best_ask, best_bid, token_id, updated_at

        If best_bid is None for this tick, the field is explicitly removed from
        the hash so no stale bid from a previous session leaks through.
        """
        combos = self._token_map.get(token_id)
        if not combos or best_ask is None:
            return

        now_ts = str(time.time())
        pipe = self._r.pipeline(transaction=False)

        for sym, tf, direction in combos:
            key = f"{PRICE_KEY_PREFIX}:{sym}:{tf}:{direction}"
            mapping: dict[str, str] = {
                "best_ask": str(best_ask),
                "token_id": token_id,
                "updated_at": now_ts,
            }
            if best_bid is not None:
                mapping["best_bid"] = str(best_bid)
            else:
                # Remove stale bid from previous session / tick
                pipe.hdel(key, "best_bid")
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, PRICE_CACHE_TTL_S)

        try:
            await pipe.execute()
        except Exception as exc:
            logger.error("RedisWriter.update_price failed: %s", exc)

    async def clear_price_keys(self, combos: list[tuple[str, str, str]]) -> None:
        """
        Delete Redis price keys for the given (sym, tf, dir) combos.

        Called when session rotates (new token_id detected by TokenRegistry)
        so the UI shows "no data" immediately instead of stale prices until
        the 60-second TTL expires naturally.
        """
        if not combos:
            return

        pipe = self._r.pipeline(transaction=False)
        for sym, tf, direction in combos:
            key = f"{PRICE_KEY_PREFIX}:{sym}:{tf}:{direction}"
            pipe.delete(key)

        try:
            await pipe.execute()
            logger.info(
                "Cleared %d rotated price key(s): %s",
                len(combos),
                [f"{s}:{t}:{d}" for s, t, d in combos],
            )
        except Exception as exc:
            logger.error("RedisWriter.clear_price_keys failed: %s", exc)

    async def update_orderbook(
        self,
        token_id: str,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> None:
        """
        Write top-N orderbook depth for all (sym, tf, dir) combos mapped to this token_id.

        Dual-write strategy:
        - Current session tokens: write legacy key ``orderbook:{SYM}:{TF}:{DIR}``
          (backward compat) AND session key ``orderbook:{SYM}:{TF}:{DIR}:{candle_ts}``
        - Future session tokens: write ONLY session key

        bids/asks are JSON arrays of [price, size] pairs (already sorted by caller).
        """
        # Need at least one map to have combos for this token
        legacy_combos = self._token_map.get(token_id)
        session_combos = self._session_token_map.get(token_id)
        if not legacy_combos and not session_combos:
            return

        bids_json = json.dumps([[float(p), float(s)] for p, s in bids])
        asks_json = json.dumps([[float(p), float(s)] for p, s in asks])

        # Skip write if orderbook data is unchanged
        cached = self._last_ob_json.get(token_id)
        if cached is not None and cached == (bids_json, asks_json):
            return
        self._last_ob_json[token_id] = (bids_json, asks_json)

        now_ts = str(time.time())
        ob_mapping = {
            "bids": bids_json,
            "asks": asks_json,
            "updated_at": now_ts,
        }
        pipe = self._r.pipeline(transaction=False)

        # Track which (sym, tf, dir) already got a session-keyed write
        # to avoid double-publish for current session
        written_session_keys: set[str] = set()

        # ── Session-keyed writes (current + future) ──
        if session_combos:
            for sym, tf, direction, candle_ts in session_combos:
                session_key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}:{candle_ts}"
                pipe.hset(session_key, mapping=ob_mapping)
                pipe.expire(session_key, PRICE_CACHE_TTL_S)
                written_session_keys.add(f"{sym}:{tf}:{direction}:{candle_ts}")

        # ── Legacy writes (current session only, backward compat) ──
        if legacy_combos:
            for sym, tf, direction in legacy_combos:
                key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}"
                pipe.hset(key, mapping=ob_mapping)
                pipe.expire(key, PRICE_CACHE_TTL_S)

        try:
            await pipe.execute()

            # Publish change notifications for WebSocket subscribers
            # Legacy combos: publish without session (backward compat)
            if legacy_combos:
                for sym, tf, direction in legacy_combos:
                    payload = json.dumps({
                        "symbol": sym,
                        "timeframe": tf,
                        "direction": direction,
                        "bids": bids_json,
                        "asks": asks_json,
                        "updated_at": now_ts,
                    })
                    await self._r.publish("orderbook:updates", payload)

            # Session combos: publish with session field for future sessions
            if session_combos:
                for sym, tf, direction, candle_ts in session_combos:
                    current_ts = self._current_sessions.get(tf, 0)
                    if candle_ts == current_ts:
                        continue  # already published via legacy path
                    payload = json.dumps({
                        "symbol": sym,
                        "timeframe": tf,
                        "direction": direction,
                        "session": candle_ts,
                        "bids": bids_json,
                        "asks": asks_json,
                        "updated_at": now_ts,
                    })
                    await self._r.publish("orderbook:updates", payload)
        except Exception as exc:
            logger.error("RedisWriter.update_orderbook failed: %s", exc)

    async def publish_bracket_exit(
        self,
        bo_id: int,
        trigger: str,
        exit_price: float,
        exit_filled: float,
        order_id: str,
        exit_at: str | None = None,
        walk_prices: str = "",
    ) -> None:
        """Publish a bracket exit event to the Redis stream."""
        try:
            fields: dict[str, str] = {
                "bo_id": str(bo_id),
                "trigger": trigger,
                "exit_price": str(exit_price),
                "exit_filled": str(exit_filled),
                "order_id": order_id,
            }
            if exit_at:
                fields["exit_at"] = exit_at
            if walk_prices:
                fields["walk_prices"] = walk_prices
            await self._r.xadd(
                STREAM_BRACKET_EXITS,
                fields,
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
            logger.info(
                "Bracket exit published: bo_id=%d trigger=%s exit_price=%.6f",
                bo_id, trigger, exit_price,
            )
        except Exception as exc:
            logger.error("RedisWriter.publish_bracket_exit failed: %s", exc)

    async def publish_order_cancel(
        self,
        bo_id: int,
        order_id: str,
        reason: str = "TTL_EXPIRED",
        filled: float = 0.0,
        avg_entry_price: float = 0.0,
    ) -> None:
        """Publish an order cancel event to the Redis stream.

        Includes partial fill data so the DB consumer can distinguish
        between zero-fill cancels and partial-fill expiries.
        """
        try:
            await self._r.xadd(
                STREAM_ORDER_CANCELS,
                {
                    "bo_id": str(bo_id),
                    "order_id": order_id,
                    "reason": reason,
                    "filled": str(filled),
                    "avg_entry_price": str(avg_entry_price),
                },
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
            logger.info(
                "Order cancel published: bo_id=%d reason=%s",
                bo_id, reason,
            )
        except Exception as exc:
            logger.error("RedisWriter.publish_order_cancel failed: %s", exc)

    async def publish_order_fill(
        self,
        bo_id: int,
        order_id: str,
        filled: float,
        avg_entry_price: float,
        status: str,
        walk_prices: str = "",
    ) -> None:
        """Publish a fill update event to the Redis stream.

        Called whenever an order gets a (partial or full) fill so the DB
        can keep avg_price / num_shares in sync with the matching engine.
        """
        try:
            fields: dict[str, str] = {
                "bo_id": str(bo_id),
                "order_id": order_id,
                "filled": str(filled),
                "avg_entry_price": str(avg_entry_price),
                "status": status,
            }
            if walk_prices:
                fields["walk_prices"] = walk_prices
            await self._r.xadd(
                STREAM_ORDER_FILLS,
                fields,
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
            logger.info(
                "Order fill published: bo_id=%d filled=%.6f avg=%.6f status=%s",
                bo_id, filled, avg_entry_price, status,
            )
        except Exception as exc:
            logger.error("RedisWriter.publish_order_fill failed: %s", exc)

    async def publish_market_resolved(
        self,
        asset_id: str,
        winning_outcome: str = "",
        timestamp: str = "",
    ) -> None:
        """Publish a market resolution event to the Redis stream."""
        try:
            fields: dict[str, str] = {
                "asset_id": asset_id,
            }
            if winning_outcome:
                fields["winning_outcome"] = winning_outcome
            if timestamp:
                fields["timestamp"] = timestamp
            await self._r.xadd(
                STREAM_MARKET_RESOLVED,
                fields,
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
            logger.info(
                "Market resolved published: asset_id=%s winning=%s",
                asset_id[:16], winning_outcome,
            )
        except Exception as exc:
            logger.error("RedisWriter.publish_market_resolved failed: %s", exc)
