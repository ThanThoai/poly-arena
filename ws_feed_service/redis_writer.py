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
    STREAM_MAXLEN,
)

logger = logging.getLogger(__name__)


class RedisWriter:
    """Writes price cache entries and bracket exit stream events to Redis."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._r = redis_client
        # token_id → [(symbol, timeframe, direction)]
        self._token_map: dict[str, list[tuple[str, str, str]]] = {}

    def register_token_mapping(
        self, mapping: dict[tuple[str, str, str], str]
    ) -> list[tuple[str, str, str]]:
        """
        Build reverse map from TokenRegistry._mapping.

        mapping: {(symbol, timeframe, direction): token_id}

        Returns the list of (sym, tf, dir) combos whose token_id was rotated
        (i.e. the new session started) so the caller can clear stale Redis keys.
        """
        # Build OLD forward map (combo → old token_id) before clearing
        old_forward: dict[tuple[str, str, str], str] = {
            combo: token_id
            for token_id, combos in self._token_map.items()
            for combo in combos
        }

        self._token_map.clear()
        for (sym, tf, direction), token_id in mapping.items():
            self._token_map.setdefault(token_id, []).append((sym, tf, direction))

        # Any combo whose token_id changed → its Redis price key is now stale
        rotated_combos: list[tuple[str, str, str]] = [
            combo
            for combo, new_token_id in mapping.items()
            if old_forward.get(combo) not in (None, new_token_id)
        ]

        logger.info(
            "RedisWriter: registered %d token(s) → %d price key(s)%s",
            len(self._token_map),
            sum(len(v) for v in self._token_map.values()),
            f", {len(rotated_combos)} key(s) rotated" if rotated_combos else "",
        )
        return rotated_combos

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

        Key format: orderbook:{SYM}:{TF}:{DIR}
        Hash fields: bids, asks, updated_at

        bids/asks are JSON arrays of [price, size] pairs (already sorted by caller).
        """
        combos = self._token_map.get(token_id)
        if not combos:
            return

        now_ts = str(time.time())
        bids_json = json.dumps([[float(p), float(s)] for p, s in bids])
        asks_json = json.dumps([[float(p), float(s)] for p, s in asks])

        pipe = self._r.pipeline(transaction=False)
        for sym, tf, direction in combos:
            key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}"
            pipe.hset(key, mapping={
                "bids": bids_json,
                "asks": asks_json,
                "updated_at": now_ts,
            })
            pipe.expire(key, PRICE_CACHE_TTL_S)

        try:
            await pipe.execute()
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
