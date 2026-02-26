"""
RedisWriter — writes price data and bracket exit events to Redis.

Used by the WS Feed Service to publish data that FastAPI consumes.
"""

import logging
import time
from typing import Optional

import redis.asyncio as aioredis

from ws_feed_service.config import (
    PRICE_KEY_PREFIX,
    PRICE_CACHE_TTL_S,
    STREAM_BRACKET_EXITS,
    STREAM_MAXLEN,
)

logger = logging.getLogger(__name__)


class RedisWriter:
    """Writes price cache entries and bracket exit stream events to Redis."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._r = redis_client
        # token_id → [(symbol, timeframe, direction)]
        self._token_map: dict[str, list[tuple[str, str, str]]] = {}

    def register_token_mapping(self, mapping: dict[tuple[str, str, str], str]) -> None:
        """
        Build reverse map from TokenRegistry._mapping.

        mapping: {(symbol, timeframe, direction): token_id}
        """
        self._token_map.clear()
        for (sym, tf, direction), token_id in mapping.items():
            self._token_map.setdefault(token_id, []).append((sym, tf, direction))
        logger.info(
            "RedisWriter: registered %d token(s) → %d price key(s)",
            len(self._token_map),
            sum(len(v) for v in self._token_map.values()),
        )

    async def update_price(self, token_id: str, best_ask: Optional[float]) -> None:
        """
        Write best_ask for all (sym, tf, dir) combos mapped to this token_id.

        Key format: price:{SYM}:{TF}:{DIR}
        Hash fields: best_ask, token_id, updated_at
        """
        combos = self._token_map.get(token_id)
        if not combos or best_ask is None:
            return

        now_ts = str(time.time())
        pipe = self._r.pipeline(transaction=False)

        for sym, tf, direction in combos:
            key = f"{PRICE_KEY_PREFIX}:{sym}:{tf}:{direction}"
            pipe.hset(key, mapping={
                "best_ask": str(best_ask),
                "token_id": token_id,
                "updated_at": now_ts,
            })
            pipe.expire(key, PRICE_CACHE_TTL_S)

        try:
            await pipe.execute()
        except Exception as exc:
            logger.error("RedisWriter.update_price failed: %s", exc)

    async def publish_bracket_exit(
        self,
        bo_id: int,
        trigger: str,
        exit_price: float,
        exit_filled: float,
        order_id: str,
    ) -> None:
        """Publish a bracket exit event to the Redis stream."""
        try:
            await self._r.xadd(
                STREAM_BRACKET_EXITS,
                {
                    "bo_id": str(bo_id),
                    "trigger": trigger,
                    "exit_price": str(exit_price),
                    "exit_filled": str(exit_filled),
                    "order_id": order_id,
                },
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
            logger.info(
                "Bracket exit published: bo_id=%d trigger=%s exit_price=%.6f",
                bo_id, trigger, exit_price,
            )
        except Exception as exc:
            logger.error("RedisWriter.publish_bracket_exit failed: %s", exc)
