"""
WebSocket endpoint for real-time orderbook depth updates.

Subscribes to Redis pub/sub channel ``orderbook:updates`` and forwards
matching updates to connected clients.

Protocol:
  - Client connects → server sends snapshot of current orderbook state
  - Client sends filter: {"symbol": "BTC", "timeframe": "M5"}
  - Server sends new snapshot matching the filter
  - Server streams incremental updates via Redis pub/sub

Each WS client gets its own Redis connection for pub/sub to avoid
blocking the shared connection pool.
"""

import asyncio
import json
import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from services.redis_client import get_async_redis, REDIS_URL
from ws_feed_service.config import ORDERBOOK_KEY_PREFIX

logger = logging.getLogger(__name__)
router = APIRouter()

_OB_SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
_OB_DIRECTIONS = ["UP", "DOWN"]


async def _send_snapshot(
    ws: WebSocket,
    r: aioredis.Redis,
    symbol: str | None,
    timeframe: str | None,
) -> None:
    """Read current + future session orderbook state from Redis and send to client."""
    syms = [symbol.upper()] if symbol else _OB_SYMBOLS
    tfs = [timeframe.upper()] if timeframe else ["M5", "M15", "H1"]
    dirs = _OB_DIRECTIONS

    combos = [(s, t, d) for s in syms for t in tfs for d in dirs]
    if not combos:
        return

    # ── Legacy keys (current session, no session field) ──
    pipe = r.pipeline(transaction=False)
    for sym, tf, dir_ in combos:
        pipe.hgetall(f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{dir_}")

    try:
        results = await pipe.execute()
    except Exception as exc:
        logger.warning("WS snapshot Redis read failed: %s", exc)
        return

    for (sym, tf, dir_), data in zip(combos, results):
        if not data:
            continue
        try:
            bids = json.loads(data.get("bids", "[]"))
            asks = json.loads(data.get("asks", "[]"))
        except (json.JSONDecodeError, TypeError):
            continue
        if not bids and not asks:
            continue
        await ws.send_json({
            "type": "snapshot",
            "symbol": sym,
            "timeframe": tf,
            "direction": dir_,
            "bids": bids,
            "asks": asks,
            "updated_at": data.get("updated_at"),
        })

    # ── Future session keys (scan orderbook:{sym}:{tf}:{dir}:{ts}) ──
    for sym, tf, dir_ in combos:
        pattern = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{dir_}:*"
        try:
            session_keys = []
            async for key in r.scan_iter(match=pattern, count=50):
                session_keys.append(key if isinstance(key, str) else key.decode())
            if not session_keys:
                continue

            pipe2 = r.pipeline(transaction=False)
            for sk in session_keys:
                pipe2.hgetall(sk)
            session_results = await pipe2.execute()

            for sk, data in zip(session_keys, session_results):
                if not data:
                    continue
                # Extract candle_ts from key: orderbook:SYM:TF:DIR:candle_ts
                parts = sk.split(":")
                if len(parts) < 5:
                    continue
                try:
                    candle_ts = int(parts[-1])
                except (ValueError, IndexError):
                    continue
                try:
                    bids = json.loads(data.get("bids", "[]"))
                    asks = json.loads(data.get("asks", "[]"))
                except (json.JSONDecodeError, TypeError):
                    continue
                if not bids and not asks:
                    continue
                await ws.send_json({
                    "type": "snapshot",
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": dir_,
                    "session": candle_ts,
                    "bids": bids,
                    "asks": asks,
                    "updated_at": data.get("updated_at"),
                })
        except Exception as exc:
            logger.debug("WS future session scan failed for %s:%s:%s: %s", sym, tf, dir_, exc)


@router.websocket("/ws/orderbook")
async def orderbook_ws(ws: WebSocket):
    await ws.accept()

    filter_symbol: str | None = None
    filter_timeframe: str | None = None
    snapshot_requested = asyncio.Event()

    # Use shared Redis for reading snapshot data
    r = get_async_redis()

    # Create a dedicated Redis connection for pub/sub
    # (pub/sub blocks the connection, can't share with regular commands)
    pubsub_redis: aioredis.Redis | None = None
    pubsub = None

    try:
        pubsub_redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        pubsub = pubsub_redis.pubsub()
        await pubsub.subscribe("orderbook:updates")
    except Exception as exc:
        logger.warning("WS orderbook: Redis pub/sub connect failed: %s", exc)
        # Send error and close gracefully
        try:
            await ws.send_json({"type": "error", "message": "Redis unavailable"})
        except Exception:
            pass
        await ws.close(code=1011, reason="Redis unavailable")
        if pubsub_redis:
            await pubsub_redis.aclose()
        return

    # Send initial snapshot
    try:
        await _send_snapshot(ws, r, None, None)
    except Exception as exc:
        logger.debug("WS initial snapshot failed: %s", exc)

    async def _read_client():
        """Read filter messages from client."""
        nonlocal filter_symbol, filter_timeframe
        try:
            while True:
                msg = await ws.receive_text()
                try:
                    data = json.loads(msg)
                    filter_symbol = data.get("symbol")
                    filter_timeframe = data.get("timeframe")
                    snapshot_requested.set()
                except (json.JSONDecodeError, TypeError):
                    pass
        except (WebSocketDisconnect, Exception):
            pass

    reader_task = asyncio.create_task(_read_client())

    try:
        while True:
            # Check if client sent a new filter → send snapshot
            if snapshot_requested.is_set():
                snapshot_requested.clear()
                try:
                    await _send_snapshot(ws, r, filter_symbol, filter_timeframe)
                except Exception:
                    break

            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=0.5,
            )
            if msg is None:
                continue
            if msg["type"] != "message":
                continue

            try:
                payload = json.loads(msg["data"])
            except (json.JSONDecodeError, TypeError):
                continue

            # Apply client filter
            if filter_symbol and payload.get("symbol") != filter_symbol.upper():
                continue
            if filter_timeframe and payload.get("timeframe") != filter_timeframe.upper():
                continue

            # Parse pre-serialized JSON strings back to arrays
            try:
                bids = json.loads(payload["bids"]) if isinstance(payload["bids"], str) else payload["bids"]
                asks = json.loads(payload["asks"]) if isinstance(payload["asks"], str) else payload["asks"]
            except (json.JSONDecodeError, KeyError):
                continue

            msg_out: dict = {
                "type": "update",
                "symbol": payload["symbol"],
                "timeframe": payload["timeframe"],
                "direction": payload["direction"],
                "bids": bids,
                "asks": asks,
                "updated_at": payload.get("updated_at"),
            }
            if "session" in payload:
                msg_out["session"] = payload["session"]
            await ws.send_json(msg_out)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        reader_task.cancel()
        try:
            await pubsub.unsubscribe("orderbook:updates")
            await pubsub.close()
        except Exception:
            pass
        if pubsub_redis:
            await pubsub_redis.aclose()
