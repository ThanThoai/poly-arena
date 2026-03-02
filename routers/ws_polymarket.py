"""
WebSocket proxy for Polymarket orderbook data.

Proxies orderbook data from Redis pub/sub (fed by ws_feed_service) to UI
clients in a Polymarket-compatible format, so the UI doesn't need direct
Polymarket WebSocket connections or Gamma API access.

Protocol (client -> server):
  - "PING"  -> server replies "PONG"
  - {"type": "subscribe", "symbol": "BTC", "timeframe": "M5"}

Protocol (server -> client):
  - {"type": "token_map", "tokens": {asset_id: {direction, session}, ...}}
  - {"event_type": "book", "asset_id": "...", "bids": [...], "asks": [...]}
"""

import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from config.timing import TF_SECONDS
from services.polymarket import PolymarketClient
from services.redis_client import get_async_redis, REDIS_URL
from ws_feed_service.config import ORDERBOOK_KEY_PREFIX

logger = logging.getLogger(__name__)
router = APIRouter()

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pm-discover")


# ── Token discovery helpers ──────────────────────────────────────────────────

def _discover_tokens_sync(
    symbol: str, timeframe: str
) -> dict | None:
    """Discover token IDs for current + 2 future sessions (runs in threadpool).

    Returns:
        {
            "token_map": {asset_id: {"direction": "UP"|"DOWN", "session": int}, ...},
            "reverse_map": {(direction, session): asset_id, ...},
            "period": int,
        }
    """
    tf_upper = timeframe.upper()
    period = TF_SECONDS.get(tf_upper, 300)
    now = int(time.time())
    current_ts = now - (now % period)
    sessions = [current_ts, current_ts + period, current_ts + period * 2]

    token_map: dict[str, dict] = {}
    reverse_map: dict[tuple[str, int], str] = {}

    client = PolymarketClient()
    try:
        for ts in sessions:
            for direction in ("UP", "DOWN"):
                try:
                    token_id = client.get_token_id_at(symbol, timeframe, direction, ts)
                    if token_id:
                        token_map[token_id] = {
                            "direction": direction,
                            "session": ts,
                        }
                        reverse_map[(direction, ts)] = token_id
                except Exception as exc:
                    logger.debug(
                        "Token discovery failed: %s/%s/%s/%d: %s",
                        symbol, timeframe, direction, ts, exc,
                    )
    finally:
        client.close()

    if not token_map:
        return None

    return {
        "token_map": token_map,
        "reverse_map": reverse_map,
        "period": period,
    }


async def _discover_tokens(symbol: str, timeframe: str) -> dict | None:
    """Async wrapper — runs synchronous PolymarketClient in threadpool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor, _discover_tokens_sync, symbol, timeframe
    )


# ── Snapshot helper ──────────────────────────────────────────────────────────

async def _send_snapshots(
    ws: WebSocket,
    r: aioredis.Redis,
    symbol: str,
    timeframe: str,
    reverse_map: dict[tuple[str, int], str],
) -> None:
    """Read orderbook state from Redis and send as Polymarket-format book events."""
    sym = symbol.upper()
    tf = timeframe.upper()

    for direction in ("UP", "DOWN"):
        # Current session (legacy key without timestamp)
        key = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}"
        data = await r.hgetall(key)
        if data:
            # Determine current session timestamp
            period = TF_SECONDS.get(tf, 300)
            now = int(time.time())
            current_ts = now - (now % period)
            asset_id = reverse_map.get((direction, current_ts))
            if asset_id:
                await _send_book_event(ws, asset_id, data)

        # Future session keys: orderbook:{SYM}:{TF}:{DIR}:{candle_ts}
        pattern = f"{ORDERBOOK_KEY_PREFIX}:{sym}:{tf}:{direction}:*"
        async for skey in r.scan_iter(match=pattern, count=50):
            sk = skey if isinstance(skey, str) else skey.decode()
            parts = sk.split(":")
            if len(parts) < 5:
                continue
            try:
                candle_ts = int(parts[-1])
            except (ValueError, IndexError):
                continue
            asset_id = reverse_map.get((direction, candle_ts))
            if not asset_id:
                continue
            sdata = await r.hgetall(sk)
            if sdata:
                await _send_book_event(ws, asset_id, sdata)


async def _send_book_event(
    ws: WebSocket,
    asset_id: str,
    data: dict,
) -> None:
    """Transform Redis hash data into a Polymarket-format book event."""
    try:
        raw_bids = json.loads(data.get("bids", "[]"))
        raw_asks = json.loads(data.get("asks", "[]"))
    except (json.JSONDecodeError, TypeError):
        return

    if not raw_bids and not raw_asks:
        return

    # Redis stores as [[price, size], ...] — convert to Polymarket format
    bids = [{"price": str(b[0]), "size": str(b[1])} for b in raw_bids]
    asks = [{"price": str(a[0]), "size": str(a[1])} for a in raw_asks]

    await ws.send_json({
        "event_type": "book",
        "asset_id": asset_id,
        "bids": bids,
        "asks": asks,
        "timestamp": data.get("updated_at"),
    })


# ── Main WebSocket endpoint ─────────────────────────────────────────────────

@router.websocket("/ws/polymarket")
async def polymarket_proxy_ws(ws: WebSocket):
    await ws.accept()

    symbol: str | None = None
    timeframe: str | None = None
    reverse_map: dict[tuple[str, int], str] = {}
    period: int = 300
    boundary_task: asyncio.Task | None = None
    pubsub_redis: aioredis.Redis | None = None
    pubsub = None
    streaming = False

    async def _schedule_boundary(sym: str, tf: str):
        """Wait for next candle boundary, then re-discover tokens and send updates."""
        nonlocal reverse_map, period, boundary_task
        try:
            while True:
                now_s = int(time.time())
                next_bound = ((now_s // period) + 1) * period
                wait_s = next_bound - now_s + 2  # +2s buffer
                await asyncio.sleep(wait_s)

                result = await _discover_tokens(sym, tf)
                if not result:
                    continue

                reverse_map = result["reverse_map"]
                period = result["period"]

                # Send updated token map
                try:
                    await ws.send_json({
                        "type": "token_map",
                        "tokens": result["token_map"],
                    })
                except Exception:
                    return

                # Send new snapshots
                r = get_async_redis()
                try:
                    await _send_snapshots(ws, r, sym, tf, reverse_map)
                except Exception:
                    return

        except asyncio.CancelledError:
            return

    async def _handle_subscribe(sym: str, tf: str):
        """Process a subscribe request: discover tokens, send map + snapshots, start streaming."""
        nonlocal symbol, timeframe, reverse_map, period, boundary_task, streaming
        nonlocal pubsub_redis, pubsub

        symbol = sym.upper()
        timeframe = tf.upper()

        # Cancel previous boundary timer
        if boundary_task and not boundary_task.done():
            boundary_task.cancel()

        # Discover tokens
        result = await _discover_tokens(symbol, timeframe)
        if not result:
            await ws.send_json({
                "type": "error",
                "message": f"No tokens found for {symbol}/{timeframe}",
            })
            return

        reverse_map = result["reverse_map"]
        period = result["period"]

        # Send token map
        await ws.send_json({
            "type": "token_map",
            "tokens": result["token_map"],
        })

        # Send initial snapshots
        r = get_async_redis()
        await _send_snapshots(ws, r, symbol, timeframe, reverse_map)

        # Start boundary rotation
        boundary_task = asyncio.create_task(
            _schedule_boundary(symbol, timeframe)
        )

        # Set up pub/sub if not already streaming
        if not streaming:
            try:
                pubsub_redis = aioredis.from_url(REDIS_URL, decode_responses=True)
                pubsub = pubsub_redis.pubsub()
                await pubsub.subscribe("orderbook:updates")
                streaming = True
            except Exception as exc:
                logger.warning("WS polymarket: Redis pub/sub failed: %s", exc)
                await ws.send_json({"type": "error", "message": "Redis unavailable"})

    async def _read_client():
        """Read messages from client (PING, subscribe)."""
        try:
            while True:
                raw = await ws.receive_text()
                if raw == "PING":
                    await ws.send_text("PONG")
                    continue
                try:
                    data = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if data.get("type") == "subscribe":
                    sym = data.get("symbol")
                    tf = data.get("timeframe")
                    if sym and tf:
                        await _handle_subscribe(sym, tf)
        except (WebSocketDisconnect, Exception):
            pass

    async def _stream_updates():
        """Read Redis pub/sub and forward matching updates to client."""
        try:
            while True:
                if not streaming or pubsub is None:
                    await asyncio.sleep(0.1)
                    continue

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

                # Filter by current subscription
                if not symbol or not timeframe:
                    continue
                if payload.get("symbol") != symbol:
                    continue
                if payload.get("timeframe") != timeframe:
                    continue

                # Look up asset_id from reverse map
                direction = payload.get("direction")
                session = payload.get("session")
                if session is None:
                    # Legacy message (current session) — compute session ts
                    p = TF_SECONDS.get(timeframe, 300)
                    now = int(time.time())
                    session = now - (now % p)

                asset_id = reverse_map.get((direction, session))
                if not asset_id:
                    continue

                # Parse bids/asks
                try:
                    raw_bids = json.loads(payload["bids"]) if isinstance(payload["bids"], str) else payload["bids"]
                    raw_asks = json.loads(payload["asks"]) if isinstance(payload["asks"], str) else payload["asks"]
                except (json.JSONDecodeError, KeyError):
                    continue

                # Convert to Polymarket format
                bids = [{"price": str(b[0]), "size": str(b[1])} for b in raw_bids]
                asks = [{"price": str(a[0]), "size": str(a[1])} for a in raw_asks]

                await ws.send_json({
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": payload.get("updated_at"),
                })

        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as exc:
            logger.debug("WS polymarket stream error: %s", exc)

    # Run client reader and pub/sub streamer concurrently
    reader_task = asyncio.create_task(_read_client())
    stream_task = asyncio.create_task(_stream_updates())

    try:
        # Wait for either task to finish (client disconnect or error)
        done, pending = await asyncio.wait(
            {reader_task, stream_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
    except Exception:
        reader_task.cancel()
        stream_task.cancel()
    finally:
        if boundary_task and not boundary_task.done():
            boundary_task.cancel()
        if pubsub:
            try:
                await pubsub.unsubscribe("orderbook:updates")
                await pubsub.close()
            except Exception:
                pass
        if pubsub_redis:
            try:
                await pubsub_redis.aclose()
            except Exception:
                pass
