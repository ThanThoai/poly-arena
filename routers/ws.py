"""
WebSocket endpoint for real-time orderbook depth updates.

Subscribes to the shared OrderbookBroadcaster (single Redis pub/sub connection)
and forwards matching updates to connected clients.

Protocol:
  - Client connects → server sends snapshot of current orderbook state
  - Client sends filter: {"symbol": "BTC", "timeframe": "M5"}
  - Server sends new snapshot matching the filter
  - Server streams incremental updates via broadcaster queue
  - Server sends {"type":"ping"} every 15s; client should reply {"type":"pong"}
  - If no pong received within 30s, server closes connection
"""

import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from services.orderbook_broadcaster import broadcaster, snapshot_cache
from services.redis_client import get_async_redis

logger = logging.getLogger(__name__)
router = APIRouter()

_WS_PING_INTERVAL = 15   # send ping every N seconds
_WS_PONG_TIMEOUT = 30    # close if no pong within N seconds
_SNAPSHOT_RESYNC_INTERVAL = 30  # periodic full snapshot resync


@router.websocket("/ws/orderbook")
async def orderbook_ws(ws: WebSocket):
    await ws.accept()

    filter_symbol: str | None = None
    filter_timeframe: str | None = None
    snapshot_requested = asyncio.Event()
    last_pong = time.monotonic()
    closed = False

    r = get_async_redis()
    queue = await broadcaster.subscribe()

    # Send initial snapshot
    try:
        entries = await snapshot_cache.get_snapshot(r, None, None)
        for entry in entries:
            await ws.send_json(entry)
    except Exception as exc:
        logger.debug("WS initial snapshot failed: %s", exc)

    async def _read_client():
        """Read filter messages and pong responses from client."""
        nonlocal filter_symbol, filter_timeframe, last_pong
        try:
            while True:
                msg = await ws.receive_text()
                # Handle pong (text or JSON)
                if msg == "PONG" or msg == '{"type":"pong"}':
                    last_pong = time.monotonic()
                    continue
                try:
                    data = json.loads(msg)
                    if data.get("type") == "pong":
                        last_pong = time.monotonic()
                        continue
                    filter_symbol = data.get("symbol")
                    filter_timeframe = data.get("timeframe")
                    snapshot_requested.set()
                except (json.JSONDecodeError, TypeError):
                    pass
        except (WebSocketDisconnect, Exception):
            pass

    async def _ping_loop():
        """Send periodic ping and close on pong timeout."""
        nonlocal closed
        try:
            while not closed:
                await asyncio.sleep(_WS_PING_INTERVAL)
                if closed:
                    break
                # Check pong timeout
                if time.monotonic() - last_pong > _WS_PONG_TIMEOUT:
                    logger.info("WS orderbook client pong timeout — closing")
                    closed = True
                    try:
                        await ws.close(1000, "Pong timeout")
                    except Exception:
                        pass
                    return
                try:
                    await ws.send_json({"type": "ping"})
                except Exception:
                    return
        except asyncio.CancelledError:
            pass

    async def _snapshot_resync_loop():
        """Periodic full snapshot resync to recover from dropped messages."""
        try:
            while not closed:
                await asyncio.sleep(_SNAPSHOT_RESYNC_INTERVAL)
                if closed:
                    break
                try:
                    entries = await snapshot_cache.get_snapshot(
                        r, filter_symbol, filter_timeframe,
                    )
                    for entry in entries:
                        await ws.send_json(entry)
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    reader_task = asyncio.create_task(_read_client())
    ping_task = asyncio.create_task(_ping_loop())
    resync_task = asyncio.create_task(_snapshot_resync_loop())

    try:
        while not closed:
            # Check if client sent a new filter → send snapshot
            if snapshot_requested.is_set():
                snapshot_requested.clear()
                try:
                    entries = await snapshot_cache.get_snapshot(
                        r, filter_symbol, filter_timeframe,
                    )
                    for entry in entries:
                        await ws.send_json(entry)
                except Exception:
                    break

            # Read from broadcaster queue with short timeout to interleave snapshot checks
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue

            # Apply client filter
            if filter_symbol and msg.symbol != filter_symbol.upper():
                continue
            if filter_timeframe and msg.timeframe != filter_timeframe.upper():
                continue

            msg_out: dict = {
                "type": "update",
                "symbol": msg.symbol,
                "timeframe": msg.timeframe,
                "direction": msg.direction,
                "bids": msg.bids,
                "asks": msg.asks,
                "updated_at": msg.updated_at,
            }
            if msg.session is not None:
                msg_out["session"] = msg.session
            await ws.send_json(msg_out)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        closed = True
        reader_task.cancel()
        ping_task.cancel()
        resync_task.cancel()
        await broadcaster.unsubscribe(queue)
