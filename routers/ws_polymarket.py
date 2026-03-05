"""
WebSocket proxy for Polymarket orderbook data.

Proxies orderbook data from the shared OrderbookBroadcaster to UI clients in a
Polymarket-compatible format, so the UI doesn't need direct Polymarket WebSocket
connections or Gamma API access.

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

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from config.timing import TF_SECONDS
from services.orderbook_broadcaster import (
    broadcaster,
    snapshot_cache,
    token_cache,
    ParsedOrderbookMessage,
)
from services.redis_client import get_async_redis

logger = logging.getLogger(__name__)
router = APIRouter()

_WS_PING_INTERVAL = 15   # send ping every N seconds
_WS_PONG_TIMEOUT = 30    # close if no pong within N seconds


# ── Helpers ──────────────────────────────────────────────────────────────────

def _book_event_from_parsed(
    msg: ParsedOrderbookMessage,
    asset_id: str,
) -> dict:
    """Build a Polymarket-format book event from a parsed broadcaster message."""
    bids = [{"price": str(b[0]), "size": str(b[1])} for b in msg.bids]
    asks = [{"price": str(a[0]), "size": str(a[1])} for a in msg.asks]
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "bids": bids,
        "asks": asks,
        "timestamp": msg.updated_at,
    }


def _book_event_from_snapshot(
    entry: dict,
    reverse_map: dict[tuple[str, int], str],
    period: int,
) -> dict | None:
    """Convert a snapshot cache entry to Polymarket book event format."""
    direction = entry.get("direction")
    session = entry.get("session")
    if session is None:
        # Should not happen — snapshot entries now always include session.
        logger.warning("Snapshot entry missing session: %s", entry.get("direction"))
        now = int(time.time())
        session = now - (now % period)

    asset_id = reverse_map.get((direction, session))
    if not asset_id:
        return None

    raw_bids = entry.get("bids", [])
    raw_asks = entry.get("asks", [])
    if not raw_bids and not raw_asks:
        return None

    bids = [{"price": str(b[0]), "size": str(b[1])} for b in raw_bids]
    asks = [{"price": str(a[0]), "size": str(a[1])} for a in raw_asks]
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "bids": bids,
        "asks": asks,
        "timestamp": entry.get("updated_at"),
    }


# ── Main WebSocket endpoint ─────────────────────────────────────────────────

@router.websocket("/ws/polymarket")
async def polymarket_proxy_ws(ws: WebSocket):
    await ws.accept()

    symbol: str | None = None
    timeframe: str | None = None
    reverse_map: dict[tuple[str, int], str] = {}
    period: int = 300
    boundary_task: asyncio.Task | None = None
    ping_task: asyncio.Task | None = None
    queue: asyncio.Queue | None = None
    last_pong = time.monotonic()
    closed = False

    async def _schedule_boundary(sym: str, tf: str):
        """Wait for next candle boundary, then re-discover tokens and send updates."""
        nonlocal reverse_map, period
        try:
            while True:
                now_s = int(time.time())
                next_bound = ((now_s // period) + 1) * period
                wait_s = next_bound - now_s + 2  # +2s buffer
                await asyncio.sleep(wait_s)

                # Invalidate and re-discover via cache (coalesced)
                await token_cache.invalidate(sym, tf)
                result = await token_cache.discover(sym, tf)
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

                # Send new snapshots via cache
                r = get_async_redis()
                try:
                    entries = await snapshot_cache.get_snapshot(r, sym, tf)
                    for entry in entries:
                        evt = _book_event_from_snapshot(entry, reverse_map, period)
                        if evt:
                            await ws.send_json(evt)
                except Exception:
                    return

        except asyncio.CancelledError:
            return

    async def _handle_subscribe(sym: str, tf: str):
        """Process a subscribe request: discover tokens, send map + snapshots, start streaming."""
        nonlocal symbol, timeframe, reverse_map, period, boundary_task, queue

        symbol = sym.upper()
        timeframe = tf.upper()

        # Cancel previous boundary timer
        if boundary_task and not boundary_task.done():
            boundary_task.cancel()

        # Discover tokens (coalesced via cache)
        result = await token_cache.discover(symbol, timeframe)
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

        # Send initial snapshots via cache
        r = get_async_redis()
        entries = await snapshot_cache.get_snapshot(r, symbol, timeframe)
        for entry in entries:
            evt = _book_event_from_snapshot(entry, reverse_map, period)
            if evt:
                await ws.send_json(evt)

        # Start boundary rotation
        boundary_task = asyncio.create_task(
            _schedule_boundary(symbol, timeframe)
        )

        # Subscribe to broadcaster if not already
        if queue is None:
            queue = await broadcaster.subscribe()

    async def _ping_loop():
        """Send periodic ping and close on pong timeout."""
        nonlocal closed
        try:
            while not closed:
                await asyncio.sleep(_WS_PING_INTERVAL)
                if closed:
                    break
                if time.monotonic() - last_pong > _WS_PONG_TIMEOUT:
                    logger.info("WS polymarket client pong timeout — closing")
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

    async def _read_client():
        """Read messages from client (PING, subscribe, pong)."""
        nonlocal last_pong
        try:
            while True:
                raw = await ws.receive_text()
                if raw == "PING":
                    await ws.send_text("PONG")
                    last_pong = time.monotonic()
                    continue
                if raw == "PONG" or raw == '{"type":"pong"}':
                    last_pong = time.monotonic()
                    continue
                try:
                    data = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if data.get("type") == "pong":
                    last_pong = time.monotonic()
                    continue
                if data.get("type") == "subscribe":
                    sym = data.get("symbol")
                    tf = data.get("timeframe")
                    if sym and tf:
                        await _handle_subscribe(sym, tf)
        except (WebSocketDisconnect, Exception):
            pass

    async def _stream_updates():
        """Read broadcaster queue and forward matching updates to client."""
        try:
            while True:
                if queue is None:
                    await asyncio.sleep(0.1)
                    continue

                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                # Filter by current subscription
                if not symbol or not timeframe:
                    continue
                if msg.symbol != symbol:
                    continue
                if msg.timeframe != timeframe:
                    continue

                # Look up asset_id from reverse map
                direction = msg.direction
                session = msg.session
                if session is None:
                    # Should not happen — all pub/sub messages now include session.
                    # Fallback: derive from current time (may be wrong at boundary).
                    logger.warning("Broadcaster message missing session: %s/%s/%s",
                                   msg.symbol, msg.timeframe, direction)
                    p = TF_SECONDS.get(timeframe, 300)
                    now = int(time.time())
                    session = now - (now % p)

                asset_id = reverse_map.get((direction, session))
                if not asset_id:
                    continue

                await ws.send_json(_book_event_from_parsed(msg, asset_id))

        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as exc:
            logger.debug("WS polymarket stream error: %s", exc)

    # Run client reader, streamer, and ping loop concurrently
    reader_task = asyncio.create_task(_read_client())
    stream_task = asyncio.create_task(_stream_updates())
    ping_task = asyncio.create_task(_ping_loop())

    try:
        done, pending = await asyncio.wait(
            {reader_task, stream_task, ping_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
    except Exception:
        reader_task.cancel()
        stream_task.cancel()
        ping_task.cancel()
    finally:
        closed = True
        if boundary_task and not boundary_task.done():
            boundary_task.cancel()
        if ping_task and not ping_task.done():
            ping_task.cancel()
        if queue is not None:
            await broadcaster.unsubscribe(queue)
