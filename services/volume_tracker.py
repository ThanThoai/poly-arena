"""
Per-minute volume tracking from Polymarket WebSocket trade events.

Accumulates volume in Redis hashes keyed by session:
  volume:{SYM}:{TF}:{candle_ts}

Each field:
  {minute_ts}:{direction}:amt   → cumulative dollar volume (price × size)
  {minute_ts}:{direction}:cnt   → trade count
  {minute_ts}:{direction}:sz    → cumulative share volume (size)
"""

import json
import logging
import time

logger = logging.getLogger(__name__)

VOLUME_KEY_PREFIX = "volume"
VOLUME_TTL_S = 1800  # 30 min TTL
VOLUME_PUBSUB_CHANNEL = "volume:updates"


def _minute_ts() -> int:
    """Current time floored to the minute."""
    now = int(time.time())
    return now - (now % 60)


async def record_trade_volume(
    r,
    symbol: str,
    timeframe: str,
    candle_ts: int,
    direction: str,
    price: float,
    size: float,
) -> None:
    """
    Record a Polymarket trade event as volume (async Redis).

    Called from WS Feed Service when a last_trade_price event arrives.
    price × size = dollar volume.
    """
    minute = _minute_ts()
    dollar_vol = round(price * size, 4)
    key = f"{VOLUME_KEY_PREFIX}:{symbol}:{timeframe}:{candle_ts}"

    try:
        pipe = r.pipeline(transaction=False)
        pipe.hincrbyfloat(key, f"{minute}:{direction}:amt", dollar_vol)
        pipe.hincrby(key, f"{minute}:{direction}:cnt", 1)
        pipe.hincrbyfloat(key, f"{minute}:{direction}:sz", round(size, 4))
        pipe.expire(key, VOLUME_TTL_S)
        pipe.publish(VOLUME_PUBSUB_CHANNEL, json.dumps({
            "symbol": symbol,
            "timeframe": timeframe,
            "session": candle_ts,
            "minute": minute,
            "direction": direction,
            "amount": dollar_vol,
            "size": round(size, 4),
        }))
        await pipe.execute()
    except Exception as exc:
        logger.debug("record_trade_volume failed: %s", exc)


def get_session_volume(r, symbol: str, timeframe: str, candle_ts: int) -> list[dict]:
    """
    Read all volume data for a session from Redis.

    Returns list of {minute, up_amount, down_amount, up_trades, down_trades,
    up_size, down_size} sorted by minute timestamp.
    """
    key = f"{VOLUME_KEY_PREFIX}:{symbol}:{timeframe}:{candle_ts}"
    try:
        data = r.hgetall(key)
    except Exception:
        return []

    if not data:
        return []

    # Parse hash fields: "{minute}:{direction}:{metric}"
    minutes: dict[int, dict] = {}
    for field, value in data.items():
        field_str = field.decode() if isinstance(field, bytes) else field
        value_str = value.decode() if isinstance(value, bytes) else value

        parts = field_str.split(":")
        if len(parts) != 3:
            continue

        minute = int(parts[0])
        direction = parts[1]  # UP or DOWN
        metric = parts[2]     # amt, cnt, or sz

        if minute not in minutes:
            minutes[minute] = {
                "minute": minute,
                "up_amount": 0.0, "down_amount": 0.0,
                "up_trades": 0, "down_trades": 0,
                "up_size": 0.0, "down_size": 0.0,
            }

        if direction == "UP":
            if metric == "amt":
                minutes[minute]["up_amount"] = round(float(value_str), 4)
            elif metric == "cnt":
                minutes[minute]["up_trades"] = int(float(value_str))
            elif metric == "sz":
                minutes[minute]["up_size"] = round(float(value_str), 4)
        elif direction == "DOWN":
            if metric == "amt":
                minutes[minute]["down_amount"] = round(float(value_str), 4)
            elif metric == "cnt":
                minutes[minute]["down_trades"] = int(float(value_str))
            elif metric == "sz":
                minutes[minute]["down_size"] = round(float(value_str), 4)

    return sorted(minutes.values(), key=lambda x: x["minute"])
