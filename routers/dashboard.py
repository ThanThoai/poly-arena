import json
import time

from fastapi import APIRouter

from services.redis_client import get_sync_redis
from scheduler_service.config import HEARTBEAT_KEY

router = APIRouter()


@router.get("/scheduler/status")
def get_scheduler_status():
    try:
        raw = get_sync_redis().get(HEARTBEAT_KEY)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {"status": "DOWN"}


@router.get("/ws-health")
def ws_health():
    """
    WS pipeline health: price staleness, stream consumer lag,
    broadcaster status, scheduler heartbeat.
    """
    from services.orderbook_broadcaster import broadcaster

    sr = get_sync_redis()
    now = time.time()
    result: dict = {
        "broadcaster": {
            "connected": broadcaster.is_connected,
            "subscribers": broadcaster.subscriber_count,
        },
        "scheduler": {"status": "DOWN"},
        "prices": [],
        "streams": {},
    }

    # Scheduler heartbeat
    try:
        raw = sr.get(HEARTBEAT_KEY)
        if raw:
            result["scheduler"] = json.loads(raw)
            result["scheduler"]["status"] = "UP"
        else:
            result["scheduler"] = {"status": "DOWN"}
    except Exception:
        pass

    # Price staleness check
    _SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
    _TFS = ["M5", "M15", "H1"]
    _DIRS = ["UP", "DOWN"]
    stale_count = 0
    for sym in _SYMBOLS:
        for tf in _TFS:
            for d in _DIRS:
                key = f"price:{sym}:{tf}:{d}"
                try:
                    data = sr.hgetall(key)
                    if not data:
                        continue
                    updated = float(data.get("updated_at", "0") or "0")
                    age = round(now - updated, 1)
                    is_stale = age > 45
                    if is_stale:
                        stale_count += 1
                    result["prices"].append({
                        "key": f"{sym}:{tf}:{d}",
                        "age_s": age,
                        "stale": is_stale,
                        "best_ask": data.get("best_ask"),
                    })
                except Exception:
                    pass
    result["stale_price_count"] = stale_count

    # Stream consumer lag
    for stream_name in [
        "stream:bracket:exits",
        "stream:order:fills",
        "stream:order:cancels",
        "stream:market:resolved",
    ]:
        try:
            info = sr.xinfo_groups(stream_name)
            groups = []
            for g in info:
                groups.append({
                    "name": g.get("name", ""),
                    "pending": g.get("pending", 0),
                    "consumers": g.get("consumers", 0),
                    "last_delivered": g.get("last-delivered-id", ""),
                })
            stream_len = sr.xlen(stream_name)
            result["streams"][stream_name] = {
                "length": stream_len,
                "groups": groups,
            }
        except Exception:
            result["streams"][stream_name] = {"length": 0, "groups": [], "error": "stream not found"}

    return result
