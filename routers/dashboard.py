import json

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
