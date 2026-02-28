"""
Order Trace System — captures every micro-step of an order's lifecycle.

Traces are stored as a JSON array on each BinaryOption row and also published
to Redis for real-time UI updates via Pub/Sub.

Stages: VALIDATION, MATCHING, MONITORING, SETTLEMENT
"""

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_REDIS_TRACE_MAX = 10  # Keep latest N traces per order in Redis


def make_trace(
    stage: str,
    action: str,
    details: str,
    data: Optional[dict] = None,
) -> dict:
    """
    Create a trace dict with ISO timestamp.

    Args:
        stage:   VALIDATION | MATCHING | MONITORING | SETTLEMENT
        action:  Specific operation (e.g. REST_SWEEP, WS_TRIGGER, PRE_VALIDATION)
        details: Human-readable string explaining the logic
        data:    Optional JSON-serializable dict with prices/quantities
    """
    trace = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "action": action,
        "details": details,
    }
    if data is not None:
        trace["data"] = data
    return trace


def append_trace(bo, trace: dict) -> None:
    """
    Append a trace to a BinaryOption's traces list (SQLAlchemy-safe).

    SQLAlchemy's JSON column mutation tracking requires reassignment,
    so we deepcopy, append, and reassign.
    """
    current = bo.traces or []
    current = deepcopy(current)
    current.append(trace)
    bo.traces = current


def publish_trace_to_redis(redis_client, bo_id: int, trace: dict) -> None:
    """
    Publish a trace to Redis for real-time UI updates.

    - LPUSH to list `trace:{bo_id}` + LTRIM to keep latest N
    - PUBLISH to channel `trace:channel:{bo_id}` for Pub/Sub subscribers
    """
    list_key = f"trace:{bo_id}"
    channel = f"trace:channel:{bo_id}"
    try:
        payload = json.dumps(trace)
        redis_client.lpush(list_key, payload)
        redis_client.ltrim(list_key, 0, _REDIS_TRACE_MAX - 1)
        redis_client.publish(channel, payload)
    except Exception as exc:
        logger.warning("Failed to publish trace for BO #%d: %s", bo_id, exc)
