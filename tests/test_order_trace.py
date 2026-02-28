"""
Tests for the Order Trace System (v2 spec Section 7).

Verifies:
  - make_trace creates correct structure
  - append_trace appends to BO traces list (SQLAlchemy-safe)
  - publish_trace_to_redis uses LPUSH + LTRIM + PUBLISH
"""

import json
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from services.order_trace import make_trace, append_trace, publish_trace_to_redis


def test_make_trace_basic():
    """make_trace returns a dict with required fields."""
    trace = make_trace("VALIDATION", "PRE_VALIDATION_OK", "All good")
    assert trace["stage"] == "VALIDATION"
    assert trace["action"] == "PRE_VALIDATION_OK"
    assert trace["details"] == "All good"
    assert "timestamp" in trace
    # Verify ISO format
    datetime.fromisoformat(trace["timestamp"])
    assert "data" not in trace


def test_make_trace_with_data():
    """make_trace includes optional data dict."""
    data = {"best_ask": 0.52, "tp_price": 0.70}
    trace = make_trace("MATCHING", "REST_SWEEP", "Sweep done", data)
    assert trace["data"] == data
    assert trace["stage"] == "MATCHING"


def test_append_trace_to_empty():
    """append_trace on a BO with no traces creates the list."""
    bo = MagicMock()
    bo.traces = None

    trace = make_trace("VALIDATION", "TEST", "test detail")
    append_trace(bo, trace)

    assert bo.traces is not None
    assert len(bo.traces) == 1
    assert bo.traces[0]["action"] == "TEST"


def test_append_trace_preserves_existing():
    """append_trace appends to existing traces without mutation."""
    existing = [make_trace("VALIDATION", "FIRST", "first")]
    bo = MagicMock()
    bo.traces = existing

    trace = make_trace("MATCHING", "SECOND", "second")
    append_trace(bo, trace)

    assert len(bo.traces) == 2
    assert bo.traces[0]["action"] == "FIRST"
    assert bo.traces[1]["action"] == "SECOND"
    # Original list should not be mutated (deepcopy)
    assert len(existing) == 1


def test_publish_trace_to_redis():
    """publish_trace_to_redis calls LPUSH, LTRIM, and PUBLISH."""
    mock_redis = MagicMock()
    trace = make_trace("MONITORING", "WS_TRIGGER", "TP hit at $0.72")

    publish_trace_to_redis(mock_redis, 42, trace)

    # Verify LPUSH
    mock_redis.lpush.assert_called_once()
    call_args = mock_redis.lpush.call_args
    assert call_args[0][0] == "trace:42"
    payload = call_args[0][1]
    parsed = json.loads(payload)
    assert parsed["action"] == "WS_TRIGGER"

    # Verify LTRIM
    mock_redis.ltrim.assert_called_once_with("trace:42", 0, 9)

    # Verify PUBLISH
    mock_redis.publish.assert_called_once()
    pub_args = mock_redis.publish.call_args
    assert pub_args[0][0] == "trace:channel:42"


def test_publish_trace_handles_error():
    """publish_trace_to_redis should not raise on Redis errors."""
    mock_redis = MagicMock()
    mock_redis.lpush.side_effect = Exception("connection lost")

    trace = make_trace("SETTLEMENT", "CANDLE_FETCH", "Fetching candle")
    # Should not raise
    publish_trace_to_redis(mock_redis, 99, trace)
