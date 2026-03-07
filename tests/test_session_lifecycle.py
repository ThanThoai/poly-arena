"""
Tests for ws_feed_service/session_lifecycle.py

Verifies:
  - ensure_future_sessions creates missing sessions (3 per sym/tf)
  - Pre-create buffer adds extra session when <20s to boundary
  - cleanup_expired_sessions archives sessions (retains in registry)
  - purge_archived_sessions removes old ARCHIVED + deletes Redis keys
  - cleanup_expired_sessions RPOPs orphaned orders
"""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from config.timing import TF_SECONDS, SESSION_CLEANUP_DELAY_S, ARCHIVED_RETENTION_S
from services.session_engine import SessionState
from services.session_manager import SessionManager
from ws_feed_service.session_lifecycle import (
    _expected_candle_opens,
    ensure_future_sessions,
    cleanup_expired_sessions,
    purge_archived_sessions,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_registry_stub(token_sessions=None):
    """Create a mock TokenRegistry with essential attributes."""
    registry = MagicMock()
    registry._mapping = {}
    registry._future_mapping = {}
    registry._token_sessions = token_sessions or {}
    registry.get_token_id.return_value = None
    registry.get_future_token_ids.return_value = []
    registry.get_all_token_mapping.return_value = {}
    registry.get_current_candle_open.side_effect = lambda tf: (
        int(time.time()) - (int(time.time()) % TF_SECONDS[tf])
    )
    return registry


def _populate_registry_tokens(registry, sym, tf, candle_open):
    """Add UP+DOWN tokens to registry stub for a given session."""
    up_token = f"tok-{sym}-{tf}-UP-{candle_open}"
    down_token = f"tok-{sym}-{tf}-DOWN-{candle_open}"
    period_s = TF_SECONDS[tf]
    now_ts = int(time.time())
    current_open = now_ts - (now_ts % period_s)

    registry._token_sessions[up_token] = candle_open
    registry._token_sessions[down_token] = candle_open

    if candle_open == current_open:
        registry._mapping[(sym, tf, "UP")] = up_token
        registry._mapping[(sym, tf, "DOWN")] = down_token
        # Make get_token_id return for current candle
        def _get_token_id(s, t, d, _up=up_token, _down=down_token, _sym=sym, _tf=tf):
            if s == _sym and t == _tf:
                return _up if d == "UP" else _down
            return None
        registry.get_token_id.side_effect = _get_token_id
    else:
        registry._future_mapping.setdefault((sym, tf, "UP"), []).append(up_token)
        registry._future_mapping.setdefault((sym, tf, "DOWN"), []).append(down_token)

    return up_token, down_token


# ── Tests: _expected_candle_opens ────────────────────────────────────────────


class TestExpectedCandleOpens:
    def test_returns_3_opens_normally(self):
        """Should return current + 2 future = 3 candle opens."""
        now_ts = 1709313000  # aligned to M5
        opens = _expected_candle_opens("M5", now_ts, num_future=2)
        assert len(opens) == 3
        assert opens[0] == now_ts
        assert opens[1] == now_ts + 300
        assert opens[2] == now_ts + 600

    def test_pre_create_buffer_adds_extra(self):
        """When <20s to boundary, should add 4th candle open."""
        period = TF_SECONDS["M5"]  # 300
        base = 1709313000
        # 15s before next boundary
        now_ts = base + period - 15
        opens = _expected_candle_opens("M5", now_ts, num_future=2)
        # Current candle is base, so 3 normal + 1 extra = 4
        assert len(opens) == 4

    def test_no_extra_when_far_from_boundary(self):
        """When >20s to boundary, should return exactly 3."""
        now_ts = 1709313000 + 100  # 200s to next boundary
        opens = _expected_candle_opens("M5", now_ts, num_future=2)
        assert len(opens) == 3


# ── Tests: ensure_future_sessions ────────────────────────────────────────────


def _mock_resolve_tokens(sym, tf, candle_open, pm=None):
    """Mock _resolve_tokens that returns deterministic UP+DOWN tokens + empty books."""
    tokens = {
        "UP": f"tok-{sym}-{tf}-UP-{candle_open}",
        "DOWN": f"tok-{sym}-{tf}-DOWN-{candle_open}",
    }
    initial_books = {}
    return tokens, initial_books


class TestEnsureFutureSessions:
    # Use a fixed timestamp 150s into an M5 candle (far from 20s pre-create buffer)
    STABLE_TS = 1709313150  # 1709313000 + 150

    @patch("ws_feed_service.session_lifecycle.SYMBOLS", ["BTC"])
    @patch("ws_feed_service.session_lifecycle.TIMEFRAMES", ["M5"])
    @patch("ws_feed_service.session_lifecycle._resolve_tokens", side_effect=_mock_resolve_tokens)
    @patch("ws_feed_service.session_lifecycle.time")
    def test_creates_missing_sessions(self, mock_time, mock_resolve):
        """Should create 3 sessions for a single (sym, tf) combo."""
        mock_time.time.return_value = self.STABLE_TS
        sm = SessionManager()
        registry = _make_registry_stub()
        writer = MagicMock()

        period_s = TF_SECONDS["M5"]
        current_open = self.STABLE_TS - (self.STABLE_TS % period_s)

        created, new_token_ids = ensure_future_sessions(sm, None, writer, registry)

        assert created == 3
        assert len(new_token_ids) == 6  # 3 sessions × 2 directions
        sessions = sm.list_sessions()
        assert len(sessions) == 3

        # Verify states
        states = {s.session_id: s.state for s in sessions}
        active_sid = f"BTC:M5:{current_open}"
        assert states[active_sid] == SessionState.ACTIVE
        for i in range(1, 3):
            prefetch_sid = f"BTC:M5:{current_open + i * period_s}"
            assert states[prefetch_sid] == SessionState.PREFETCH

    @patch("ws_feed_service.session_lifecycle.SYMBOLS", ["BTC"])
    @patch("ws_feed_service.session_lifecycle.TIMEFRAMES", ["M5"])
    @patch("ws_feed_service.session_lifecycle._resolve_tokens", side_effect=_mock_resolve_tokens)
    @patch("ws_feed_service.session_lifecycle.time")
    def test_skips_existing_sessions(self, mock_time, mock_resolve):
        """Should not recreate sessions that already exist."""
        mock_time.time.return_value = self.STABLE_TS
        sm = SessionManager()
        registry = _make_registry_stub()
        writer = MagicMock()

        period_s = TF_SECONDS["M5"]
        current_open = self.STABLE_TS - (self.STABLE_TS % period_s)

        # Pre-create one session
        sm.create_session(
            f"BTC:M5:{current_open}",
            {"UP": "existing-up", "DOWN": "existing-down"},
            initial_state=SessionState.ACTIVE,
        )

        created, new_token_ids = ensure_future_sessions(sm, None, writer, registry)

        # Should create 2 new (skipped existing)
        assert created == 2
        assert len(sm.list_sessions()) == 3

    @patch("ws_feed_service.session_lifecycle.SYMBOLS", ["BTC"])
    @patch("ws_feed_service.session_lifecycle.TIMEFRAMES", ["M5"])
    def test_skips_when_no_tokens(self):
        """Should skip session creation when tokens can't be resolved."""
        sm = SessionManager()
        registry = _make_registry_stub()
        writer = MagicMock()

        # Don't populate any tokens — all resolution should fail
        with patch("ws_feed_service.session_lifecycle._resolve_tokens", return_value=({}, {})):
            created, new_token_ids = ensure_future_sessions(sm, None, writer, registry)

        assert created == 0
        assert len(new_token_ids) == 0
        assert len(sm.list_sessions()) == 0

    @patch("ws_feed_service.session_lifecycle.SYMBOLS", ["BTC"])
    @patch("ws_feed_service.session_lifecycle.TIMEFRAMES", ["M5"])
    @patch("ws_feed_service.session_lifecycle._resolve_tokens", side_effect=_mock_resolve_tokens)
    @patch("ws_feed_service.session_lifecycle.time")
    def test_returns_new_token_ids(self, mock_time, mock_resolve):
        """Should return new token IDs for caller to subscribe (feed not called directly)."""
        mock_time.time.return_value = self.STABLE_TS
        sm = SessionManager()
        registry = _make_registry_stub()
        writer = MagicMock()

        created, new_token_ids = ensure_future_sessions(sm, None, writer, registry)

        # 3 sessions × 2 directions = 6 token IDs returned
        assert created == 3
        assert len(new_token_ids) == 6


# ── Tests: cleanup_expired_sessions ──────────────────────────────────────────


class TestCleanupExpiredSessions:
    def test_archives_expired_session(self, fake_sync_redis):
        """Should archive sessions past cleanup threshold (retained in registry)."""
        sm = SessionManager()

        # Create an "old" session that should be expired
        period_s = TF_SECONDS["M5"]
        old_candle = int(time.time()) - period_s - SESSION_CLEANUP_DELAY_S - 5
        session_id = f"BTC:M5:{old_candle}"
        sm.create_session(
            session_id,
            {"UP": "old-up", "DOWN": "old-down"},
            initial_state=SessionState.ACTIVE,
        )

        cleaned = cleanup_expired_sessions(sm, fake_sync_redis)
        assert cleaned == 1
        # Non-ARCHIVED list is empty, but session retained as ARCHIVED
        assert len(sm.list_sessions()) == 0
        assert len(sm.list_archived_sessions()) == 1
        assert sm.get_session(session_id).state == SessionState.ARCHIVED

    def test_skips_active_session(self, fake_sync_redis):
        """Should not clean up sessions that haven't expired yet."""
        sm = SessionManager()

        period_s = TF_SECONDS["M5"]
        now_ts = int(time.time())
        current_open = now_ts - (now_ts % period_s)
        session_id = f"BTC:M5:{current_open}"
        sm.create_session(
            session_id,
            {"UP": "active-up", "DOWN": "active-down"},
            initial_state=SessionState.ACTIVE,
        )

        cleaned = cleanup_expired_sessions(sm, fake_sync_redis)
        assert cleaned == 0
        assert len(sm.list_sessions()) == 1

    def test_rpops_orphaned_orders(self, fake_sync_redis):
        """Should RPOP orphaned orders from expired queue."""
        sm = SessionManager()

        period_s = TF_SECONDS["M5"]
        old_candle = int(time.time()) - period_s - SESSION_CLEANUP_DELAY_S - 5
        session_id = f"BTC:M5:{old_candle}"
        sm.create_session(
            session_id,
            {"UP": "old-up", "DOWN": "old-down"},
            initial_state=SessionState.ACTIVE,
        )

        # Push orphaned orders
        queue_key = f"queue:orders:{session_id}"
        fake_sync_redis.lpush(queue_key, json.dumps({"bo_id": 1}))
        fake_sync_redis.lpush(queue_key, json.dumps({"bo_id": 2}))

        cleaned = cleanup_expired_sessions(sm, fake_sync_redis)
        assert cleaned == 1

        # Queue should be empty now
        assert fake_sync_redis.llen(queue_key) == 0

    def test_handles_settling_sessions(self, fake_sync_redis):
        """Should archive SETTLING sessions past threshold."""
        sm = SessionManager()

        period_s = TF_SECONDS["M5"]
        old_candle = int(time.time()) - period_s - SESSION_CLEANUP_DELAY_S - 5
        session_id = f"BTC:M5:{old_candle}"
        sm.create_session(
            session_id,
            {"UP": "old-up", "DOWN": "old-down"},
            initial_state=SessionState.SETTLING,
        )

        cleaned = cleanup_expired_sessions(sm, fake_sync_redis)
        assert cleaned == 1
        assert len(sm.list_sessions()) == 0
        assert len(sm.list_archived_sessions()) == 1


class TestPurgeArchivedSessions:
    def test_purges_old_archived_sessions(self, fake_sync_redis):
        """Should purge ARCHIVED sessions past ARCHIVED_RETENTION_S after settling."""
        from datetime import datetime, timezone, timedelta

        sm = SessionManager()

        period_s = TF_SECONDS["M5"]
        old_candle = int(time.time()) - period_s - SESSION_CLEANUP_DELAY_S - 5
        session_id = f"BTC:M5:{old_candle}"
        sm.create_session(
            session_id,
            {"UP": "old-up", "DOWN": "old-down"},
            initial_state=SessionState.ACTIVE,
        )

        # Archive it
        cleanup_expired_sessions(sm, fake_sync_redis)
        assert len(sm.list_archived_sessions()) == 1

        # settling_at is recent — purge should NOT remove it yet
        purged = purge_archived_sessions(sm, fake_sync_redis)
        assert purged == 0
        assert len(sm.list_archived_sessions()) == 1

        # Backdate settling_at to > ARCHIVED_RETENTION_S ago
        engine = sm.get_session(session_id)
        engine.settling_at = datetime.now(timezone.utc) - timedelta(seconds=ARCHIVED_RETENTION_S + 5)

        purged = purge_archived_sessions(sm, fake_sync_redis)
        assert purged == 1
        assert len(sm.list_archived_sessions()) == 0
        assert sm.get_session(session_id) is None

    def test_deletes_redis_keys_on_purge(self, fake_sync_redis):
        """Should delete queue + orderbook Redis keys when purging."""
        from datetime import datetime, timezone, timedelta

        sm = SessionManager()

        period_s = TF_SECONDS["M5"]
        old_candle = int(time.time()) - period_s - SESSION_CLEANUP_DELAY_S - 5
        session_id = f"BTC:M5:{old_candle}"
        sm.create_session(
            session_id,
            {"UP": "old-up", "DOWN": "old-down"},
            initial_state=SessionState.ACTIVE,
        )

        # Create Redis keys
        queue_key = f"queue:orders:{session_id}"
        ob_up_key = f"orderbook:BTC:M5:UP:{old_candle}"
        ob_down_key = f"orderbook:BTC:M5:DOWN:{old_candle}"
        fake_sync_redis.lpush(queue_key, "test")
        fake_sync_redis.set(ob_up_key, "test")
        fake_sync_redis.set(ob_down_key, "test")

        # Archive it
        cleanup_expired_sessions(sm, fake_sync_redis)

        # Redis keys still exist after archive (not yet purged)
        assert fake_sync_redis.exists(ob_up_key) == 1
        assert fake_sync_redis.exists(ob_down_key) == 1

        # Backdate settling_at and purge
        engine = sm.get_session(session_id)
        engine.settling_at = datetime.now(timezone.utc) - timedelta(seconds=ARCHIVED_RETENTION_S + 5)

        purge_archived_sessions(sm, fake_sync_redis)

        assert fake_sync_redis.exists(queue_key) == 0
        assert fake_sync_redis.exists(ob_up_key) == 0
        assert fake_sync_redis.exists(ob_down_key) == 0

    def test_retains_recent_archived(self, fake_sync_redis):
        """Should NOT purge ARCHIVED sessions within ARCHIVED_RETENTION_S."""
        sm = SessionManager()

        period_s = TF_SECONDS["M5"]
        old_candle = int(time.time()) - period_s - SESSION_CLEANUP_DELAY_S - 5
        session_id = f"BTC:M5:{old_candle}"
        sm.create_session(
            session_id,
            {"UP": "old-up", "DOWN": "old-down"},
            initial_state=SessionState.ACTIVE,
        )

        cleanup_expired_sessions(sm, fake_sync_redis)

        # settling_at is just now — should NOT be purged
        purged = purge_archived_sessions(sm, fake_sync_redis)
        assert purged == 0
        assert len(sm.list_archived_sessions()) == 1
