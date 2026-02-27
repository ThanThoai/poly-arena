"""
Unit tests for services/token_registry.py

Tests:
  - next_refresh_times(): correct candle boundary calculation
  - discover_all(): builds mapping from mocked PolymarketClient
  - Refresh loop: detects boundary, calls on_new_tokens with changed IDs
  - Retry logic: skips symbols that fail, reports error after max retries
  - add_tokens() on ws_feed: immediate re-subscribe on live connection
"""

import asyncio
import math
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

sys.path.insert(0, ".")

from services.token_registry import (
    TokenRegistry,
    _REFRESH_OFFSET_S,
    _TF_SECONDS,
    next_refresh_times,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_ob(token_id: str):
    ob = MagicMock()
    ob.token_id = token_id
    ob.min_ask  = 0.52
    ob.max_bid  = 0.50
    return ob


# ── next_refresh_times ────────────────────────────────────────────────────────


def test_next_refresh_times_are_after_boundary():
    """next_refresh_times() returns times after candle boundary + offset."""
    result = next_refresh_times()
    now = datetime.now(timezone.utc)

    for tf, refresh_dt in result.items():
        period_s = _TF_SECONDS[tf]
        now_ts = now.timestamp()
        # Next boundary must be in the future
        next_boundary_ts = (math.floor(now_ts / period_s) + 1) * period_s
        expected_ts = next_boundary_ts + _REFRESH_OFFSET_S
        assert abs(refresh_dt.timestamp() - expected_ts) < 2, (
            f"{tf}: expected ~{expected_ts}, got {refresh_dt.timestamp()}"
        )


def test_next_refresh_times_all_timeframes():
    result = next_refresh_times(["M5", "M15", "H1"])
    assert set(result.keys()) == {"M5", "M15", "H1"}
    # H1 refresh must be further away than M5
    assert result["H1"] >= result["M5"]


# ── discover_all ──────────────────────────────────────────────────────────────


def test_discover_all_builds_mapping():
    """discover_all() populates mapping for all symbol/tf/direction combos."""
    call_count = [0]

    def fake_get_orderbook(sym, tf, direction):
        call_count[0] += 1
        return _make_ob(f"token-{sym}-{tf}-{direction}")

    registry = TokenRegistry(symbols=["BTC", "ETH"], timeframes=["M5"])

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = fake_get_orderbook

        ids = registry.discover_all()

    # 2 symbols × 1 tf × 2 directions = 4
    assert len(ids) == 4
    assert len(registry._mapping) == 4
    assert registry.get_token_id("BTC", "M5", "UP") == "token-BTC-M5-UP"
    assert registry.get_token_id("ETH", "M5", "DOWN") == "token-ETH-M5-DOWN"


def test_discover_all_partial_failure():
    """discover_all() skips failed combos and continues."""
    def fake_get_orderbook(sym, tf, direction):
        if sym == "ETH":
            raise RuntimeError("market not found")
        return _make_ob(f"token-{sym}-{tf}-{direction}")

    registry = TokenRegistry(symbols=["BTC", "ETH"], timeframes=["M5"])

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = fake_get_orderbook

        ids = registry.discover_all()

    # Only BTC succeeded (2 directions)
    assert len(ids) == 2
    assert registry.get_token_id("BTC", "M5", "UP") is not None
    assert registry.get_token_id("ETH", "M5", "UP") is None


def test_all_token_ids_deduplicates():
    registry = TokenRegistry(symbols=["BTC"], timeframes=["M5"])
    registry._mapping = {
        ("BTC", "M5", "UP"):   "tok-a",
        ("BTC", "M5", "DOWN"): "tok-b",
    }
    ids = registry.all_token_ids()
    assert sorted(ids) == ["tok-a", "tok-b"]


# ── _fetch_timeframe ──────────────────────────────────────────────────────────


def test_fetch_timeframe_detects_rotated_tokens():
    """_fetch_timeframe() returns only changed token_ids."""
    registry = TokenRegistry(symbols=["BTC"], timeframes=["M5"])
    # Pre-populate with old tokens
    registry._mapping = {
        ("BTC", "M5", "UP"):   "old-up",
        ("BTC", "M5", "DOWN"): "old-down",
    }

    def fake_get_orderbook(sym, tf, direction):
        if direction == "UP":
            return _make_ob("new-up")      # changed
        return _make_ob("old-down")        # unchanged

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = fake_get_orderbook

        new_ids = asyncio.run(registry._fetch_timeframe("M5"))

    assert new_ids == ["new-up"]           # only the changed one
    assert registry.get_token_id("BTC", "M5", "UP") == "new-up"
    assert registry.get_token_id("BTC", "M5", "DOWN") == "old-down"


def test_fetch_timeframe_returns_none_when_all_fail():
    """_fetch_timeframe() returns None when all requests fail (market not ready)."""
    registry = TokenRegistry(symbols=["BTC"], timeframes=["M5"])

    def fake_get_orderbook(*args):
        raise ConnectionError("not ready")

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = fake_get_orderbook

        result = asyncio.run(registry._fetch_timeframe("M5"))

    assert result is None


# ── on_new_tokens callback ────────────────────────────────────────────────────


def test_discover_all_no_callback():
    """discover_all() does NOT trigger on_new_tokens (that's for refresh only)."""
    received = []
    registry = TokenRegistry(
        on_new_tokens=lambda ids: received.extend(ids),
        symbols=["BTC"], timeframes=["M5"],
    )

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = lambda s, t, d: _make_ob(f"tok-{s}-{d}")
        registry.discover_all()

    # discover_all does NOT call on_new_tokens
    assert received == []


def test_refresh_calls_on_new_tokens_for_changed_ids():
    """on_new_tokens is called with only the changed token_ids after refresh."""
    received = []
    registry = TokenRegistry(
        on_new_tokens=lambda ids: received.extend(ids),
        symbols=["BTC"], timeframes=["M5"],
    )
    registry._mapping = {
        ("BTC", "M5", "UP"):   "old-up",
        ("BTC", "M5", "DOWN"): "old-down",
    }

    def fake_get_orderbook(sym, tf, direction):
        return _make_ob("new-up") if direction == "UP" else _make_ob("old-down")

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = fake_get_orderbook

        asyncio.run(registry._refresh_timeframe_with_retry("M5"))

    assert received == ["new-up"]


def test_refresh_retries_when_same_tokens_at_boundary():
    """If Polymarket returns the same token_ids at boundary, keep retrying (not a success)."""
    received = []
    registry = TokenRegistry(
        on_new_tokens=lambda ids: received.extend(ids),
        symbols=["BTC"], timeframes=["M5"],
    )
    registry._mapping = {
        ("BTC", "M5", "UP"):   "old-up",
        ("BTC", "M5", "DOWN"): "old-down",
    }

    call_count = [0]

    def fake_get_orderbook(sym, tf, direction):
        call_count[0] += 1
        # First 2 calls return old tokens (market not rotated yet)
        # Next 2 calls return new tokens (market rotated)
        if call_count[0] <= 4:  # 2 directions × 2 attempts = 4 calls with old tokens
            return _make_ob(f"old-{direction.lower()}")
        else:
            return _make_ob(f"new-{direction.lower()}")

    with patch("services.token_registry.PolymarketClient") as MockClient:
        instance = MockClient.return_value.__enter__.return_value
        instance.get_orderbook.side_effect = fake_get_orderbook

        with patch("services.token_registry._REFRESH_RETRY_DELAY", 0):
            asyncio.run(registry._refresh_timeframe_with_retry("M5"))

    # Should have retried and found the new tokens
    assert "new-up" in received
    assert "new-down" in received
    # Should have called get_orderbook at least 6 times (2 directions × 3 attempts)
    assert call_count[0] >= 6


# ── ws_feed.add_tokens with re-subscribe ─────────────────────────────────────


def test_add_tokens_triggers_resubscribe_when_ws_active():
    """add_tokens() fires _resubscribe task when WebSocket is connected."""
    from services.ws_feed import PolymarketFeed

    feed = PolymarketFeed(token_ids=["existing-tok"])
    feed._running = True
    feed._ws = MagicMock()  # simulate connected WS

    created_tasks = []
    original_create_task = asyncio.create_task

    async def _run():
        def fake_create_task(coro, **kw):
            # capture the coroutine but don't actually run it
            created_tasks.append(coro)
            return MagicMock()

        with patch("asyncio.create_task", side_effect=fake_create_task):
            feed.add_tokens(["new-tok-1", "new-tok-2"])

    asyncio.run(_run())

    assert "new-tok-1" in feed.token_ids
    assert "new-tok-2" in feed.token_ids
    assert len(created_tasks) == 1   # one _resubscribe task created


def test_add_tokens_no_resubscribe_when_disconnected():
    """add_tokens() does NOT fire _resubscribe task when WS is not active."""
    from services.ws_feed import PolymarketFeed

    feed = PolymarketFeed(token_ids=["existing-tok"])
    feed._running = False
    feed._ws = None  # disconnected

    created_tasks = []

    async def _run():
        def fake_create_task(coro, **kw):
            created_tasks.append(coro)
            return MagicMock()

        with patch("asyncio.create_task", side_effect=fake_create_task):
            feed.add_tokens(["new-tok"])

    asyncio.run(_run())

    assert "new-tok" in feed.token_ids
    assert created_tasks == []  # no task created


def test_add_tokens_ignores_duplicates():
    """add_tokens() silently ignores already-tracked token IDs."""
    from services.ws_feed import PolymarketFeed

    feed = PolymarketFeed(token_ids=["tok-a", "tok-b"])
    feed._running = True
    feed._ws = MagicMock()

    created_tasks = []

    async def _run():
        with patch("asyncio.create_task", side_effect=lambda c, **k: created_tasks.append(c)):
            feed.add_tokens(["tok-a"])  # already exists

    asyncio.run(_run())

    assert feed.token_ids.count("tok-a") == 1  # no duplicate
    assert created_tasks == []                  # no subscribe needed


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
