"""
Tests for Pre-Validation Against Best Ask (v2 spec Section 2).

Verifies:
  - TP > best_ask → accepted
  - TP <= best_ask → rejected 400
  - SL < best_ask → accepted
  - SL >= best_ask → rejected 400
"""

import json
import time
import pytest

pytestmark = pytest.mark.skip(reason="TP/SL feature temporarily disabled")

from decimal import Decimal

from models import Bot, BOResult
from ws_feed_service.config import ORDERBOOK_KEY_PREFIX
from config.timing import TF_SECONDS


@pytest.fixture
def bot_with_key(db):
    """Create a bot with sufficient balance."""
    import secrets
    bot = Bot(
        bot_name="pre-val-bot",
        api_key=secrets.token_urlsafe(32),
        is_active=True,
        balance=10000.0,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


def _seed_orderbook(redis_client, best_ask=0.50, symbol="BTC", tf="M5", direction="UP"):
    """Seed a session-keyed orderbook snapshot with a configurable best_ask."""
    period_s = TF_SECONDS[tf]
    now_ts = int(time.time())
    candle_open = now_ts - (now_ts % period_s)
    key = f"{ORDERBOOK_KEY_PREFIX}:{symbol}:{tf}:{direction}:{candle_open}"
    asks = [[best_ask, 500.0], [best_ask + 0.01, 300.0]]
    redis_client.hset(key, mapping={
        "asks": json.dumps(asks),
        "bids": json.dumps([[best_ask - 0.01, 400.0]]),
        "updated_at": str(time.time()),
    })


def test_tp_above_best_ask_accepted(client, bot_with_key, fake_sync_redis):
    """TP price > best_ask should pass pre-validation."""
    _seed_orderbook(fake_sync_redis, best_ask=0.50)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.70,  # > 0.50 best_ask
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 201, resp.json()
    data = resp.json()
    assert data["tp_price"] == 0.70


def test_tp_at_best_ask_rejected(client, bot_with_key, fake_sync_redis):
    """TP price <= best_ask should be rejected."""
    _seed_orderbook(fake_sync_redis, best_ask=0.50)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.50,  # == 0.50 best_ask → rejected
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 400
    assert "TP price" in resp.json()["detail"]
    assert "must be higher" in resp.json()["detail"]


def test_tp_below_best_ask_rejected(client, bot_with_key, fake_sync_redis):
    """TP price < best_ask should be rejected."""
    _seed_orderbook(fake_sync_redis, best_ask=0.50)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.40,  # < 0.50 best_ask → rejected
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 400


def test_sl_below_best_ask_accepted(client, bot_with_key, fake_sync_redis):
    """SL price < best_ask should pass pre-validation."""
    _seed_orderbook(fake_sync_redis, best_ask=0.50)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "sl_price": 0.30,  # < 0.50 best_ask
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 201, resp.json()
    data = resp.json()
    assert data["sl_price"] == 0.30


def test_sl_at_best_ask_rejected(client, bot_with_key, fake_sync_redis):
    """SL price >= best_ask should be rejected."""
    _seed_orderbook(fake_sync_redis, best_ask=0.50)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "sl_price": 0.50,  # == 0.50 best_ask → rejected
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 400
    assert "SL price" in resp.json()["detail"]
    assert "must be lower" in resp.json()["detail"]


def test_sl_above_best_ask_rejected(client, bot_with_key, fake_sync_redis):
    """SL price > best_ask should be rejected."""
    _seed_orderbook(fake_sync_redis, best_ask=0.60)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "sl_price": 0.70,  # > 0.60 best_ask → rejected
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 400


def test_no_condition_skips_prevalidation(client, bot_with_key, fake_sync_redis):
    """Order without TP or SL should skip pre-validation entirely."""
    _seed_orderbook(fake_sync_redis, best_ask=0.50)
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": bot_with_key.api_key},
    )
    assert resp.status_code == 201
