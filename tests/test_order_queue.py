"""
Tests for order creation paths: MARKET (snapshot fill) vs LIMIT (ME queue).

Verifies:
  - MARKET orders fill immediately from Redis orderbook snapshot
  - MARKET orders return avg_price, num_shares, me_order_status=None
  - MARKET orders with bracket return me_order_status="PREFILLED" and queue to ME
  - LIMIT orders queue to ME with avg_price=None, me_order_status="PENDING"
  - 503 if orderbook snapshot unavailable for MARKET
"""

import json
import time
from unittest.mock import patch, MagicMock

import pytest

from ws_feed_service.config import QUEUE_ORDERS_PREFIX, ORDERBOOK_KEY_PREFIX
from config.timing import TF_SECONDS


def _current_candle_open(tf="M5"):
    """Compute the candle_open for the current session."""
    period_s = TF_SECONDS[tf]
    now_ts = int(time.time())
    return now_ts - (now_ts % period_s)


def _session_queue_key(symbol="BTC", tf="M5"):
    """Compute the per-session queue key for current candle."""
    candle_open = _current_candle_open(tf)
    return f"{QUEUE_ORDERS_PREFIX}:{symbol}:{tf}:{candle_open}"


def _any_session_queue_len(redis_client, prefix="queue:orders:"):
    """Sum lengths of all per-session queues in fakeredis."""
    total = 0
    for key in redis_client.keys(f"{prefix}*"):
        key_str = key.decode() if isinstance(key, bytes) else key
        # Skip the deprecated queue:orders:new
        if key_str == "queue:orders:new":
            continue
        total += redis_client.llen(key)
    return total


def _pop_any_session_queue(redis_client, prefix="queue:orders:"):
    """Pop from any per-session queue."""
    for key in redis_client.keys(f"{prefix}*"):
        key_str = key.decode() if isinstance(key, bytes) else key
        if key_str == "queue:orders:new":
            continue
        raw = redis_client.rpop(key)
        if raw is not None:
            return raw
    return None


def _seed_orderbook(redis_client, symbol="BTC", tf="M5", direction="UP", candle_open=None):
    """Seed a fake orderbook snapshot in Redis using session-keyed key."""
    if candle_open is None:
        candle_open = _current_candle_open(tf)
    key = f"{ORDERBOOK_KEY_PREFIX}:{symbol}:{tf}:{direction}:{candle_open}"
    asks = [[0.52, 500.0], [0.53, 300.0], [0.54, 200.0]]
    redis_client.hset(key, mapping={
        "asks": json.dumps(asks),
        "bids": json.dumps([[0.51, 400.0], [0.50, 600.0]]),
        "updated_at": str(time.time()),
    })


# ── LIMIT order → queue ──────────────────────────────────────────────────────


def test_create_bo_limit_order_pushes_to_queue(client, test_bot, fake_sync_redis):
    """LIMIT order should LPUSH to per-session queue with me_order_status=PENDING."""
    bot_name, api_key = test_bot

    # Seed orderbook so the session availability check passes
    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["limit_price"] == 0.45
    assert data["avg_price"] is None
    assert data["num_shares"] is None
    assert data["me_order_status"] == "PENDING"

    # Check Redis queue (per-session key)
    queue_len = _any_session_queue_len(fake_sync_redis)
    assert queue_len == 1

    raw = _pop_any_session_queue(fake_sync_redis)
    order = json.loads(raw)
    assert order["bo_id"] == data["id"]
    assert order["side"] == "BUY"
    assert order["limit_price"] == 0.45
    assert order["timeframe"] == "M5"
    assert order["symbol"] == "BTC"
    assert order["forecast"] == "GREEN"
    assert order["direction"] == "UP"


# ── MARKET order → snapshot fill ─────────────────────────────────────────────


def test_create_bo_market_order_fills_from_snapshot(client, test_bot, fake_sync_redis):
    """MARKET order should fill immediately from orderbook snapshot."""
    bot_name, api_key = test_bot

    # Seed orderbook snapshot in Redis (session-keyed)
    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["limit_price"] is None
    # MARKET orders are filled immediately
    assert data["avg_price"] is not None
    assert data["avg_price"] > 0
    assert data["num_shares"] is not None
    assert data["num_shares"] > 0
    # No bracket → me_order_status is None (not queued to ME)
    assert data["me_order_status"] is None

    # No order should be in the ME queue
    queue_len = _any_session_queue_len(fake_sync_redis)
    assert queue_len == 0


def test_create_bo_market_with_bracket_queues_prefilled(client, test_bot, fake_sync_redis):
    """MARKET order with TP/SL should fill immediately and queue as PREFILLED."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.70,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["avg_price"] is not None
    assert data["num_shares"] is not None
    assert data["me_order_status"] == "PREFILLED"

    # Prefilled order should be queued to ME for bracket monitoring
    queue_len = _any_session_queue_len(fake_sync_redis)
    assert queue_len == 1

    raw = _pop_any_session_queue(fake_sync_redis)
    order = json.loads(raw)
    assert order["bo_id"] == data["id"]
    assert order["prefilled"] is True


def test_create_bo_market_applies_taker_fee(client, test_bot, fake_sync_redis, db):
    """MARKET order should deduct taker fee from balance at API level."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    from models import Bot
    bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
    initial_balance = bot.balance

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["entry_fee"] > 0

    db.refresh(bot)
    # Balance should be reduced by amount + taker fee
    assert bot.balance < initial_balance - 10.0


# ── Orderbook unavailable → 503 ──────────────────────────────────────────────


def test_create_bo_no_orderbook_returns_503(client, test_bot, fake_sync_redis):
    """When orderbook snapshot is unavailable, return 503."""
    bot_name, api_key = test_bot

    # Don't seed any orderbook data
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 503
    assert "unavailable" in resp.json()["detail"].lower()


# ── MARKET without orderbook snapshot → 503 ─────────────────────────────────


def test_create_bo_market_no_orderbook_returns_503(client, test_bot, fake_sync_redis):
    """MARKET order with no orderbook snapshot should return 503."""
    bot_name, api_key = test_bot

    # Don't seed orderbook — it should fail
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 503
    assert "orderbook" in resp.json()["detail"].lower() or "unavailable" in resp.json()["detail"].lower()


# ── Session queue routing ────────────────────────────────────────────────────


def test_limit_order_queue_key_matches_session(client, test_bot, fake_sync_redis):
    """LIMIT order queue key must contain the correct candle_open for current session."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201

    # Verify queue key is session-specific
    expected_candle_open = _current_candle_open("M5")
    expected_key = f"queue:orders:BTC:M5:{expected_candle_open}"

    # The order should be in exactly this key
    queue_len = fake_sync_redis.llen(expected_key)
    assert queue_len == 1, f"Expected 1 order in {expected_key}, got {queue_len}"

    raw = fake_sync_redis.rpop(expected_key)
    order = json.loads(raw)
    assert order["session_id"] == f"BTC:M5:{expected_candle_open}"
    assert order["bo_id"] == resp.json()["id"]


def test_limit_different_timeframes_route_to_different_queues(client, test_bot, fake_sync_redis):
    """LIMIT orders for M5 and M15 should go to different session queues."""
    bot_name, api_key = test_bot

    # Seed orderbooks for both timeframes
    _seed_orderbook(fake_sync_redis, tf="M5", direction="UP")
    _seed_orderbook(fake_sync_redis, tf="M15", direction="UP")

    # M5 order
    resp_m5 = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 5.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )
    assert resp_m5.status_code == 201

    # M15 order
    resp_m15 = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M15",
            "forecast": "GREEN",
            "amount": 5.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )
    assert resp_m15.status_code == 201

    # Verify they went to different queues
    m5_candle = _current_candle_open("M5")
    m15_candle = _current_candle_open("M15")

    m5_key = f"queue:orders:BTC:M5:{m5_candle}"
    m15_key = f"queue:orders:BTC:M15:{m15_candle}"

    assert fake_sync_redis.llen(m5_key) == 1, f"M5 queue should have 1 order"
    assert fake_sync_redis.llen(m15_key) == 1, f"M15 queue should have 1 order"

    # Verify session_id in payload matches queue key
    m5_order = json.loads(fake_sync_redis.rpop(m5_key))
    m15_order = json.loads(fake_sync_redis.rpop(m15_key))
    assert m5_order["session_id"] == f"BTC:M5:{m5_candle}"
    assert m15_order["session_id"] == f"BTC:M15:{m15_candle}"


def test_prefilled_bracket_uses_same_session_queue_as_limit(client, test_bot, fake_sync_redis):
    """MARKET+bracket prefilled order should route to the same session queue format as LIMIT."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "tp_price": 0.70,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["me_order_status"] == "PREFILLED"

    # Verify the prefilled order is in the correct session queue
    expected_candle_open = _current_candle_open("M5")
    expected_key = f"queue:orders:BTC:M5:{expected_candle_open}"

    queue_len = fake_sync_redis.llen(expected_key)
    assert queue_len == 1, f"Expected 1 prefilled order in {expected_key}, got {queue_len}"

    raw = fake_sync_redis.rpop(expected_key)
    order = json.loads(raw)
    assert order["prefilled"] is True
    assert order["session_id"] == f"BTC:M5:{expected_candle_open}"
    assert order["bo_id"] == data["id"]


def test_multiple_limit_orders_same_session_share_queue(client, test_bot, fake_sync_redis):
    """Multiple LIMIT orders for the same session should all be in the same queue."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    for i in range(3):
        resp = client.post(
            "/poly-arena/binary-options/",
            json={
                "symbol": "BTC",
                "timeframe": "M5",
                "forecast": "GREEN",
                "amount": 3.0,
                "limit_price": 0.45,
            },
            headers={"x-api-key": api_key},
        )
        assert resp.status_code == 201

    # All 3 should be in the same session queue
    candle_open = _current_candle_open("M5")
    key = f"queue:orders:BTC:M5:{candle_open}"

    assert fake_sync_redis.llen(key) == 3, "All 3 orders should be in the same session queue"


# ── LIMIT fill conditions per forecast (GREEN/RED → UP/DOWN book) ────────


def test_limit_buy_green_uses_up_token_and_asks():
    """
    GREEN forecast → UP token → LIMIT BUY matches against UP book's asks.
    Fill condition: ask_price <= limit_price.
    """
    from decimal import Decimal
    from services.matching_engine import ShadowOrderbook, OrderSide, OrderStatus

    book = ShadowOrderbook("token-UP-btc-m5")
    # UP book: asks at 0.52 and 0.55
    book.asks[Decimal("0.52")] = Decimal("100")
    book.asks[Decimal("0.55")] = Decimal("100")
    book.bids[Decimal("0.50")] = Decimal("100")

    # LIMIT BUY at 0.53 → should fill 100 shares at 0.52 (ask <= limit)
    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.53"),
        quantity=Decimal("100"),
        order_type="LIMIT",
    )

    assert order.status == OrderStatus.FILLED
    assert order.filled == Decimal("100")
    assert order.avg_entry_price == Decimal("0.52")  # filled at best ask


def test_limit_buy_green_no_fill_when_ask_above_limit():
    """
    GREEN forecast → UP token → LIMIT BUY stays PENDING when all asks > limit_price.
    """
    from decimal import Decimal
    from services.matching_engine import ShadowOrderbook, OrderSide, OrderStatus

    book = ShadowOrderbook("token-UP-btc-m5")
    # UP book: best ask at 0.55, above limit 0.50
    book.asks[Decimal("0.55")] = Decimal("200")
    book.bids[Decimal("0.48")] = Decimal("100")

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("100"),
        order_type="LIMIT",
    )

    assert order.status == OrderStatus.PENDING
    assert order.filled == Decimal("0")


def test_limit_buy_green_fills_when_ask_drops():
    """
    GREEN forecast → LIMIT BUY fills after book update brings ask below limit.
    Simulates: place order → book updates → run_matching triggers fill.
    """
    from decimal import Decimal
    from services.matching_engine import ShadowOrderbook, OrderSide, OrderStatus

    book = ShadowOrderbook("token-UP-btc-m5")
    # Initial: ask too high
    book.asks[Decimal("0.60")] = Decimal("200")
    book.bids[Decimal("0.45")] = Decimal("100")

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.52"),
        quantity=Decimal("50"),
        order_type="LIMIT",
    )
    assert order.status == OrderStatus.PENDING

    # Book update: new ask at 0.51 (below limit 0.52)
    book.apply_changes([{"side": "ask", "price": "0.51", "size": "100"}])
    book.run_matching()

    assert order.status == OrderStatus.FILLED
    assert order.filled == Decimal("50")
    assert order.avg_entry_price == Decimal("0.51")


def test_limit_buy_red_uses_down_token_and_asks():
    """
    RED forecast → DOWN token → LIMIT BUY matches against DOWN book's asks.
    Same fill condition as GREEN but on a different book (different token_id).
    """
    from decimal import Decimal
    from services.matching_engine import ShadowOrderbook, OrderSide, OrderStatus

    book = ShadowOrderbook("token-DOWN-btc-m5")
    # DOWN book: asks at 0.48 and 0.50
    book.asks[Decimal("0.48")] = Decimal("150")
    book.asks[Decimal("0.50")] = Decimal("100")
    book.bids[Decimal("0.46")] = Decimal("100")

    # LIMIT BUY at 0.49 → should fill 150 at 0.48
    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.49"),
        quantity=Decimal("150"),
        order_type="LIMIT",
    )

    assert order.status == OrderStatus.FILLED
    assert order.filled == Decimal("150")
    assert order.avg_entry_price == Decimal("0.48")


def test_limit_buy_partial_fill_across_levels():
    """
    LIMIT BUY at 0.54 with asks at 0.52 (50 qty) and 0.54 (100 qty).
    Should fill 50 at 0.52 + 100 at 0.54 = 150 total.
    """
    from decimal import Decimal
    from services.matching_engine import ShadowOrderbook, OrderSide, OrderStatus

    book = ShadowOrderbook("token-UP-btc-m5")
    book.asks[Decimal("0.52")] = Decimal("50")
    book.asks[Decimal("0.54")] = Decimal("100")
    book.asks[Decimal("0.56")] = Decimal("200")  # above limit, should NOT fill
    book.bids[Decimal("0.50")] = Decimal("100")

    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.54"),
        quantity=Decimal("200"),
        order_type="LIMIT",
    )

    # Only 50 + 100 = 150 filled (0.56 > 0.54 limit)
    assert order.filled == Decimal("150")
    assert order.status == OrderStatus.PARTIAL
    # avg_entry = (50*0.52 + 100*0.54) / 150 = (26 + 54) / 150 = 0.5333...
    expected_avg = (Decimal("50") * Decimal("0.52") + Decimal("100") * Decimal("0.54")) / Decimal("150")
    assert abs(order.avg_entry_price - expected_avg) < Decimal("0.0001")


def test_limit_green_queue_payload_has_up_direction(client, test_bot, fake_sync_redis):
    """GREEN LIMIT → queue payload should contain direction=UP."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,
        },
        headers={"x-api-key": api_key},
    )
    assert resp.status_code == 201

    raw = _pop_any_session_queue(fake_sync_redis)
    order = json.loads(raw)
    assert order["direction"] == "UP"
    assert order["forecast"] == "GREEN"
    assert order["side"] == "BUY"
    assert order["limit_price"] == 0.45


def test_limit_red_queue_payload_has_down_direction(client, test_bot, fake_sync_redis):
    """RED LIMIT → queue payload should contain direction=DOWN."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="DOWN")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "RED",
            "amount": 10.0,
            "limit_price": 0.40,
        },
        headers={"x-api-key": api_key},
    )
    assert resp.status_code == 201

    raw = _pop_any_session_queue(fake_sync_redis)
    order = json.loads(raw)
    assert order["direction"] == "DOWN"
    assert order["forecast"] == "RED"
    assert order["side"] == "BUY"
    assert order["limit_price"] == 0.40


# ── Aggressive LIMIT (limit_price >= best_ask) → taker fee ───────────────────


def test_aggressive_limit_fills_immediately_as_taker(client, test_bot, fake_sync_redis):
    """LIMIT with limit_price >= best_ask should fill immediately like MARKET (taker fee)."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")  # best_ask = 0.52

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.55,  # >= best_ask 0.52 → aggressive
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    # Should be filled immediately (not queued)
    assert data["avg_price"] is not None
    assert data["avg_price"] > 0
    assert data["num_shares"] is not None
    assert data["num_shares"] > 0
    # Taker fee should be charged
    assert data["entry_fee"] > 0
    # No bracket → me_order_status is None
    assert data["me_order_status"] is None
    # No order in ME queue
    queue_len = _any_session_queue_len(fake_sync_redis)
    assert queue_len == 0


def test_aggressive_limit_deducts_taker_fee_from_balance(client, test_bot, fake_sync_redis, db):
    """Aggressive LIMIT should deduct both amount and taker fee from balance."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    from models import Bot
    bot = db.query(Bot).filter(Bot.bot_name == bot_name).first()
    initial_balance = bot.balance

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.52,  # == best_ask → aggressive
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["entry_fee"] > 0

    db.refresh(bot)
    # Balance = initial - amount - entry_fee
    expected_balance = round(initial_balance - 10.0 - data["entry_fee"], 8)
    assert abs(bot.balance - expected_balance) < 0.01


def test_aggressive_limit_with_bracket_queues_prefilled(client, test_bot, fake_sync_redis):
    """Aggressive LIMIT + bracket → fill as taker, queue PREFILLED for monitoring."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.55,
            "tp_price": 0.70,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["avg_price"] is not None
    assert data["entry_fee"] > 0
    assert data["me_order_status"] == "PREFILLED"

    # Prefilled order queued for bracket monitoring
    queue_len = _any_session_queue_len(fake_sync_redis)
    assert queue_len == 1


def test_passive_limit_no_fee_queued_to_me(client, test_bot, fake_sync_redis):
    """Passive LIMIT (limit < best_ask) should queue to ME with entry_fee=0."""
    bot_name, api_key = test_bot

    _seed_orderbook(fake_sync_redis, direction="UP")  # best_ask = 0.52

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.45,  # < best_ask 0.52 → passive
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["avg_price"] is None
    assert data["entry_fee"] == 0
    assert data["me_order_status"] == "PENDING"

    # Should be queued to ME
    queue_len = _any_session_queue_len(fake_sync_redis)
    assert queue_len == 1


def _seed_wide_spread_orderbook(redis_client, symbol="BTC", tf="M5", direction="UP", candle_open=None):
    """Seed orderbook with wide price spread: asks from 0.10 to 0.60."""
    if candle_open is None:
        candle_open = _current_candle_open(tf)
    key = f"{ORDERBOOK_KEY_PREFIX}:{symbol}:{tf}:{direction}:{candle_open}"
    asks = [[0.10, 50.0], [0.20, 50.0], [0.30, 50.0], [0.40, 50.0], [0.50, 50.0], [0.60, 50.0]]
    redis_client.hset(key, mapping={
        "asks": json.dumps(asks),
        "bids": json.dumps([[0.09, 100.0]]),
        "updated_at": str(time.time()),
    })


def test_aggressive_limit_walks_up_to_limit_price(client, test_bot, fake_sync_redis):
    """Aggressive LIMIT should fill all asks from min up to limit_price, not just slippage band."""
    bot_name, api_key = test_bot

    # Orderbook: asks at [0.10, 0.20, 0.30, 0.40, 0.50, 0.60]
    _seed_wide_spread_orderbook(fake_sync_redis)

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
            "limit_price": 0.50,  # >= best_ask 0.10, should fill asks 0.10-0.50
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["avg_price"] is not None
    # avg_price should be between 0.10 and 0.50 (weighted avg of multiple levels)
    # NOT just 0.10 (which would happen if slippage capped at best_ask * 1.1 = 0.11)
    assert data["avg_price"] > 0.10, (
        f"avg_price={data['avg_price']} — should walk asks above 0.10 up to limit 0.50"
    )
    assert data["avg_price"] <= 0.50
    # walk_prices should include multiple levels
    assert data["walk_prices"] is not None
    walk_entry = data["walk_prices"].get("entry", [])
    assert len(walk_entry) > 1, (
        f"Expected multiple fill levels, got {len(walk_entry)}: {walk_entry}"
    )


def test_session_keyed_orderbook_used(client, test_bot, fake_sync_redis):
    """Session-keyed orderbook should be used for order fills."""
    bot_name, api_key = test_bot

    # Session-keyed key has correct data (0.52)
    candle_open = _current_candle_open("M5")
    _seed_orderbook(fake_sync_redis, direction="UP", candle_open=candle_open)

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["avg_price"] is not None
    assert data["avg_price"] >= 0.52


# ── Exact fee value verification ─────────────────────────────────────────────


def test_market_order_fee_matches_formula_exactly(client, test_bot, fake_sync_redis, db):
    """Verify entry_fee matches qty × price × 0.25 × (p×(1-p))².

    Orderbook: asks at [0.52, 500]. For a $10 order:
      shares = 10 / 0.52 ≈ 19.2308
      fee = 19.2308 × 0.52 × 0.25 × (0.52 × 0.48)²
          = 10.0 × 0.25 × (0.52 × 0.48)²
          ≈ 0.15575
    """
    from config.fees import nominal_fee_per_level

    bot_name, api_key = test_bot
    _seed_orderbook(fake_sync_redis, direction="UP")  # asks: [[0.52, 500], ...]

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )

    assert resp.status_code == 201
    data = resp.json()

    # All $10 fills at price 0.52 (first ask level has 500 qty, more than enough)
    assert data["avg_price"] == 0.52
    shares = data["num_shares"]
    assert abs(shares - 10.0 / 0.52) < 0.01  # ~19.23 shares

    # Verify fee matches formula: qty × price × 0.25 × (p×(1-p))²
    expected_fee = nominal_fee_per_level(shares, 0.52)
    assert abs(data["entry_fee"] - expected_fee) < 1e-6, (
        f"entry_fee={data['entry_fee']} != expected={expected_fee} "
        f"(qty={shares} shares, price=0.52)"
    )

    # Sanity: fee ≈ cost × FEE_RATE × (p(1-p))² = $10 × 0.25 × 0.0623 ≈ 0.156
    assert 0.10 < data["entry_fee"] < 0.25, (
        f"Fee {data['entry_fee']} outside expected range for $10 order at 0.52"
    )
