"""
Tests for Market Resolution (v2 spec Section 5).

Verifies:
  - publish_market_resolved writes to stream:market:resolved
  - Matching engine _handle_market_resolved clears TP/SL and sets position_closed
  - Settlement skips position_closed orders
"""

import pytest
from decimal import Decimal

from models import BinaryOption, Bot, BOResult, BOSymbol, BOTimeframe, BOForecast
from ws_feed_service.config import STREAM_MARKET_RESOLVED
from ws_feed_service.redis_writer import RedisWriter
from services.matching_engine import (
    MatchingEngine, ShadowOrderbook, OrderSide, OrderStatus,
)


@pytest.mark.asyncio
async def test_publish_market_resolved_writes_to_stream(fake_async_redis):
    """publish_market_resolved should XADD to stream:market:resolved."""
    writer = RedisWriter(fake_async_redis)

    await writer.publish_market_resolved(
        asset_id="test-asset-123",
        winning_outcome="YES",
        timestamp="2026-02-28T12:00:00Z",
    )

    entries = await fake_async_redis.xrange(STREAM_MARKET_RESOLVED)
    assert len(entries) == 1
    _msg_id, data = entries[0]
    assert data["asset_id"] == "test-asset-123"
    assert data["winning_outcome"] == "YES"


def test_matching_engine_market_resolved_clears_conditions():
    """_handle_market_resolved should set position_closed=True and clear TP/SL."""
    engine = MatchingEngine()
    token_id = "resolved-token-abc"
    book = engine.get_or_create_book(token_id)

    # Add some asks/bids for the book
    book.apply_snapshot(
        bids=[{"price": "0.48", "size": "100"}],
        asks=[{"price": "0.52", "size": "100"}],
    )

    # Place an order with TP
    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.52"),
        quantity=Decimal("10"),
        tp_price=Decimal("0.80"),
        order_type="MARKET",
    )

    assert order.tp_price == Decimal("0.80")
    assert order.position_closed is False

    # Resolve market
    engine._handle_market_resolved(token_id)

    # Verify order state
    assert order.tp_price is None
    assert order.sl_price is None
    assert order.position_closed is True


def test_matching_engine_market_resolved_cancels_unfilled():
    """Unfilled orders should be CANCELED on market resolution."""
    engine = MatchingEngine()
    token_id = "resolved-token-def"
    book = engine.get_or_create_book(token_id)

    # Place an order with no book data (stays PENDING)
    order, _ = book.place_virtual_order(
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        quantity=Decimal("10"),
        sl_price=Decimal("0.30"),
        order_type="LIMIT",
    )

    assert order.status == OrderStatus.PENDING

    engine._handle_market_resolved(token_id)

    assert order.status == OrderStatus.CANCELED
    assert order.position_closed is True
    assert order.sl_price is None


def test_settlement_skips_position_closed(db):
    """settle_pending_trades should skip orders with position_closed=True."""
    from services.settlement import settle_pending_trades
    from datetime import datetime, timezone, timedelta

    bot = Bot(
        bot_name="resolved-bot",
        api_key="key-resolved-test",
        is_active=True,
        balance=100.0,
    )
    db.add(bot)
    db.commit()

    bo = BinaryOption(
        bot_name="resolved-bot",
        symbol=BOSymbol.BTC,
        timeframe=BOTimeframe.M5,
        forecast=BOForecast.GREEN,
        amount=10.0,
        result=BOResult.PENDING,
        avg_price=0.50,
        num_shares=20.0,
        settlement_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        position_closed=True,  # Already resolved
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)
    bo_id = bo.id

    # Run settlement — should skip position_closed orders
    settle_pending_trades(db)

    db.expire_all()
    bo = db.get(BinaryOption, bo_id)
    # Should still be PENDING (not settled by scheduler)
    assert bo.result == BOResult.PENDING
