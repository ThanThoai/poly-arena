"""
Tests for bracket exit Redis Stream.

Verifies:
  - RedisWriter.publish_bracket_exit writes to stream:bracket:exits
  - Consumer logic reads from stream and updates DB correctly
  - Idempotent: existing exit_trigger is not overwritten
"""

import pytest

from models import BinaryOption, Bot, BOResult, BOSymbol, BOTimeframe, BOForecast
from ws_feed_service.config import STREAM_BRACKET_EXITS
from ws_feed_service.redis_writer import RedisWriter
from database import SessionLocal


@pytest.mark.asyncio
async def test_publish_bracket_exit_writes_to_stream(fake_async_redis):
    """publish_bracket_exit should XADD to stream:bracket:exits."""
    writer = RedisWriter(fake_async_redis)

    await writer.publish_bracket_exit(
        bo_id=42,
        trigger="TP",
        exit_price=0.72,
        exit_filled=200.0,
        order_id="order-abc-123",
    )

    entries = await fake_async_redis.xrange(STREAM_BRACKET_EXITS)
    assert len(entries) == 1
    _msg_id, data = entries[0]
    assert data["bo_id"] == "42"
    assert data["trigger"] == "TP"
    assert data["exit_price"] == "0.72"
    assert data["exit_filled"] == "200.0"
    assert data["order_id"] == "order-abc-123"


@pytest.mark.asyncio
async def test_consume_bracket_exit_updates_db(db, fake_async_redis):
    """
    Simulate what _consume_bracket_exits does: read a stream message
    and update the BO in the DB.
    """
    # Create a pending BO
    bot = Bot(bot_name="test-bot-stream", api_key="key-stream-test", is_active=True)
    db.add(bot)
    db.commit()

    bo = BinaryOption(
        bot_name="test-bot-stream",
        symbol=BOSymbol.BTC,
        timeframe=BOTimeframe.M5,
        forecast=BOForecast.GREEN,
        amount=10.0,
        result=BOResult.PENDING,
        avg_price=0.50,
        num_shares=20.0,
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)
    bo_id = bo.id

    # Publish a bracket exit to the stream
    await fake_async_redis.xadd(STREAM_BRACKET_EXITS, {
        "bo_id": str(bo_id),
        "trigger": "SL",
        "exit_price": "0.35",
        "exit_filled": "20.0",
        "order_id": "order-xyz",
    })

    # Simulate consumer logic: create group, read, process, ack
    group = "test-workers"
    consumer = "test-consumer"
    try:
        await fake_async_redis.xgroup_create(STREAM_BRACKET_EXITS, group, id="0", mkstream=True)
    except Exception:
        pass

    results = await fake_async_redis.xreadgroup(
        group, consumer,
        {STREAM_BRACKET_EXITS: ">"},
        count=10,
    )

    assert len(results) > 0
    for _stream, messages in results:
        for msg_id, data in messages:
            inner_bo_id = int(data["bo_id"])
            trigger = data["trigger"]
            exit_price = float(data["exit_price"])
            exit_filled = float(data["exit_filled"])

            session = SessionLocal()
            try:
                inner_bo = session.get(BinaryOption, inner_bo_id)
                assert inner_bo is not None
                if inner_bo.exit_trigger is None:
                    inner_bo.exit_trigger = trigger
                    inner_bo.exit_price = exit_price
                    inner_bo.exit_filled = exit_filled
                    session.commit()
            finally:
                session.close()

            await fake_async_redis.xack(STREAM_BRACKET_EXITS, group, msg_id)

    # Verify DB was updated
    db.expire_all()
    bo = db.get(BinaryOption, bo_id)
    assert bo.exit_trigger == "SL"
    assert bo.exit_price == 0.35
    assert bo.exit_filled == 20.0


@pytest.mark.asyncio
async def test_consume_bracket_exit_idempotent(db, fake_async_redis):
    """If exit_trigger is already set, consumer should not overwrite it."""
    bot = Bot(bot_name="test-bot-idem", api_key="key-idem-test", is_active=True)
    db.add(bot)
    db.commit()

    bo = BinaryOption(
        bot_name="test-bot-idem",
        symbol=BOSymbol.ETH,
        timeframe=BOTimeframe.M15,
        forecast=BOForecast.RED,
        amount=5.0,
        result=BOResult.PENDING,
        avg_price=0.60,
        num_shares=8.33,
        exit_trigger="TP",
        exit_price=0.80,
        exit_filled=8.33,
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)
    bo_id = bo.id

    # Publish a conflicting SL exit
    await fake_async_redis.xadd(STREAM_BRACKET_EXITS, {
        "bo_id": str(bo_id),
        "trigger": "SL",
        "exit_price": "0.30",
        "exit_filled": "8.33",
        "order_id": "order-conflict",
    })

    # Simulate consumer
    group = "test-workers-idem"
    consumer = "test-consumer-idem"
    try:
        await fake_async_redis.xgroup_create(STREAM_BRACKET_EXITS, group, id="0", mkstream=True)
    except Exception:
        pass

    results = await fake_async_redis.xreadgroup(
        group, consumer,
        {STREAM_BRACKET_EXITS: ">"},
        count=10,
    )

    for _stream, messages in results:
        for msg_id, data in messages:
            inner_bo_id = int(data["bo_id"])
            session = SessionLocal()
            try:
                inner_bo = session.get(BinaryOption, inner_bo_id)
                if inner_bo is not None and inner_bo.exit_trigger is None:
                    inner_bo.exit_trigger = data["trigger"]
                    inner_bo.exit_price = float(data["exit_price"])
                    inner_bo.exit_filled = float(data["exit_filled"])
                    session.commit()
            finally:
                session.close()

    # Should still be TP (not overwritten)
    db.expire_all()
    bo = db.get(BinaryOption, bo_id)
    assert bo.exit_trigger == "TP"
    assert bo.exit_price == 0.80
