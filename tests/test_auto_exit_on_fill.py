"""
Tests for _handle_order_fill behavior.

Since MARKET orders are now filled at API level (snapshot fill), only LIMIT
fills arrive through _handle_order_fill. This file tests:
  - Fill updates avg_price/num_shares/me_order_status
  - No auto-exit on TP-violated fill (ME handles it)
  - Maker rebate applied for LIMIT fills
"""

import pytest
import asyncio
from unittest.mock import patch, MagicMock
from decimal import Decimal

from models import BinaryOption, Bot, BalanceHistory, BOResult, BOSymbol, BOTimeframe, BOForecast
from main import _handle_order_fill


@pytest.fixture
def bot_and_bo(db):
    """Create a bot for testing."""
    bot = Bot(
        bot_name="auto-exit-bot",
        api_key="key-auto-exit-test",
        is_active=True,
        balance=10000.0,
        initial_balance=10000.0,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


def _make_bo(db, bot, tp_price=None, sl_price=None, amount=100.0, limit_price=0.50):
    """Create a pending BO."""
    bo = BinaryOption(
        bot_name=bot.bot_name,
        symbol=BOSymbol.BTC,
        timeframe=BOTimeframe.M5,
        forecast=BOForecast.GREEN,
        amount=amount,
        result=BOResult.PENDING,
        avg_price=None,
        num_shares=None,
        limit_price=limit_price,
        tp_price=tp_price,
        sl_price=sl_price,
        me_order_status="PENDING",
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)
    return bo


@pytest.mark.asyncio
async def test_fill_updates_price_and_status(db, bot_and_bo, fake_async_redis):
    """Fill should update avg_price, num_shares, me_order_status."""
    bot = bot_and_bo
    bo = _make_bo(db, bot, limit_price=0.50, amount=100.0)

    bot.balance = round(bot.balance - bo.amount, 8)
    db.commit()

    mock_r = fake_async_redis

    walk_prices = '[{"price": 0.50, "qty": 200.0, "cost": 100.0}]'
    await _handle_order_fill(
        mock_r, "stream:order:fills", "test-group", "msg-1",
        {
            "bo_id": str(bo.id),
            "filled": "200.0",
            "avg_entry_price": "0.50",
            "status": "FILLED",
            "order_id": "ord-123",
            "walk_prices": walk_prices,
        },
    )

    db.refresh(bo)

    assert bo.avg_price == pytest.approx(0.50)
    assert bo.num_shares == pytest.approx(200.0)
    assert bo.me_order_status == "FILLED"


@pytest.mark.asyncio
async def test_no_auto_exit_on_tp_violated_fill(db, bot_and_bo, fake_async_redis):
    """When fill avg_entry >= tp_price, should NOT auto-exit (ME handles it)."""
    bot = bot_and_bo
    # TP = 0.55, but order fills at 0.60 (above TP)
    bo = _make_bo(db, bot, tp_price=0.55, limit_price=0.50, amount=100.0)

    bot.balance = round(bot.balance - bo.amount, 8)
    db.commit()

    mock_r = fake_async_redis

    walk_prices = '[{"price": 0.60, "qty": 166.67, "cost": 100.0}]'
    await _handle_order_fill(
        mock_r, "stream:order:fills", "test-group", "msg-2",
        {
            "bo_id": str(bo.id),
            "filled": "166.67",
            "avg_entry_price": "0.60",
            "status": "FILLED",
            "order_id": "ord-456",
            "walk_prices": walk_prices,
        },
    )

    db.refresh(bo)

    # Fill data updated
    assert bo.avg_price == pytest.approx(0.60)
    assert bo.num_shares == pytest.approx(166.67)
    assert bo.me_order_status == "FILLED"
    # No auto-exit — ME bracket monitoring will handle this
    assert bo.exit_trigger is None
    assert bo.result == BOResult.PENDING


@pytest.mark.asyncio
async def test_limit_fill_applies_maker_rebate(db, bot_and_bo, fake_async_redis):
    """LIMIT order fill (limit_price set) should add maker rebate to balance."""
    bot = bot_and_bo
    bo = _make_bo(db, bot, limit_price=0.50, amount=100.0)

    bot.balance = round(bot.balance - bo.amount, 8)
    initial_balance = bot.balance
    db.commit()

    mock_r = fake_async_redis

    walk_prices = '[{"price": 0.50, "qty": 200.0, "cost": 100.0}]'
    await _handle_order_fill(
        mock_r, "stream:order:fills", "test-group", "msg-4",
        {
            "bo_id": str(bo.id),
            "filled": "200.0",
            "avg_entry_price": "0.50",
            "status": "FILLED",
            "order_id": "ord-101",
            "walk_prices": walk_prices,
        },
    )

    db.refresh(bo)
    db.refresh(bot)

    # Maker rebate should have been added (entry_fee goes negative)
    assert bo.entry_fee < 0
    assert bot.balance > initial_balance


@pytest.mark.asyncio
async def test_aggressive_limit_remainder_receives_maker_rebate(db, bot_and_bo, fake_async_redis):
    """Aggressive LIMIT remainder (entry_fee > 0 from REST taker fill) should
    still receive maker rebate when ME fills the remainder portion."""
    bot = bot_and_bo
    bo = _make_bo(db, bot, limit_price=0.55, amount=100.0)

    # Simulate aggressive LIMIT: REST already filled part as taker
    # entry_fee > 0 from REST taker fill portion
    bo.entry_fee = 0.25  # taker fee from REST fill
    bo.avg_price = 0.50
    bo.num_shares = 120.0
    bo.me_order_status = "PARTIAL"
    bot.balance = round(bot.balance - bo.amount - bo.entry_fee, 8)
    initial_balance = bot.balance
    db.commit()

    mock_r = fake_async_redis

    # ME fills the remainder portion (maker fill)
    walk_prices = '[{"price": 0.53, "qty": 75.47, "cost": 40.0}]'
    await _handle_order_fill(
        mock_r, "stream:order:fills", "test-group", "msg-5",
        {
            "bo_id": str(bo.id),
            "filled": "195.47",
            "avg_entry_price": "0.5115",
            "status": "FILLED",
            "order_id": "ord-201",
            "walk_prices": walk_prices,
        },
    )

    db.refresh(bo)
    db.refresh(bot)

    # Maker rebate applied on ME fill portion: entry_fee reduced from 0.25
    from config.fees import maker_rebate_from_levels
    expected_rebate = maker_rebate_from_levels([{"price": 0.53, "qty": 75.47}])
    assert expected_rebate > 0
    assert bo.entry_fee == pytest.approx(0.25 - expected_rebate, abs=1e-6)
    assert bot.balance == pytest.approx(initial_balance + expected_rebate, abs=1e-6)
