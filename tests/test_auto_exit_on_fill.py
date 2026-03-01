"""
Tests for Auto-Exit on fill when TP < entry or SL > entry.

Verifies:
  - LIMIT order FILLED with avg_entry >= tp_price → auto-exit TP
  - LIMIT order FILLED with avg_entry <= sl_price → auto-exit SL
  - LIMIT order FILLED with normal TP/SL → no auto-exit
"""

import pytest
import asyncio
from unittest.mock import patch, MagicMock
from decimal import Decimal

from models import BinaryOption, Bot, BalanceHistory, BOResult, BOSymbol, BOTimeframe, BOForecast
from main import _handle_order_fill


@pytest.fixture
def bot_and_bo(db):
    """Create a bot + pending LIMIT BO with TP for testing."""
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


def _make_bo(db, bot, tp_price=None, sl_price=None, amount=100.0):
    """Create a pending LIMIT BO."""
    bo = BinaryOption(
        bot_name=bot.bot_name,
        symbol=BOSymbol.BTC,
        timeframe=BOTimeframe.M5,
        forecast=BOForecast.GREEN,
        amount=amount,
        result=BOResult.PENDING,
        avg_price=None,
        num_shares=None,
        limit_price=0.50,
        tp_price=tp_price,
        sl_price=sl_price,
        me_order_status="PENDING",
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)
    return bo


@pytest.mark.asyncio
async def test_auto_exit_tp_violated_at_fill(db, bot_and_bo, fake_async_redis):
    """When LIMIT order fills at avg_entry >= tp_price, should auto-exit."""
    bot = bot_and_bo
    # TP = 0.55, but order fills at 0.60 (above TP) → should auto-exit
    bo = _make_bo(db, bot, tp_price=0.55, amount=100.0)
    initial_balance = bot.balance

    # Deduct balance like the router would
    bot.balance = round(bot.balance - bo.amount, 8)
    db.commit()

    mock_r = fake_async_redis

    # Mock REST bid fetch: bids at 0.58
    with patch("main.fetch_best_bid_from_rest") as mock_bid, \
         patch("main._get_token_id", return_value="tok-123"):
        mock_bid.return_value = (
            0.58,
            [(Decimal("0.58"), Decimal("500"))],
        )

        await _handle_order_fill(
            mock_r, "stream:order:fills", "test-group", "msg-1",
            {
                "bo_id": str(bo.id),
                "filled": "200.0",
                "avg_entry_price": "0.60",
                "status": "FILLED",
                "order_id": "ord-123",
            },
        )

    # Refresh from DB
    db.refresh(bo)
    db.refresh(bot)

    # Exit data should be recorded, but profit deferred to session-end
    assert bo.exit_trigger == "TP"
    assert bo.exit_price == pytest.approx(0.58, abs=0.01)
    assert bo.result == BOResult.PENDING  # deferred to scheduler
    assert bo.avg_price == pytest.approx(0.60)
    assert bo.traces is not None
    # Look for SLIPPAGE_VIOLATION trace
    stages = [t["action"] for t in bo.traces]
    assert "SLIPPAGE_VIOLATION" in stages
    assert "AUTO_EXIT_RECORDED" in stages


@pytest.mark.asyncio
async def test_auto_exit_sl_violated_at_fill(db, bot_and_bo, fake_async_redis):
    """When LIMIT order fills at avg_entry <= sl_price, should auto-exit."""
    bot = bot_and_bo
    # SL = 0.45, but order fills at 0.40 (below SL) → should auto-exit
    bo = _make_bo(db, bot, sl_price=0.45, amount=100.0)

    bot.balance = round(bot.balance - bo.amount, 8)
    db.commit()

    mock_r = fake_async_redis

    with patch("main.fetch_best_bid_from_rest") as mock_bid, \
         patch("main._get_token_id", return_value="tok-456"):
        mock_bid.return_value = (
            0.38,
            [(Decimal("0.38"), Decimal("500"))],
        )

        await _handle_order_fill(
            mock_r, "stream:order:fills", "test-group", "msg-2",
            {
                "bo_id": str(bo.id),
                "filled": "250.0",
                "avg_entry_price": "0.40",
                "status": "FILLED",
                "order_id": "ord-456",
            },
        )

    db.refresh(bo)

    assert bo.exit_trigger == "SL"
    assert bo.result == BOResult.PENDING  # deferred to scheduler
    stages = [t["action"] for t in bo.traces]
    assert "SLIPPAGE_VIOLATION" in stages


@pytest.mark.asyncio
async def test_no_auto_exit_when_condition_not_violated(db, bot_and_bo, fake_async_redis):
    """When LIMIT order fills normally (TP > entry), no auto-exit should happen."""
    bot = bot_and_bo
    # TP = 0.70, order fills at 0.50 → normal, no violation
    bo = _make_bo(db, bot, tp_price=0.70, amount=100.0)

    bot.balance = round(bot.balance - bo.amount, 8)
    db.commit()

    mock_r = fake_async_redis

    # Should NOT call fetch_best_bid_from_rest at all
    with patch("main.fetch_best_bid_from_rest") as mock_bid, \
         patch("main._get_token_id", return_value="tok-789"):

        await _handle_order_fill(
            mock_r, "stream:order:fills", "test-group", "msg-3",
            {
                "bo_id": str(bo.id),
                "filled": "200.0",
                "avg_entry_price": "0.50",
                "status": "FILLED",
                "order_id": "ord-789",
            },
        )

    db.refresh(bo)

    # Should still be PENDING (no auto-exit)
    assert bo.result == BOResult.PENDING
    assert bo.exit_trigger is None
    assert bo.avg_price == pytest.approx(0.50)
    assert bo.num_shares == pytest.approx(200.0)
    # fetch_best_bid should not have been called
    mock_bid.assert_not_called()
