"""
Tests for binary settlement payout math (docs/system_improve.md Section 6).

Verifies the actual `_settle_single_trade` function from settlement.py
using a real DB session, covering:
  - RED candle + GREEN forecast → LOSS, profit = -avg_price × num_shares
  - RED candle + RED forecast   → WIN,  profit = (1 - avg_price) × num_shares
  - GREEN candle + GREEN forecast → WIN
  - Doji candle (close == open) → treated as GREEN
  - Bracket exit (TP triggered) → shadow profit formula
  - Partial exit → shadow profit for exited portion + binary for remainder
"""

import pytest
from models import BalanceHistory, BinaryOption, Bot, BOResult, BOSymbol, BOTimeframe, BOForecast
from services.settlement import _settle_single_trade


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_bot(db, balance=10_000.0):
    bot = Bot(bot_name="payout-bot", api_key="payout-key-123", is_active=True, balance=balance)
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


def _make_bo(db, bot, **kwargs):
    defaults = dict(
        bot_name=bot.bot_name,
        symbol=BOSymbol.BTC,
        timeframe=BOTimeframe.M5,
        forecast=BOForecast.GREEN,
        amount=100.0,
        result=BOResult.PENDING,
        avg_price=0.50,
        num_shares=200.0,
    )
    defaults.update(kwargs)
    bo = BinaryOption(**defaults)
    db.add(bo)
    db.commit()
    db.refresh(bo)
    return bo


# ═══════════════════════════════════════════════════════════════
# 1. Binary settlement — no bracket
# ═══════════════════════════════════════════════════════════════


def test_red_candle_green_forecast_loss(db):
    """RED candle + GREEN forecast → LOSS, profit = -avg_price × num_shares."""
    bot = _make_bot(db)
    bo = _make_bo(db, bot, forecast=BOForecast.GREEN, avg_price=0.40, num_shares=250.0, amount=100.0)

    _settle_single_trade(bo, open_price=110.0, close_price=100.0, db=db)
    db.commit()

    assert bo.result == BOResult.LOSS
    assert round(bo.profit, 8) == round(-0.40 * 250.0, 8)
    assert bo.price_open == 110.0
    assert bo.price_close == 100.0


def test_red_candle_red_forecast_win(db):
    """RED candle + RED forecast → WIN, profit = (1 - avg_price) × num_shares, payout = $1/share."""
    bot = _make_bot(db)
    bo = _make_bo(db, bot, forecast=BOForecast.RED, avg_price=0.48, num_shares=208.33, amount=100.0)

    _settle_single_trade(bo, open_price=110.0, close_price=100.0, db=db)
    db.commit()

    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.48) * 208.33, 2)


def test_green_candle_green_forecast_win(db):
    """GREEN candle + GREEN forecast → WIN."""
    bot = _make_bot(db)
    bo = _make_bo(db, bot, forecast=BOForecast.GREEN, avg_price=0.30, num_shares=333.33, amount=100.0)

    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.30) * 333.33, 2)


def test_green_candle_red_forecast_loss(db):
    """GREEN candle + RED forecast → LOSS."""
    bot = _make_bot(db)
    bo = _make_bo(db, bot, forecast=BOForecast.RED, avg_price=0.50, num_shares=200.0, amount=100.0)

    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    assert bo.result == BOResult.LOSS
    assert round(bo.profit, 8) == round(-0.50 * 200.0, 8)


def test_doji_candle_treated_as_green(db):
    """Doji candle (close == open) → treated as GREEN."""
    bot = _make_bot(db)
    bo = _make_bo(db, bot, forecast=BOForecast.GREEN, avg_price=0.50, num_shares=200.0, amount=100.0)

    _settle_single_trade(bo, open_price=100.0, close_price=100.0, db=db)
    db.commit()

    # Doji = GREEN → GREEN forecast wins
    assert bo.result == BOResult.WIN
    assert round(bo.profit, 2) == round((1 - 0.50) * 200.0, 2)


def test_doji_candle_red_forecast_loss(db):
    """Doji candle (close == open) → GREEN, so RED forecast loses."""
    bot = _make_bot(db)
    bo = _make_bo(db, bot, forecast=BOForecast.RED, avg_price=0.50, num_shares=200.0, amount=100.0)

    _settle_single_trade(bo, open_price=100.0, close_price=100.0, db=db)
    db.commit()

    assert bo.result == BOResult.LOSS


# ═══════════════════════════════════════════════════════════════
# 2. Balance update verification
# ═══════════════════════════════════════════════════════════════


def test_balance_updated_on_win(db):
    """Bot balance increases by (amount + profit) on WIN."""
    bot = _make_bot(db, balance=9900.0)  # 100 deducted at order creation
    bo = _make_bo(db, bot, forecast=BOForecast.GREEN, avg_price=0.50, num_shares=200.0, amount=100.0)

    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    db.refresh(bot)
    expected_payout = 100.0 + round((1 - 0.50) * 200.0, 8)  # amount + profit
    assert round(bot.balance, 2) == round(9900.0 + expected_payout, 2)


def test_balance_updated_on_loss(db):
    """Bot balance increases by (amount + profit) on LOSS — profit is negative."""
    bot = _make_bot(db, balance=9900.0)
    bo = _make_bo(db, bot, forecast=BOForecast.GREEN, avg_price=0.50, num_shares=200.0, amount=100.0)

    _settle_single_trade(bo, open_price=110.0, close_price=100.0, db=db)
    db.commit()

    db.refresh(bot)
    profit = round(-0.50 * 200.0, 8)
    expected_payout = 100.0 + profit  # amount + negative profit
    assert round(bot.balance, 2) == round(9900.0 + expected_payout, 2)


def test_balance_history_created(db):
    """BalanceHistory row is created after settlement."""
    bot = _make_bot(db, balance=9900.0)
    bo = _make_bo(db, bot, forecast=BOForecast.GREEN, avg_price=0.50, num_shares=200.0, amount=100.0)

    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    history = db.query(BalanceHistory).filter(BalanceHistory.trade_id == bo.id).first()
    assert history is not None
    assert history.bot_name == bot.bot_name


# ═══════════════════════════════════════════════════════════════
# 3. Bracket exit — shadow profit
# ═══════════════════════════════════════════════════════════════


def test_bracket_tp_exit_shadow_profit(db):
    """TP triggered → shadow profit = (exit_price - avg_price) × exit_filled."""
    bot = _make_bot(db)
    bo = _make_bo(
        db, bot,
        avg_price=0.50, num_shares=200.0, amount=100.0,
        tp_price=0.70,
        exit_trigger="TP", exit_price=0.72, exit_filled=200.0,
    )

    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    assert bo.result == BOResult.WIN
    assert round(bo.profit, 4) == round((0.72 - 0.50) * 200.0, 4)


def test_bracket_sl_exit_shadow_profit(db):
    """SL triggered → shadow profit (negative)."""
    bot = _make_bot(db)
    bo = _make_bo(
        db, bot,
        avg_price=0.50, num_shares=200.0, amount=100.0,
        sl_price=0.35,
        exit_trigger="SL", exit_price=0.33, exit_filled=200.0,
    )

    _settle_single_trade(bo, open_price=110.0, close_price=100.0, db=db)
    db.commit()

    assert bo.result == BOResult.LOSS
    assert round(bo.profit, 4) == round((0.33 - 0.50) * 200.0, 4)


# ═══════════════════════════════════════════════════════════════
# 4. Partial exit — shadow + binary remainder
# ═══════════════════════════════════════════════════════════════


def test_partial_exit_tp_with_binary_remainder_win(db):
    """
    Partial TP exit: 150 of 200 shares exited via TP.
    Remainder (50 shares) settled via binary (GREEN candle + GREEN forecast → WIN).
    """
    bot = _make_bot(db)
    bo = _make_bo(
        db, bot,
        forecast=BOForecast.GREEN,
        avg_price=0.50, num_shares=200.0, amount=100.0,
        tp_price=0.70,
        exit_trigger="TP", exit_price=0.72, exit_filled=150.0,
    )

    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    shadow_profit = round((0.72 - 0.50) * 150.0, 8)
    remainder_profit = round((1 - 0.50) * 50.0, 8)  # binary WIN
    expected = shadow_profit + remainder_profit

    assert bo.result == BOResult.WIN
    assert round(bo.profit, 4) == round(expected, 4)


def test_partial_exit_sl_with_binary_remainder_loss(db):
    """
    Partial SL exit: 100 of 200 shares exited via SL.
    Remainder (100 shares) settled via binary (RED candle + GREEN forecast → LOSS).
    """
    bot = _make_bot(db)
    bo = _make_bo(
        db, bot,
        forecast=BOForecast.GREEN,
        avg_price=0.50, num_shares=200.0, amount=100.0,
        sl_price=0.35,
        exit_trigger="SL", exit_price=0.33, exit_filled=100.0,
    )

    _settle_single_trade(bo, open_price=110.0, close_price=100.0, db=db)
    db.commit()

    shadow_profit = round((0.33 - 0.50) * 100.0, 8)
    remainder_profit = round(-0.50 * 100.0, 8)  # binary LOSS
    expected = shadow_profit + remainder_profit

    assert bo.result == BOResult.LOSS
    assert round(bo.profit, 4) == round(expected, 4)


def test_partial_exit_sl_with_binary_remainder_win(db):
    """
    Partial SL exit: 100 of 200 shares exited via SL.
    Remainder (100 shares) settled via binary (GREEN candle + GREEN forecast → WIN).
    Net profit could be positive if remainder WIN outweighs SL loss.
    """
    bot = _make_bot(db)
    bo = _make_bo(
        db, bot,
        forecast=BOForecast.GREEN,
        avg_price=0.50, num_shares=200.0, amount=100.0,
        sl_price=0.45,
        exit_trigger="SL", exit_price=0.44, exit_filled=100.0,
    )

    # GREEN candle → remainder wins
    _settle_single_trade(bo, open_price=100.0, close_price=110.0, db=db)
    db.commit()

    shadow_profit = round((0.44 - 0.50) * 100.0, 8)   # -6.0
    remainder_profit = round((1 - 0.50) * 100.0, 8)    # +50.0
    expected = shadow_profit + remainder_profit          # +44.0

    assert bo.result == BOResult.WIN  # net positive
    assert round(bo.profit, 4) == round(expected, 4)
