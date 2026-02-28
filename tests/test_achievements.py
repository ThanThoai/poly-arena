"""Tests for the achievement system."""

import pytest
from datetime import datetime, timezone

from models import (
    AchievementDefinition,
    BalanceHistory,
    BinaryOption,
    Bot,
    BotAchievement,
    BOForecast,
    BOResult,
    BOSymbol,
    BOTimeframe,
)
from services.achievement_seeder import seed_achievements
from services.achievements import on_trade_resolved


@pytest.fixture(autouse=True)
def _seed(db):
    """Seed achievement definitions before each test."""
    seed_achievements(db)
    yield


def _make_bot(db, bot_name="ach-bot", balance=10000.0, initial_balance=10000.0, user_id=None):
    import secrets
    bot = Bot(
        bot_name=bot_name,
        api_key=secrets.token_urlsafe(32),
        is_active=True,
        balance=balance,
        initial_balance=initial_balance,
        user_id=user_id,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


def _make_trade(
    db,
    bot_name="ach-bot",
    result=BOResult.WIN,
    amount=100.0,
    avg_price=0.50,
    num_shares=None,
    symbol=BOSymbol.BTC,
    timeframe=BOTimeframe.M5,
    forecast=BOForecast.GREEN,
    profit=None,
):
    bo = BinaryOption(
        bot_name=bot_name,
        symbol=symbol,
        timeframe=timeframe,
        forecast=forecast,
        amount=amount,
        result=result,
        avg_price=avg_price,
        num_shares=num_shares or (amount / avg_price if avg_price else None),
        profit=profit,
        settlement_at=datetime.now(timezone.utc),
    )
    db.add(bo)
    db.commit()
    db.refresh(bo)
    return bo


# ── peak-buyer ───────────────────────────────────────────────────────────


class TestPeakBuyer:
    def test_triggers_on_high_price_loss(self, db):
        bot = _make_bot(db)
        bo = _make_trade(db, result=BOResult.LOSS, avg_price=0.99)
        awarded = on_trade_resolved(bo, db)
        assert "peak-buyer" in awarded

    def test_no_trigger_on_win(self, db):
        bot = _make_bot(db)
        bo = _make_trade(db, result=BOResult.WIN, avg_price=0.99)
        awarded = on_trade_resolved(bo, db)
        assert "peak-buyer" not in awarded

    def test_no_trigger_on_low_price(self, db):
        bot = _make_bot(db)
        bo = _make_trade(db, result=BOResult.LOSS, avg_price=0.50)
        awarded = on_trade_resolved(bo, db)
        assert "peak-buyer" not in awarded


# ── blind-sniper ─────────────────────────────────────────────────────────


class TestBlindSniper:
    def test_triggers_on_low_price_win(self, db):
        bot = _make_bot(db)
        bo = _make_trade(db, result=BOResult.WIN, avg_price=0.05)
        awarded = on_trade_resolved(bo, db)
        assert "blind-sniper" in awarded

    def test_no_trigger_on_loss(self, db):
        bot = _make_bot(db)
        bo = _make_trade(db, result=BOResult.LOSS, avg_price=0.05)
        awarded = on_trade_resolved(bo, db)
        assert "blind-sniper" not in awarded


# ── pink-slip-seeker ─────────────────────────────────────────────────────


class TestPinkSlipSeeker:
    def test_triggers_on_large_stake(self, db):
        bot = _make_bot(db, initial_balance=10000.0)
        bo = _make_trade(db, amount=9500.0, avg_price=0.50, result=BOResult.WIN)
        awarded = on_trade_resolved(bo, db)
        assert "pink-slip-seeker" in awarded

    def test_no_trigger_on_small_stake(self, db):
        bot = _make_bot(db, initial_balance=10000.0)
        bo = _make_trade(db, amount=500.0, avg_price=0.50, result=BOResult.WIN)
        awarded = on_trade_resolved(bo, db)
        assert "pink-slip-seeker" not in awarded


# ── the-martyr ───────────────────────────────────────────────────────────


class TestTheMartyr:
    def test_triggers_on_bankruptcy(self, db):
        bot = _make_bot(db, balance=0.0)
        bo = _make_trade(db, result=BOResult.LOSS)
        awarded = on_trade_resolved(bo, db)
        assert "the-martyr" in awarded

    def test_no_trigger_with_remaining_balance(self, db):
        bot = _make_bot(db, balance=500.0)
        bo = _make_trade(db, result=BOResult.LOSS)
        awarded = on_trade_resolved(bo, db)
        assert "the-martyr" not in awarded


# ── golden-incense (10 losses) ───────────────────────────────────────────


class TestGoldenIncense:
    def test_triggers_on_10_loss_streak(self, db):
        bot = _make_bot(db)
        for _ in range(10):
            _make_trade(db, result=BOResult.LOSS)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "golden-incense" in awarded

    def test_no_trigger_on_9_losses(self, db):
        bot = _make_bot(db)
        for _ in range(9):
            _make_trade(db, result=BOResult.LOSS)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "golden-incense" not in awarded


# ── anti-midas (15 losses) ───────────────────────────────────────────────


class TestAntiMidas:
    def test_triggers_on_15_loss_streak(self, db):
        bot = _make_bot(db)
        for _ in range(15):
            _make_trade(db, result=BOResult.LOSS)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "anti-midas" in awarded


# ── immortal-sniper ──────────────────────────────────────────────────────


class TestImmortalSniper:
    def test_triggers_on_5_low_price_wins(self, db):
        bot = _make_bot(db)
        for _ in range(5):
            _make_trade(db, result=BOResult.WIN, avg_price=0.10)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "immortal-sniper" in awarded

    def test_no_trigger_with_high_price(self, db):
        bot = _make_bot(db)
        for _ in range(5):
            _make_trade(db, result=BOResult.WIN, avg_price=0.50)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "immortal-sniper" not in awarded


# ── dust-collector ───────────────────────────────────────────────────────


class TestDustCollector:
    def test_triggers_on_100_dust_trades(self, db):
        bot = _make_bot(db)
        for _ in range(100):
            _make_trade(db, amount=0.50, avg_price=0.50, result=BOResult.LOSS)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "dust-collector" in awarded


# ── penny-pincher ────────────────────────────────────────────────────────


class TestPennyPincher:
    def test_triggers_on_50_consecutive_min_stake(self, db):
        bot = _make_bot(db)
        for _ in range(50):
            _make_trade(db, amount=0.50, avg_price=0.50, result=BOResult.WIN)
        bo = db.query(BinaryOption).order_by(BinaryOption.id.desc()).first()
        awarded = on_trade_resolved(bo, db)
        assert "penny-pincher" in awarded


# ── phoenix-down ─────────────────────────────────────────────────────────


class TestPhoenixDown:
    def test_triggers_on_recovery(self, db):
        bot = _make_bot(db, balance=10000.0, initial_balance=10000.0)
        # Record history with a dip below $1
        db.add(BalanceHistory(bot_name=bot.bot_name, balance=0.50))
        db.add(BalanceHistory(bot_name=bot.bot_name, balance=10000.0))
        db.commit()
        bo = _make_trade(db, result=BOResult.WIN)
        awarded = on_trade_resolved(bo, db)
        assert "phoenix-down" in awarded

    def test_no_trigger_without_dip(self, db):
        bot = _make_bot(db, balance=10000.0, initial_balance=10000.0)
        db.add(BalanceHistory(bot_name=bot.bot_name, balance=9500.0))
        db.commit()
        bo = _make_trade(db, result=BOResult.WIN)
        awarded = on_trade_resolved(bo, db)
        assert "phoenix-down" not in awarded


# ── Deduplication ────────────────────────────────────────────────────────


class TestDedup:
    def test_achievement_not_awarded_twice(self, db):
        bot = _make_bot(db)
        bo1 = _make_trade(db, result=BOResult.LOSS, avg_price=0.99)
        awarded1 = on_trade_resolved(bo1, db)
        assert "peak-buyer" in awarded1

        bo2 = _make_trade(db, result=BOResult.LOSS, avg_price=0.99)
        awarded2 = on_trade_resolved(bo2, db)
        assert "peak-buyer" not in awarded2


# ── API endpoints ────────────────────────────────────────────────────────


class TestAchievementAPI:
    def test_list_definitions(self, client, db):
        resp = client.get("/poly-arena/achievements/")
        assert resp.status_code == 200
        data = resp.json()
        slugs = [d["slug"] for d in data]
        assert "peak-buyer" in slugs
        assert "phoenix-down" in slugs
        assert len(data) >= 10

    def test_bot_achievements_empty(self, client, db):
        bot = _make_bot(db, bot_name="api-bot")
        resp = client.get(f"/poly-arena/achievements/bot/{bot.id}")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_bot_achievements_after_award(self, client, db):
        bot = _make_bot(db, bot_name="api-bot2")
        bo = _make_trade(db, bot_name="api-bot2", result=BOResult.LOSS, avg_price=0.99)
        on_trade_resolved(bo, db)
        resp = client.get(f"/poly-arena/achievements/bot/{bot.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["slug"] == "peak-buyer"
