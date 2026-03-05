"""Tests for the Futures trading engine and order flow."""

import time
import pytest

from services.futures_engine import FuturesEngine
from config.futures_fees import (
    calc_taker_fee, calc_maker_fee, calc_initial_margin, calc_liquidation_price,
)


# ── Fee calculation tests ───────────────────────────────────────────────────


def test_taker_fee():
    """Taker fee = notional × 0.04%."""
    fee = calc_taker_fee(1.0, 50000)  # 1 BTC @ 50000
    assert fee == round(50000 * 0.0004, 8)


def test_maker_fee():
    """Maker fee = notional × 0.02%."""
    fee = calc_maker_fee(1.0, 50000)
    assert fee == round(50000 * 0.0002, 8)


def test_initial_margin():
    margin = calc_initial_margin(1.0, 50000, 10)
    assert margin == 5000.0


def test_liquidation_price_long():
    liq = calc_liquidation_price(50000, "LONG", 10)
    # LONG: entry × (1 - 1/leverage + 0.005) = 50000 × 0.905 = 45250
    assert liq == 45250.0


def test_liquidation_price_short():
    liq = calc_liquidation_price(50000, "SHORT", 10)
    # SHORT: entry × (1 + 1/leverage - 0.005) = 50000 × 1.095 = 54750
    assert liq == 54750.0


# ── Engine tests ────────────────────────────────────────────────────────────


def test_engine_market_order_long_tp():
    """LONG position triggers TP when price rises above tp_price."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 1,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.01,
        "entry_price": 50000,
        "leverage": 10,
        "margin": 50,
        "liquidation_price": 45250,
        "tp_price": 51000,
        "sl_price": 49000,
        "exchange": "binance",
    })

    # Price below TP — no event
    events = engine.update_price("BTC", 50500)
    assert len(events) == 0

    # Price hits TP
    events = engine.update_price("BTC", 51000)
    assert len(events) == 1
    assert events[0]["type"] == "position_close"
    assert events[0]["trigger"] == "TP"
    assert events[0]["exit_price"] == 51000


def test_engine_market_order_short_sl():
    """SHORT position triggers SL when price rises above sl_price."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 2,
        "bot_name": "test-bot",
        "symbol": "ETH",
        "side": "SHORT",
        "size": 1.0,
        "entry_price": 3000,
        "leverage": 5,
        "margin": 600,
        "liquidation_price": 3585,
        "tp_price": 2800,
        "sl_price": 3100,
        "exchange": "binance",
    })

    # Price at entry — no event
    events = engine.update_price("ETH", 3000)
    assert len(events) == 0

    # Price hits SL
    events = engine.update_price("ETH", 3100)
    assert len(events) == 1
    assert events[0]["trigger"] == "SL"
    assert events[0]["realized_pnl"] < 0


def test_engine_liquidation():
    """LONG position gets liquidated when price drops to liq_price."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 3,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.01,
        "entry_price": 50000,
        "leverage": 10,
        "margin": 50,
        "liquidation_price": 45250,
        "tp_price": None,
        "sl_price": None,
        "exchange": "binance",
    })

    events = engine.update_price("BTC", 45250)
    assert len(events) == 1
    assert events[0]["trigger"] == "LIQ"
    assert events[0]["realized_pnl"] == -50  # loses entire margin


def test_engine_limit_order_fill():
    """LONG limit order fills when price drops to limit_price."""
    engine = FuturesEngine()
    engine.register_order({
        "id": 10,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.01,
        "limit_price": 48000,
        "leverage": 10,
        "tp_price": 50000,
        "sl_price": 47000,
        "exchange": "binance",
    })

    # Price above limit — no fill
    events = engine.update_price("BTC", 49000)
    assert len(events) == 0

    # Price hits limit
    events = engine.update_price("BTC", 48000)
    assert len(events) == 1
    assert events[0]["type"] == "order_fill"
    assert events[0]["fill_price"] == 48000


def test_engine_short_limit_order_fill():
    """SHORT limit order fills when price rises to limit_price."""
    engine = FuturesEngine()
    engine.register_order({
        "id": 11,
        "bot_name": "test-bot",
        "symbol": "ETH",
        "side": "SHORT",
        "size": 1.0,
        "limit_price": 3200,
        "leverage": 5,
        "exchange": "binance",
    })

    events = engine.update_price("ETH", 3100)
    assert len(events) == 0

    events = engine.update_price("ETH", 3200)
    assert len(events) == 1
    assert events[0]["type"] == "order_fill"
    assert events[0]["side"] == "SHORT"


def test_engine_order_ttl_expiry():
    """Order with expires_at should expire when time passes."""
    engine = FuturesEngine()
    engine.register_order({
        "id": 20,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.01,
        "limit_price": 45000,
        "leverage": 10,
        "exchange": "binance",
        "expires_at": time.time() - 1,  # already expired
    })

    events = engine.update_price("BTC", 50000)
    assert len(events) == 1
    assert events[0]["type"] == "order_expire"
    assert events[0]["order_id"] == 20


def test_engine_pnl_calculation_long():
    """Verify unrealized PnL for LONG position."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 30,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.1,
        "entry_price": 50000,
        "leverage": 10,
        "margin": 500,
        "liquidation_price": 45250,
        "tp_price": None,
        "sl_price": None,
        "exchange": "binance",
    })

    events = engine.update_price("BTC", 51000)
    assert len(events) == 0  # no trigger

    # Check PnL in engine state
    pos = engine._open_positions[30]
    # LONG PnL = (51000 - 50000) × 0.1 = 100
    assert pos["unrealized_pnl"] == 100.0


def test_engine_pnl_calculation_short():
    """Verify unrealized PnL for SHORT position."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 31,
        "bot_name": "test-bot",
        "symbol": "ETH",
        "side": "SHORT",
        "size": 1.0,
        "entry_price": 3000,
        "leverage": 5,
        "margin": 600,
        "liquidation_price": 3585,
        "tp_price": None,
        "sl_price": None,
        "exchange": "binance",
    })

    events = engine.update_price("ETH", 2900)
    assert len(events) == 0

    pos = engine._open_positions[31]
    # SHORT PnL = (3000 - 2900) × 1.0 = 100
    assert pos["unrealized_pnl"] == 100.0


def test_engine_sl_priority_over_tp():
    """SL triggers before TP when both conditions met (e.g., gap down through SL)."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 40,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.01,
        "entry_price": 50000,
        "leverage": 10,
        "margin": 50,
        "liquidation_price": 45250,
        "tp_price": 51000,
        "sl_price": 49000,
        "exchange": "binance",
    })

    # Price drops below SL
    events = engine.update_price("BTC", 48000)
    assert len(events) == 1
    assert events[0]["trigger"] == "SL"


def test_engine_liq_priority_over_sl():
    """Liquidation takes priority over SL."""
    engine = FuturesEngine()
    engine.register_position({
        "id": 41,
        "bot_name": "test-bot",
        "symbol": "BTC",
        "side": "LONG",
        "size": 0.01,
        "entry_price": 50000,
        "leverage": 10,
        "margin": 50,
        "liquidation_price": 45250,
        "tp_price": 55000,
        "sl_price": 46000,
        "exchange": "binance",
    })

    # Price drops below both SL and LIQ
    events = engine.update_price("BTC", 45000)
    assert len(events) == 1
    assert events[0]["trigger"] == "LIQ"
    assert events[0]["realized_pnl"] == -50
