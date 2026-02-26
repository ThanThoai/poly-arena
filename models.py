import enum
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Enum as SAEnum, Float, Integer, String

from database import Base


def _now():
    return datetime.now(timezone.utc)


INITIAL_BALANCE = 10_000.0


class Bot(Base):
    __tablename__ = "bots"

    id              = Column(Integer, primary_key=True, index=True)
    bot_name        = Column(String(100), unique=True, nullable=False, index=True)
    api_key         = Column(String(64),  unique=True, nullable=False, index=True)
    is_active       = Column(Boolean, default=True)
    initial_balance = Column(Float, default=INITIAL_BALANCE)
    balance         = Column(Float, default=INITIAL_BALANCE)   # current equity
    created_at      = Column(DateTime(timezone=True), default=_now)


class BOSymbol(str, enum.Enum):
    BTC = "BTC"
    ETH = "ETH"
    SOL = "SOL"
    XRP = "XRP"


class BOTimeframe(str, enum.Enum):
    M5  = "M5"
    M15 = "M15"
    H1  = "H1"


class BOForecast(str, enum.Enum):
    GREEN = "GREEN"
    RED   = "RED"


class BOResult(str, enum.Enum):
    PENDING = "PENDING"
    WIN     = "WIN"
    LOSS    = "LOSS"
    TIE     = "TIE"


class BalanceHistory(Base):
    __tablename__ = "balance_history"

    id          = Column(Integer, primary_key=True, index=True)
    bot_name    = Column(String(100), nullable=False, index=True)
    balance     = Column(Float, nullable=False)
    trade_id    = Column(Integer, nullable=True)   # trade that triggered the change
    recorded_at = Column(DateTime(timezone=True), default=_now)


class BinaryOption(Base):
    __tablename__ = "binary_options"

    id          = Column(Integer, primary_key=True, index=True)
    bot_name    = Column(String(100), nullable=False, index=True)
    symbol      = Column(SAEnum(BOSymbol), nullable=False, index=True)
    timeframe   = Column(SAEnum(BOTimeframe), nullable=False)
    forecast    = Column(SAEnum(BOForecast), nullable=False)
    amount      = Column(Float, nullable=False)
    result      = Column(SAEnum(BOResult), default=BOResult.PENDING)
    profit      = Column(Float, nullable=True)
    price_open  = Column(Float, nullable=True)   # Binance candle open price
    price_close = Column(Float, nullable=True)   # Binance candle close price
    avg_price   = Column(Float, nullable=True)   # Polymarket min_ask khi khớp lệnh
    num_shares  = Column(Float, nullable=True)   # amount / avg_price
    reason             = Column(String, nullable=True)  # lý do đặt lệnh (tùy chọn)
    order_received_at  = Column(DateTime(timezone=True), nullable=True)  # thời điểm API nhận request
    ask_fetched_at     = Column(DateTime(timezone=True), nullable=True)  # thời điểm nhận được min_ask từ Polymarket
    settlement_at      = Column(DateTime(timezone=True), nullable=True)
    created_at         = Column(DateTime(timezone=True), default=_now)
    updated_at         = Column(DateTime(timezone=True), default=_now, onupdate=_now)

    # ── Order type ───────────────────────────────────────────────────────────
    limit_price  = Column(Float, nullable=True)    # None = MARKET, set = LIMIT order

    # ── Bracket Order (TP/SL) tracking ──────────────────────────────────────
    tp_price     = Column(Float, nullable=True)    # Take Profit price (optional)
    sl_price     = Column(Float, nullable=True)    # Stop Loss price (optional)
    exit_price   = Column(Float, nullable=True)    # avg exit price when TP/SL fires (shadow)
    exit_trigger = Column(String(20), nullable=True)  # "TP" | "SL" — set when bracket fires
    exit_filled  = Column(Float, nullable=True)    # qty exited via TP/SL
    me_order_id  = Column(String(64), nullable=True)  # matching engine SimulatedOrder ID
