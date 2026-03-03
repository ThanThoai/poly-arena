import enum
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Enum as SAEnum, ForeignKey, Integer, JSON, Numeric, String, UniqueConstraint, text

from database import Base


def _now():
    return datetime.now(timezone.utc)


USER_INITIAL_BALANCE = 5_000.0
INITIAL_BALANCE = 1_000.0


class BotStatus(str, enum.Enum):
    ACTIVE  = "ACTIVE"
    PAUSED  = "PAUSED"
    DELETED = "DELETED"


class User(Base):
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    username        = Column(String(100), unique=True, nullable=False, index=True)
    email           = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    initial_balance = Column(Numeric(18, 8, asdecimal=False), default=USER_INITIAL_BALANCE)
    is_active       = Column(Boolean, default=True)
    is_admin        = Column(Boolean, default=False, server_default=text("false"))
    created_at      = Column(DateTime(timezone=True), default=_now)


class UserSettings(Base):
    __tablename__ = "user_settings"

    id         = Column(Integer, primary_key=True)
    user_id    = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    settings   = Column(JSON, nullable=False, default=dict)
    updated_at = Column(DateTime(timezone=True), default=_now, onupdate=_now)


class Bot(Base):
    __tablename__ = "bots"

    id              = Column(Integer, primary_key=True, index=True)
    bot_name        = Column(String(100), unique=True, nullable=False, index=True)
    api_key         = Column(String(64),  unique=True, nullable=False, index=True)
    is_active       = Column(Boolean, default=True)
    initial_balance = Column(Numeric(18, 8, asdecimal=False), default=INITIAL_BALANCE)
    balance         = Column(Numeric(18, 8, asdecimal=False), default=INITIAL_BALANCE)   # current equity
    status          = Column(String(10), default="ACTIVE", nullable=False, server_default=text("'ACTIVE'"))
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at      = Column(DateTime(timezone=True), default=_now)


class BOSymbol(str, enum.Enum):
    BTC = "BTC"
    ETH = "ETH"


class BOTimeframe(str, enum.Enum):
    M5  = "M5"
    M15 = "M15"


class BOForecast(str, enum.Enum):
    GREEN = "GREEN"
    RED   = "RED"


class BOResult(str, enum.Enum):
    PENDING   = "PENDING"
    WIN       = "WIN"
    LOSS      = "LOSS"
    TIE       = "TIE"
    CANCELLED = "CANCELLED"


class BalanceHistory(Base):
    __tablename__ = "balance_history"

    id          = Column(Integer, primary_key=True, index=True)
    bot_name    = Column(String(100), nullable=False, index=True)
    balance     = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    trade_id    = Column(Integer, nullable=True)   # trade that triggered the change
    recorded_at = Column(DateTime(timezone=True), default=_now)


class UserBalanceHistory(Base):
    __tablename__ = "user_balance_history"

    id          = Column(Integer, primary_key=True, index=True)
    user_id     = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    balance     = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    trade_id    = Column(Integer, nullable=True)   # trade that triggered the change
    bot_id      = Column(Integer, nullable=True)   # which bot's trade triggered this
    pnl_amount  = Column(Numeric(18, 8, asdecimal=False), nullable=True)  # profit/loss of that trade
    recorded_at = Column(DateTime(timezone=True), default=_now)


class UserBalanceSnapshot(Base):
    __tablename__ = "user_balance_snapshots"

    id          = Column(Integer, primary_key=True, index=True)
    user_id     = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    balance     = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    bot_balance = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    available   = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    session_id  = Column(String(50), nullable=True)
    recorded_at = Column(DateTime(timezone=True), default=_now)


class BinaryOption(Base):
    __tablename__ = "binary_options"

    id          = Column(Integer, primary_key=True, index=True)
    bot_name    = Column(String(100), nullable=False, index=True)
    symbol      = Column(SAEnum(BOSymbol, create_constraint=False), nullable=False, index=True)
    timeframe   = Column(SAEnum(BOTimeframe, create_constraint=False), nullable=False)
    forecast    = Column(SAEnum(BOForecast, create_constraint=False), nullable=False)
    amount      = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    result      = Column(SAEnum(BOResult, create_constraint=False), default=BOResult.PENDING)
    profit      = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    price_open  = Column(Numeric(18, 8, asdecimal=False), nullable=True)   # Binance candle open price
    price_close = Column(Numeric(18, 8, asdecimal=False), nullable=True)   # Binance candle close price
    avg_price   = Column(Numeric(18, 8, asdecimal=False), nullable=True)   # Polymarket min_ask khi khớp lệnh
    num_shares  = Column(Numeric(18, 8, asdecimal=False), nullable=True)   # amount / avg_price
    reason             = Column(String, nullable=True)  # lý do đặt lệnh (tùy chọn)
    order_received_at  = Column(DateTime(timezone=True), nullable=True)  # thời điểm API nhận request
    ask_fetched_at     = Column(DateTime(timezone=True), nullable=True)  # thời điểm nhận được min_ask từ Polymarket
    settlement_at      = Column(DateTime(timezone=True), nullable=True)
    created_at         = Column(DateTime(timezone=True), default=_now)
    updated_at         = Column(DateTime(timezone=True), default=_now, onupdate=_now)

    # ── Order type ───────────────────────────────────────────────────────────
    limit_price  = Column(Numeric(18, 8, asdecimal=False), nullable=True)    # None = MARKET, set = LIMIT order
    entry_fee    = Column(Numeric(18, 8, asdecimal=False), nullable=True, default=0)  # fee charged at fill (taker=30bps, maker=0)

    # ── Bracket Order (TP/SL) tracking ──────────────────────────────────────
    tp_price     = Column(Numeric(18, 8, asdecimal=False), nullable=True)    # Take Profit price (optional)
    sl_price     = Column(Numeric(18, 8, asdecimal=False), nullable=True)    # Stop Loss price (optional)
    exit_price   = Column(Numeric(18, 8, asdecimal=False), nullable=True)    # avg exit price when TP/SL fires (shadow)
    exit_trigger = Column(String(20), nullable=True)  # "TP" | "SL" — set when bracket fires
    exit_filled  = Column(Numeric(18, 8, asdecimal=False), nullable=True)    # qty exited via TP/SL
    exit_at      = Column(DateTime(timezone=True), nullable=True)   # timestamp when TP/SL triggered
    me_order_id     = Column(String(64), nullable=True)  # matching engine SimulatedOrder ID
    me_order_status = Column(String(20), nullable=True)  # PENDING | PARTIAL | FILLED | CANCELED
    ttl             = Column(Integer, nullable=True)     # TTL in seconds; None = use candle expiry

    # ── Walk prices: per-level fill details ────────────────────────────────────
    walk_prices     = Column(JSON, nullable=True)        # {"entry": [{price, qty, cost}], "exit": [{price, qty, cost}]}

    # ── Order Trace & Market Resolution ──────────────────────────────────────
    traces          = Column(JSON, nullable=True)        # [{timestamp, stage, action, details, data}]
    position_closed = Column(Boolean, default=False)     # True when market resolved or bracket fully exited
    session_offset  = Column(Integer, default=0)          # 0 = current session, 1 = next session
    session_id      = Column(String(64), nullable=True, index=True)   # e.g. "BTC:M5:1709313000"
    candle_open     = Column(Integer, nullable=True)                  # Unix ts of candle open boundary


# ── Achievement System ─────────────────────────────────────────────────────

class AchievementTier(str, enum.Enum):
    BRONZE   = "BRONZE"
    SILVER   = "SILVER"
    GOLD     = "GOLD"
    PLATINUM = "PLATINUM"


class AchievementDefinition(Base):
    __tablename__ = "achievement_definitions"

    id          = Column(Integer, primary_key=True, index=True)
    slug        = Column(String(100), unique=True, nullable=False, index=True)
    name        = Column(String(200), nullable=False)
    description = Column(String(500), nullable=False)
    tier        = Column(String(20), nullable=False)
    category    = Column(String(100), nullable=False)


class BotAchievement(Base):
    __tablename__ = "bot_achievements"
    __table_args__ = (
        UniqueConstraint("bot_id", "achievement_id", name="uq_bot_achievement"),
    )

    id             = Column(Integer, primary_key=True, index=True)
    bot_id         = Column(Integer, ForeignKey("bots.id"), nullable=False, index=True)
    achievement_id = Column(Integer, ForeignKey("achievement_definitions.id"), nullable=False)
    earned_at      = Column(DateTime(timezone=True), default=_now)
    metadata_      = Column("metadata", JSON, nullable=True)


# ── Price History ─────────────────────────────────────────────────────────────

class PriceHistory(Base):
    __tablename__ = "price_history"

    id          = Column(Integer, primary_key=True, index=True)
    symbol      = Column(String(10), nullable=False, index=True)
    timeframe   = Column(String(10), nullable=False, index=True)
    direction   = Column(String(10), nullable=False, index=True)
    best_ask    = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    best_bid    = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    bids        = Column(JSON, nullable=True)   # [[price, size], ...]
    asks        = Column(JSON, nullable=True)   # [[price, size], ...]
    candle_ts   = Column(Integer, nullable=True, index=True)  # candle open ts — tags which session this snapshot belongs to
    recorded_at = Column(DateTime(timezone=True), default=_now, index=True)
