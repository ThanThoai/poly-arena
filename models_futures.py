"""SQLAlchemy models for Futures trading."""

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Enum as SAEnum, ForeignKey,
    Integer, JSON, Numeric, String, Index,
)

from database import Base


def _now():
    return datetime.now(timezone.utc)


class FuturesSide(str, enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class FuturesOrderType(str, enum.Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class FuturesOrderStatus(str, enum.Enum):
    PENDING = "PENDING"       # Limit order waiting to fill
    FILLED = "FILLED"         # Fully filled, position opened
    CANCELLED = "CANCELLED"   # Cancelled before fill
    EXPIRED = "EXPIRED"       # TTL expired


class FuturesPositionStatus(str, enum.Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    LIQUIDATED = "LIQUIDATED"


# Shared SAEnum instances — reused across tables to avoid PostgreSQL
# "DuplicateObject: type already exists" when create_all() runs.
_side_enum = SAEnum(FuturesSide, name="futures_side_enum", create_constraint=False)
_order_type_enum = SAEnum(FuturesOrderType, name="futures_order_type_enum", create_constraint=False)
_order_status_enum = SAEnum(FuturesOrderStatus, name="futures_order_status_enum", create_constraint=False)
_position_status_enum = SAEnum(FuturesPositionStatus, name="futures_position_status_enum", create_constraint=False)


class FuturesPosition(Base):
    """An open or closed futures position."""
    __tablename__ = "futures_positions"

    id = Column(Integer, primary_key=True, index=True)
    bot_name = Column(String(100), nullable=False, index=True)
    symbol = Column(String(20), nullable=False)            # BTC, ETH, SOL, XRP
    exchange = Column(String(20), nullable=False, default="binance")
    side = Column(_side_enum, nullable=False)
    status = Column(
        _position_status_enum,
        nullable=False,
        default=FuturesPositionStatus.OPEN,
    )

    # Size & pricing
    size = Column(Numeric(18, 8, asdecimal=False), nullable=False)           # quantity in base asset
    entry_price = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    exit_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)      # filled on close
    mark_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)      # last known mark

    # Leverage & margin
    leverage = Column(Integer, nullable=False, default=10)
    margin = Column(Numeric(18, 8, asdecimal=False), nullable=False)         # initial margin
    liquidation_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)

    # P&L & fees
    unrealized_pnl = Column(Numeric(18, 8, asdecimal=False), default=0)
    realized_pnl = Column(Numeric(18, 8, asdecimal=False), default=0)
    entry_fee = Column(Numeric(18, 8, asdecimal=False), default=0)
    exit_fee = Column(Numeric(18, 8, asdecimal=False), default=0)

    # TP/SL
    tp_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    sl_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    exit_trigger = Column(String(10), nullable=True)  # "TP", "SL", "MANUAL", "LIQ"

    # Reason / note
    reason = Column(String(500), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=_now)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), default=_now, onupdate=_now)

    __table_args__ = (
        Index("ix_futures_positions_status", "status"),
        Index("ix_futures_positions_symbol", "symbol"),
    )


class FuturesOrder(Base):
    """A pending limit order for futures."""
    __tablename__ = "futures_orders"

    id = Column(Integer, primary_key=True, index=True)
    bot_name = Column(String(100), nullable=False, index=True)
    symbol = Column(String(20), nullable=False)
    exchange = Column(String(20), nullable=False, default="binance")
    side = Column(_side_enum, nullable=False)
    order_type = Column(_order_type_enum, nullable=False)
    status = Column(
        _order_status_enum,
        nullable=False,
        default=FuturesOrderStatus.PENDING,
    )

    # Order params
    size = Column(Numeric(18, 8, asdecimal=False), nullable=False)
    limit_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    leverage = Column(Integer, nullable=False, default=10)

    # TP/SL (applied when position opens)
    tp_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)
    sl_price = Column(Numeric(18, 8, asdecimal=False), nullable=True)

    # TTL
    ttl = Column(Integer, nullable=True)  # seconds, NULL = GTC
    expires_at = Column(DateTime(timezone=True), nullable=True)

    # Reason / note
    reason = Column(String(500), nullable=True)

    # Link to position (set on fill)
    position_id = Column(Integer, ForeignKey("futures_positions.id"), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=_now)
    updated_at = Column(DateTime(timezone=True), default=_now, onupdate=_now)
