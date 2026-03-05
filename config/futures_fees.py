"""Fee configuration for Futures trading — per-exchange fee schedules.

Binance Futures (USDT-M):
    Maker: 0.02%  (0.0002)
    Taker: 0.04%  (0.0004)

Fee = notional_value × rate
Notional value = size × price
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class FuturesFeeSchedule:
    exchange: str
    maker_rate: float
    taker_rate: float


BINANCE_FEES = FuturesFeeSchedule(
    exchange="binance",
    maker_rate=0.0002,
    taker_rate=0.0004,
)

# Registry — extend when adding new exchanges
FEE_SCHEDULES: dict[str, FuturesFeeSchedule] = {
    "binance": BINANCE_FEES,
}

# Default leverage limits
MAX_LEVERAGE = 50
DEFAULT_LEVERAGE = 10

# Maintenance margin rate (for liquidation calculation)
# Simplified: single tier. Binance uses tiered brackets but for paper trading
# a flat rate is sufficient.
MAINTENANCE_MARGIN_RATE = 0.005  # 0.5%


def calc_fee(notional: float, rate: float) -> float:
    """Calculate fee for a given notional value."""
    return round(notional * rate, 8)


def calc_taker_fee(size: float, price: float, exchange: str = "binance") -> float:
    """Taker fee for opening/closing a position."""
    schedule = FEE_SCHEDULES[exchange]
    return calc_fee(size * price, schedule.taker_rate)


def calc_maker_fee(size: float, price: float, exchange: str = "binance") -> float:
    """Maker fee (limit order fill)."""
    schedule = FEE_SCHEDULES[exchange]
    return calc_fee(size * price, schedule.maker_rate)


def calc_initial_margin(size: float, price: float, leverage: int) -> float:
    """Initial margin = notional / leverage."""
    return round((size * price) / leverage, 8)


def calc_liquidation_price(
    entry_price: float,
    side: str,
    leverage: int,
    maintenance_margin_rate: float = MAINTENANCE_MARGIN_RATE,
) -> float:
    """Estimate liquidation price for a position.

    LONG:  liq = entry × (1 - 1/leverage + maintenance_margin_rate)
    SHORT: liq = entry × (1 + 1/leverage - maintenance_margin_rate)
    """
    if side == "LONG":
        return round(entry_price * (1 - 1 / leverage + maintenance_margin_rate), 8)
    else:
        return round(entry_price * (1 + 1 / leverage - maintenance_margin_rate), 8)
