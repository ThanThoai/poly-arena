"""Dynamic Fee Curve for PolyArena — mirrors Polymarket Crypto markets.

Formula per fill level:
    nominal_fee = matched_qty × feeRate × (price × (1 - price))^exponent

Role-based application:
    Taker: pays 100% of nominal_fee
    Maker: receives rebate = 20% of nominal_fee (added to balance)
    Resolution: $0 (no fee on settlement payout)
"""

FEE_RATE = 0.25
EXPONENT = 2
MAKER_REBATE_PCT = 0.20


def nominal_fee_per_level(qty: float, price: float) -> float:
    """Compute nominal fee for a single fill level."""
    return round(qty * FEE_RATE * (price * (1 - price)) ** EXPONENT, 8)


def taker_fee_from_levels(levels: list[dict]) -> float:
    """Sum taker fees across multiple walk levels.

    Each level dict must have 'qty' and 'price' keys.
    """
    total = 0.0
    for lv in levels:
        total += nominal_fee_per_level(lv["qty"], lv["price"])
    return round(total, 8)


def maker_rebate_from_levels(levels: list[dict]) -> float:
    """Sum maker rebates across multiple walk levels.

    Rebate = MAKER_REBATE_PCT × nominal_fee per level.
    """
    total = 0.0
    for lv in levels:
        total += nominal_fee_per_level(lv["qty"], lv["price"]) * MAKER_REBATE_PCT
    return round(total, 8)


def estimate_max_taker_fee(amount: float) -> float:
    """Estimate worst-case taker fee for upfront balance check.

    Fee is maximized at price=0.50 where (p*(1-p))^2 = 0.0625.
    Assume all qty bought at price 0.50 → qty = amount / 0.50.
    """
    qty_at_worst = amount / 0.50
    return round(nominal_fee_per_level(qty_at_worst, 0.50), 8)
