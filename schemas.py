from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator

from models import BOResult, BOSymbol, BOTimeframe, BOForecast


class BOCreate(BaseModel):
    symbol:    BOSymbol
    timeframe: BOTimeframe
    forecast:  BOForecast
    amount:    float
    reason:    Optional[str] = None

    @field_validator("amount")
    @classmethod
    def amount_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be positive")
        return v


class BOResponse(BaseModel):
    id:                 int
    bot_name:           str
    symbol:             BOSymbol
    timeframe:          BOTimeframe
    forecast:           BOForecast
    amount:             float
    result:             BOResult
    profit:             Optional[float]
    price_open:         Optional[float]
    price_close:        Optional[float]
    avg_price:          Optional[float]
    num_shares:         Optional[float]
    reason:             Optional[str]
    order_received_at:  Optional[datetime] = None
    ask_fetched_at:     Optional[datetime] = None
    settlement_at:      Optional[datetime]
    created_at:         Optional[datetime] = None
    updated_at:         Optional[datetime]

    model_config = {"from_attributes": True}


class BOStats(BaseModel):
    total:        int
    wins:         int
    losses:       int
    pending:      int
    win_rate:     float
    total_profit: float
    total_amount: float


class BOBotStats(BaseModel):
    bot_name:     str
    total:        int
    wins:         int
    losses:       int
    pending:      int
    win_rate:     float
    total_profit: float
    total_amount: float
    roi:          float


class BOTimeframeStats(BaseModel):
    timeframe:    str
    total:        int
    wins:         int
    losses:       int
    pending:      int
    win_rate:     float
    total_profit: float
    avg_amount:   float


class BOForecastStats(BaseModel):
    forecast:     str
    total:        int
    wins:         int
    losses:       int
    pending:      int
    win_rate:     float
    total_profit: float


# ── Bot schemas ───────────────────────────────────────────────────────────────

class BalanceHistoryResponse(BaseModel):
    id:          int
    bot_name:    str
    balance:     float
    trade_id:    Optional[int] = None
    recorded_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class BotCreate(BaseModel):
    bot_name: str


class BotRename(BaseModel):
    new_bot_name: str
    api_key: str


class BotResponse(BaseModel):
    id:              int
    bot_name:        str
    api_key:         str
    is_active:       bool
    initial_balance: float
    balance:         float
    created_at:      Optional[datetime] = None

    model_config = {"from_attributes": True}


class BotPublic(BaseModel):
    """List view — api_key is not exposed."""
    id:              int
    bot_name:        str
    is_active:       bool
    initial_balance: float
    balance:         float
    created_at:      Optional[datetime] = None

    model_config = {"from_attributes": True}
