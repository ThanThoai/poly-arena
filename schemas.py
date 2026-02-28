from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from models import BOResult, BOSymbol, BOTimeframe, BOForecast


class BOCreate(BaseModel):
    symbol:      BOSymbol
    timeframe:   BOTimeframe
    forecast:    BOForecast
    amount:      float
    reason:      Optional[str]   = None
    limit_price: Optional[float] = None   # None = MARKET order; set = LIMIT order
    tp_price:    Optional[float] = None   # Take Profit — triggers shadow profit on WIN
    sl_price:    Optional[float] = None   # Stop Loss   — triggers shadow profit on LOSS
    ttl:                Optional[int]   = None   # TTL in seconds; auto-cancel if unfilled within TTL
    slippage_tolerance: Optional[float] = None   # 0.0-1.0; None = 10% default for MARKET orders

    @field_validator("amount")
    @classmethod
    def amount_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be positive")
        return v

    @field_validator("limit_price", "tp_price", "sl_price")
    @classmethod
    def price_in_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0 < v < 1):
            raise ValueError("price must be between 0 and 1 (exclusive)")
        return v

    @field_validator("ttl")
    @classmethod
    def ttl_positive(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError("ttl must be positive (seconds)")
        return v

    @field_validator("slippage_tolerance")
    @classmethod
    def slippage_in_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0 < v <= 1):
            raise ValueError("slippage_tolerance must be between 0 (exclusive) and 1 (inclusive)")
        return v

    @model_validator(mode="after")
    def validate_bracket_prices(self) -> "BOCreate":
        tp = self.tp_price
        sl = self.sl_price
        limit = self.limit_price

        # TP must be greater than SL
        if tp is not None and sl is not None and tp <= sl:
            raise ValueError("tp_price must be greater than sl_price")

        # For LIMIT orders, validate TP/SL relative to limit_price
        if limit is not None:
            if tp is not None and tp <= limit:
                raise ValueError("tp_price must be greater than limit_price for LIMIT BUY orders")
            if sl is not None and sl >= limit:
                raise ValueError("sl_price must be less than limit_price for LIMIT BUY orders")

        return self


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
    # Order type
    limit_price:  Optional[float] = None
    # Bracket Order fields
    tp_price:     Optional[float] = None
    sl_price:     Optional[float] = None
    exit_price:   Optional[float] = None
    exit_trigger: Optional[str]   = None
    exit_filled:  Optional[float] = None
    exit_at:      Optional[datetime] = None
    me_order_id:     Optional[str]   = None
    me_order_status: Optional[str]   = None
    ttl:             Optional[int]   = None
    walk_prices:     Optional[dict]  = None

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
    initial_balance: float = 10000.0


class BotRename(BaseModel):
    new_bot_name: str
    api_key: Optional[str] = None


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
    user_id:         Optional[int] = None
    owner_name:      Optional[str] = None
    created_at:      Optional[datetime] = None

    model_config = {"from_attributes": True}


# ── Auth schemas ──────────────────────────────────────────────────────────────

class UserRegister(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    email: Optional[str] = None
    password: str = Field(..., min_length=6, max_length=128)


class UserLogin(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    initial_balance: float
    allocated_balance: float
    available_balance: float

    model_config = {"from_attributes": True}


class UserSettingsUpdate(BaseModel):
    settings: dict


class UserSettingsResponse(BaseModel):
    settings: dict

    model_config = {"from_attributes": True}
