from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from models import BOResult, BOSymbol, BOTimeframe, BOForecast


class BOCreate(BaseModel):
    symbol:      BOSymbol
    timeframe:   BOTimeframe
    forecast:    BOForecast
    amount:      float
    reason:      Optional[str]   = None
    limit_price: Optional[float] = None   # None = MARKET order; set = LIMIT order
    tp_price:    Optional[float] = None   # Take-profit price for bracket monitoring
    sl_price:    Optional[float] = None   # Stop-loss price for bracket monitoring
    ttl:                Optional[int]   = None   # TTL in seconds; auto-cancel if unfilled within TTL
    slippage_tolerance: Optional[float] = None   # 0.0-1.0; None = 10% default for MARKET orders
    session_offset:     Optional[int]   = Field(default=0, ge=0, le=3)  # 0 = current, 1-3 = future sessions
    timestamp:          Optional[int]   = None   # Unix timestamp (seconds) to target a specific candle session

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

    @field_validator("timestamp")
    @classmethod
    def timestamp_positive(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError("timestamp must be a positive Unix timestamp (seconds)")
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
    traces:          Optional[list]  = None
    position_closed: Optional[bool]  = None
    session_offset:  Optional[int]   = None
    entry_fee:       Optional[float] = None

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


# ── P&L schemas ──────────────────────────────────────────────────────────────

class BotPnlResponse(BaseModel):
    bot_name: str
    status: str = "ACTIVE"
    initial_balance: float
    current_balance: float
    realized_pnl: float
    realized_pnl_pct: float
    wins: int
    losses: int
    pending: int
    total_trades: int
    win_rate: float
    avg_profit_per_trade: float
    total_fees: float = 0.0


class UserPnlResponse(BaseModel):
    user_id: int
    username: str
    initial_balance: float
    allocated_balance: float
    available_balance: float
    current_balance: float
    realized_pnl: float
    realized_pnl_pct: float
    wins: int
    losses: int
    pending: int
    total_trades: int
    win_rate: float
    avg_profit_per_trade: float
    total_fees: float = 0.0
    bots: List[BotPnlResponse]


# ── Bot schemas ───────────────────────────────────────────────────────────────

class BalanceHistoryResponse(BaseModel):
    id:          int
    bot_name:    str
    balance:     float
    trade_id:    Optional[int] = None
    recorded_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class UserBalanceHistoryResponse(BaseModel):
    id:          int
    user_id:     int
    balance:     float
    trade_id:    Optional[int] = None
    bot_id:      Optional[int] = None
    pnl_amount:  Optional[float] = None
    recorded_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class BotCreate(BaseModel):
    bot_name: str
    initial_balance: float = 1000.0


class BotRename(BaseModel):
    new_bot_name: str
    api_key: Optional[str] = None


class BotBalanceAdjust(BaseModel):
    balance: float = Field(..., gt=0)


class BotResponse(BaseModel):
    id:              int
    bot_name:        str
    api_key:         str
    is_active:       bool
    status:          str = "ACTIVE"
    initial_balance: float
    balance:         float
    created_at:      Optional[datetime] = None

    model_config = {"from_attributes": True}


class BotPublic(BaseModel):
    """List view — api_key is not exposed."""
    id:              int
    bot_name:        str
    is_active:       bool
    status:          str = "ACTIVE"
    initial_balance: float
    balance:         float
    user_id:         Optional[int] = None
    owner_name:      Optional[str] = None
    user_initial_balance: Optional[float] = None  # user's total pool balance
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
    total_balance: float = 0.0       # sum of current bot balances
    total_pnl: float = 0.0           # total_balance - allocated_balance
    is_admin: bool = False

    model_config = {"from_attributes": True}


class UserSettingsUpdate(BaseModel):
    settings: dict


class UserSettingsResponse(BaseModel):
    settings: dict

    model_config = {"from_attributes": True}


# ── Admin schemas ────────────────────────────────────────────────────────────

class AdminBalanceAdjust(BaseModel):
    balance: float


class AdminCreateAdmin(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=6, max_length=128)
    email: Optional[str] = None


class BotPerformanceResponse(BaseModel):
    bot_name: str
    status: str
    initial_balance: float
    current_balance: float
    realized_pnl: float
    realized_pnl_pct: float
    wins: int
    losses: int
    pending: int
    win_rate: float
    recent_trades: List[BOResponse] = []
    balance_history: List[BalanceHistoryResponse] = []


class PriceHistoryResponse(BaseModel):
    id: int
    symbol: str
    timeframe: str
    direction: str
    best_ask: Optional[float] = None
    best_bid: Optional[float] = None
    bids: Optional[list] = None   # [[price, size], ...]
    asks: Optional[list] = None   # [[price, size], ...]
    candle_ts: Optional[int] = None
    recorded_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class AdminUserResponse(BaseModel):
    id: int
    username: str
    email: str
    initial_balance: float
    is_active: bool
    is_admin: bool
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ── Trade Inspector schemas ──────────────────────────────────────────────────

class TimelineEvent(BaseModel):
    timestamp: str
    category: str       # trace | price | fill_entry | fill_exit
    action: str
    details: str = ""
    data: Optional[dict | list] = None

class SessionInfo(BaseModel):
    symbol: str
    timeframe: str
    direction: str
    session_start: int
    session_end: int
    session_offset: int = 0   # 0 = current candle, 1-3 = future

class TradeInspectResponse(BaseModel):
    trade: BOResponse
    timeline: List[TimelineEvent]
    session: SessionInfo


# ── Achievement schemas ──────────────────────────────────────────────────────

class AchievementDefinitionResponse(BaseModel):
    id:          int
    slug:        str
    name:        str
    description: str
    tier:        str
    category:    str

    model_config = {"from_attributes": True}


class BotAchievementResponse(BaseModel):
    id:             int
    bot_id:         int
    bot_name:       str
    achievement_id: int
    slug:           str
    name:           str
    description:    str
    tier:           str
    earned_at:      Optional[datetime] = None
    metadata_:      Optional[dict] = None

    model_config = {"from_attributes": True}
