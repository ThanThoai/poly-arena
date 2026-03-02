import secrets
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import BalanceHistory, BinaryOption, Bot, BOResult, User, UserBalanceHistory
from schemas import (
    BalanceHistoryResponse, BotCreate, BotPnlResponse, BotPublic, BotRename,
    BotResponse, UserBalanceHistoryResponse, UserPnlResponse,
)

router = APIRouter()


@router.post("/", response_model=BotResponse, status_code=201)
def create_bot(
    payload: BotCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    name = payload.bot_name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="bot_name cannot be empty")

    if db.query(Bot).filter(Bot.bot_name == name).first():
        raise HTTPException(status_code=409, detail=f"Bot '{name}' already exists")

    # Validate balance pool
    allocated = sum(
        row[0] or 0
        for row in db.query(Bot.initial_balance).filter(Bot.user_id == user.id).all()
    )
    available = (user.initial_balance or 0) - allocated
    if payload.initial_balance > available:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient balance pool. Available: ${available:,.2f}, Requested: ${payload.initial_balance:,.2f}",
        )

    bot = Bot(
        bot_name=name,
        api_key=secrets.token_urlsafe(32),
        initial_balance=payload.initial_balance,
        balance=payload.initial_balance,
        user_id=user.id,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


@router.get("/", response_model=List[BotPublic])
def list_bots(db: Session = Depends(get_db)):
    bots = db.query(Bot).order_by(Bot.created_at.desc()).all()
    user_ids = {b.user_id for b in bots if b.user_id}
    user_map = {}
    user_balance_map = {}
    if user_ids:
        users = db.query(User).filter(User.id.in_(user_ids)).all()
        user_map = {u.id: u.username for u in users}
        user_balance_map = {u.id: u.initial_balance or 0 for u in users}
    for b in bots:
        b.owner_name = user_map.get(b.user_id)  # type: ignore[attr-defined]
        b.user_initial_balance = user_balance_map.get(b.user_id)  # type: ignore[attr-defined]
    return bots


@router.get("/my", response_model=List[BotResponse])
def list_my_bots(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List only bots owned by the authenticated user."""
    return db.query(Bot).filter(Bot.user_id == user.id).order_by(Bot.created_at.desc()).all()


@router.patch("/{bot_id}/rename", response_model=BotPublic)
def rename_bot(
    bot_id: int,
    payload: BotRename,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    if bot.user_id != user.id:
        raise HTTPException(status_code=403, detail="Not your bot")

    # api_key check is optional — skip if not provided (owner already verified via JWT)
    if payload.api_key and bot.api_key != payload.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

    new_name = payload.new_bot_name.strip()
    if not new_name:
        raise HTTPException(status_code=422, detail="new_bot_name cannot be empty")

    if new_name == bot.bot_name:
        raise HTTPException(status_code=422, detail="New name must differ from current name")

    if db.query(Bot).filter(Bot.bot_name == new_name).first():
        raise HTTPException(status_code=409, detail=f"Bot '{new_name}' already exists")

    old_name = bot.bot_name
    bot.bot_name = new_name

    db.query(BalanceHistory).filter(BalanceHistory.bot_name == old_name).update(
        {BalanceHistory.bot_name: new_name}, synchronize_session=False
    )
    db.query(BinaryOption).filter(BinaryOption.bot_name == old_name).update(
        {BinaryOption.bot_name: new_name}, synchronize_session=False
    )

    db.commit()
    db.refresh(bot)
    return bot


@router.get("/balance-history", response_model=List[BalanceHistoryResponse])
def get_balance_history(
    bot_name: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    bots_q = db.query(Bot)
    if bot_name:
        bots_q = bots_q.filter(Bot.bot_name == bot_name)
    bots = bots_q.all()

    seed_records = [
        BalanceHistory(
            id=0,
            bot_name=b.bot_name,
            balance=b.initial_balance,
            trade_id=None,
            recorded_at=b.created_at,
        )
        for b in bots
    ]

    q = db.query(BalanceHistory)
    if bot_name:
        q = q.filter(BalanceHistory.bot_name == bot_name)
    history = q.order_by(BalanceHistory.recorded_at.asc()).limit(limit).all()

    return sorted(seed_records + history, key=lambda r: (r.recorded_at or ""))


@router.get("/user-balance-history", response_model=List[UserBalanceHistoryResponse])
def get_user_balance_history(
    user: User = Depends(get_current_user),
    limit: int = Query(500, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    """Get balance history for the authenticated user based on Realized P&L."""
    # Seed record: user's initial balance at account creation
    seed = UserBalanceHistory(
        id=0,
        user_id=user.id,
        balance=user.initial_balance or 0,
        trade_id=None,
        recorded_at=user.created_at,
    )

    history = (
        db.query(UserBalanceHistory)
        .filter(UserBalanceHistory.user_id == user.id)
        .order_by(UserBalanceHistory.recorded_at.asc())
        .limit(limit)
        .all()
    )

    return sorted([seed] + history, key=lambda r: (r.recorded_at or ""))


# ── P&L helpers ──────────────────────────────────────────────────────────────

def _bot_pnl(bot: Bot, trades: list) -> BotPnlResponse:
    """Build a BotPnlResponse from a Bot and its trades."""
    wins = sum(1 for t in trades if t.result == BOResult.WIN)
    losses = sum(1 for t in trades if t.result == BOResult.LOSS)
    pending = sum(1 for t in trades if t.result == BOResult.PENDING)
    decided = wins + losses
    realized_pnl = round(sum(t.profit or 0 for t in trades if t.result in (BOResult.WIN, BOResult.LOSS)), 8)
    initial = bot.initial_balance or 0
    return BotPnlResponse(
        bot_name=bot.bot_name,
        initial_balance=initial,
        current_balance=bot.balance or 0,
        realized_pnl=realized_pnl,
        realized_pnl_pct=round(realized_pnl / initial * 100, 2) if initial else 0.0,
        wins=wins,
        losses=losses,
        pending=pending,
        total_trades=len(trades),
        win_rate=round(wins / decided * 100, 2) if decided else 0.0,
        avg_profit_per_trade=round(realized_pnl / decided, 8) if decided else 0.0,
    )


# ── P&L endpoints ───────────────────────────────────────────────────────────

@router.get("/pnl", response_model=List[BotPnlResponse])
def bots_pnl(
    bot_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """P&L for all bots (public). Optional bot_name filter."""
    bot_q = db.query(Bot)
    if bot_name:
        bot_q = bot_q.filter(Bot.bot_name == bot_name)
    bots = bot_q.all()

    bot_names = [b.bot_name for b in bots]
    if not bot_names:
        return []

    trades = db.query(BinaryOption).filter(BinaryOption.bot_name.in_(bot_names)).all()
    trades_by_bot: dict[str, list] = {name: [] for name in bot_names}
    for t in trades:
        trades_by_bot.setdefault(t.bot_name, []).append(t)

    return sorted(
        [_bot_pnl(b, trades_by_bot.get(b.bot_name, [])) for b in bots],
        key=lambda x: x.realized_pnl,
        reverse=True,
    )


@router.get("/user-pnl", response_model=UserPnlResponse)
def user_pnl(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """P&L for the authenticated user, aggregated across all bots."""
    bots = db.query(Bot).filter(Bot.user_id == user.id).all()
    bot_names = [b.bot_name for b in bots]

    trades = (
        db.query(BinaryOption).filter(BinaryOption.bot_name.in_(bot_names)).all()
        if bot_names else []
    )
    trades_by_bot: dict[str, list] = {name: [] for name in bot_names}
    for t in trades:
        trades_by_bot.setdefault(t.bot_name, []).append(t)

    bot_pnls = [_bot_pnl(b, trades_by_bot.get(b.bot_name, [])) for b in bots]

    # Aggregate
    wins = sum(bp.wins for bp in bot_pnls)
    losses = sum(bp.losses for bp in bot_pnls)
    pending = sum(bp.pending for bp in bot_pnls)
    total_trades = sum(bp.total_trades for bp in bot_pnls)
    realized_pnl = round(sum(bp.realized_pnl for bp in bot_pnls), 8)
    decided = wins + losses

    allocated = sum(b.initial_balance or 0 for b in bots)
    available = (user.initial_balance or 0) - allocated
    current_balance = round(sum(b.balance or 0 for b in bots) + available, 8)
    initial = user.initial_balance or 0

    return UserPnlResponse(
        user_id=user.id,
        username=user.username,
        initial_balance=initial,
        allocated_balance=allocated,
        available_balance=available,
        current_balance=current_balance,
        realized_pnl=realized_pnl,
        realized_pnl_pct=round(realized_pnl / initial * 100, 2) if initial else 0.0,
        wins=wins,
        losses=losses,
        pending=pending,
        total_trades=total_trades,
        win_rate=round(wins / decided * 100, 2) if decided else 0.0,
        avg_profit_per_trade=round(realized_pnl / decided, 8) if decided else 0.0,
        bots=bot_pnls,
    )
