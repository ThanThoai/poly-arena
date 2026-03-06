import secrets
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from sqlalchemy import func as sa_func

from database import get_db
from models import BalanceHistory, BinaryOption, Bot, BOResult, User, UserBalanceHistory, UserBalanceSnapshot
from models_futures import FuturesPosition, FuturesPositionStatus, FuturesOrder, FuturesOrderStatus
from schemas import (
    BalanceHistoryResponse, BOResponse, BotBalanceAdjust, BotCreate, BotPerformanceResponse,
    BotPnlResponse, BotPublic, BotRename, BotResponse,
    UserBalanceHistoryResponse, UserBalanceSnapshotResponse, UserPnlResponse,
)

router = APIRouter()


@router.post("", response_model=BotResponse, status_code=201)
def create_bot(
    payload: BotCreate,
    db: Session = Depends(get_db),
):
    name = payload.bot_name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="bot_name cannot be empty")

    existing = db.query(Bot).filter(Bot.bot_name == name).first()
    if existing:
        if payload.get_or_create:
            return existing
        raise HTTPException(status_code=409, detail=f"Bot '{name}' already exists")

    bot = Bot(
        bot_name=name,
        api_key=secrets.token_urlsafe(32),
        initial_balance=payload.initial_balance,
        balance=payload.initial_balance,
        user_id=None,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


@router.get("", response_model=List[BotPublic])
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


@router.patch("/{bot_id}/rename", response_model=BotPublic)
def rename_bot(
    bot_id: int,
    payload: BotRename,
    db: Session = Depends(get_db),
):
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    if not payload.api_key or bot.api_key != payload.api_key:
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


@router.delete("/{bot_id}", status_code=200)
def delete_bot(
    bot_id: int,
    api_key: str = Query(..., description="Bot API key for verification"),
    db: Session = Depends(get_db),
):
    """Delete (deactivate) a bot."""
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    if bot.api_key != api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if not bot.is_active:
        raise HTTPException(status_code=400, detail="Bot is already deactivated")

    # Check for pending trades
    pending_count = (
        db.query(BinaryOption)
        .filter(BinaryOption.bot_name == bot.bot_name, BinaryOption.result == BOResult.PENDING)
        .count()
    )
    if pending_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Bot has {pending_count} pending trade(s). Cancel or wait for settlement first.",
        )

    bot.is_active = False
    bot.status = "DELETED"
    bot.balance = 0

    db.commit()
    return {
        "detail": f"Bot '{bot.bot_name}' deactivated.",
    }


@router.put("/{bot_id}/balance", response_model=BotResponse)
def adjust_bot_balance(
    bot_id: int,
    payload: BotBalanceAdjust,
    db: Session = Depends(get_db),
):
    """Adjust a bot's balance (initial_balance and current balance)."""
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    if not bot.is_active:
        raise HTTPException(status_code=400, detail="Bot is deactivated")

    new_balance = payload.balance
    old_initial = bot.initial_balance or 0
    delta = new_balance - old_initial

    # Adjust both initial_balance and current balance by the same delta
    bot.initial_balance = new_balance
    bot.balance = round((bot.balance or 0) + delta, 8)

    db.add(BalanceHistory(
        bot_name=bot.bot_name,
        balance=bot.balance,
        trade_id=None,
    ))

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

    now = datetime.now(timezone.utc)

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

    # Current bot equity (cash + locked positions) as the latest data point
    # b.balance = initial + realized_pnl - open_locked - net_fees (cash only)
    # To match BalanceHistory which stores equity, add back open_locked.
    bot_locked: dict[str, float] = {}
    for b in bots:
        # Binary options: amount locked in PENDING orders
        bo_locked = (
            db.query(sa_func.coalesce(sa_func.sum(BinaryOption.amount), 0.0))
            .filter(
                BinaryOption.bot_name == b.bot_name,
                BinaryOption.result == BOResult.PENDING,
            )
            .scalar()
        ) or 0.0

        # Futures: margin locked in OPEN positions (already deducted from bot.balance)
        fut_pos_margin = (
            db.query(sa_func.coalesce(sa_func.sum(FuturesPosition.margin), 0.0))
            .filter(
                FuturesPosition.bot_name == b.bot_name,
                FuturesPosition.status == FuturesPositionStatus.OPEN,
            )
            .scalar()
        ) or 0.0

        # Futures: margin reserved by PENDING limit orders
        fut_ord_margin = (
            db.query(sa_func.coalesce(
                sa_func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage), 0.0
            ))
            .filter(
                FuturesOrder.bot_name == b.bot_name,
                FuturesOrder.status == FuturesOrderStatus.PENDING,
            )
            .scalar()
        ) or 0.0

        bot_locked[b.bot_name] = bo_locked + fut_pos_margin + fut_ord_margin

    current_records = [
        BalanceHistory(
            id=0,
            bot_name=b.bot_name,
            balance=round((b.balance or 0) + bot_locked.get(b.bot_name, 0), 8),
            trade_id=None,
            recorded_at=now,
        )
        for b in bots
    ]

    q = db.query(BalanceHistory)
    if bot_name:
        q = q.filter(BalanceHistory.bot_name == bot_name)
    history = q.order_by(BalanceHistory.recorded_at.asc()).limit(limit).all()

    return sorted(seed_records + history + current_records, key=lambda r: (r.recorded_at or ""))


@router.get("/user-balance-history", response_model=List[UserBalanceHistoryResponse])
def get_user_balance_history(
    user_id: Optional[int] = Query(None),
    limit: int = Query(500, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    """Get balance history (public). Optional user_id filter."""
    if user_id is not None:
        target_users = db.query(User).filter(User.id == user_id).all()
    else:
        target_users = db.query(User).filter(User.is_active == True).all()

    seeds = []
    for u in target_users:
        seeds.append(UserBalanceHistory(
            id=0,
            user_id=u.id,
            balance=u.initial_balance or 0,
            trade_id=None,
            recorded_at=u.created_at,
        ))

    q = db.query(UserBalanceHistory)
    if user_id is not None:
        q = q.filter(UserBalanceHistory.user_id == user_id)
    history = q.order_by(UserBalanceHistory.recorded_at.asc()).limit(limit).all()

    return sorted(seeds + history, key=lambda r: (r.recorded_at or ""))


@router.get("/user-balance-snapshots", response_model=List[UserBalanceSnapshotResponse])
def get_user_balance_snapshots(
    user_id: Optional[int] = Query(None),
    limit: int = Query(500, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    """Get periodic balance snapshots for all users (public). Optional user_id filter.

    Includes seed records (initial balance at account creation) for each user.
    """
    # Determine which users to include
    if user_id is not None:
        target_users = db.query(User).filter(User.id == user_id).all()
    else:
        target_users = db.query(User).filter(User.is_active == True).all()

    # Build seed records: initial balance at account creation
    seeds = []
    for u in target_users:
        init_bal = u.initial_balance or 0
        seeds.append(UserBalanceSnapshot(
            id=0,
            user_id=u.id,
            recorded_at=u.created_at,
            session_id=None,
            candle_open=None,
            unallocated=init_bal,
            bot_cash=0,
            bo_locked=0,
            futures_locked=0,
            equity=init_bal,
            bo_unrealized_pnl=0,
            futures_unrealized_pnl=0,
            unrealized_pnl=0,
            net_liquidation=init_bal,
            cumulative_realized_pnl=0,
            session_realized_pnl=0,
            snapshot_delta=None,
            active_bot_count=0,
            open_bo_count=0,
            open_futures_count=0,
        ))

    q = db.query(UserBalanceSnapshot)
    if user_id is not None:
        q = q.filter(UserBalanceSnapshot.user_id == user_id)
    history = q.order_by(UserBalanceSnapshot.recorded_at.asc()).limit(limit).all()

    return sorted(seeds + history, key=lambda r: (r.recorded_at or ""))


# ── Pause / Resume ───────────────────────────────────────────────────────────

@router.patch("/{bot_id}/pause", response_model=BotResponse)
def pause_bot(
    bot_id: int,
    api_key: str = Query(..., description="Bot API key for verification"),
    db: Session = Depends(get_db),
):
    """Pause an ACTIVE bot. Bot must have no pending trades."""
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    if bot.api_key != api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if getattr(bot, "status", "ACTIVE") != "ACTIVE":
        raise HTTPException(status_code=400, detail=f"Bot is {bot.status}. Only ACTIVE bots can be paused.")

    pending_count = (
        db.query(BinaryOption)
        .filter(BinaryOption.bot_name == bot.bot_name, BinaryOption.result == BOResult.PENDING)
        .count()
    )
    if pending_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Bot has {pending_count} pending trade(s). Cancel or wait for settlement first.",
        )

    bot.status = "PAUSED"
    db.commit()
    db.refresh(bot)
    return bot


@router.patch("/{bot_id}/resume", response_model=BotResponse)
def resume_bot(
    bot_id: int,
    api_key: str = Query(..., description="Bot API key for verification"),
    db: Session = Depends(get_db),
):
    """Resume a PAUSED bot. Bot must have balance > 0."""
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    if bot.api_key != api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if getattr(bot, "status", "ACTIVE") != "PAUSED":
        raise HTTPException(status_code=400, detail=f"Bot is {bot.status}. Only PAUSED bots can be resumed.")
    if (bot.balance or 0) <= 0:
        raise HTTPException(status_code=400, detail="Bot has zero balance. Top up before resuming.")

    bot.status = "ACTIVE"
    db.commit()
    db.refresh(bot)
    return bot


# ── Equity Curve & Performance ───────────────────────────────────────────────

@router.get("/equity-curve")
def equity_curve(
    db: Session = Depends(get_db),
):
    """Return aggregate balance history as an equity curve for charting."""
    rows = (
        db.query(UserBalanceHistory)
        .order_by(UserBalanceHistory.recorded_at.asc())
        .all()
    )
    return [{"t": r.recorded_at.isoformat() if r.recorded_at else None, "v": r.balance} for r in rows]


@router.get("/{bot_id}/performance", response_model=BotPerformanceResponse)
def bot_performance(
    bot_id: int,
    db: Session = Depends(get_db),
):
    """Consolidated performance view for a single bot."""
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    trades = db.query(BinaryOption).filter(BinaryOption.bot_name == bot.bot_name).all()
    wins = sum(1 for t in trades if t.result == BOResult.WIN)
    losses = sum(1 for t in trades if t.result == BOResult.LOSS)
    pending = sum(1 for t in trades if t.result == BOResult.PENDING)
    decided = wins + losses
    realized_pnl = round(sum(t.profit or 0 for t in trades if t.result in (BOResult.WIN, BOResult.LOSS)), 8)
    initial = bot.initial_balance or 0

    # Recent settled trades (last 20)
    settled = [t for t in trades if t.result in (BOResult.WIN, BOResult.LOSS)]
    settled.sort(key=lambda t: t.settlement_at or t.updated_at or t.created_at or "", reverse=True)
    recent = settled[:20]

    # Balance history for charting
    history = (
        db.query(BalanceHistory)
        .filter(BalanceHistory.bot_name == bot.bot_name)
        .order_by(BalanceHistory.recorded_at.asc())
        .all()
    )

    locked = _bot_locked_margin(db, bot.bot_name)
    return BotPerformanceResponse(
        bot_name=bot.bot_name,
        status=getattr(bot, "status", "ACTIVE") or "ACTIVE",
        initial_balance=initial,
        current_balance=round((bot.balance or 0) + locked, 8),
        realized_pnl=realized_pnl,
        realized_pnl_pct=round(realized_pnl / initial * 100, 2) if initial else 0.0,
        wins=wins,
        losses=losses,
        pending=pending,
        win_rate=round(wins / decided * 100, 2) if decided else 0.0,
        recent_trades=recent,
        balance_history=history,
    )


# ── P&L helpers ──────────────────────────────────────────────────────────────

def _bot_locked_margin(db: Session, bot_name: str) -> float:
    """Sum of margin locked in open futures positions + pending limit orders."""
    fut_pos_margin = (
        db.query(sa_func.coalesce(sa_func.sum(FuturesPosition.margin), 0.0))
        .filter(FuturesPosition.bot_name == bot_name, FuturesPosition.status == FuturesPositionStatus.OPEN)
        .scalar()
    ) or 0.0
    fut_ord_margin = (
        db.query(sa_func.coalesce(
            sa_func.sum(FuturesOrder.size * FuturesOrder.limit_price / FuturesOrder.leverage), 0.0
        ))
        .filter(FuturesOrder.bot_name == bot_name, FuturesOrder.status == FuturesOrderStatus.PENDING)
        .scalar()
    ) or 0.0
    bo_locked = (
        db.query(sa_func.coalesce(sa_func.sum(BinaryOption.amount), 0.0))
        .filter(BinaryOption.bot_name == bot_name, BinaryOption.result == BOResult.PENDING)
        .scalar()
    ) or 0.0
    return bo_locked + fut_pos_margin + fut_ord_margin


def _bot_pnl(db: Session, bot: Bot, trades: list) -> BotPnlResponse:
    """Build a BotPnlResponse from a Bot and its trades."""
    wins = sum(1 for t in trades if t.result == BOResult.WIN)
    losses = sum(1 for t in trades if t.result == BOResult.LOSS)
    pending = sum(1 for t in trades if t.result == BOResult.PENDING)
    decided = wins + losses
    realized_pnl = round(sum(t.profit or 0 for t in trades if t.result in (BOResult.WIN, BOResult.LOSS)), 8)
    total_fees = round(sum(t.entry_fee or 0 for t in trades), 8)
    initial = bot.initial_balance or 0
    locked = _bot_locked_margin(db, bot.bot_name)
    return BotPnlResponse(
        bot_name=bot.bot_name,
        status=getattr(bot, "status", "ACTIVE") or "ACTIVE",
        initial_balance=initial,
        current_balance=round((bot.balance or 0) + locked, 8),
        realized_pnl=realized_pnl,
        realized_pnl_pct=round(realized_pnl / initial * 100, 2) if initial else 0.0,
        wins=wins,
        losses=losses,
        pending=pending,
        total_trades=len(trades),
        win_rate=round(wins / decided * 100, 2) if decided else 0.0,
        avg_profit_per_trade=round(realized_pnl / decided, 8) if decided else 0.0,
        total_fees=total_fees,
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
        [_bot_pnl(db, b, trades_by_bot.get(b.bot_name, [])) for b in bots],
        key=lambda x: x.realized_pnl,
        reverse=True,
    )


def _user_pnl(db: Session, user: User, bots: list[Bot], trades_by_bot: dict[str, list]) -> UserPnlResponse:
    """Build UserPnlResponse for a single user.

    Only ACTIVE bots contribute to realized_pnl — deleted bots' P&L has already
    been absorbed into user.initial_balance (see delete_bot), so including their
    trades here would double-count.
    """
    active_bots = [b for b in bots if getattr(b, 'is_active', True)]
    active_pnls = [_bot_pnl(db, b, trades_by_bot.get(b.bot_name, [])) for b in active_bots]

    wins = sum(bp.wins for bp in active_pnls)
    losses = sum(bp.losses for bp in active_pnls)
    pending = sum(bp.pending for bp in active_pnls)
    total_trades = sum(bp.total_trades for bp in active_pnls)
    realized_pnl = round(sum(bp.realized_pnl for bp in active_pnls), 8)
    total_fees = round(sum(bp.total_fees for bp in active_pnls), 8)
    decided = wins + losses

    allocated = sum(b.initial_balance or 0 for b in active_bots)
    available = (user.initial_balance or 0) - allocated
    bot_equity = sum(bp.current_balance for bp in active_pnls)
    current_balance = round(bot_equity + available, 8)
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
        total_fees=total_fees,
        bots=active_pnls,
    )


@router.get("/user-pnl", response_model=List[UserPnlResponse])
def user_pnl(
    user_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """P&L for users (public). Optional user_id filter."""
    if user_id is not None:
        users = db.query(User).filter(User.id == user_id).all()
    else:
        users = db.query(User).filter(User.is_active == True).all()

    all_bots = db.query(Bot).all()
    bot_names = [b.bot_name for b in all_bots]

    all_trades = (
        db.query(BinaryOption).filter(BinaryOption.bot_name.in_(bot_names)).all()
        if bot_names else []
    )
    trades_by_bot: dict[str, list] = {}
    for t in all_trades:
        trades_by_bot.setdefault(t.bot_name, []).append(t)

    bots_by_user: dict[int, list[Bot]] = {}
    for b in all_bots:
        if b.user_id:
            bots_by_user.setdefault(b.user_id, []).append(b)

    result = []
    for u in users:
        user_bots = bots_by_user.get(u.id, [])
        if not user_bots:
            continue
        result.append(_user_pnl(db, u, user_bots, trades_by_bot))

    return sorted(result, key=lambda x: x.realized_pnl, reverse=True)


@router.get("/user-pnl-all", response_model=List[UserPnlResponse])
def user_pnl_all(db: Session = Depends(get_db)):
    """P&L for all users (public, no auth required)."""
    users = db.query(User).filter(User.is_active == True).all()
    all_bots = db.query(Bot).all()
    bot_names = [b.bot_name for b in all_bots]

    all_trades = (
        db.query(BinaryOption).filter(BinaryOption.bot_name.in_(bot_names)).all()
        if bot_names else []
    )
    trades_by_bot: dict[str, list] = {}
    for t in all_trades:
        trades_by_bot.setdefault(t.bot_name, []).append(t)

    # Group bots by user
    bots_by_user: dict[int, list[Bot]] = {}
    for b in all_bots:
        if b.user_id:
            bots_by_user.setdefault(b.user_id, []).append(b)

    result = []
    for u in users:
        user_bots = bots_by_user.get(u.id, [])
        if not user_bots:
            continue
        result.append(_user_pnl(db, u, user_bots, trades_by_bot))

    return sorted(result, key=lambda x: x.realized_pnl, reverse=True)
