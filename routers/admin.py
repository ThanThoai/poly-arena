from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from auth import get_admin_user, hash_password
from config.timing import TF_SECONDS
from database import get_db
from models import (
    BalanceHistory,
    BinaryOption,
    Bot,
    BotAchievement,
    PriceHistory,
    User,
    UserBalanceHistory,
    UserSettings,
)
from schemas import (
    AdminBalanceAdjust,
    AdminCreateAdmin,
    AdminUserResponse,
    BOResponse,
    BotPublic,
    PriceHistoryResponse,
    SessionInfo,
    TimelineEvent,
    TokenResponse,
    TradeInspectResponse,
)
from auth import create_access_token

router = APIRouter()


# ── Users ────────────────────────────────────────────────────────────────────

@router.get("/users", response_model=list[AdminUserResponse])
def list_users(
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    return db.query(User).order_by(User.id).all()


@router.delete("/users/{user_id}")
def delete_user(
    user_id: int,
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Deactivate all bots owned by this user
    bots = db.query(Bot).filter(Bot.user_id == user_id).all()
    for bot in bots:
        bot.is_active = False

    user.is_active = False
    db.commit()
    return {"detail": f"User {user.username} deactivated with {len(bots)} bot(s)"}


@router.put("/users/{user_id}/balance", response_model=AdminUserResponse)
def adjust_user_balance(
    user_id: int,
    payload: AdminBalanceAdjust,
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    user.initial_balance = payload.balance
    db.commit()
    db.refresh(user)
    return user


# ── Bots ─────────────────────────────────────────────────────────────────────

@router.get("/bots", response_model=list[BotPublic])
def list_bots(
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    bots = db.query(Bot).order_by(Bot.id).all()
    results = []
    for bot in bots:
        owner = db.get(User, bot.user_id) if bot.user_id else None
        results.append(BotPublic(
            id=bot.id,
            bot_name=bot.bot_name,
            is_active=bot.is_active,
            initial_balance=bot.initial_balance,
            balance=bot.balance,
            user_id=bot.user_id,
            owner_name=owner.username if owner else None,
            created_at=bot.created_at,
        ))
    return results


@router.delete("/bots/{bot_id}")
def delete_bot(
    bot_id: int,
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    bot = db.get(Bot, bot_id)
    if bot is None:
        raise HTTPException(status_code=404, detail="Bot not found")

    bot.is_active = False
    db.commit()
    return {"detail": f"Bot {bot.bot_name} deactivated"}


@router.delete("/bots/{bot_id}/trades")
def delete_bot_trades(
    bot_id: int,
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    bot = db.get(Bot, bot_id)
    if bot is None:
        raise HTTPException(status_code=404, detail="Bot not found")

    count = db.query(BinaryOption).filter(
        BinaryOption.bot_name == bot.bot_name
    ).delete()

    # Also clean up balance history
    db.query(BalanceHistory).filter(
        BalanceHistory.bot_name == bot.bot_name
    ).delete()

    # Reset bot balance to initial
    bot.balance = bot.initial_balance
    db.commit()
    return {"detail": f"Deleted {count} trade(s) for bot {bot.bot_name}"}


# ── Admin management ─────────────────────────────────────────────────────────

@router.post("/create-admin", response_model=AdminUserResponse, status_code=201)
def create_admin(
    payload: AdminCreateAdmin,
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    email = payload.email or f"{payload.username}@polyarena.local"

    existing = db.query(User).filter(
        or_(User.username == payload.username, User.email == email)
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Username or email already taken")

    user = User(
        username=payload.username,
        email=email,
        hashed_password=hash_password(payload.password),
        is_admin=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


# ── Price History ───────────────────────────────────────────────────────────

@router.get("/price-history", response_model=list[PriceHistoryResponse])
def get_price_history(
    symbol: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    direction: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(1000, ge=1, le=50000),
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    q = db.query(PriceHistory)
    if symbol:
        q = q.filter(PriceHistory.symbol == symbol.upper())
    if timeframe:
        q = q.filter(PriceHistory.timeframe == timeframe.upper())
    if direction:
        q = q.filter(PriceHistory.direction == direction.upper())
    if start_time:
        q = q.filter(PriceHistory.recorded_at >= start_time)
    if end_time:
        q = q.filter(PriceHistory.recorded_at <= end_time)
    return q.order_by(PriceHistory.recorded_at.desc()).limit(limit).all()


# ── Trade Inspector ────────────────────────────────────────────────────────

@router.get("/inspect/{trade_id}", response_model=TradeInspectResponse)
def inspect_trade(
    trade_id: int,
    admin: User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    trade = db.get(BinaryOption, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="Trade not found")

    # Determine session window
    tf_secs = TF_SECONDS.get(trade.timeframe.value if hasattr(trade.timeframe, 'value') else trade.timeframe, 300)
    created_ts = trade.created_at.replace(tzinfo=timezone.utc) if trade.created_at and trade.created_at.tzinfo is None else trade.created_at
    session_start_unix = int(created_ts.timestamp()) // tf_secs * tf_secs if created_ts else 0
    session_end_unix = session_start_unix + tf_secs

    session_start_dt = datetime.fromtimestamp(session_start_unix, tz=timezone.utc)
    session_end_dt = datetime.fromtimestamp(session_end_unix, tz=timezone.utc)

    # Direction from forecast: GREEN=UP, RED=DOWN
    direction = "UP" if (trade.forecast.value if hasattr(trade.forecast, 'value') else trade.forecast) == "GREEN" else "DOWN"

    # Query PriceHistory for the session window
    symbol_val = trade.symbol.value if hasattr(trade.symbol, 'value') else trade.symbol
    tf_val = trade.timeframe.value if hasattr(trade.timeframe, 'value') else trade.timeframe
    price_rows = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.symbol == symbol_val,
            PriceHistory.timeframe == tf_val,
            PriceHistory.direction == direction,
            PriceHistory.recorded_at >= session_start_dt,
            PriceHistory.recorded_at <= session_end_dt,
        )
        .order_by(PriceHistory.recorded_at)
        .all()
    )

    # Build timeline
    timeline: list[dict] = []

    # 1. traces
    for t in (trade.traces or []):
        timeline.append({
            "timestamp": t.get("timestamp", ""),
            "category": "trace",
            "action": t.get("action", t.get("stage", "")),
            "details": t.get("details", ""),
            "data": t.get("data"),
        })

    # 2. walk_prices
    wp = trade.walk_prices or {}
    if wp.get("entry"):
        timeline.append({
            "timestamp": created_ts.isoformat() if created_ts else "",
            "category": "fill_entry",
            "action": "entry_fill",
            "details": f"{len(wp['entry'])} level(s), avg={trade.avg_price}",
            "data": wp["entry"],
        })
    if wp.get("exit"):
        exit_ts = trade.exit_at or trade.settlement_at or trade.updated_at
        if exit_ts and exit_ts.tzinfo is None:
            exit_ts = exit_ts.replace(tzinfo=timezone.utc)
        timeline.append({
            "timestamp": exit_ts.isoformat() if exit_ts else "",
            "category": "fill_exit",
            "action": f"exit_fill ({trade.exit_trigger or 'settlement'})",
            "details": f"{len(wp['exit'])} level(s), exit_price={trade.exit_price}",
            "data": wp["exit"],
        })

    # 3. price_history
    for ph in price_rows:
        timeline.append({
            "timestamp": ph.recorded_at.isoformat() if ph.recorded_at else "",
            "category": "price",
            "action": "price_snapshot",
            "details": f"bid={ph.best_bid} ask={ph.best_ask}",
            "data": {"bids": ph.bids, "asks": ph.asks},
        })

    # Sort chronologically
    timeline.sort(key=lambda x: x["timestamp"])

    return TradeInspectResponse(
        trade=BOResponse.model_validate(trade),
        timeline=[TimelineEvent(**e) for e in timeline],
        session=SessionInfo(
            symbol=symbol_val,
            timeframe=tf_val,
            direction=direction,
            session_start=session_start_unix,
            session_end=session_end_unix,
        ),
    )
