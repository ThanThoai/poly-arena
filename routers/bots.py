import secrets
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
from models import BalanceHistory, BinaryOption, Bot
from schemas import BalanceHistoryResponse, BotCreate, BotPublic, BotRename, BotResponse

router = APIRouter()


@router.post("/", response_model=BotResponse, status_code=201)
def create_bot(payload: BotCreate, db: Session = Depends(get_db)):
    """
    Tạo bot mới và sinh api_key.
    api_key chỉ được trả về một lần duy nhất lúc tạo.
    """
    name = payload.bot_name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="bot_name không được để trống")

    if db.query(Bot).filter(Bot.bot_name == name).first():
        raise HTTPException(
            status_code=409,
            detail=f"Bot '{name}' đã tồn tại",
        )

    bot = Bot(bot_name=name, api_key=secrets.token_urlsafe(32))
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


@router.get("/", response_model=List[BotPublic])
def list_bots(db: Session = Depends(get_db)):
    """Danh sách bots (không bao gồm api_key)."""
    return db.query(Bot).order_by(Bot.created_at.desc()).all()


@router.patch("/{bot_id}/rename", response_model=BotPublic)
def rename_bot(bot_id: int, payload: BotRename, db: Session = Depends(get_db)):
    """
    Rename a bot. Requires the bot's api_key for verification.
    Also updates bot_name in balance_history and binary_options.
    """
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot không tìm thấy")

    if bot.api_key != payload.api_key:
        raise HTTPException(status_code=401, detail="API key không hợp lệ")

    new_name = payload.new_bot_name.strip()
    if not new_name:
        raise HTTPException(status_code=422, detail="new_bot_name không được để trống")

    if new_name == bot.bot_name:
        raise HTTPException(status_code=422, detail="Tên mới phải khác tên hiện tại")

    if db.query(Bot).filter(Bot.bot_name == new_name).first():
        raise HTTPException(status_code=409, detail=f"Bot '{new_name}' đã tồn tại")

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
    limit:    int           = Query(10000, ge=1, le=50000),
    db: Session = Depends(get_db),
):
    """Lịch sử balance của bot(s), sắp xếp theo thời gian tăng dần.
    Luôn bao gồm điểm đầu tiên là initial_balance của mỗi bot.
    """
    bots_q = db.query(Bot)
    if bot_name:
        bots_q = bots_q.filter(Bot.bot_name == bot_name)
    bots = bots_q.all()

    # Tạo record ảo cho initial balance của từng bot
    seed_records = [
        BalanceHistory(
            id          = 0,
            bot_name    = b.bot_name,
            balance     = b.initial_balance,
            trade_id    = None,
            recorded_at = b.created_at,
        )
        for b in bots
    ]

    q = db.query(BalanceHistory)
    if bot_name:
        q = q.filter(BalanceHistory.bot_name == bot_name)
    history = q.order_by(BalanceHistory.recorded_at.asc()).limit(limit).all()

    return sorted(seed_records + history, key=lambda r: (r.recorded_at or ""))


