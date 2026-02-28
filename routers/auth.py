from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from auth import create_access_token, get_current_user, hash_password, verify_password
from database import get_db
from models import Bot, User, UserSettings
from schemas import TokenResponse, UserLogin, UserRegister, UserResponse, UserSettingsUpdate, UserSettingsResponse

router = APIRouter()


@router.post("/register", response_model=TokenResponse, status_code=201)
def register(payload: UserRegister, db: Session = Depends(get_db)):
    email = payload.email or f"{payload.username}@polyarena.local"

    existing = db.query(User).filter(
        or_(User.username == payload.username, User.email == email)
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="username already taken")

    user = User(
        username=payload.username,
        email=email,
        hashed_password=hash_password(payload.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return TokenResponse(access_token=create_access_token(user.id))


@router.post("/login", response_model=TokenResponse)
def login(payload: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(
        or_(User.username == payload.username, User.email == payload.username)
    ).first()

    if user is None or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    return TokenResponse(access_token=create_access_token(user.id))


@router.get("/settings", response_model=UserSettingsResponse)
def get_settings(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    row = db.query(UserSettings).filter(UserSettings.user_id == user.id).first()
    if row is None:
        row = UserSettings(user_id=user.id, settings={})
        db.add(row)
        db.commit()
        db.refresh(row)
    return UserSettingsResponse(settings=row.settings or {})


@router.put("/settings", response_model=UserSettingsResponse)
def update_settings(
    payload: UserSettingsUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.query(UserSettings).filter(UserSettings.user_id == user.id).first()
    if row is None:
        row = UserSettings(user_id=user.id, settings=payload.settings)
        db.add(row)
    else:
        row.settings = payload.settings
    db.commit()
    db.refresh(row)
    return UserSettingsResponse(settings=row.settings)


@router.get("/me", response_model=UserResponse)
def me(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    allocated = (
        db.query(Bot)
        .filter(Bot.user_id == user.id)
        .with_entities(Bot.initial_balance)
        .all()
    )
    allocated_balance = sum(row[0] or 0 for row in allocated)
    available_balance = (user.initial_balance or 0) - allocated_balance

    return UserResponse(
        id=user.id,
        username=user.username,
        email=user.email,
        initial_balance=user.initial_balance or 0,
        allocated_balance=allocated_balance,
        available_balance=available_balance,
    )
