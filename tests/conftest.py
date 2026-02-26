"""
Test fixtures for PolyArena.

Test environment uses:
  - SQLite test database (separate from production orders.db)
  - fakeredis (no real Redis needed)
  - FastAPI TestClient on ephemeral port
"""

import os
import sys

# ── Environment MUST be set before any app imports ───────────────────────────
os.environ["DATABASE_URL"] = "sqlite:///./test_orders.db"
os.environ["REDIS_URL"] = "redis://localhost:6380"

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import fakeredis
import fakeredis.aioredis

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base

# ── Test database ────────────────────────────────────────────────────────────

TEST_DB_URL = "sqlite:///./test_orders.db"

_test_engine = create_engine(
    TEST_DB_URL, connect_args={"check_same_thread": False},
)
TestSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=_test_engine,
)

# ── Shared fake Redis server (survives across fixtures in same test) ─────────

_fake_server = fakeredis.FakeServer()

# Pre-create module-level clients so they can be patched BEFORE app import
_fake_sync = fakeredis.FakeRedis(server=_fake_server, decode_responses=True)
_fake_async = fakeredis.aioredis.FakeRedis(server=_fake_server, decode_responses=True)

# ── Eagerly patch redis_client BEFORE main.py is ever imported ───────────────
import services.redis_client as _rc
_rc._sync_client = _fake_sync
_rc._async_client = _fake_async
_rc.get_sync_redis = lambda: _fake_sync
_rc.get_async_redis = lambda: _fake_async

# ── Eagerly patch database BEFORE main.py is ever imported ───────────────────
import database as _db_mod
_db_mod.engine = _test_engine
_db_mod.SessionLocal = TestSessionLocal
_original_get_db = _db_mod.get_db

def _get_test_db():
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.close()

_db_mod.get_db = _get_test_db


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def setup_test_db():
    """Create all tables before each test, drop after."""
    Base.metadata.create_all(bind=_test_engine)
    yield
    Base.metadata.drop_all(bind=_test_engine)
    # Flush Redis between tests
    _fake_sync.flushall()


@pytest.fixture()
def db():
    """Provide a clean DB session for a test."""
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture()
def fake_sync_redis():
    """Sync fakeredis client."""
    return _fake_sync


@pytest.fixture()
def fake_async_redis():
    """Async fakeredis client."""
    return _fake_async


# ── FastAPI test client ──────────────────────────────────────────────────────

async def _noop_consumer():
    """Replacement for _consume_bracket_exits in tests — sleeps forever."""
    import asyncio
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        return


@pytest.fixture()
def client():
    """
    FastAPI TestClient with test DB and fake Redis.
    Patches are already applied at module level.

    The bracket exit consumer is replaced with a no-op to avoid
    tight-looping (fakeredis xreadgroup doesn't truly block).
    """
    from unittest.mock import patch as _patch

    with _patch("services.scheduler.start_scheduler"), \
         _patch("services.scheduler.stop_scheduler"), \
         _patch("main._consume_bracket_exits", _noop_consumer):
        from fastapi.testclient import TestClient
        from main import app
        with TestClient(app, base_url="http://testserver:8099") as c:
            yield c


# ── Helper: create a test bot ────────────────────────────────────────────────

@pytest.fixture()
def test_bot(db):
    """Create a bot in the test DB and return (bot_name, api_key)."""
    import secrets
    from models import Bot
    bot = Bot(
        bot_name="test-bot",
        api_key=secrets.token_urlsafe(32),
        is_active=True,
    )
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot.bot_name, bot.api_key
