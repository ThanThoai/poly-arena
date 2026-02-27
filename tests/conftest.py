"""
Test fixtures for PolyArena.

Test environment uses:
  - SQLite test database (separate from production orders.db)
  - fakeredis when no real Redis available, real Redis in docker-compose
  - FastAPI TestClient on port 8099
"""

import os
import sys

# ── Environment: set DATABASE_URL if not already provided (docker-compose sets it) ──
os.environ.setdefault("DATABASE_URL", "sqlite:///./test_orders.db")
os.environ.setdefault("REDIS_URL", "redis://localhost:6380")

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import redis as _real_redis
import fakeredis
import fakeredis.aioredis

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base

# ── Test database ────────────────────────────────────────────────────────────

TEST_DB_URL = os.environ["DATABASE_URL"]

_test_engine = create_engine(
    TEST_DB_URL,
    connect_args={"check_same_thread": False} if TEST_DB_URL.startswith("sqlite") else {},
)
TestSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=_test_engine,
)

# ── Redis: try real connection, fall back to fakeredis ───────────────────────

_USE_REAL_REDIS = False

try:
    _probe = _real_redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    _probe.ping()
    _probe.close()
    _USE_REAL_REDIS = True
except Exception:
    pass

if _USE_REAL_REDIS:
    # Real Redis — use actual clients from redis_client module
    import services.redis_client as _rc
    _sync_redis = _rc.get_sync_redis()
    _async_redis = _rc.get_async_redis()
else:
    # Fake Redis — patch singletons before any app import
    _fake_server = fakeredis.FakeServer()
    _sync_redis = fakeredis.FakeRedis(server=_fake_server, decode_responses=True)
    _async_redis = fakeredis.aioredis.FakeRedis(server=_fake_server, decode_responses=True)

    import services.redis_client as _rc
    _rc._sync_client = _sync_redis
    _rc._async_client = _async_redis
    _rc.get_sync_redis = lambda: _sync_redis
    _rc.get_async_redis = lambda: _async_redis


# ── Eagerly patch database BEFORE main.py is ever imported ───────────────────
import database as _db_mod
_db_mod.engine = _test_engine
_db_mod.SessionLocal = TestSessionLocal

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
    _sync_redis.flushall()


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
    """Sync Redis client (real or fake depending on environment)."""
    return _sync_redis


@pytest.fixture()
def fake_async_redis():
    """Async Redis client (real or fake depending on environment)."""
    return _async_redis


# ── FastAPI test client ──────────────────────────────────────────────────────

async def _noop_consumer():
    """Replacement for _consume_bracket_exits in tests.

    fakeredis xreadgroup(block=N) returns immediately instead of blocking,
    which causes an infinite tight loop. With real Redis the consumer works
    but is unnecessary during unit tests. Replace with a no-op in both cases.
    """
    import asyncio
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        return


@pytest.fixture()
def client():
    """
    FastAPI TestClient with test DB and Redis.
    Uses port 8099 to avoid conflict with production.
    """
    from unittest.mock import patch as _patch

    with _patch("services.scheduler.start_scheduler"), \
         _patch("services.scheduler.stop_scheduler"), \
         _patch("main._consume_bracket_exits", _noop_consumer), \
         _patch("main._consume_order_cancels", _noop_consumer), \
         _patch("main._consume_order_fills", _noop_consumer):
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
