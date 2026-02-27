import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://polyarena:polyarena@localhost:5432/polyarena",
)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=300,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_timescaledb() -> None:
    """Enable TimescaleDB extension if available (safe to call on plain PostgreSQL)."""
    from sqlalchemy import text

    log = logging.getLogger(__name__)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            log.info("TimescaleDB extension enabled")
    except Exception as exc:
        log.warning(
            "TimescaleDB extension not available (running on plain PostgreSQL): %s", exc
        )


def run_alembic_upgrade() -> None:
    """Run Alembic migrations to head."""
    from alembic.config import Config
    from alembic import command

    log = logging.getLogger(__name__)
    alembic_cfg = Config(os.path.join(os.path.dirname(__file__), "alembic.ini"))
    alembic_cfg.set_main_option("sqlalchemy.url", SQLALCHEMY_DATABASE_URL)
    command.upgrade(alembic_cfg, "head")
    log.info("Alembic migrations applied (head)")
