"""Initial schema — all tables and enums.

Revision ID: 001
Revises:
Create Date: 2026-02-27

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Enum definitions matching models.py
_ENUMS = [
    ("bosymbol",    ("BTC", "ETH", "SOL", "XRP")),
    ("botimeframe", ("M5", "M15", "H1")),
    ("boforecast",  ("GREEN", "RED")),
    ("boresult",    ("PENDING", "WIN", "LOSS", "TIE", "CANCELLED")),
]


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing_tables = inspector.get_table_names()

    # ── Create ENUM types (PostgreSQL only, idempotent) ──────────────────
    if bind.dialect.name == "postgresql":
        for type_name, values in _ENUMS:
            values_str = ", ".join(f"'{v}'" for v in values)
            bind.execute(sa.text(f"""
                DO $$ BEGIN
                    CREATE TYPE {type_name} AS ENUM ({values_str});
                EXCEPTION
                    WHEN duplicate_object THEN NULL;
                END $$
            """))

    # ── bots ─────────────────────────────────────────────────────────────
    if "bots" not in existing_tables:
        op.create_table(
            "bots",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("bot_name", sa.String(100), unique=True, nullable=False, index=True),
            sa.Column("api_key", sa.String(64), unique=True, nullable=False, index=True),
            sa.Column("is_active", sa.Boolean, server_default=sa.text("true")),
            sa.Column("initial_balance", sa.Numeric(18, 8), server_default=sa.text("10000.0")),
            sa.Column("balance", sa.Numeric(18, 8), server_default=sa.text("10000.0")),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )

    # ── balance_history ──────────────────────────────────────────────────
    if "balance_history" not in existing_tables:
        op.create_table(
            "balance_history",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("bot_name", sa.String(100), nullable=False, index=True),
            sa.Column("balance", sa.Numeric(18, 8), nullable=False),
            sa.Column("trade_id", sa.Integer, nullable=True),
            sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )

    # ── binary_options ───────────────────────────────────────────────────
    if "binary_options" not in existing_tables:
        op.create_table(
            "binary_options",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("bot_name", sa.String(100), nullable=False, index=True),
            sa.Column("symbol", sa.String, nullable=False, index=True),
            sa.Column("timeframe", sa.String, nullable=False),
            sa.Column("forecast", sa.String, nullable=False),
            sa.Column("amount", sa.Numeric(18, 8), nullable=False),
            sa.Column("result", sa.String, server_default=sa.text("'PENDING'")),
            sa.Column("profit", sa.Numeric(18, 8), nullable=True),
            sa.Column("price_open", sa.Numeric(18, 8), nullable=True),
            sa.Column("price_close", sa.Numeric(18, 8), nullable=True),
            sa.Column("avg_price", sa.Numeric(18, 8), nullable=True),
            sa.Column("num_shares", sa.Numeric(18, 8), nullable=True),
            sa.Column("reason", sa.String, nullable=True),
            sa.Column("order_received_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("ask_fetched_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("settlement_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("limit_price", sa.Numeric(18, 8), nullable=True),
            sa.Column("tp_price", sa.Numeric(18, 8), nullable=True),
            sa.Column("sl_price", sa.Numeric(18, 8), nullable=True),
            sa.Column("exit_price", sa.Numeric(18, 8), nullable=True),
            sa.Column("exit_trigger", sa.String(20), nullable=True),
            sa.Column("exit_filled", sa.Numeric(18, 8), nullable=True),
            sa.Column("exit_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("me_order_id", sa.String(64), nullable=True),
            sa.Column("me_order_status", sa.String(20), nullable=True),
            sa.Column("ttl", sa.Integer, nullable=True),
        )


def downgrade() -> None:
    op.drop_table("binary_options")
    op.drop_table("balance_history")
    op.drop_table("bots")

    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        for type_name, _ in reversed(_ENUMS):
            bind.execute(sa.text(f"DROP TYPE IF EXISTS {type_name}"))
