"""Add bot status enum, pnl snapshot fields to user_balance_history.

Revision ID: 012
Revises: 011
Create Date: 2026-03-02

Idempotent: safe to run on any database.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "012"
down_revision: Union[str, None] = "011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    # 1. Add 'status' column to bots
    if "bots" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("bots")}
        if "status" not in existing_cols:
            op.add_column(
                "bots",
                sa.Column("status", sa.String(10), nullable=False, server_default="ACTIVE"),
            )
            # Backfill: deleted bots
            op.execute("UPDATE bots SET status = 'DELETED' WHERE is_active = false")

    # 2. Add 'bot_id' and 'pnl_amount' to user_balance_history
    if "user_balance_history" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("user_balance_history")}
        if "bot_id" not in existing_cols:
            op.add_column(
                "user_balance_history",
                sa.Column("bot_id", sa.Integer, nullable=True),
            )
        if "pnl_amount" not in existing_cols:
            op.add_column(
                "user_balance_history",
                sa.Column("pnl_amount", sa.Numeric(18, 8), nullable=True),
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    if "user_balance_history" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("user_balance_history")}
        if "pnl_amount" in existing_cols:
            op.drop_column("user_balance_history", "pnl_amount")
        if "bot_id" in existing_cols:
            op.drop_column("user_balance_history", "bot_id")

    if "bots" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("bots")}
        if "status" in existing_cols:
            op.drop_column("bots", "status")
