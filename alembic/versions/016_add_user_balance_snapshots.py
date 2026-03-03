"""Add user_balance_snapshots table.

Revision ID: 016
Revises: 015
Create Date: 2026-03-03

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "016"
down_revision: Union[str, None] = "015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    tables = inspector.get_table_names()

    if "user_balance_snapshots" not in tables:
        op.create_table(
            "user_balance_snapshots",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False, index=True),
            sa.Column("balance", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("bot_balance", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("available", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("session_id", sa.String(50), nullable=True),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=True),
        )
        op.create_index(
            "ix_user_balance_snapshots_user_recorded",
            "user_balance_snapshots",
            ["user_id", "recorded_at"],
        )


def downgrade() -> None:
    op.drop_index("ix_user_balance_snapshots_user_recorded", table_name="user_balance_snapshots")
    op.drop_table("user_balance_snapshots")
