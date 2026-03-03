"""Add session_id and candle_open columns to binary_options.

Revision ID: 015
Revises: 014
Create Date: 2026-03-03

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "015"
down_revision: Union[str, None] = "014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    columns = [c["name"] for c in inspector.get_columns("binary_options")]

    if "session_id" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("session_id", sa.String(64), nullable=True),
        )
        op.create_index("ix_binary_options_session_id", "binary_options", ["session_id"])

    if "candle_open" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("candle_open", sa.Integer, nullable=True),
        )


def downgrade() -> None:
    op.drop_index("ix_binary_options_session_id", table_name="binary_options")
    op.drop_column("binary_options", "session_id")
    op.drop_column("binary_options", "candle_open")
