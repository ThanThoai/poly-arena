"""Add candle_ts column to price_history for session tagging.

Revision ID: 014
Revises: 013
Create Date: 2026-03-02

Idempotent: safe to run on any database.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "014"
down_revision: Union[str, None] = "013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    if "price_history" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("price_history")}
        if "candle_ts" not in existing_cols:
            op.add_column(
                "price_history",
                sa.Column("candle_ts", sa.Integer, nullable=True),
            )
            op.create_index(
                "ix_price_history_candle_ts",
                "price_history",
                ["candle_ts"],
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    if "price_history" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("price_history")}
        if "candle_ts" in existing_cols:
            op.drop_index("ix_price_history_candle_ts", table_name="price_history")
            op.drop_column("price_history", "candle_ts")
