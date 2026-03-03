"""Add price_history table for recording throttled price snapshots.

Revision ID: 010
Revises: 009
Create Date: 2026-03-02

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    tables = inspector.get_table_names()

    if "price_history" not in tables:
        op.create_table(
            "price_history",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("symbol", sa.String(10), nullable=False, index=True),
            sa.Column("timeframe", sa.String(10), nullable=False, index=True),
            sa.Column("direction", sa.String(10), nullable=False, index=True),
            sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
            sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
            sa.Column(
                "recorded_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
                index=True,
            ),
        )

    # Add missing columns idempotently (handles tables created outside migration)
    if "price_history" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("price_history")}
        if "best_ask" not in existing_cols:
            op.add_column("price_history", sa.Column("best_ask", sa.Numeric(18, 8), nullable=True))
        if "best_bid" not in existing_cols:
            op.add_column("price_history", sa.Column("best_bid", sa.Numeric(18, 8), nullable=True))
        if "bids" not in existing_cols:
            op.add_column("price_history", sa.Column("bids", sa.JSON, nullable=True))
        if "asks" not in existing_cols:
            op.add_column("price_history", sa.Column("asks", sa.JSON, nullable=True))

    # Composite index for filtered queries
    indexes = [idx["name"] for idx in inspector.get_indexes("price_history")] if "price_history" in inspector.get_table_names() else []
    if "ix_price_history_combo_time" not in indexes:
        op.create_index(
            "ix_price_history_combo_time",
            "price_history",
            ["symbol", "timeframe", "direction", "recorded_at"],
        )


def downgrade() -> None:
    op.drop_index("ix_price_history_combo_time", table_name="price_history")
    op.drop_table("price_history")
