"""Ensure bids/asks columns exist in price_history (catch-up for DBs already at 010).

Revision ID: 011
Revises: 010
Create Date: 2026-03-02

Idempotent: safe to run on any database.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    if "price_history" not in inspector.get_table_names():
        return  # table doesn't exist yet — 010 will create it with all columns

    existing_cols = {c["name"] for c in inspector.get_columns("price_history")}
    if "bids" not in existing_cols:
        op.add_column("price_history", sa.Column("bids", sa.JSON, nullable=True))
    if "asks" not in existing_cols:
        op.add_column("price_history", sa.Column("asks", sa.JSON, nullable=True))


def downgrade() -> None:
    op.drop_column("price_history", "asks")
    op.drop_column("price_history", "bids")
