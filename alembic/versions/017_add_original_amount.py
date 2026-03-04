"""Add original_amount column to binary_options.

Revision ID: 017
Revises: 016
Create Date: 2026-03-04

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "017"
down_revision: Union[str, None] = "016"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    columns = [c["name"] for c in inspector.get_columns("binary_options")]

    if "original_amount" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("original_amount", sa.Numeric(18, 8, asdecimal=False), nullable=True),
        )
        # Backfill: set original_amount = amount for existing rows
        op.execute("UPDATE binary_options SET original_amount = amount WHERE original_amount IS NULL")


def downgrade() -> None:
    op.drop_column("binary_options", "original_amount")
