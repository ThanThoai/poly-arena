"""Add walk_prices JSON column to binary_options.

Revision ID: 004
Revises: 003
Create Date: 2026-02-28

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    columns = [c["name"] for c in inspector.get_columns("binary_options")]

    if "walk_prices" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("walk_prices", sa.JSON, nullable=True),
        )


def downgrade() -> None:
    op.drop_column("binary_options", "walk_prices")
