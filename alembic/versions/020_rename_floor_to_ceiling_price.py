"""Rename floor_price to ceiling_price in binary_options.

Revision ID: 020
Revises: 019
Create Date: 2026-03-04

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect as sa_inspect

revision: str = "020"
down_revision: Union[str, None] = "019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = "binary_options"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = [c["name"] for c in inspector.get_columns(_TABLE)]

    if "floor_price" in existing and "ceiling_price" not in existing:
        op.alter_column(_TABLE, "floor_price", new_column_name="ceiling_price")
    # If fresh DB already has ceiling_price (from create_all), nothing to do


def downgrade() -> None:
    op.alter_column(_TABLE, "ceiling_price", new_column_name="floor_price")
