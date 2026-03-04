"""Add order_type and floor_price columns to binary_options.

Revision ID: 019
Revises: 018
Create Date: 2026-03-04

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "019"
down_revision: Union[str, None] = "018"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = "binary_options"
_COLUMNS = [
    ("order_type",  sa.String(10)),
    ("floor_price", sa.Numeric(18, 8, asdecimal=False)),
]


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = [c["name"] for c in inspector.get_columns(_TABLE)]

    for col_name, col_type in _COLUMNS:
        if col_name not in existing:
            op.add_column(_TABLE, sa.Column(col_name, col_type, nullable=True))

    # Backfill: set order_type = 'FAK' for existing rows
    if "order_type" not in existing:
        op.execute("UPDATE binary_options SET order_type = 'FAK' WHERE order_type IS NULL")


def downgrade() -> None:
    for col_name, _ in _COLUMNS:
        op.drop_column(_TABLE, col_name)
