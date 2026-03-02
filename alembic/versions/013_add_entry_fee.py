"""Add entry_fee column to binary_options.

Revision ID: 013
Revises: 012
Create Date: 2026-03-02

Idempotent: safe to run on any database.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "013"
down_revision: Union[str, None] = "012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    if "binary_options" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("binary_options")}
        if "entry_fee" not in existing_cols:
            op.add_column(
                "binary_options",
                sa.Column("entry_fee", sa.Numeric(18, 8), nullable=True, server_default="0"),
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)

    if "binary_options" in inspector.get_table_names():
        existing_cols = {c["name"] for c in inspector.get_columns("binary_options")}
        if "entry_fee" in existing_cols:
            op.drop_column("binary_options", "entry_fee")
