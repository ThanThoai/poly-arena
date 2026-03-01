"""Add session_offset column to binary_options.

Revision ID: 007
Revises: 006
Create Date: 2026-03-01

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    columns = [c["name"] for c in inspector.get_columns("binary_options")]

    if "session_offset" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("session_offset", sa.Integer, server_default="0"),
        )


def downgrade() -> None:
    op.drop_column("binary_options", "session_offset")
