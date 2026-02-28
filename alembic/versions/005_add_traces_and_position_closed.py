"""Add traces (JSON) and position_closed (Boolean) columns to binary_options.

Revision ID: 005
Revises: 004
Create Date: 2026-02-28

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    columns = [c["name"] for c in inspector.get_columns("binary_options")]

    if "traces" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("traces", sa.JSON, nullable=True),
        )

    if "position_closed" not in columns:
        op.add_column(
            "binary_options",
            sa.Column("position_closed", sa.Boolean, server_default="false", nullable=True),
        )


def downgrade() -> None:
    op.drop_column("binary_options", "position_closed")
    op.drop_column("binary_options", "traces")
