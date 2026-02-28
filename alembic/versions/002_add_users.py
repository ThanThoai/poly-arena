"""Add users table and user_id FK on bots.

Revision ID: 002
Revises: 001
Create Date: 2026-02-28

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing_tables = inspector.get_table_names()

    if "users" not in existing_tables:
        op.create_table(
            "users",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("username", sa.String(100), unique=True, nullable=False, index=True),
            sa.Column("email", sa.String(255), unique=True, nullable=False, index=True),
            sa.Column("hashed_password", sa.String(255), nullable=False),
            sa.Column("initial_balance", sa.Numeric(18, 8), server_default=sa.text("50000.0")),
            sa.Column("is_active", sa.Boolean, server_default=sa.text("true")),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )

    # Add user_id column to bots (nullable for backward compat)
    if "bots" in existing_tables:
        columns = [col["name"] for col in inspector.get_columns("bots")]
        if "user_id" not in columns:
            op.add_column(
                "bots",
                sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=True),
            )


def downgrade() -> None:
    op.drop_column("bots", "user_id")
    op.drop_table("users")
