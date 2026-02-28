"""Add achievement_definitions and bot_achievements tables.

Revision ID: 006
Revises: 005
Create Date: 2026-02-28

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = inspector.get_table_names()

    if "achievement_definitions" not in existing:
        op.create_table(
            "achievement_definitions",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("slug", sa.String(100), unique=True, nullable=False, index=True),
            sa.Column("name", sa.String(200), nullable=False),
            sa.Column("description", sa.String(500), nullable=False),
            sa.Column("tier", sa.String(20), nullable=False),
            sa.Column("category", sa.String(100), nullable=False),
        )

    if "bot_achievements" not in existing:
        op.create_table(
            "bot_achievements",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("bot_id", sa.Integer, sa.ForeignKey("bots.id"), nullable=False, index=True),
            sa.Column("achievement_id", sa.Integer, sa.ForeignKey("achievement_definitions.id"), nullable=False),
            sa.Column("earned_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("metadata", sa.JSON, nullable=True),
            sa.UniqueConstraint("bot_id", "achievement_id", name="uq_bot_achievement"),
        )


def downgrade() -> None:
    op.drop_table("bot_achievements")
    op.drop_table("achievement_definitions")
