"""Add unrealized_pnl to user_balance_snapshots.

Revision ID: 021
Revises: 020
Create Date: 2026-03-05

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "021"
down_revision: Union[str, None] = "020"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = "user_balance_snapshots"
_COLUMN = "unrealized_pnl"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = [c["name"] for c in inspector.get_columns(_TABLE)]

    if _COLUMN not in existing:
        op.add_column(
            _TABLE,
            sa.Column(_COLUMN, sa.Numeric(18, 8, asdecimal=False), nullable=True),
        )


def downgrade() -> None:
    op.drop_column(_TABLE, _COLUMN)
