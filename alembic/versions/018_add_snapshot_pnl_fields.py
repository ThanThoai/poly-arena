"""Add session_pnl, prev_balance, bot_pnl to user_balance_snapshots.

Revision ID: 018
Revises: 017
Create Date: 2026-03-04

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "018"
down_revision: Union[str, None] = "017"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = "user_balance_snapshots"
_COLUMNS = [
    ("session_pnl",  sa.Numeric(18, 8, asdecimal=False)),
    ("prev_balance", sa.Numeric(18, 8, asdecimal=False)),
    ("bot_pnl",      sa.Numeric(18, 8, asdecimal=False)),
]


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = [c["name"] for c in inspector.get_columns(_TABLE)]

    for col_name, col_type in _COLUMNS:
        if col_name not in existing:
            op.add_column(_TABLE, sa.Column(col_name, col_type, nullable=True))


def downgrade() -> None:
    for col_name, _ in _COLUMNS:
        op.drop_column(_TABLE, col_name)
