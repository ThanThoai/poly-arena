"""Drop session_id, symbol, timeframe, candle_open from bot_settlement_ledger.

Now stores ONE aggregated row per bot per settlement batch instead of per session.

Revision ID: 026
Revises: 025
Create Date: 2026-03-09
"""
from alembic import op
import sqlalchemy as sa

revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_bot_settlement_ledger_session_id", table_name="bot_settlement_ledger")
    op.drop_column("bot_settlement_ledger", "session_id")
    op.drop_column("bot_settlement_ledger", "symbol")
    op.drop_column("bot_settlement_ledger", "timeframe")
    op.drop_column("bot_settlement_ledger", "candle_open")


def downgrade() -> None:
    op.add_column("bot_settlement_ledger", sa.Column("candle_open", sa.Integer(), nullable=True))
    op.add_column("bot_settlement_ledger", sa.Column("timeframe", sa.String(5), nullable=True))
    op.add_column("bot_settlement_ledger", sa.Column("symbol", sa.String(10), nullable=True))
    op.add_column("bot_settlement_ledger", sa.Column("session_id", sa.String(64), nullable=True))
    op.create_index("ix_bot_settlement_ledger_session_id", "bot_settlement_ledger", ["session_id"])
