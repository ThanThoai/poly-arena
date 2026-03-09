"""Add bot_settlement_ledger table for incremental balance tracking per session.

Revision ID: 025
Revises: 024
Create Date: 2026-03-09
"""
from alembic import op
import sqlalchemy as sa

revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bot_settlement_ledger",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("bot_name", sa.String(100), nullable=False, index=True),

        # Session context
        sa.Column("session_id", sa.String(64), nullable=True, index=True),
        sa.Column("symbol", sa.String(10), nullable=True),
        sa.Column("timeframe", sa.String(5), nullable=True),
        sa.Column("candle_open", sa.Integer(), nullable=True),

        # Incremental balance tracking
        sa.Column("prev_balance", sa.Numeric(18, 8, asdecimal=False), nullable=False),
        sa.Column("total_profit", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"),
        sa.Column("total_fee", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"),
        sa.Column("delta", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"),
        sa.Column("new_balance", sa.Numeric(18, 8, asdecimal=False), nullable=False),

        # Session summary
        sa.Column("session_result", sa.String(10), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("win_count", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("loss_count", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("trade_ids", sa.JSON(), nullable=True),

        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("bot_settlement_ledger")
