"""Redesign user_balance_snapshots: replace flat fields with structured
capital breakdown, equity, net_liquidation, and per-asset unrealized PnL.

Revision ID: 024
Revises: 023
Create Date: 2026-03-06
"""
from alembic import op
import sqlalchemy as sa

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- Drop old columns --
    op.drop_column("user_balance_snapshots", "balance")
    op.drop_column("user_balance_snapshots", "bot_balance")
    op.drop_column("user_balance_snapshots", "available")
    op.drop_column("user_balance_snapshots", "session_pnl")
    op.drop_column("user_balance_snapshots", "prev_balance")
    op.drop_column("user_balance_snapshots", "bot_pnl")

    # -- Add new columns --
    # Session context
    op.add_column("user_balance_snapshots", sa.Column("candle_open", sa.Integer(), nullable=True))

    # Capital breakdown
    op.add_column("user_balance_snapshots", sa.Column("unallocated", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("bot_cash", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("bo_locked", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("futures_locked", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))

    # Equity
    op.add_column("user_balance_snapshots", sa.Column("equity", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))

    # Mark-to-market (split BO vs futures)
    op.add_column("user_balance_snapshots", sa.Column("bo_unrealized_pnl", sa.Numeric(18, 8, asdecimal=False), nullable=True, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("futures_unrealized_pnl", sa.Numeric(18, 8, asdecimal=False), nullable=True, server_default="0"))
    # unrealized_pnl already exists — keep it

    # Net liquidation
    op.add_column("user_balance_snapshots", sa.Column("net_liquidation", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))

    # P&L tracking
    op.add_column("user_balance_snapshots", sa.Column("cumulative_realized_pnl", sa.Numeric(18, 8, asdecimal=False), nullable=True, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("session_realized_pnl", sa.Numeric(18, 8, asdecimal=False), nullable=True, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("snapshot_delta", sa.Numeric(18, 8, asdecimal=False), nullable=True))

    # Metadata
    op.add_column("user_balance_snapshots", sa.Column("active_bot_count", sa.Integer(), nullable=True, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("open_bo_count", sa.Integer(), nullable=True, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("open_futures_count", sa.Integer(), nullable=True, server_default="0"))


def downgrade() -> None:
    # -- Remove new columns --
    op.drop_column("user_balance_snapshots", "open_futures_count")
    op.drop_column("user_balance_snapshots", "open_bo_count")
    op.drop_column("user_balance_snapshots", "active_bot_count")
    op.drop_column("user_balance_snapshots", "snapshot_delta")
    op.drop_column("user_balance_snapshots", "session_realized_pnl")
    op.drop_column("user_balance_snapshots", "cumulative_realized_pnl")
    op.drop_column("user_balance_snapshots", "net_liquidation")
    op.drop_column("user_balance_snapshots", "futures_unrealized_pnl")
    op.drop_column("user_balance_snapshots", "bo_unrealized_pnl")
    op.drop_column("user_balance_snapshots", "equity")
    op.drop_column("user_balance_snapshots", "futures_locked")
    op.drop_column("user_balance_snapshots", "bo_locked")
    op.drop_column("user_balance_snapshots", "bot_cash")
    op.drop_column("user_balance_snapshots", "unallocated")
    op.drop_column("user_balance_snapshots", "candle_open")

    # -- Restore old columns --
    op.add_column("user_balance_snapshots", sa.Column("balance", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("bot_balance", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("available", sa.Numeric(18, 8, asdecimal=False), nullable=False, server_default="0"))
    op.add_column("user_balance_snapshots", sa.Column("session_pnl", sa.Numeric(18, 8, asdecimal=False), nullable=True))
    op.add_column("user_balance_snapshots", sa.Column("prev_balance", sa.Numeric(18, 8, asdecimal=False), nullable=True))
    op.add_column("user_balance_snapshots", sa.Column("bot_pnl", sa.Numeric(18, 8, asdecimal=False), nullable=True))
