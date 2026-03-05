"""Add futures_positions and futures_orders tables.

Revision ID: 022
Revises: 021
Create Date: 2026-03-05

Idempotent: safe to run on both fresh and existing databases.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = "022"
down_revision: Union[str, None] = "021"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing = inspector.get_table_names()

    if "futures_positions" not in existing:
        op.create_table(
            "futures_positions",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("bot_name", sa.String(100), nullable=False, index=True),
            sa.Column("symbol", sa.String(20), nullable=False),
            sa.Column("exchange", sa.String(20), nullable=False, server_default="binance"),
            sa.Column("side", sa.String(10), nullable=False),
            sa.Column("status", sa.String(20), nullable=False, server_default="OPEN"),
            sa.Column("size", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("entry_price", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("exit_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("mark_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("leverage", sa.Integer, nullable=False, server_default="10"),
            sa.Column("margin", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("liquidation_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("unrealized_pnl", sa.Numeric(18, 8, asdecimal=False), server_default="0"),
            sa.Column("realized_pnl", sa.Numeric(18, 8, asdecimal=False), server_default="0"),
            sa.Column("entry_fee", sa.Numeric(18, 8, asdecimal=False), server_default="0"),
            sa.Column("exit_fee", sa.Numeric(18, 8, asdecimal=False), server_default="0"),
            sa.Column("tp_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("sl_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("exit_trigger", sa.String(10), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )
        op.create_index("ix_futures_positions_status", "futures_positions", ["status"])
        op.create_index("ix_futures_positions_symbol", "futures_positions", ["symbol"])

    if "futures_orders" not in existing:
        op.create_table(
            "futures_orders",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("bot_name", sa.String(100), nullable=False, index=True),
            sa.Column("symbol", sa.String(20), nullable=False),
            sa.Column("exchange", sa.String(20), nullable=False, server_default="binance"),
            sa.Column("side", sa.String(10), nullable=False),
            sa.Column("order_type", sa.String(10), nullable=False),
            sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
            sa.Column("size", sa.Numeric(18, 8, asdecimal=False), nullable=False),
            sa.Column("limit_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("leverage", sa.Integer, nullable=False, server_default="10"),
            sa.Column("tp_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("sl_price", sa.Numeric(18, 8, asdecimal=False), nullable=True),
            sa.Column("ttl", sa.Integer, nullable=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("position_id", sa.Integer, sa.ForeignKey("futures_positions.id"), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )


def downgrade() -> None:
    op.drop_table("futures_orders")
    op.drop_table("futures_positions")
