"""Add reason column to futures_positions and futures_orders

Revision ID: 023
Revises: 022
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("futures_positions", sa.Column("reason", sa.String(500), nullable=True))
    op.add_column("futures_orders", sa.Column("reason", sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column("futures_orders", "reason")
    op.drop_column("futures_positions", "reason")
