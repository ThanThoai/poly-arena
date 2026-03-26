"""Add dual-mode fill source columns for REST vs WS comparison.

Adds:
- bots.balance_rest, bots.balance_ws, bots.ws_initial_balance
- binary_options.fill_source, binary_options.pair_id
- bot_settlement_ledger.fill_source

Revision ID: 027
"""

from alembic import op
import sqlalchemy as sa


revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Bot dual balance pools
    op.add_column("bots", sa.Column("balance_rest", sa.Numeric(18, 8), nullable=True))
    op.add_column("bots", sa.Column("balance_ws", sa.Numeric(18, 8), nullable=True))
    op.add_column("bots", sa.Column("ws_initial_balance", sa.Numeric(18, 8), nullable=True))

    # BinaryOption fill source tracking
    op.add_column("binary_options", sa.Column("fill_source", sa.String(4), nullable=True))
    op.add_column("binary_options", sa.Column("pair_id", sa.Integer(), nullable=True))
    op.create_index("ix_binary_options_fill_source", "binary_options", ["fill_source"])

    # Settlement ledger per-source tracking
    op.add_column("bot_settlement_ledger", sa.Column("fill_source", sa.String(4), nullable=True))

    # Backfill bots: both pools start from current balance
    op.execute(
        "UPDATE bots SET "
        "  balance_rest = balance, "
        "  balance_ws = balance, "
        "  ws_initial_balance = balance"
    )

    # Backfill orders + ledger: all existing data is REST-sourced
    op.execute("UPDATE binary_options SET fill_source = 'REST'")
    op.execute("UPDATE bot_settlement_ledger SET fill_source = 'REST'")


def downgrade() -> None:
    op.drop_column("bot_settlement_ledger", "fill_source")
    op.drop_index("ix_binary_options_fill_source", table_name="binary_options")
    op.drop_column("binary_options", "pair_id")
    op.drop_column("binary_options", "fill_source")
    op.drop_column("bots", "ws_initial_balance")
    op.drop_column("bots", "balance_ws")
    op.drop_column("bots", "balance_rest")
