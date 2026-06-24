"""Add portfolio trade ledger

Revision ID: 20260411_0006
Revises: 20260411_0005
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0006"
down_revision = "20260411_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trade_ledger",
        sa.Column("trade_id", sa.String(length=64), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("qty", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_recommendation_id", sa.String(length=64), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("realized_pnl_delta", sa.Float(), nullable=False, server_default="0"),
        sa.Column("holding_status_after", sa.String(length=16), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("trade_id"),
    )
    op.create_index(op.f("ix_trade_ledger_ticker"), "trade_ledger", ["ticker"], unique=False)
    op.create_index(op.f("ix_trade_ledger_side"), "trade_ledger", ["side"], unique=False)
    op.create_index(op.f("ix_trade_ledger_executed_at"), "trade_ledger", ["executed_at"], unique=False)
    op.create_index(
        op.f("ix_trade_ledger_source_recommendation_id"),
        "trade_ledger",
        ["source_recommendation_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_trade_ledger_source_recommendation_id"), table_name="trade_ledger")
    op.drop_index(op.f("ix_trade_ledger_executed_at"), table_name="trade_ledger")
    op.drop_index(op.f("ix_trade_ledger_side"), table_name="trade_ledger")
    op.drop_index(op.f("ix_trade_ledger_ticker"), table_name="trade_ledger")
    op.drop_table("trade_ledger")
