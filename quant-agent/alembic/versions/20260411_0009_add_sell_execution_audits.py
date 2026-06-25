"""Add sell execution audit records

Revision ID: 20260411_0009
Revises: 20260411_0008
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0009"
down_revision = "20260411_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sell_execution_audits",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("qty", sa.Float(), nullable=False),
        sa.Column("sell_price", sa.Float(), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("execution_mode", sa.String(length=16), nullable=False, server_default="paper"),
        sa.Column("dry_run", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("broker_order_id", sa.String(length=128), nullable=True),
        sa.Column("adapter_message", sa.Text(), nullable=True),
        sa.Column("applied_to_ledger", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="recorded"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("source_recommendation_id", sa.String(length=64), nullable=True),
        sa.Column("realized_pnl_delta", sa.Float(), nullable=False, server_default="0"),
        sa.Column("estimated_realized_pnl_delta", sa.Float(), nullable=True),
        sa.Column("remaining_qty", sa.Float(), nullable=False),
        sa.Column("holding_status_after", sa.String(length=16), nullable=True),
    )
    op.create_index("ix_sell_execution_audits_ticker", "sell_execution_audits", ["ticker"])
    op.create_index("ix_sell_execution_audits_submitted_at", "sell_execution_audits", ["submitted_at"])
    op.create_index(
        "ix_sell_execution_audits_source_recommendation_id",
        "sell_execution_audits",
        ["source_recommendation_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_sell_execution_audits_source_recommendation_id", table_name="sell_execution_audits")
    op.drop_index("ix_sell_execution_audits_submitted_at", table_name="sell_execution_audits")
    op.drop_index("ix_sell_execution_audits_ticker", table_name="sell_execution_audits")
    op.drop_table("sell_execution_audits")
