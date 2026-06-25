"""Add holding control audit records

Revision ID: 20260411_0012
Revises: 20260411_0011
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0012"
down_revision = "20260411_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "holding_control_audits",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("source_recommendation_id", sa.String(length=64), nullable=True),
        sa.Column("old_stop_loss", sa.Float(), nullable=False),
        sa.Column("new_stop_loss", sa.Float(), nullable=False),
        sa.Column("old_take_profit1", sa.Float(), nullable=False),
        sa.Column("new_take_profit1", sa.Float(), nullable=False),
        sa.Column("old_take_profit2", sa.Float(), nullable=False),
        sa.Column("new_take_profit2", sa.Float(), nullable=False),
        sa.Column("old_note", sa.Text(), nullable=True),
        sa.Column("new_note", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.String(length=128), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_holding_control_audits_ticker", "holding_control_audits", ["ticker"])
    op.create_index(
        "ix_holding_control_audits_source_recommendation_id",
        "holding_control_audits",
        ["source_recommendation_id"],
    )
    op.create_index(
        "ix_holding_control_audits_updated_at",
        "holding_control_audits",
        ["updated_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_holding_control_audits_updated_at", table_name="holding_control_audits")
    op.drop_index(
        "ix_holding_control_audits_source_recommendation_id",
        table_name="holding_control_audits",
    )
    op.drop_index("ix_holding_control_audits_ticker", table_name="holding_control_audits")
    op.drop_table("holding_control_audits")
