"""Add holding watch table for user buys

Revision ID: 20260411_0003
Revises: 20260411_0002
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0003"
down_revision = "20260411_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "holding_watches",
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("qty", sa.Float(), nullable=False),
        sa.Column("avg_buy_price", sa.Float(), nullable=False),
        sa.Column("bought_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_recommendation_id", sa.String(length=64), nullable=True),
        sa.Column("stop_loss", sa.Float(), nullable=False),
        sa.Column("take_profit1", sa.Float(), nullable=False),
        sa.Column("take_profit2", sa.Float(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("ticker"),
    )
    op.create_index(op.f("ix_holding_watches_source_recommendation_id"), "holding_watches", ["source_recommendation_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_holding_watches_source_recommendation_id"), table_name="holding_watches")
    op.drop_table("holding_watches")
