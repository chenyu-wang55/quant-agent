"""Add holding sell controls

Revision ID: 20260411_0005
Revises: 20260411_0004
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0005"
down_revision = "20260411_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "holding_watches",
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column("holding_watches", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("holding_watches", sa.Column("last_sell_price", sa.Float(), nullable=True))
    op.add_column("holding_watches", sa.Column("last_sell_reason", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("holding_watches", "last_sell_reason")
    op.drop_column("holding_watches", "last_sell_price")
    op.drop_column("holding_watches", "closed_at")
    op.drop_column("holding_watches", "realized_pnl")
