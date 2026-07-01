"""Add autopilot daily loss gate

Revision ID: 20260411_0021
Revises: 20260411_0020
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0021"
down_revision = "20260411_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("max_daily_realized_loss_pct", sa.Float(), nullable=False, server_default="0.03"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "max_daily_realized_loss_pct")
