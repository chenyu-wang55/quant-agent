"""Add autopilot buy price drift gate

Revision ID: 20260411_0025
Revises: 20260411_0024
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0025"
down_revision = "20260411_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("max_auto_buy_price_drift_pct", sa.Float(), nullable=False, server_default="0.03"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "max_auto_buy_price_drift_pct")
