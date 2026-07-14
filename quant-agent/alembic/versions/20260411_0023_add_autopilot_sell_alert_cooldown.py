"""Add autopilot sell alert cooldown

Revision ID: 20260411_0023
Revises: 20260411_0022
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0023"
down_revision = "20260411_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("sell_alert_cooldown_minutes", sa.Integer(), nullable=False, server_default="60"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "sell_alert_cooldown_minutes")
