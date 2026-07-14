"""Add autopilot daily budgets

Revision ID: 20260411_0016
Revises: 20260411_0015
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0016"
down_revision = "20260411_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("max_daily_auto_approvals", sa.Integer(), nullable=False, server_default="3"),
    )
    op.add_column(
        "autopilot_policies",
        sa.Column("max_daily_auto_buys", sa.Integer(), nullable=False, server_default="3"),
    )
    op.add_column(
        "autopilot_policies",
        sa.Column("max_daily_auto_sells", sa.Integer(), nullable=False, server_default="10"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "max_daily_auto_sells")
    op.drop_column("autopilot_policies", "max_daily_auto_buys")
    op.drop_column("autopilot_policies", "max_daily_auto_approvals")
