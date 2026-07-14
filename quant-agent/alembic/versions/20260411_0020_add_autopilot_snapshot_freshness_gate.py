"""Add autopilot snapshot freshness gate

Revision ID: 20260411_0020
Revises: 20260411_0019
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0020"
down_revision = "20260411_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("max_snapshot_bar_age_minutes", sa.Integer(), nullable=False, server_default="4320"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "max_snapshot_bar_age_minutes")
