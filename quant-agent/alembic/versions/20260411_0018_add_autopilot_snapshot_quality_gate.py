"""Add autopilot snapshot quality gate

Revision ID: 20260411_0018
Revises: 20260411_0017
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0018"
down_revision = "20260411_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("min_snapshot_bar_coverage", sa.Float(), nullable=False, server_default="1.0"),
    )
    op.add_column(
        "autopilot_policies",
        sa.Column("min_snapshot_fundamental_coverage", sa.Float(), nullable=False, server_default="1.0"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "min_snapshot_fundamental_coverage")
    op.drop_column("autopilot_policies", "min_snapshot_bar_coverage")
