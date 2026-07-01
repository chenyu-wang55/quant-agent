"""Add autopilot position reconciliation gate

Revision ID: 20260411_0027
Revises: 20260411_0026
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0027"
down_revision = "20260411_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("require_position_reconciliation", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "autopilot_policies",
        sa.Column(
            "max_position_reconciliation_age_minutes",
            sa.Integer(),
            nullable=False,
            server_default="1440",
        ),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "max_position_reconciliation_age_minutes")
    op.drop_column("autopilot_policies", "require_position_reconciliation")
