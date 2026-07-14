"""Add autopilot rebuy cooldown

Revision ID: 20260411_0017
Revises: 20260411_0016
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0017"
down_revision = "20260411_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("rebuy_cooldown_minutes", sa.Integer(), nullable=False, server_default="240"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "rebuy_cooldown_minutes")
