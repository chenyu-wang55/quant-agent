"""Add autopilot order dedupe gate

Revision ID: 20260411_0022
Revises: 20260411_0021
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0022"
down_revision = "20260411_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column("order_dedupe_minutes", sa.Integer(), nullable=False, server_default="1440"),
    )


def downgrade() -> None:
    op.drop_column("autopilot_policies", "order_dedupe_minutes")
