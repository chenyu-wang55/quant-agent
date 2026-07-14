"""Add paper shadow readiness policy

Revision ID: 20260411_0032
Revises: 20260411_0031
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0032"
down_revision = "20260411_0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "autopilot_policies",
        sa.Column(
            "min_paper_shadow_trading_days",
            sa.Integer(),
            nullable=False,
            server_default="20",
        ),
    )


def downgrade() -> None:
    with op.batch_alter_table("autopilot_policies") as batch:
        batch.drop_column("min_paper_shadow_trading_days")
