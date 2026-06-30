"""Add durable autopilot policies

Revision ID: 20260411_0014
Revises: 20260411_0013
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0014"
down_revision = "20260411_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "autopilot_policies",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("enabled", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("auto_approve_recommendations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("auto_execute_approved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("auto_execution_mode", sa.String(length=32), nullable=False, server_default="paper"),
        sa.Column("auto_approve_min_confidence", sa.Float(), nullable=False, server_default="0.72"),
        sa.Column("auto_approve_min_composite", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("max_auto_approvals", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_auto_buys", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_auto_sells", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("account_equity", sa.Float(), nullable=False, server_default="100000.0"),
        sa.Column("risk_per_trade_pct", sa.Float(), nullable=False, server_default="0.01"),
        sa.Column("max_position_pct", sa.Float(), nullable=False, server_default="0.10"),
        sa.Column("max_gross_exposure_pct", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("max_sector_exposure_pct", sa.Float(), nullable=False, server_default="0.30"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.String(length=128), nullable=False),
    )
    op.create_index("ix_autopilot_policies_updated_at", "autopilot_policies", ["updated_at"])


def downgrade() -> None:
    op.drop_index("ix_autopilot_policies_updated_at", table_name="autopilot_policies")
    op.drop_table("autopilot_policies")
