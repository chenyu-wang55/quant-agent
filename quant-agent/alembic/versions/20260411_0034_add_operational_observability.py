"""Add persistent operational metrics and alerts

Revision ID: 20260411_0034
Revises: 20260411_0033
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0034"
down_revision = "20260411_0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "operational_metrics",
        sa.Column("metric", sa.String(length=128), primary_key=True),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("value", sa.Float(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_operational_metrics_updated_at",
        "operational_metrics",
        ["updated_at"],
    )
    op.create_table(
        "operational_alerts",
        sa.Column("alert_key", sa.String(length=128), primary_key=True),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_operational_alerts_category",
        "operational_alerts",
        ["category"],
    )
    op.create_index(
        "ix_operational_alerts_status",
        "operational_alerts",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index("ix_operational_alerts_status", table_name="operational_alerts")
    op.drop_index("ix_operational_alerts_category", table_name="operational_alerts")
    op.drop_table("operational_alerts")
    op.drop_index("ix_operational_metrics_updated_at", table_name="operational_metrics")
    op.drop_table("operational_metrics")
