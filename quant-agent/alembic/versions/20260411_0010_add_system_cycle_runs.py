"""Add system cycle run audit records

Revision ID: 20260411_0010
Revises: 20260411_0009
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0010"
down_revision = "20260411_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "system_cycle_runs",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("job", sa.String(length=64), nullable=False, server_default="system_cycle"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="success"),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=True),
        sa.Column("strategy_config_id", sa.String(length=64), nullable=True),
        sa.Column("recommendation_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sell_alert_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("consumed_event_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("pending_event_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("auto_execution_enabled", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("top_recommendations_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("sell_alerts_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("consumed_event_type_counts_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("metrics_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index("ix_system_cycle_runs_started_at", "system_cycle_runs", ["started_at"])
    op.create_index("ix_system_cycle_runs_source_snapshot_id", "system_cycle_runs", ["source_snapshot_id"])
    op.create_index("ix_system_cycle_runs_strategy_config_id", "system_cycle_runs", ["strategy_config_id"])


def downgrade() -> None:
    op.drop_index("ix_system_cycle_runs_strategy_config_id", table_name="system_cycle_runs")
    op.drop_index("ix_system_cycle_runs_source_snapshot_id", table_name="system_cycle_runs")
    op.drop_index("ix_system_cycle_runs_started_at", table_name="system_cycle_runs")
    op.drop_table("system_cycle_runs")
