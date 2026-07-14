"""Add strategy config snapshots

Revision ID: 20260411_0007
Revises: 20260411_0006
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0007"
down_revision = "20260411_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_configs",
        sa.Column("strategy_config_id", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("config_hash", sa.String(length=64), nullable=False),
        sa.Column("run_type", sa.String(length=64), nullable=False),
        sa.Column("snapshot_mode", sa.String(length=32), nullable=False),
        sa.Column("universe", sa.String(length=64), nullable=False),
        sa.Column("universe_rules_json", sa.JSON(), nullable=False),
        sa.Column("signal_config_json", sa.JSON(), nullable=False),
        sa.Column("price_plan_config_json", sa.JSON(), nullable=False),
        sa.Column("risk_policy_json", sa.JSON(), nullable=False),
        sa.Column("publication_json", sa.JSON(), nullable=False),
        sa.Column("execution_mode", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("strategy_config_id"),
    )
    op.create_index(op.f("ix_strategy_configs_config_hash"), "strategy_configs", ["config_hash"], unique=False)
    op.add_column("recommendations", sa.Column("strategy_config_id", sa.String(length=64), nullable=True))
    op.create_index(
        op.f("ix_recommendations_strategy_config_id"),
        "recommendations",
        ["strategy_config_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_recommendations_strategy_config_id"), table_name="recommendations")
    op.drop_column("recommendations", "strategy_config_id")
    op.drop_index(op.f("ix_strategy_configs_config_hash"), table_name="strategy_configs")
    op.drop_table("strategy_configs")
