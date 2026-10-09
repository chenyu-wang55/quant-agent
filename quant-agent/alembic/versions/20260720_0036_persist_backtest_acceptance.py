"""Persist auditable backtest acceptance evidence

Revision ID: 20260720_0036
Revises: 20260411_0035
Create Date: 2026-07-20
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260720_0036"
down_revision = "20260411_0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("config_hash", sa.String(length=64), nullable=False),
        sa.Column("strategy_config_id", sa.String(length=64), nullable=False),
        sa.Column("dataset_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("accepted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("result_json", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index("ix_backtest_runs_created_at", "backtest_runs", ["created_at"])
    op.create_index("ix_backtest_runs_config_hash", "backtest_runs", ["config_hash"])
    op.create_index("ix_backtest_runs_strategy_config_id", "backtest_runs", ["strategy_config_id"])
    op.create_index("ix_backtest_runs_dataset_fingerprint", "backtest_runs", ["dataset_fingerprint"])
    op.create_index("ix_backtest_runs_accepted", "backtest_runs", ["accepted"])


def downgrade() -> None:
    op.drop_index("ix_backtest_runs_accepted", table_name="backtest_runs")
    op.drop_index("ix_backtest_runs_dataset_fingerprint", table_name="backtest_runs")
    op.drop_index("ix_backtest_runs_strategy_config_id", table_name="backtest_runs")
    op.drop_index("ix_backtest_runs_config_hash", table_name="backtest_runs")
    op.drop_index("ix_backtest_runs_created_at", table_name="backtest_runs")
    op.drop_table("backtest_runs")
