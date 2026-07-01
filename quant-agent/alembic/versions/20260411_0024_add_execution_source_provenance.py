"""Add execution source provenance

Revision ID: 20260411_0024
Revises: 20260411_0023
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0024"
down_revision = "20260411_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name in (
        "paper_orders",
        "holding_control_audits",
        "trade_ledger",
        "sell_execution_audits",
        "sell_alert_audits",
    ):
        op.add_column(
            table_name,
            sa.Column("source_snapshot_id", sa.String(length=128), nullable=True),
        )
        op.add_column(
            table_name,
            sa.Column("strategy_config_id", sa.String(length=64), nullable=True),
        )
        op.create_index(
            f"ix_{table_name}_source_snapshot_id",
            table_name,
            ["source_snapshot_id"],
            unique=False,
        )
        op.create_index(
            f"ix_{table_name}_strategy_config_id",
            table_name,
            ["strategy_config_id"],
            unique=False,
        )


def downgrade() -> None:
    for table_name in (
        "sell_alert_audits",
        "sell_execution_audits",
        "trade_ledger",
        "holding_control_audits",
        "paper_orders",
    ):
        op.drop_index(f"ix_{table_name}_strategy_config_id", table_name=table_name)
        op.drop_index(f"ix_{table_name}_source_snapshot_id", table_name=table_name)
        op.drop_column(table_name, "strategy_config_id")
        op.drop_column(table_name, "source_snapshot_id")
