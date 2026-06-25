"""Add sell alert audit records

Revision ID: 20260411_0011
Revises: 20260411_0010
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0011"
down_revision = "20260411_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sell_alert_audits",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("current_price", sa.Float(), nullable=False),
        sa.Column("stop_loss", sa.Float(), nullable=False),
        sa.Column("take_profit1", sa.Float(), nullable=False),
        sa.Column("take_profit2", sa.Float(), nullable=False),
        sa.Column("source_recommendation_id", sa.String(length=64), nullable=True),
        sa.Column("message_cn", sa.Text(), nullable=False),
        sa.Column("suggested_action_cn", sa.Text(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("monitor_run_id", sa.String(length=64), nullable=True),
    )
    op.create_index("ix_sell_alert_audits_ticker", "sell_alert_audits", ["ticker"])
    op.create_index("ix_sell_alert_audits_level", "sell_alert_audits", ["level"])
    op.create_index("ix_sell_alert_audits_reason_code", "sell_alert_audits", ["reason_code"])
    op.create_index(
        "ix_sell_alert_audits_source_recommendation_id",
        "sell_alert_audits",
        ["source_recommendation_id"],
    )
    op.create_index("ix_sell_alert_audits_generated_at", "sell_alert_audits", ["generated_at"])
    op.create_index("ix_sell_alert_audits_monitor_run_id", "sell_alert_audits", ["monitor_run_id"])


def downgrade() -> None:
    op.drop_index("ix_sell_alert_audits_monitor_run_id", table_name="sell_alert_audits")
    op.drop_index("ix_sell_alert_audits_generated_at", table_name="sell_alert_audits")
    op.drop_index("ix_sell_alert_audits_source_recommendation_id", table_name="sell_alert_audits")
    op.drop_index("ix_sell_alert_audits_reason_code", table_name="sell_alert_audits")
    op.drop_index("ix_sell_alert_audits_level", table_name="sell_alert_audits")
    op.drop_index("ix_sell_alert_audits_ticker", table_name="sell_alert_audits")
    op.drop_table("sell_alert_audits")
