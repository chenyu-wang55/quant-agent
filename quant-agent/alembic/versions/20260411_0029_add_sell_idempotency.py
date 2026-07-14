"""Add sell-execution idempotency identifiers

Revision ID: 20260411_0029
Revises: 20260411_0028
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0029"
down_revision = "20260411_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sell_execution_audits",
        sa.Column("client_order_id", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "sell_execution_audits",
        sa.Column("idempotency_key", sa.String(length=128), nullable=True),
    )
    op.create_index(
        op.f("ix_sell_execution_audits_client_order_id"),
        "sell_execution_audits",
        ["client_order_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_sell_execution_audits_idempotency_key"),
        "sell_execution_audits",
        ["idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_sell_execution_audits_idempotency_key"),
        table_name="sell_execution_audits",
    )
    op.drop_index(
        op.f("ix_sell_execution_audits_client_order_id"),
        table_name="sell_execution_audits",
    )
    op.drop_column("sell_execution_audits", "idempotency_key")
    op.drop_column("sell_execution_audits", "client_order_id")
