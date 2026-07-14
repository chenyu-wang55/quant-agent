"""Add paper-order idempotency identifiers

Revision ID: 20260411_0028
Revises: 20260411_0027
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0028"
down_revision = "20260411_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("paper_orders", sa.Column("client_order_id", sa.String(length=64), nullable=True))
    op.add_column("paper_orders", sa.Column("idempotency_key", sa.String(length=128), nullable=True))
    op.create_index(
        op.f("ix_paper_orders_client_order_id"),
        "paper_orders",
        ["client_order_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_paper_orders_idempotency_key"),
        "paper_orders",
        ["idempotency_key"],
        unique=True,
    )
    op.create_index(
        op.f("ix_paper_orders_broker_order_id"),
        "paper_orders",
        ["broker_order_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_paper_orders_broker_order_id"), table_name="paper_orders")
    op.drop_index(op.f("ix_paper_orders_idempotency_key"), table_name="paper_orders")
    op.drop_index(op.f("ix_paper_orders_client_order_id"), table_name="paper_orders")
    op.drop_column("paper_orders", "idempotency_key")
    op.drop_column("paper_orders", "client_order_id")
