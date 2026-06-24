"""Add order execution adapter fields

Revision ID: 20260411_0008
Revises: 20260411_0007
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0008"
down_revision = "20260411_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "paper_orders",
        sa.Column("execution_mode", sa.String(length=16), nullable=False, server_default="paper"),
    )
    op.add_column(
        "paper_orders",
        sa.Column("dry_run", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "paper_orders",
        sa.Column("broker_order_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "paper_orders",
        sa.Column("adapter_message", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("paper_orders", "adapter_message")
    op.drop_column("paper_orders", "broker_order_id")
    op.drop_column("paper_orders", "dry_run")
    op.drop_column("paper_orders", "execution_mode")
