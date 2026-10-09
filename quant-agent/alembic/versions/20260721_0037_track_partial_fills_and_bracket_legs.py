"""Track cumulative fills and broker protection legs

Revision ID: 20260721_0037
Revises: 20260720_0036
Create Date: 2026-07-21
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260721_0037"
down_revision = "20260720_0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("paper_orders") as batch:
        batch.add_column(
            sa.Column("filled_qty", sa.Float(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("broker_protection_json", sa.JSON(), nullable=True))
    op.execute(
        "UPDATE paper_orders SET filled_qty = qty "
        "WHERE status = 'filled' AND filled_qty = 0"
    )


def downgrade() -> None:
    with op.batch_alter_table("paper_orders") as batch:
        batch.drop_column("broker_protection_json")
        batch.drop_column("filled_qty")
