"""Add point-in-time provenance and corporate-action fields

Revision ID: 20260411_0031
Revises: 20260411_0030
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0031"
down_revision = "20260411_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("market_bars") as batch:
        batch.add_column(sa.Column("adjusted_close", sa.Float(), nullable=True))
        batch.add_column(sa.Column("dividend", sa.Float(), nullable=False, server_default="0"))
        batch.add_column(sa.Column("split_factor", sa.Float(), nullable=False, server_default="0"))
        batch.add_column(
            sa.Column("quality_status", sa.String(length=32), nullable=False, server_default="legacy_unverified")
        )
        batch.add_column(sa.Column("provenance_json", sa.JSON(), nullable=True))
    with op.batch_alter_table("snapshot_securities") as batch:
        batch.add_column(sa.Column("provenance_json", sa.JSON(), nullable=True))
    with op.batch_alter_table("snapshot_fundamentals") as batch:
        batch.add_column(sa.Column("period_end", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("available_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("provenance_json", sa.JSON(), nullable=True))
    with op.batch_alter_table("snapshot_events") as batch:
        batch.add_column(sa.Column("provenance_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("snapshot_events") as batch:
        batch.drop_column("provenance_json")
    with op.batch_alter_table("snapshot_fundamentals") as batch:
        batch.drop_column("provenance_json")
        batch.drop_column("available_at")
        batch.drop_column("period_end")
    with op.batch_alter_table("snapshot_securities") as batch:
        batch.drop_column("provenance_json")
    with op.batch_alter_table("market_bars") as batch:
        batch.drop_column("provenance_json")
        batch.drop_column("quality_status")
        batch.drop_column("split_factor")
        batch.drop_column("dividend")
        batch.drop_column("adjusted_close")
