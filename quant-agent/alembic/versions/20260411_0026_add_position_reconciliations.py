"""Add position reconciliation audits

Revision ID: 20260411_0026
Revises: 20260411_0025
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0026"
down_revision = "20260411_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "position_reconciliations",
        sa.Column("reconciliation_id", sa.String(length=64), nullable=False),
        sa.Column("broker", sa.String(length=64), nullable=False),
        sa.Column("account_id", sa.String(length=128), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("blocks_auto_execution", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("local_position_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("broker_position_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("matched_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mismatch_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("missing_in_broker_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("broker_only_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("qty_tolerance", sa.Float(), nullable=False, server_default="0.000001"),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("items_json", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("reconciliation_id"),
    )
    op.create_index(
        "ix_position_reconciliations_broker",
        "position_reconciliations",
        ["broker"],
    )
    op.create_index(
        "ix_position_reconciliations_account_id",
        "position_reconciliations",
        ["account_id"],
    )
    op.create_index(
        "ix_position_reconciliations_checked_at",
        "position_reconciliations",
        ["checked_at"],
    )
    op.create_index(
        "ix_position_reconciliations_as_of",
        "position_reconciliations",
        ["as_of"],
    )
    op.create_index(
        "ix_position_reconciliations_status",
        "position_reconciliations",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index("ix_position_reconciliations_status", table_name="position_reconciliations")
    op.drop_index("ix_position_reconciliations_as_of", table_name="position_reconciliations")
    op.drop_index("ix_position_reconciliations_checked_at", table_name="position_reconciliations")
    op.drop_index("ix_position_reconciliations_account_id", table_name="position_reconciliations")
    op.drop_index("ix_position_reconciliations_broker", table_name="position_reconciliations")
    op.drop_table("position_reconciliations")
