"""Add durable system event records

Revision ID: 20260411_0013
Revises: 20260411_0012
Create Date: 2026-04-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260411_0013"
down_revision = "20260411_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "system_event_records",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
    )
    op.create_index("ix_system_event_records_event_type", "system_event_records", ["event_type"])
    op.create_index("ix_system_event_records_created_at", "system_event_records", ["created_at"])
    op.create_index("ix_system_event_records_status", "system_event_records", ["status"])


def downgrade() -> None:
    op.drop_index("ix_system_event_records_status", table_name="system_event_records")
    op.drop_index("ix_system_event_records_created_at", table_name="system_event_records")
    op.drop_index("ix_system_event_records_event_type", table_name="system_event_records")
    op.drop_table("system_event_records")
