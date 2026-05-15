"""Add recommendation analysis json

Revision ID: 20260411_0002
Revises: 20260411_0001
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0002"
down_revision = "20260411_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("recommendations", sa.Column("analysis_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("recommendations", "analysis_json")
