"""Add serialized portfolio risk reservations

Revision ID: 20260411_0033
Revises: 20260411_0032
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0033"
down_revision = "20260411_0032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "portfolio_risk_reservations",
        sa.Column("order_id", sa.String(length=64), primary_key=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("requested_notional", sa.Float(), nullable=False),
        sa.Column("account_equity", sa.Float(), nullable=False),
        sa.Column("max_gross_exposure_pct", sa.Float(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_portfolio_risk_reservations_ticker",
        "portfolio_risk_reservations",
        ["ticker"],
    )
    op.create_index(
        "ix_portfolio_risk_reservations_status",
        "portfolio_risk_reservations",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_portfolio_risk_reservations_status",
        table_name="portfolio_risk_reservations",
    )
    op.drop_index(
        "ix_portfolio_risk_reservations_ticker",
        table_name="portfolio_risk_reservations",
    )
    op.drop_table("portfolio_risk_reservations")
