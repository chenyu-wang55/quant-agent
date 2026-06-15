"""Add source snapshot replay tables

Revision ID: 20260411_0004
Revises: 20260411_0003
Create Date: 2026-04-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260411_0004"
down_revision = "20260411_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "source_snapshots",
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("universe", sa.String(length=64), nullable=False),
        sa.Column("provider_name", sa.String(length=128), nullable=False),
        sa.Column("tickers", sa.JSON(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("source_snapshot_id"),
    )
    op.create_index(op.f("ix_source_snapshots_as_of"), "source_snapshots", ["as_of"], unique=False)

    op.create_table(
        "snapshot_securities",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("sector", sa.String(length=64), nullable=False),
        sa.Column("market_cap_usd", sa.Float(), nullable=False),
        sa.Column("avg_dollar_volume", sa.Float(), nullable=False),
        sa.Column("last_price", sa.Float(), nullable=False),
        sa.Column("spread_bps", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_snapshot_securities_source_snapshot_id"), "snapshot_securities", ["source_snapshot_id"], unique=False)
    op.create_index(op.f("ix_snapshot_securities_ticker"), "snapshot_securities", ["ticker"], unique=False)

    op.create_table(
        "snapshot_market_bars",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vendor_id", sa.String(length=64), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_snapshot_market_bars_source_snapshot_id"), "snapshot_market_bars", ["source_snapshot_id"], unique=False)
    op.create_index(op.f("ix_snapshot_market_bars_ticker"), "snapshot_market_bars", ["ticker"], unique=False)
    op.create_index(op.f("ix_snapshot_market_bars_timestamp"), "snapshot_market_bars", ["timestamp"], unique=False)

    op.create_table(
        "snapshot_fundamentals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pe_ttm", sa.Float(), nullable=False),
        sa.Column("roe", sa.Float(), nullable=False),
        sa.Column("revenue_growth_yoy", sa.Float(), nullable=False),
        sa.Column("eps_revision_30d", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_snapshot_fundamentals_source_snapshot_id"), "snapshot_fundamentals", ["source_snapshot_id"], unique=False)
    op.create_index(op.f("ix_snapshot_fundamentals_ticker"), "snapshot_fundamentals", ["ticker"], unique=False)

    op.create_table(
        "snapshot_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("vendor_source_id", sa.String(length=128), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False),
        sa.Column("tickers", sa.JSON(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("sentiment", sa.Float(), nullable=False),
        sa.Column("relevance", sa.Float(), nullable=False),
        sa.Column("horizon", sa.String(length=32), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_snapshot_events_source_snapshot_id"), "snapshot_events", ["source_snapshot_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_snapshot_events_source_snapshot_id"), table_name="snapshot_events")
    op.drop_table("snapshot_events")
    op.drop_index(op.f("ix_snapshot_fundamentals_ticker"), table_name="snapshot_fundamentals")
    op.drop_index(op.f("ix_snapshot_fundamentals_source_snapshot_id"), table_name="snapshot_fundamentals")
    op.drop_table("snapshot_fundamentals")
    op.drop_index(op.f("ix_snapshot_market_bars_timestamp"), table_name="snapshot_market_bars")
    op.drop_index(op.f("ix_snapshot_market_bars_ticker"), table_name="snapshot_market_bars")
    op.drop_index(op.f("ix_snapshot_market_bars_source_snapshot_id"), table_name="snapshot_market_bars")
    op.drop_table("snapshot_market_bars")
    op.drop_index(op.f("ix_snapshot_securities_ticker"), table_name="snapshot_securities")
    op.drop_index(op.f("ix_snapshot_securities_source_snapshot_id"), table_name="snapshot_securities")
    op.drop_table("snapshot_securities")
    op.drop_index(op.f("ix_source_snapshots_as_of"), table_name="source_snapshots")
    op.drop_table("source_snapshots")
