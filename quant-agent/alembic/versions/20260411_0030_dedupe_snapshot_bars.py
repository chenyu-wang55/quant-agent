"""Deduplicate snapshot market bars into base rows plus compact references

Revision ID: 20260411_0030
Revises: 20260411_0029
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260411_0030"
down_revision = "20260411_0029"
branch_labels = None
depends_on = None


BAR_COLUMNS = "vendor_id, ticker, timestamp, open, high, low, close, volume"
BAR_JOIN = " AND ".join(
    f"b.{column} = s.{column}"
    for column in ("vendor_id", "ticker", "timestamp", "open", "high", "low", "close", "volume")
)


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        sa.text(
            "DELETE FROM market_bars WHERE id NOT IN ("
            "SELECT MIN(id) FROM market_bars GROUP BY " + BAR_COLUMNS + ")"
        )
    )
    op.drop_index("ix_market_bars_ticker", table_name="market_bars")
    op.drop_index("ix_market_bars_timestamp", table_name="market_bars")
    op.create_index(
        "uq_market_bars_content",
        "market_bars",
        ["vendor_id", "ticker", "timestamp", "open", "high", "low", "close", "volume"],
        unique=True,
    )
    op.create_table(
        "snapshot_storage_keys",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
    )
    op.create_index(
        "ix_snapshot_storage_keys_source_snapshot_id",
        "snapshot_storage_keys",
        ["source_snapshot_id"],
        unique=True,
    )
    op.create_table(
        "snapshot_market_bar_refs",
        sa.Column("snapshot_key", sa.Integer(), nullable=False),
        sa.Column("bar_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("snapshot_key", "bar_id"),
    )
    op.create_index(
        "ix_snapshot_market_bar_refs_bar_id",
        "snapshot_market_bar_refs",
        ["bar_id"],
        unique=False,
    )
    connection.execute(
        sa.text(
            "INSERT INTO snapshot_storage_keys (source_snapshot_id) "
            "SELECT source_snapshot_id FROM source_snapshots ORDER BY created_at, source_snapshot_id"
        )
    )
    connection.execute(
        sa.text(
            "INSERT OR IGNORE INTO market_bars (ticker, timestamp, open, high, low, close, volume, vendor_id) "
            "SELECT ticker, timestamp, open, high, low, close, volume, vendor_id "
            "FROM snapshot_market_bars"
        )
    )
    connection.execute(
        sa.text(
            "INSERT OR IGNORE INTO snapshot_market_bar_refs (snapshot_key, bar_id) "
            "SELECT k.id, b.id FROM snapshot_market_bars s "
            "JOIN snapshot_storage_keys k ON k.source_snapshot_id = s.source_snapshot_id "
            f"JOIN market_bars b ON {BAR_JOIN}"
        )
    )
    old_count = int(connection.scalar(sa.text("SELECT COUNT(*) FROM snapshot_market_bars")) or 0)
    ref_count = int(connection.scalar(sa.text("SELECT COUNT(*) FROM snapshot_market_bar_refs")) or 0)
    if old_count != ref_count:
        raise RuntimeError(
            f"Snapshot bar migration count mismatch: old={old_count}, refs={ref_count}"
        )
    op.drop_table("snapshot_market_bars")
    op.create_index(
        "ix_market_bars_ticker_timestamp",
        "market_bars",
        ["ticker", "timestamp"],
        unique=False,
    )


def downgrade() -> None:
    connection = op.get_bind()
    op.drop_index("ix_market_bars_ticker_timestamp", table_name="market_bars")
    op.create_table(
        "snapshot_market_bars",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source_snapshot_id", sa.String(length=128), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vendor_id", sa.String(length=64), nullable=False),
    )
    op.create_index(
        "ix_snapshot_market_bars_source_snapshot_id",
        "snapshot_market_bars",
        ["source_snapshot_id"],
    )
    op.create_index("ix_snapshot_market_bars_ticker", "snapshot_market_bars", ["ticker"])
    op.create_index("ix_snapshot_market_bars_timestamp", "snapshot_market_bars", ["timestamp"])
    connection.execute(
        sa.text(
            "INSERT INTO snapshot_market_bars "
            "(source_snapshot_id, ticker, timestamp, open, high, low, close, volume, vendor_id) "
            "SELECT k.source_snapshot_id, b.ticker, b.timestamp, b.open, b.high, b.low, "
            "b.close, b.volume, b.vendor_id FROM snapshot_market_bar_refs r "
            "JOIN snapshot_storage_keys k ON k.id = r.snapshot_key "
            "JOIN market_bars b ON b.id = r.bar_id"
        )
    )
    op.drop_index("ix_snapshot_market_bar_refs_bar_id", table_name="snapshot_market_bar_refs")
    op.drop_table("snapshot_market_bar_refs")
    op.drop_index(
        "ix_snapshot_storage_keys_source_snapshot_id",
        table_name="snapshot_storage_keys",
    )
    op.drop_table("snapshot_storage_keys")
    op.drop_index("uq_market_bars_content", table_name="market_bars")
    op.create_index("ix_market_bars_ticker", "market_bars", ["ticker"])
    op.create_index("ix_market_bars_timestamp", "market_bars", ["timestamp"])
