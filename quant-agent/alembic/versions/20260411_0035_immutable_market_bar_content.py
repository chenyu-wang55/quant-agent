"""Make deduplicated market-bar content immutable

Revision ID: 20260411_0035
Revises: 20260411_0034
Create Date: 2026-07-14
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa

from alembic import op

revision = "20260411_0035"
down_revision = "20260411_0034"
branch_labels = None
depends_on = None


def _timestamp_text(value: datetime | str) -> str:
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, str)
        else value
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(timespec="microseconds")


def _provenance(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _content_hash(row: Any) -> str:
    payload = {
        "adjusted_close": float(row.adjusted_close) if row.adjusted_close is not None else None,
        "close": float(row.close),
        "dividend": float(row.dividend),
        "high": float(row.high),
        "low": float(row.low),
        "open": float(row.open),
        "provenance": _provenance(row.provenance_json),
        "quality_status": str(row.quality_status),
        "split_factor": float(row.split_factor),
        "ticker": str(row.ticker).upper(),
        "timestamp": _timestamp_text(row.timestamp),
        "vendor_id": str(row.vendor_id),
        "volume": float(row.volume),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _raw_identity(row: Any) -> tuple[Any, ...]:
    return (
        row.vendor_id,
        row.ticker,
        _timestamp_text(row.timestamp),
        float(row.open),
        float(row.high),
        float(row.low),
        float(row.close),
        float(row.volume),
    )


def upgrade() -> None:
    connection = op.get_bind()
    with op.batch_alter_table("market_bars") as batch:
        batch.add_column(sa.Column("content_hash", sa.String(length=64), nullable=True))

    rows = connection.execute(
        sa.text(
            "SELECT id, vendor_id, ticker, timestamp, open, high, low, close, volume, "
            "adjusted_close, dividend, split_factor, quality_status, provenance_json "
            "FROM market_bars ORDER BY id"
        )
    ).all()
    updates = [{"id": int(row.id), "content_hash": _content_hash(row)} for row in rows]
    for start in range(0, len(updates), 1000):
        connection.execute(
            sa.text("UPDATE market_bars SET content_hash=:content_hash WHERE id=:id"),
            updates[start : start + 1000],
        )

    with op.batch_alter_table("market_bars") as batch:
        batch.alter_column(
            "content_hash",
            existing_type=sa.String(length=64),
            nullable=False,
        )
    op.create_index(
        "uq_market_bars_content_hash",
        "market_bars",
        ["content_hash"],
        unique=True,
    )
    op.drop_index("uq_market_bars_content", table_name="market_bars")


def downgrade() -> None:
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT id, vendor_id, ticker, timestamp, open, high, low, close, volume "
            "FROM market_bars ORDER BY id"
        )
    ).all()
    grouped: dict[tuple[Any, ...], list[int]] = {}
    for row in rows:
        grouped.setdefault(_raw_identity(row), []).append(int(row.id))
    for ids in grouped.values():
        keep_id = ids[0]
        for duplicate_id in ids[1:]:
            connection.execute(
                sa.text(
                    "INSERT OR IGNORE INTO snapshot_market_bar_refs (snapshot_key, bar_id) "
                    "SELECT snapshot_key, :keep_id FROM snapshot_market_bar_refs "
                    "WHERE bar_id=:duplicate_id"
                ),
                {"keep_id": keep_id, "duplicate_id": duplicate_id},
            )
            connection.execute(
                sa.text("DELETE FROM snapshot_market_bar_refs WHERE bar_id=:duplicate_id"),
                {"duplicate_id": duplicate_id},
            )
            connection.execute(
                sa.text("DELETE FROM market_bars WHERE id=:duplicate_id"),
                {"duplicate_id": duplicate_id},
            )

    op.drop_index("uq_market_bars_content_hash", table_name="market_bars")
    with op.batch_alter_table("market_bars") as batch:
        batch.drop_column("content_hash")
    op.create_index(
        "uq_market_bars_content",
        "market_bars",
        ["vendor_id", "ticker", "timestamp", "open", "high", "low", "close", "volume"],
        unique=True,
    )
