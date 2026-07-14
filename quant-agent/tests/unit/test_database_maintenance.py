from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from scripts.manage_database import (
    archive_and_prune_snapshots,
    backup_database,
    build_retention_plan,
    cleanup_test_snapshots,
    inspect_test_snapshots,
    restore_database,
)


def _build_database(
    path: Path,
    *,
    operational_reference: bool = False,
    normalized_bars: bool = False,
) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE source_snapshots (
                source_snapshot_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                provider_name TEXT NOT NULL
            );
            CREATE TABLE snapshot_events (source_snapshot_id TEXT);
            CREATE TABLE snapshot_fundamentals (source_snapshot_id TEXT);
            CREATE TABLE snapshot_securities (source_snapshot_id TEXT);
            CREATE TABLE recommendations (id TEXT PRIMARY KEY, source_snapshot_id TEXT);
            CREATE TABLE system_cycle_runs (source_snapshot_id TEXT);
            CREATE TABLE paper_orders (source_snapshot_id TEXT);
            CREATE TABLE trade_ledger (source_snapshot_id TEXT);
            CREATE TABLE sell_execution_audits (source_snapshot_id TEXT);
            CREATE TABLE holding_control_audits (source_snapshot_id TEXT);
            CREATE TABLE sell_alert_audits (source_snapshot_id TEXT);
            CREATE TABLE approval_decisions (recommendation_id TEXT);
            CREATE TABLE holding_watches (source_recommendation_id TEXT);
            """
        )
        connection.execute(
            "INSERT INTO source_snapshots VALUES (?, ?, ?)",
            ("test-snapshot", "2026-07-13 10:45:00", "MockMarketDataProvider"),
        )
        connection.execute(
            "INSERT INTO recommendations VALUES (?, ?)",
            ("test-recommendation", "test-snapshot"),
        )
        if normalized_bars:
            connection.executescript(
                """
                CREATE TABLE market_bars (id INTEGER PRIMARY KEY, ticker TEXT);
                CREATE TABLE snapshot_storage_keys (
                    id INTEGER PRIMARY KEY,
                    source_snapshot_id TEXT UNIQUE
                );
                CREATE TABLE snapshot_market_bar_refs (
                    snapshot_key INTEGER,
                    bar_id INTEGER,
                    PRIMARY KEY (snapshot_key, bar_id)
                );
                INSERT INTO market_bars VALUES (1, 'AAPL');
                INSERT INTO snapshot_storage_keys VALUES (1, 'test-snapshot');
                INSERT INTO snapshot_market_bar_refs VALUES (1, 1);
                """
            )
        else:
            connection.execute("CREATE TABLE snapshot_market_bars (source_snapshot_id TEXT)")
            connection.execute("INSERT INTO snapshot_market_bars VALUES (?)", ("test-snapshot",))
        if operational_reference:
            connection.execute("INSERT INTO paper_orders VALUES (?)", ("test-snapshot",))
        connection.commit()


def test_backup_and_cleanup_exact_test_snapshot_set(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _build_database(database)

    backup_result = backup_database(database, backup)
    assert backup_result["quick_check"] == "ok"
    assert backup_result["bytes"] > 0

    inspection = inspect_test_snapshots(
        database,
        created_after="2026-07-13 10:44:00",
        created_before="2026-07-13 10:46:00",
        provider="MockMarketDataProvider",
    )
    assert inspection["snapshot_ids"] == ["test-snapshot"]
    assert inspection["operational_reference_counts"]["paper_orders"] == 0

    cleanup = cleanup_test_snapshots(
        database,
        created_after="2026-07-13 10:44:00",
        created_before="2026-07-13 10:46:00",
        provider="MockMarketDataProvider",
        expected_count=1,
        backup_path=backup,
    )
    assert cleanup["deleted"]["source_snapshots"] == 1
    assert cleanup["deleted"]["recommendations"] == 1
    assert cleanup["quick_check"] == "ok"


def test_cleanup_refuses_operational_references(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _build_database(database, operational_reference=True)
    backup_database(database, backup)

    with pytest.raises(RuntimeError, match="operational references"):
        cleanup_test_snapshots(
            database,
            created_after="2026-07-13 10:44:00",
            created_before="2026-07-13 10:46:00",
            provider="MockMarketDataProvider",
            expected_count=1,
            backup_path=backup,
        )


def test_cleanup_supports_normalized_snapshot_bar_storage(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _build_database(database, normalized_bars=True)
    backup_database(database, backup)

    inspection = inspect_test_snapshots(
        database,
        created_after="2026-07-13 10:44:00",
        created_before="2026-07-13 10:46:00",
        provider="MockMarketDataProvider",
    )
    assert inspection["child_counts"]["snapshot_market_bars"] == 1

    cleanup = cleanup_test_snapshots(
        database,
        created_after="2026-07-13 10:44:00",
        created_before="2026-07-13 10:46:00",
        provider="MockMarketDataProvider",
        expected_count=1,
        backup_path=backup,
    )
    assert cleanup["deleted"]["snapshot_market_bars"] == 1
    assert cleanup["deleted"]["snapshot_storage_keys"] == 1
    assert cleanup["deleted"]["orphan_market_bars"] == 1


def test_retention_archive_can_be_restored(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    archive = tmp_path / "archive.db"
    restored = tmp_path / "restored.db"
    _build_database(database, normalized_bars=True)

    plan = build_retention_plan(
        database,
        created_before="2026-07-14 00:00:00",
        keep_latest=0,
    )
    assert plan["snapshot_count"] == 1
    assert plan["safe_to_prune"] is True

    result = archive_and_prune_snapshots(
        database,
        created_before="2026-07-14 00:00:00",
        keep_latest=0,
        expected_count=1,
        archive_output=archive,
    )
    assert result["deleted"]["source_snapshots"] == 1
    assert result["quick_check"] == "ok"

    restore = restore_database(archive, restored)
    assert restore["quick_check"] == "ok"
    with closing(sqlite3.connect(restored)) as connection:
        assert connection.execute("SELECT count(*) FROM source_snapshots").fetchone()[0] == 1
