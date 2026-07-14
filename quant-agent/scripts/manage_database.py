from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SNAPSHOT_CHILD_TABLES = (
    "snapshot_events",
    "snapshot_fundamentals",
    "snapshot_securities",
    "recommendations",
    "system_cycle_runs",
)

OPERATIONAL_SNAPSHOT_TABLES = (
    "paper_orders",
    "trade_ledger",
    "sell_execution_audits",
    "holding_control_audits",
    "sell_alert_audits",
)

OPERATIONAL_RECOMMENDATION_TABLES = (
    "approval_decisions",
    "holding_watches",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quick_check(path: Path) -> str:
    uri = f"file:{path.resolve()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        result = connection.execute("PRAGMA quick_check").fetchone()
    return str(result[0]) if result else "missing_result"


def backup_database(database: Path, output: Path) -> dict[str, Any]:
    database = database.expanduser().resolve()
    output = output.expanduser().resolve()
    if not database.is_file():
        raise FileNotFoundError(f"database not found: {database}")
    if output.exists():
        raise FileExistsError(f"backup already exists: {output}")
    if database == output:
        raise ValueError("backup output must differ from the source database")

    output.parent.mkdir(parents=True, exist_ok=True)
    source_uri = f"file:{database}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source:
        with closing(sqlite3.connect(output)) as destination:
            source.backup(destination)

    check = _quick_check(output)
    if check != "ok":
        output.unlink(missing_ok=True)
        raise RuntimeError(f"backup integrity check failed: {check}")
    return {
        "database": str(database),
        "backup": str(output),
        "bytes": output.stat().st_size,
        "sha256": _sha256(output),
        "quick_check": check,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def restore_database(backup: Path, output: Path) -> dict[str, Any]:
    result = backup_database(backup, output)
    return {
        "backup": result["database"],
        "restored_database": result["backup"],
        "bytes": result["bytes"],
        "sha256": result["sha256"],
        "quick_check": result["quick_check"],
        "restored_at": result["created_at"],
    }


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _snapshot_ids(
    connection: sqlite3.Connection,
    *,
    created_after: str,
    created_before: str,
    provider: str,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT source_snapshot_id
        FROM source_snapshots
        WHERE created_at >= ? AND created_at < ? AND provider_name = ?
        ORDER BY created_at, source_snapshot_id
        """,
        (created_after, created_before, provider),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _count_by_snapshot_ids(
    connection: sqlite3.Connection,
    table: str,
    snapshot_ids: list[str],
) -> int:
    if not snapshot_ids or not _table_exists(connection, table):
        return 0
    placeholders = ",".join("?" for _ in snapshot_ids)
    row = connection.execute(
        f"SELECT count(*) FROM {table} WHERE source_snapshot_id IN ({placeholders})",
        snapshot_ids,
    ).fetchone()
    return int(row[0] if row else 0)


def _count_snapshot_bars(connection: sqlite3.Connection, snapshot_ids: list[str]) -> int:
    if not snapshot_ids:
        return 0
    placeholders = ",".join("?" for _ in snapshot_ids)
    if _table_exists(connection, "snapshot_market_bars"):
        row = connection.execute(
            f"SELECT count(*) FROM snapshot_market_bars "
            f"WHERE source_snapshot_id IN ({placeholders})",
            snapshot_ids,
        ).fetchone()
        return int(row[0] if row else 0)
    if not (
        _table_exists(connection, "snapshot_storage_keys")
        and _table_exists(connection, "snapshot_market_bar_refs")
    ):
        return 0
    row = connection.execute(
        "SELECT count(*) FROM snapshot_market_bar_refs r "
        "JOIN snapshot_storage_keys k ON k.id = r.snapshot_key "
        f"WHERE k.source_snapshot_id IN ({placeholders})",
        snapshot_ids,
    ).fetchone()
    return int(row[0] if row else 0)


def _recommendation_ids(connection: sqlite3.Connection, snapshot_ids: list[str]) -> list[str]:
    if not snapshot_ids or not _table_exists(connection, "recommendations"):
        return []
    placeholders = ",".join("?" for _ in snapshot_ids)
    rows = connection.execute(
        f"SELECT id FROM recommendations WHERE source_snapshot_id IN ({placeholders})",
        snapshot_ids,
    ).fetchall()
    return [str(row[0]) for row in rows]


def _count_by_recommendation_ids(
    connection: sqlite3.Connection,
    table: str,
    recommendation_ids: list[str],
) -> int:
    if not recommendation_ids or not _table_exists(connection, table):
        return 0
    placeholders = ",".join("?" for _ in recommendation_ids)
    column = "recommendation_id" if table == "approval_decisions" else "source_recommendation_id"
    row = connection.execute(
        f"SELECT count(*) FROM {table} WHERE {column} IN ({placeholders})",
        recommendation_ids,
    ).fetchone()
    return int(row[0] if row else 0)


def _snapshot_report_for_ids(
    database: Path,
    snapshot_ids: list[str],
) -> dict[str, Any]:
    uri = f"file:{database.resolve()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        recommendation_ids = _recommendation_ids(connection, snapshot_ids)
        child_counts = {
            table: _count_by_snapshot_ids(connection, table, snapshot_ids)
            for table in SNAPSHOT_CHILD_TABLES
        }
        child_counts["snapshot_market_bars"] = _count_snapshot_bars(connection, snapshot_ids)
        operational_counts = {
            table: _count_by_snapshot_ids(connection, table, snapshot_ids)
            for table in OPERATIONAL_SNAPSHOT_TABLES
        }
        operational_counts.update(
            {
                table: _count_by_recommendation_ids(connection, table, recommendation_ids)
                for table in OPERATIONAL_RECOMMENDATION_TABLES
            }
        )
    return {
        "snapshot_count": len(snapshot_ids),
        "snapshot_ids": snapshot_ids,
        "child_counts": child_counts,
        "operational_reference_counts": operational_counts,
    }


def _delete_snapshot_ids(database: Path, snapshot_ids: list[str]) -> dict[str, int]:
    if not snapshot_ids:
        return {"source_snapshots": 0}
    placeholders = ",".join("?" for _ in snapshot_ids)
    with closing(sqlite3.connect(database)) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
            deleted: dict[str, int] = {}
            if _table_exists(connection, "snapshot_market_bars"):
                cursor = connection.execute(
                    f"DELETE FROM snapshot_market_bars "
                    f"WHERE source_snapshot_id IN ({placeholders})",
                    snapshot_ids,
                )
                deleted["snapshot_market_bars"] = max(0, int(cursor.rowcount))
            elif (
                _table_exists(connection, "snapshot_storage_keys")
                and _table_exists(connection, "snapshot_market_bar_refs")
            ):
                key_rows = connection.execute(
                    "SELECT id FROM snapshot_storage_keys "
                    f"WHERE source_snapshot_id IN ({placeholders})",
                    snapshot_ids,
                ).fetchall()
                snapshot_keys = [int(row[0]) for row in key_rows]
                if snapshot_keys:
                    key_placeholders = ",".join("?" for _ in snapshot_keys)
                    cursor = connection.execute(
                        "DELETE FROM snapshot_market_bar_refs "
                        f"WHERE snapshot_key IN ({key_placeholders})",
                        snapshot_keys,
                    )
                    deleted["snapshot_market_bars"] = max(0, int(cursor.rowcount))
                else:
                    deleted["snapshot_market_bars"] = 0
                cursor = connection.execute(
                    "DELETE FROM snapshot_storage_keys "
                    f"WHERE source_snapshot_id IN ({placeholders})",
                    snapshot_ids,
                )
                deleted["snapshot_storage_keys"] = max(0, int(cursor.rowcount))
                if _table_exists(connection, "market_bars"):
                    cursor = connection.execute(
                        "DELETE FROM market_bars WHERE NOT EXISTS ("
                        "SELECT 1 FROM snapshot_market_bar_refs r WHERE r.bar_id = market_bars.id)"
                    )
                    deleted["orphan_market_bars"] = max(0, int(cursor.rowcount))
            else:
                deleted["snapshot_market_bars"] = 0
            for table in SNAPSHOT_CHILD_TABLES:
                if not _table_exists(connection, table):
                    deleted[table] = 0
                    continue
                cursor = connection.execute(
                    f"DELETE FROM {table} WHERE source_snapshot_id IN ({placeholders})",
                    snapshot_ids,
                )
                deleted[table] = max(0, int(cursor.rowcount))
            cursor = connection.execute(
                f"DELETE FROM source_snapshots WHERE source_snapshot_id IN ({placeholders})",
                snapshot_ids,
            )
            deleted["source_snapshots"] = max(0, int(cursor.rowcount))
            connection.commit()
            return deleted
        except Exception:
            connection.rollback()
            raise


def inspect_test_snapshots(
    database: Path,
    *,
    created_after: str,
    created_before: str,
    provider: str,
) -> dict[str, Any]:
    database = database.expanduser().resolve()
    uri = f"file:{database}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        snapshot_ids = _snapshot_ids(
            connection,
            created_after=created_after,
            created_before=created_before,
            provider=provider,
        )
    report = _snapshot_report_for_ids(database, snapshot_ids)
    report.update({
        "database": str(database),
        "created_after": created_after,
        "created_before": created_before,
        "provider": provider,
    })
    return report


def cleanup_test_snapshots(
    database: Path,
    *,
    created_after: str,
    created_before: str,
    provider: str,
    expected_count: int,
    backup_path: Path,
) -> dict[str, Any]:
    database = database.expanduser().resolve()
    backup_path = backup_path.expanduser().resolve()
    if not backup_path.is_file():
        raise FileNotFoundError(f"required backup not found: {backup_path}")
    if _quick_check(backup_path) != "ok":
        raise RuntimeError("required backup failed integrity check")

    report = inspect_test_snapshots(
        database,
        created_after=created_after,
        created_before=created_before,
        provider=provider,
    )
    snapshot_ids = list(report["snapshot_ids"])
    if len(snapshot_ids) != expected_count:
        raise RuntimeError(
            f"refusing cleanup: expected {expected_count} snapshots, found {len(snapshot_ids)}"
        )
    referenced = {
        table: count
        for table, count in report["operational_reference_counts"].items()
        if count
    }
    if referenced:
        raise RuntimeError(f"refusing cleanup: operational references found: {referenced}")

    deleted = _delete_snapshot_ids(database, snapshot_ids)

    check = _quick_check(database)
    if check != "ok":
        raise RuntimeError(f"database integrity check failed after cleanup: {check}")
    report.update(
        {
            "deleted": deleted,
            "backup": str(backup_path),
            "quick_check": check,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return report


def build_retention_plan(
    database: Path,
    *,
    created_before: str,
    keep_latest: int = 500,
) -> dict[str, Any]:
    database = database.expanduser().resolve()
    if keep_latest < 0:
        raise ValueError("keep_latest must be non-negative")
    uri = f"file:{database}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        rows = connection.execute(
            "SELECT source_snapshot_id FROM source_snapshots "
            "WHERE created_at < ? AND source_snapshot_id NOT IN ("
            "SELECT source_snapshot_id FROM source_snapshots "
            "ORDER BY created_at DESC, source_snapshot_id DESC LIMIT ?) "
            "ORDER BY created_at, source_snapshot_id",
            (created_before, keep_latest),
        ).fetchall()
    snapshot_ids = [str(row[0]) for row in rows]
    report = _snapshot_report_for_ids(database, snapshot_ids)
    report.update(
        {
            "database": str(database),
            "created_before": created_before,
            "keep_latest": keep_latest,
            "safe_to_prune": not any(report["operational_reference_counts"].values()),
        }
    )
    return report


def archive_and_prune_snapshots(
    database: Path,
    *,
    created_before: str,
    keep_latest: int,
    expected_count: int,
    archive_output: Path,
) -> dict[str, Any]:
    database = database.expanduser().resolve()
    plan = build_retention_plan(
        database,
        created_before=created_before,
        keep_latest=keep_latest,
    )
    snapshot_ids = list(plan["snapshot_ids"])
    if len(snapshot_ids) != expected_count:
        raise RuntimeError(
            f"refusing prune: expected {expected_count} snapshots, found {len(snapshot_ids)}"
        )
    referenced = {
        table: count
        for table, count in plan["operational_reference_counts"].items()
        if count
    }
    if referenced:
        raise RuntimeError(f"refusing prune: operational references found: {referenced}")
    archive = backup_database(database, archive_output)
    deleted = _delete_snapshot_ids(database, snapshot_ids)
    check = _quick_check(database)
    if check != "ok":
        raise RuntimeError(f"database integrity check failed after prune: {check}")
    plan.update(
        {
            "archive": archive,
            "deleted": deleted,
            "quick_check": check,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return plan


def compact_database(database: Path, *, backup_path: Path) -> dict[str, Any]:
    database = database.expanduser().resolve()
    backup_path = backup_path.expanduser().resolve()
    if not backup_path.is_file() or _quick_check(backup_path) != "ok":
        raise RuntimeError("a valid pre-compaction backup is required")
    bytes_before = database.stat().st_size
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("VACUUM")
    check = _quick_check(database)
    if check != "ok":
        raise RuntimeError(f"database integrity check failed after compaction: {check}")
    return {
        "database": str(database),
        "backup": str(backup_path),
        "bytes_before": bytes_before,
        "bytes_after": database.stat().st_size,
        "bytes_reclaimed": bytes_before - database.stat().st_size,
        "quick_check": check,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backup and safely maintain the quant-agent SQLite database")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup = subparsers.add_parser("backup", help="create and verify an online SQLite backup")
    backup.add_argument("--database", type=Path, default=Path("quant_agent_v2.db"))
    backup.add_argument("--output", type=Path, required=True)

    restore = subparsers.add_parser("restore", help="restore and verify a backup into a new database")
    restore.add_argument("--backup", type=Path, required=True)
    restore.add_argument("--output", type=Path, required=True)

    retention = subparsers.add_parser("retention-plan", help="preview snapshots eligible for archival")
    archive = subparsers.add_parser("archive-and-prune", help="archive then prune an exact snapshot set")
    for command in (retention, archive):
        command.add_argument("--database", type=Path, default=Path("quant_agent_v2.db"))
        command.add_argument("--created-before", required=True)
        command.add_argument("--keep-latest", type=int, default=500)
    archive.add_argument("--expected-count", type=int, required=True)
    archive.add_argument("--archive-output", type=Path, required=True)
    archive.add_argument("--apply", action="store_true")

    compact = subparsers.add_parser("compact", help="VACUUM a database after validating a backup")
    compact.add_argument("--database", type=Path, default=Path("quant_agent_v2.db"))
    compact.add_argument("--backup-path", type=Path, required=True)
    compact.add_argument("--apply", action="store_true")

    inspect = subparsers.add_parser("inspect-test-snapshots", help="preview snapshots matching a test window")
    cleanup = subparsers.add_parser("cleanup-test-snapshots", help="remove an exact, backed-up test snapshot set")
    for command in (inspect, cleanup):
        command.add_argument("--database", type=Path, default=Path("quant_agent_v2.db"))
        command.add_argument("--created-after", required=True)
        command.add_argument("--created-before", required=True)
        command.add_argument("--provider", default="MockMarketDataProvider")
    cleanup.add_argument("--expected-count", type=int, required=True)
    cleanup.add_argument("--backup-path", type=Path, required=True)
    cleanup.add_argument("--apply", action="store_true", help="required confirmation for destructive cleanup")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "backup":
        result = backup_database(args.database, args.output)
    elif args.command == "restore":
        result = restore_database(args.backup, args.output)
    elif args.command == "retention-plan":
        result = build_retention_plan(
            args.database,
            created_before=args.created_before,
            keep_latest=args.keep_latest,
        )
    elif args.command == "archive-and-prune":
        if not args.apply:
            raise SystemExit("archive-and-prune requires --apply after reviewing retention-plan")
        result = archive_and_prune_snapshots(
            args.database,
            created_before=args.created_before,
            keep_latest=args.keep_latest,
            expected_count=args.expected_count,
            archive_output=args.archive_output,
        )
    elif args.command == "compact":
        if not args.apply:
            raise SystemExit("compact requires --apply after creating a verified backup")
        result = compact_database(args.database, backup_path=args.backup_path)
    elif args.command == "inspect-test-snapshots":
        result = inspect_test_snapshots(
            args.database,
            created_after=args.created_after,
            created_before=args.created_before,
            provider=args.provider,
        )
    elif args.command == "cleanup-test-snapshots":
        if not args.apply:
            raise SystemExit("cleanup requires --apply after reviewing inspect-test-snapshots output")
        result = cleanup_test_snapshots(
            args.database,
            created_after=args.created_after,
            created_before=args.created_before,
            provider=args.provider,
            expected_count=args.expected_count,
            backup_path=args.backup_path,
        )
    else:  # pragma: no cover - argparse enforces commands
        raise ValueError(f"unsupported command: {args.command}")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
