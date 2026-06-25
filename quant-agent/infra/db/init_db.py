from __future__ import annotations

from pathlib import Path
import os
import sqlite3

from alembic import command
from alembic.config import Config
from infra.db.session import get_database_url


REQUIRED_TABLES = {
    "alembic_version",
    "recommendations",
    "signal_snapshots",
    "feature_snapshots",
    "event_records",
    "paper_orders",
    "positions",
    "approval_decisions",
    "execution_controls",
    "market_bars",
    "holding_watches",
    "holding_control_audits",
    "trade_ledger",
    "sell_execution_audits",
    "sell_alert_audits",
    "system_cycle_runs",
    "strategy_configs",
    "source_snapshots",
    "snapshot_securities",
    "snapshot_market_bars",
    "snapshot_fundamentals",
    "snapshot_events",
}


def _sqlite_path_from_url(db_url: str, root: Path) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    db_path = db_url.replace("sqlite:///", "", 1)
    path_obj = Path(db_path)
    if not path_obj.is_absolute():
        path_obj = root / path_obj
    return path_obj


def _has_required_sqlite_tables(path_obj: Path) -> bool:
    if not path_obj.exists():
        return False
    with sqlite3.connect(path_obj) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing = {row[0] for row in cursor.fetchall()}
    return REQUIRED_TABLES.issubset(existing)


def init_db() -> None:
    root = Path(__file__).resolve().parents[2]
    alembic_ini = root / "alembic.ini"

    cfg = Config(str(alembic_ini))
    cfg.set_main_option("script_location", str(root / "alembic"))
    db_url = get_database_url()
    sqlite_path = _sqlite_path_from_url(db_url, root)

    def _reset_and_upgrade_if_needed() -> bool:
        allow_reset = os.getenv("DB_RESET_ON_SCHEMA_CONFLICT", "0") == "1"
        if not allow_reset or sqlite_path is None:
            return False
        if sqlite_path.exists():
            sqlite_path.unlink()
        command.upgrade(cfg, "head")
        return True

    try:
        command.upgrade(cfg, "head")
    except Exception as exc:
        message = str(exc).lower()
        if "already exists" in message:
            if _reset_and_upgrade_if_needed():
                return
            raise RuntimeError(
                "Database schema conflict detected. Back up the database and set "
                "DB_RESET_ON_SCHEMA_CONFLICT=1 to allow automatic SQLite rebuild."
            ) from exc
        raise

    if sqlite_path is not None and not _has_required_sqlite_tables(sqlite_path):
        if _reset_and_upgrade_if_needed():
            return
        raise RuntimeError(
            "Database schema is missing required tables. Run Alembic manually or set "
            "DB_RESET_ON_SCHEMA_CONFLICT=1 after backing up local data."
        )


if __name__ == "__main__":
    init_db()
