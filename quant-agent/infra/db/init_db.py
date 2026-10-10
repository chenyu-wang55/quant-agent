from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from alembic.config import Config

from alembic import command
from infra.db.session import get_database_url

REQUIRED_TABLES = {
    "alembic_version",
    "recommendations",
    "signal_snapshots",
    "feature_snapshots",
    "event_records",
    "system_event_records",
    "paper_orders",
    "positions",
    "approval_decisions",
    "execution_controls",
    "autopilot_policies",
    "portfolio_risk_reservations",
    "operational_metrics",
    "operational_alerts",
    "market_bars",
    "holding_watches",
    "holding_control_audits",
    "position_reconciliations",
    "trade_ledger",
    "sell_execution_audits",
    "sell_alert_audits",
    "system_cycle_runs",
    "strategy_configs",
    "backtest_runs",
    "source_snapshots",
    "snapshot_securities",
    "snapshot_storage_keys",
    "snapshot_market_bar_refs",
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
    with closing(sqlite3.connect(path_obj)) as conn:
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

    try:
        command.upgrade(cfg, "head")
    except Exception as exc:
        message = str(exc).lower()
        if "already exists" in message:
            raise RuntimeError(
                "Database schema conflict detected. Back up the database and perform "
                "an explicit reviewed repair; automatic database rebuilding is disabled."
            ) from exc
        raise

    if sqlite_path is not None and not _has_required_sqlite_tables(sqlite_path):
        raise RuntimeError(
            "Database schema is missing required tables. Back up and run explicit maintenance."
        )


if __name__ == "__main__":
    init_db()
