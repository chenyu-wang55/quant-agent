from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from alembic import command

ROOT = Path(__file__).resolve().parents[2]


def _config(database_path: Path) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{database_path}")
    return config


def _revision(database_path: Path) -> str:
    with closing(sqlite3.connect(database_path)) as connection:
        return str(connection.execute("SELECT version_num FROM alembic_version").fetchone()[0])


def test_migration_graph_has_one_head_and_fresh_database_reaches_it(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "fresh-migration.db"
    config = _config(database_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_path}")
    heads = ScriptDirectory.from_config(config).get_heads()

    assert heads == ["20260411_0035"]
    command.upgrade(config, "head")
    assert _revision(database_path) == heads[0]
    with closing(sqlite3.connect(database_path)) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {
            "operational_metrics",
            "operational_alerts",
            "portfolio_risk_reservations",
            "snapshot_market_bar_refs",
        }.issubset(tables)
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(market_bars)")
        }
        assert "content_hash" in columns
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_observability_migrations_round_trip_from_previous_release(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "migration-round-trip.db"
    config = _config(database_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_path}")

    command.upgrade(config, "head")
    command.downgrade(config, "20260411_0031")
    assert _revision(database_path) == "20260411_0031"
    with closing(sqlite3.connect(database_path)) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "operational_metrics" not in tables
        assert "operational_alerts" not in tables
        assert "portfolio_risk_reservations" not in tables
        policy_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(autopilot_policies)")
        }
        assert "min_paper_shadow_trading_days" not in policy_columns

    command.upgrade(config, "head")
    assert _revision(database_path) == "20260411_0035"
    with closing(sqlite3.connect(database_path)) as connection:
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
