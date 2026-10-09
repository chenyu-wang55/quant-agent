from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from scripts import preflight


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _configure_point_in_time_files(tmp_path: Path, monkeypatch) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for key, env_name in preflight._DATA_ENV_NAMES.items():
        path = tmp_path / f"{key}.csv"
        path.write_text(f"kind,value\n{key},verified\n", encoding="utf-8")
        monkeypatch.setenv(env_name, str(path))
        paths[key] = path
    return paths


def test_point_in_time_check_accepts_retained_matching_report(tmp_path: Path, monkeypatch) -> None:
    paths = _configure_point_in_time_files(tmp_path, monkeypatch)
    report_path = tmp_path / "validation.json"
    report_path.write_text(
        json.dumps(
            {
                "status": "verified",
                "dataset_fingerprint": "a" * 64,
                "files": {key: {"sha256": _sha256(path)} for key, path in paths.items()},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("POINT_IN_TIME_VALIDATION_REPORT", str(report_path))

    checks = preflight.check_point_in_time_data()

    assert all(check.status == "pass" for check in checks)
    assert checks[-1].details["dataset_fingerprint"] == "a" * 64


def test_point_in_time_check_rejects_stale_report(tmp_path: Path, monkeypatch) -> None:
    paths = _configure_point_in_time_files(tmp_path, monkeypatch)
    report_path = tmp_path / "validation.json"
    report_path.write_text(
        json.dumps(
            {
                "status": "verified",
                "dataset_fingerprint": "b" * 64,
                "files": {key: {"sha256": _sha256(path)} for key, path in paths.items()},
            }
        ),
        encoding="utf-8",
    )
    paths["events"].write_text("changed after validation\n", encoding="utf-8")
    monkeypatch.setenv("POINT_IN_TIME_VALIDATION_REPORT", str(report_path))

    validation = preflight.check_point_in_time_data()[-1]

    assert validation.status == "fail"
    assert validation.details["file_mismatches"] == ["events"]


def test_database_check_requires_current_revision_and_safe_kill_switch(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "repo"
    versions = root / "alembic" / "versions"
    versions.mkdir(parents=True)
    (versions / "20260720_0001_initial.py").write_text("revision = '20260720_0001'\n")
    database = root / "quant.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE alembic_version (version_num TEXT NOT NULL)")
    connection.execute("INSERT INTO alembic_version VALUES ('20260720_0001')")
    connection.execute(
        "CREATE TABLE execution_controls (id INTEGER PRIMARY KEY, enabled INTEGER, reason TEXT)"
    )
    connection.execute("INSERT INTO execution_controls VALUES (1, 1, 'safe startup')")
    connection.commit()
    connection.close()
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database}")

    checks = preflight.check_database(root, "enabled")

    assert [check.status for check in checks] == ["pass", "pass"]
    assert checks[0].details["quick_check"] == "ok"


def test_broker_check_separates_paper_and_live_endpoints(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_BROKER_ADAPTER", "alpaca")
    monkeypatch.setenv("ALPACA_API_KEY", "key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "secret")
    monkeypatch.setenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    assert preflight.check_broker("paper").status == "pass"
    assert preflight.check_broker("live").status == "fail"

    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
    assert preflight.check_broker("live").status == "pass"


def test_runtime_security_fails_closed_for_test_mode_and_short_secrets(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "1")
    monkeypatch.setenv("QUANT_AGENT_DISABLE_AUTH", "0")
    monkeypatch.setenv("QUANT_AGENT_ACCESS_PASSWORD", "short")
    monkeypatch.setenv("QUANT_AGENT_AUTH_SIGNING_SECRET", "short")

    checks = preflight.check_runtime_security()

    assert [check.status for check in checks] == ["fail", "fail"]
