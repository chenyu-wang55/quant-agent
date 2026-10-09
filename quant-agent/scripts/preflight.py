from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DATA_ENV_NAMES = {
    "universe": "POINT_IN_TIME_UNIVERSE_CSV",
    "fundamentals": "POINT_IN_TIME_FUNDAMENTALS_CSV",
    "events": "POINT_IN_TIME_EVENTS_CSV",
    "earnings": "POINT_IN_TIME_EARNINGS_CSV",
}


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def _env_text(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_flag(name: str) -> bool:
    return _env_text(name).lower() in {"1", "true", "yes", "on"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _locked_versions(lock_path: Path) -> dict[str, str]:
    locked: dict[str, str] = {}
    for raw_line in lock_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "==" not in line:
            continue
        name, version = line.split("==", 1)
        locked[name.lower().replace("_", "-")] = version.split(";", 1)[0].strip()
    return locked


def check_python_environment(root: Path) -> list[Check]:
    expected = (root / ".venv" / "bin" / "python").resolve()
    actual = Path(sys.executable).resolve()
    isolated = sys.prefix != sys.base_prefix and actual == expected
    checks = [
        Check(
            name="python_environment",
            status="pass" if isolated else "fail",
            message=("running from repository .venv" if isolated else "run preflight with .venv/bin/python"),
            details={"actual": str(actual), "expected": str(expected), "version": sys.version.split()[0]},
        )
    ]

    lock_path = root / "requirements.lock"
    if not lock_path.is_file():
        checks.append(Check("dependency_lock", "fail", "requirements.lock is missing"))
        return checks
    missing: list[str] = []
    mismatched: list[dict[str, str]] = []
    for name, expected_version in _locked_versions(lock_path).items():
        try:
            installed = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            missing.append(name)
            continue
        if installed != expected_version:
            mismatched.append({"package": name, "installed": installed, "locked": expected_version})
    passed = not missing and not mismatched
    checks.append(
        Check(
            name="dependency_lock",
            status="pass" if passed else "fail",
            message="installed dependencies match requirements.lock" if passed else "dependency drift detected",
            details={"missing": missing, "mismatched": mismatched},
        )
    )
    return checks


def check_runtime_security() -> list[Check]:
    test_mode = _env_flag("QUANT_AGENT_TEST_MODE")
    auth_disabled = _env_flag("QUANT_AGENT_DISABLE_AUTH")
    access_password = _env_text("QUANT_AGENT_ACCESS_PASSWORD")
    signing_secret = _env_text("QUANT_AGENT_AUTH_SIGNING_SECRET")
    return [
        Check(
            "runtime_mode",
            "pass" if not test_mode else "fail",
            "production mode is active" if not test_mode else "QUANT_AGENT_TEST_MODE must be disabled",
        ),
        Check(
            "authentication",
            "pass" if not auth_disabled and len(access_password) >= 12 and len(signing_secret) >= 32 else "fail",
            "authentication secrets are configured"
            if not auth_disabled and len(access_password) >= 12 and len(signing_secret) >= 32
            else "authentication must be enabled with sufficiently long secrets",
            details={
                "auth_disabled": auth_disabled,
                "access_password_configured": bool(access_password),
                "signing_secret_configured": bool(signing_secret),
            },
        ),
    ]


def check_point_in_time_data() -> list[Check]:
    checks: list[Check] = []
    paths: dict[str, Path] = {}
    for key, env_name in _DATA_ENV_NAMES.items():
        value = _env_text(env_name)
        path = Path(value).expanduser().resolve() if value else None
        passed = path is not None and path.is_file()
        checks.append(
            Check(
                f"point_in_time_{key}",
                "pass" if passed else "fail",
                f"{env_name} is readable" if passed else f"{env_name} is missing or not a file",
                details={"path": str(path) if path is not None else None},
            )
        )
        if path is not None and path.is_file():
            paths[key] = path

    report_value = _env_text("POINT_IN_TIME_VALIDATION_REPORT")
    report_path = Path(report_value).expanduser().resolve() if report_value else None
    if report_path is None or not report_path.is_file():
        checks.append(
            Check(
                "point_in_time_validation_report",
                "fail",
                "POINT_IN_TIME_VALIDATION_REPORT is missing or not a file",
                details={"path": str(report_path) if report_path is not None else None},
            )
        )
        return checks

    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        checks.append(
            Check(
                "point_in_time_validation_report",
                "fail",
                "validation report is unreadable or invalid JSON",
                details={"error_type": type(exc).__name__},
            )
        )
        return checks

    fingerprint = str(report.get("dataset_fingerprint") or "").lower()
    report_files = report.get("files") if isinstance(report.get("files"), dict) else {}
    file_mismatches: list[str] = []
    for key, path in paths.items():
        details = report_files.get(key) if isinstance(report_files.get(key), dict) else {}
        expected_hash = str(details.get("sha256") or "").lower()
        if not _SHA256_RE.fullmatch(expected_hash) or _sha256(path) != expected_hash:
            file_mismatches.append(key)
    passed = (
        report.get("status") == "verified"
        and _SHA256_RE.fullmatch(fingerprint) is not None
        and len(paths) == len(_DATA_ENV_NAMES)
        and not file_mismatches
    )
    checks.append(
        Check(
            "point_in_time_validation_report",
            "pass" if passed else "fail",
            "validation fingerprint matches all configured data files"
            if passed
            else "validation report is unverified, incomplete, or stale",
            details={
                "path": str(report_path),
                "status": report.get("status"),
                "dataset_fingerprint": fingerprint or None,
                "file_mismatches": file_mismatches,
            },
        )
    )
    return checks


def _sqlite_path(database_url: str) -> Path | None:
    prefix = "sqlite:///"
    if not database_url.startswith(prefix):
        return None
    return Path(database_url[len(prefix) :]).expanduser().resolve()


def check_database(root: Path, require_kill_switch: str) -> list[Check]:
    database_url = _env_text("DATABASE_URL")
    path = _sqlite_path(database_url)
    if path is None or not path.is_absolute() or not path.is_file():
        return [
            Check(
                "database",
                "fail",
                "DATABASE_URL must point to an existing absolute SQLite database",
                details={"configured": bool(database_url), "path": str(path) if path else None},
            )
        ]

    expected_revision = max(
        "_".join(item.stem.split("_")[:2])
        for item in (root / "alembic" / "versions").glob("*.py")
    )
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        revision_row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        kill_row = connection.execute("SELECT enabled, reason FROM execution_controls ORDER BY id DESC LIMIT 1").fetchone()
        connection.close()
    except (sqlite3.Error, OSError) as exc:
        return [Check("database", "fail", "database inspection failed", {"error": str(exc)})]

    revision = str(revision_row[0]) if revision_row else None
    database_passed = quick_check == "ok" and revision == expected_revision
    checks = [
        Check(
            "database",
            "pass" if database_passed else "fail",
            "database integrity and migration revision are current"
            if database_passed
            else "database integrity or migration revision is not current",
            details={
                "path": str(path),
                "quick_check": quick_check,
                "revision": revision,
                "expected_revision": expected_revision,
            },
        )
    ]
    enabled = bool(kill_row[0]) if kill_row else None
    kill_passed = require_kill_switch == "any" or (
        enabled is not None and enabled == (require_kill_switch == "enabled")
    )
    checks.append(
        Check(
            "kill_switch_initial_state",
            "pass" if kill_passed else "fail",
            f"kill switch state satisfies requirement: {require_kill_switch}"
            if kill_passed
            else f"kill switch must be {require_kill_switch}",
            details={"enabled": enabled, "reason": kill_row[1] if kill_row else None},
        )
    )
    return checks


def check_broker(target: str) -> Check:
    adapter = _env_text("QUANT_BROKER_ADAPTER").lower()
    base_url = _env_text("ALPACA_BASE_URL")
    key_set = bool(_env_text("ALPACA_API_KEY") or _env_text("APCA_API_KEY_ID"))
    secret_set = bool(_env_text("ALPACA_SECRET_KEY") or _env_text("APCA_API_SECRET_KEY"))
    if target == "paper":
        passed = adapter in {"", "alpaca"} and (adapter != "alpaca" or (key_set and secret_set))
        if adapter == "alpaca":
            passed = passed and base_url.rstrip("/") == "https://paper-api.alpaca.markets"
    else:
        passed = (
            adapter == "alpaca"
            and key_set
            and secret_set
            and base_url.rstrip("/") == "https://api.alpaca.markets"
        )
    return Check(
        "broker_configuration",
        "pass" if passed else "fail",
        "broker configuration matches target" if passed else "broker configuration is incomplete or unsafe",
        details={
            "target": target,
            "adapter": adapter or None,
            "base_url": base_url or None,
            "api_key_configured": key_set,
            "api_secret_configured": secret_set,
        },
    )


def check_logging() -> Check:
    value = _env_text("QUANT_AGENT_LOG_FILE")
    path = Path(value).expanduser().resolve() if value else None
    parent = path.parent if path is not None else None
    passed = parent is not None and parent.is_dir() and os.access(parent, os.W_OK)
    return Check(
        "logging",
        "pass" if passed else "fail",
        "structured log directory is writable" if passed else "QUANT_AGENT_LOG_FILE parent must exist and be writable",
        details={"path": str(path) if path is not None else None},
    )


def run_preflight(root: Path, target: str, require_kill_switch: str) -> dict[str, Any]:
    checks = [
        *check_python_environment(root),
        *check_runtime_security(),
        *check_point_in_time_data(),
        *check_database(root, require_kill_switch),
        check_broker(target),
        check_logging(),
    ]
    passed = all(check.status == "pass" for check in checks)
    return {
        "status": "ready" if passed else "blocked",
        "target": target,
        "passed": passed,
        "checks": [asdict(check) for check in checks],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail-closed production preflight for quant-agent")
    parser.add_argument("--target", choices=["paper", "live"], default="paper")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--require-kill-switch",
        choices=["enabled", "disabled", "any"],
        default="enabled",
        help="expected safe initial state; use disabled only during an explicitly supervised window",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_preflight(args.repo_root.expanduser().resolve(), args.target, args.require_kill_switch)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
