from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.script import ScriptDirectory

from infra.db.init_db import REQUIRED_TABLES, _sqlite_path_from_url
from infra.db.session import get_database_url


class SchemaNotReadyError(RuntimeError):
    def __init__(self, details: dict[str, Any]) -> None:
        self.details = details
        super().__init__(str(details.get("reason", "database_schema_not_ready")))


def check_schema(database_url: str | None = None) -> dict[str, Any]:
    """Inspect SQLite without creating a file or performing migrations."""
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    expected = ScriptDirectory.from_config(config).get_current_head()
    path = _sqlite_path_from_url(database_url or get_database_url(), root)
    details: dict[str, Any] = {"passed": False, "expected_revision": expected, "revision": None}
    if path is None:
        return {**details, "reason": "unsupported_database_readiness_backend"}
    if not path.is_file():
        return {**details, "reason": "database_missing"}
    try:
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            revisions = (
                [row[0] for row in connection.execute("SELECT version_num FROM alembic_version")]
                if "alembic_version" in tables else []
            )
        details["revision"] = revisions[0] if len(revisions) == 1 else None
        details["missing_tables"] = sorted(REQUIRED_TABLES - tables)
        if revisions != [expected] or details["missing_tables"]:
            return {**details, "reason": "database_schema_mismatch"}
        return {**details, "passed": True, "reason": None}
    except (OSError, sqlite3.Error) as exc:
        return {**details, "reason": "database_unreadable", "error_type": type(exc).__name__}


def require_schema() -> None:
    details = check_schema()
    if not details["passed"]:
        raise SchemaNotReadyError(details)
