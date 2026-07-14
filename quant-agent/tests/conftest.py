from __future__ import annotations

import atexit
import os
import shutil
import sys
import tempfile
from pathlib import Path

# Keep tests deterministic and offline-friendly.
_TEST_DB_DIR = Path(tempfile.mkdtemp(prefix="quant-agent-tests-"))
_TEST_DB_PATH = _TEST_DB_DIR / "quant_agent_test.db"

os.environ["DATABASE_URL"] = f"sqlite:///{_TEST_DB_PATH}"
os.environ["QUANT_AGENT_TEST_MODE"] = "1"
os.environ["DATA_PROVIDER"] = "mock"
os.environ["QUANT_AGENT_ACCESS_PASSWORD"] = "test-access-password"
os.environ.pop("QUANT_BROKER_ADAPTER", None)

_CLEANED_UP = False


def _cleanup_test_database() -> None:
    global _CLEANED_UP
    if _CLEANED_UP:
        return
    _CLEANED_UP = True

    session_module = sys.modules.get("infra.db.session")
    if session_module is not None:
        session_module.dispose_engine()
    shutil.rmtree(_TEST_DB_DIR, ignore_errors=True)


def pytest_sessionfinish(session, exitstatus) -> None:
    _ = (session, exitstatus)
    _cleanup_test_database()


atexit.register(_cleanup_test_database)
