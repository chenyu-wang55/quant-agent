from __future__ import annotations

import os
from pathlib import Path

import pytest

from apps.api.dependencies import AppState
from infra.db.session import get_database_url


def _sqlite_path(database_url: str) -> Path:
    assert database_url.startswith("sqlite:///")
    return Path(database_url.removeprefix("sqlite:///"))


def test_suite_uses_disposable_database_outside_repository() -> None:
    database_path = _sqlite_path(get_database_url()).resolve()
    production_path = (Path(__file__).resolve().parents[2] / "quant_agent_v2.db").resolve()

    assert os.environ["QUANT_AGENT_TEST_MODE"] == "1"
    assert database_path != production_path
    assert database_path.name == "quant_agent_test.db"
    assert database_path.parent.name.startswith("quant-agent-tests-")


def test_destructive_reset_requires_explicit_runtime_authorization(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUANT_AGENT_TEST_MODE", raising=False)
    monkeypatch.delenv("QUANT_AGENT_ALLOW_DESTRUCTIVE_RESET", raising=False)

    state = object.__new__(AppState)
    with pytest.raises(RuntimeError, match="disabled outside test mode"):
        state.reset()
