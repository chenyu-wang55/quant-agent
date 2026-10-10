from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from alembic.config import Config
from fastapi.testclient import TestClient

from alembic import command
from apps.api import dependencies
from apps.api.dependencies import AppState, get_app_state
from apps.api.main import app
from infra.db.execution_authorization import authorize_submission
from infra.db.schema_readiness import check_schema

ROOT = Path(__file__).resolve().parents[2]


def test_process_with_stale_control_cannot_authorize_submission() -> None:
    state = get_app_state()
    state.reset()
    script = '''
from apps.api.dependencies import AppState
from datetime import datetime, timezone
from domain.entities.models import Direction, Recommendation, RiskLevel, PaperOrderRequest
from infra.db.session import dispose_engine
s = AppState()
assert not s.kill_switch.enabled
recommendation = Recommendation(
    id="cross-process-rec", generated_at=datetime.now(timezone.utc), ticker="TEST", direction=Direction.BUY,
    entry_zone_low=100, entry_zone_high=102, stop_loss=90, tp1=110, tp2=120,
    holding_period="10 days", confidence=0.8, risk_grade=RiskLevel.MEDIUM,
    thesis=[], invalid_if=[], explanation="control regression",
    score_vector={}, source_snapshot_id="control-snapshot", feature_snapshot_id="control-feature",
    signal_snapshot_id="control-signal", pattern_template="pullback",
)
calls = []
def send(**kwargs):
    calls.append(kwargs)
    raise AssertionError("broker/router I/O must not be reached")
s.execution_router.submit = send
print("ready", flush=True)
input()
try:
    s.submit_order(recommendation=recommendation, request=PaperOrderRequest(
        recommendation_id=recommendation.id, qty=1, enforce_risk_limits=False,
    ))
except ValueError as exc:
    assert "kill switch" in str(exc)
    assert s.get_kill_switch().enabled
    assert calls == []
    print("blocked", flush=True)
else:
    raise AssertionError("stale control granted permission")
finally:
    dispose_engine()
'''
    with subprocess.Popen([sys.executable, "-u", "-c", script], cwd=ROOT, env=os.environ.copy(),
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
        assert child.stdout.readline().strip() == "ready"
        state.set_kill_switch(True, "cross-process regression", "test")
        output, errors = child.communicate("submit\n", timeout=15)
        assert child.returncode == 0, errors
        assert output.strip() == "blocked"


def test_authorization_fails_closed_on_database_error(monkeypatch) -> None:
    from sqlalchemy.exc import OperationalError

    from infra.db import execution_authorization
    def unavailable():
        raise OperationalError("read control", {}, RuntimeError("database unavailable"))
    monkeypatch.setattr(execution_authorization, "SessionLocal", unavailable)
    with pytest.raises(ValueError, match="execution_control_unavailable"):
        authorize_submission(order_id="unavailable", execution_mode="live")


def test_instance_refreshes_control_from_another_instance() -> None:
    first = get_app_state()
    first.reset()
    second = AppState()
    assert not second.kill_switch.enabled
    first.set_kill_switch(True, "refresh", "test")
    assert second.get_kill_switch().enabled


@pytest.mark.parametrize("initial", ["missing", "old", "empty"])
def test_business_and_health_requests_never_migrate_or_create_database(tmp_path, monkeypatch, initial) -> None:
    path = tmp_path / "readiness.db"
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{path}")
    if initial == "old":
        command.upgrade(config, "20260411_0035")
    elif initial == "empty":
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("CREATE TABLE preserved (value TEXT)")
    before = path.read_bytes() if path.exists() else None
    monkeypatch.setattr(dependencies, "_APP_STATE", None)
    with TestClient(app) as client:
        for route in ("/health/ready", "/recommendations", "/execution/kill-switch"):
            response = client.get(route, headers={"x-access-password": "test-access-password"})
            assert response.status_code == 503, response.text
            assert response.json()["maintenance_required"]
        assert client.get("/health/live").status_code == 200
    assert (path.read_bytes() if path.exists() else None) == before
    assert not check_schema()["passed"]
