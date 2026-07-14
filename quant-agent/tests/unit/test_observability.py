from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from domain.entities.models import (
    PositionReconciliationReport,
    SourceSnapshotSummary,
    SystemCycleRun,
)
from infra.observability.health import HealthEvaluator
from infra.observability.logging import JsonFormatter, log_context
from infra.observability.metrics import MetricsStore

EXECUTE_HEADERS = {"x-access-password": "test-access-password"}


def test_metrics_survive_store_recreation_and_export_prometheus() -> None:
    state = get_app_state()
    state.reset()

    first = MetricsStore()
    first.inc("research_runs", 2)
    first.set_gauge("database_size_bytes", 123)

    recreated = MetricsStore()
    assert recreated.dump()["counters"]["research_runs"] == 2
    assert recreated.dump()["gauges"]["database_size_bytes"] == 123
    exposition = recreated.prometheus_text()
    assert "# TYPE quant_agent_research_runs counter" in exposition
    assert "quant_agent_research_runs 2" in exposition
    assert "# TYPE quant_agent_database_size_bytes gauge" in exposition


def test_json_logging_includes_correlation_fields_and_redacts_secrets() -> None:
    record = logging.LogRecord(
        name="quant.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request ?pwd=secret-value authorization=Bearer abc.def",
        args=(),
        exc_info=None,
    )
    record.event = "test_event"
    record.order_id = "order-123"
    with log_context(correlation_id="correlation-123", run_id="run-123"):
        payload = json.loads(JsonFormatter().format(record))

    assert payload["correlation_id"] == "correlation-123"
    assert payload["run_id"] == "run-123"
    assert payload["order_id"] == "order-123"
    assert payload["event"] == "test_event"
    assert "secret-value" not in payload["message"]
    assert "abc.def" not in payload["message"]


def test_health_separates_liveness_readiness_and_trading_state() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    live = client.get("/health/live", headers={"x-correlation-id": "health-probe-1"})
    assert live.status_code == 200
    assert live.headers["x-correlation-id"] == "health-probe-1"

    ready = client.get("/health/ready", headers={"x-correlation-id": "health-probe-2"})
    assert ready.status_code == 200
    assert ready.headers["x-correlation-id"] == "health-probe-2"
    report = ready.json()
    assert report["ready"] is True
    assert report["trading_ready"] is False
    assert "source_snapshot_not_ready" in report["trading_blockers"]
    assert "autopilot_disabled" in report["trading_blockers"]
    assert {item["name"] for item in report["checks"]} >= {
        "database",
        "data_provider",
        "broker",
        "source_snapshot",
        "position_reconciliation",
    }


def test_operational_alerts_persist_for_database_growth_and_cycle_failures(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    now = datetime.now(timezone.utc)
    for index in range(2):
        state.record_system_cycle_run(
            SystemCycleRun(
                id=f"error-{index}",
                started_at=now,
                finished_at=now,
                status="error",
                error_message="provider unavailable",
            )
        )
    monkeypatch.setenv("QUANT_AGENT_DB_SIZE_ALERT_BYTES", "1")
    report = HealthEvaluator(state).evaluate(external_probe=False)

    categories = {item["category"] for item in report["active_alerts"]}
    assert "database" in categories
    assert "worker" in categories
    assert report["ready"] is True


def test_operational_alerts_cover_provider_failure_and_reconciliation(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    now = datetime.now(timezone.utc)
    snapshot = SourceSnapshotSummary(
        source_snapshot_id="provider-failure-snapshot",
        created_at=now,
        as_of=now,
        universe="SP500",
        provider_name="test-provider",
        tickers=["AAPL"],
        ticker_count=1,
        bar_count=1,
        fundamental_count=1,
        event_count=0,
        recommendation_count=0,
        data_quality={
            "live_execution_allowed": False,
            "provider_status": "blocked",
            "provider_failure_count": 2,
        },
    )
    monkeypatch.setattr(state, "list_source_snapshots", lambda limit=1: [snapshot])
    state.position_reconciliation_repo.add(
        PositionReconciliationReport(
            reconciliation_id="reconciliation-mismatch",
            broker="test-broker",
            checked_at=now,
            as_of=now,
            status="mismatch",
            blocks_auto_execution=True,
            local_position_count=1,
            broker_position_count=1,
            matched_count=0,
            mismatch_count=1,
            missing_in_broker_count=0,
            broker_only_count=0,
            qty_tolerance=1e-6,
        )
    )
    state.update_autopilot_policy(
        {
            "enabled": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "live",
            "updated_by": "observability-test",
        }
    )

    report = HealthEvaluator(state).evaluate(external_probe=False)
    categories = {item["category"] for item in report["active_alerts"]}
    assert "data_provider" in categories
    assert "reconciliation" in categories
    assert report["ready"] is False
    assert "position_reconciliation_not_ready" in report["trading_blockers"]


def test_prometheus_endpoint_returns_text_format() -> None:
    state = get_app_state()
    state.metrics_store.set_gauge("test_health_value", 1)
    response = TestClient(app).get("/metrics/prometheus", headers=EXECUTE_HEADERS)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "quant_agent_test_health_value 1" in response.text
