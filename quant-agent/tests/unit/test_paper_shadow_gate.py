from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from apps.api.dependencies import AppState, get_app_state
from apps.api.main import app
from domain.entities.models import AutoExecutionMode, AutopilotPolicy, SystemCycleRun
from infra.db.init_db import init_db
from services.execution.market_calendar import xnys_session
from services.ingestion.mock_provider import MockMarketDataProvider

AUTH_HEADERS = {"x-access-password": "test-access-password"}


def _qualified_shadow_metrics() -> dict[str, object]:
    return {
        "autopilot_policy": {
            "enabled": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "paper",
        },
        "autopilot_preflight": {"can_auto_execute": True},
        "snapshot_quality_gate": {"passed": True},
        "auto_execution": {
            "enabled": True,
            "mode": "paper",
            "error_count": 0,
        },
        "paper_shadow_evidence": {
            "schema_version": 1,
            "qualified": True,
        },
    }


def _add_shadow_run(
    state: AppState,
    *,
    run_id: str,
    started_at: datetime,
    metrics: dict[str, object] | None = None,
    auto_execution_enabled: bool = True,
) -> None:
    state.system_cycle_run_repo.add(
        SystemCycleRun(
            id=run_id,
            started_at=started_at,
            finished_at=started_at + timedelta(minutes=1),
            status="success",
            auto_execution_enabled=auto_execution_enabled,
            metrics=metrics or _qualified_shadow_metrics(),
        )
    )


def _trading_datetimes(count: int) -> list[datetime]:
    cursor = datetime(2026, 1, 2, 15, 0, tzinfo=timezone.utc)
    trading_dates: list[datetime] = []
    while len(trading_dates) < count:
        if xnys_session(cursor.date()).is_trading_day:
            trading_dates.append(cursor)
        cursor += timedelta(days=1)
    return trading_dates


def test_paper_autopilot_can_run_before_shadow_gate_but_live_cannot(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "0")
    init_db()
    state = AppState(provider=MockMarketDataProvider())
    state.set_kill_switch(False, "paper shadow bootstrap test", "test")
    run_at = datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)
    paper_policy = AutopilotPolicy(
        enabled=True,
        auto_execute_approved=True,
        auto_execution_mode=AutoExecutionMode.PAPER,
        restrict_auto_execution_to_regular_hours=True,
        max_auto_buys=1,
        max_auto_sells=1,
        min_paper_shadow_trading_days=10_000,
    )

    paper_preflight = state.build_autopilot_preflight(paper_policy, as_of=run_at)
    assert paper_preflight.can_auto_execute is True
    shadow_check = next(check for check in paper_preflight.checks if check.name == "paper_shadow_readiness")
    assert shadow_check.status == "warn"

    live_preflight = state.build_autopilot_preflight(
        paper_policy.model_copy(update={"auto_execution_mode": AutoExecutionMode.LIVE}),
        as_of=run_at,
        allow_auto_live_execution=True,
    )
    assert live_preflight.can_auto_execute is False
    assert "paper_shadow_period_incomplete" in live_preflight.reasons


def test_paper_shadow_runs_require_explicit_complete_autopilot_evidence(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "0")
    init_db()
    state = AppState(provider=MockMarketDataProvider())
    run_at = _trading_datetimes(1)[0]
    invalid_cases: list[tuple[str, dict[str, object], bool]] = []

    missing_evidence = _qualified_shadow_metrics()
    missing_evidence.pop("paper_shadow_evidence")
    invalid_cases.append(("missing-evidence", missing_evidence, True))

    disabled_policy = deepcopy(_qualified_shadow_metrics())
    disabled_policy["autopilot_policy"]["enabled"] = False  # type: ignore[index]
    invalid_cases.append(("disabled-policy", disabled_policy, True))

    failed_quality = deepcopy(_qualified_shadow_metrics())
    failed_quality["snapshot_quality_gate"]["passed"] = False  # type: ignore[index]
    invalid_cases.append(("failed-quality", failed_quality, True))

    live_mode = deepcopy(_qualified_shadow_metrics())
    live_mode["autopilot_policy"]["auto_execution_mode"] = "live"  # type: ignore[index]
    live_mode["auto_execution"]["mode"] = "live"  # type: ignore[index]
    invalid_cases.append(("live-mode", live_mode, True))

    failed_run = deepcopy(_qualified_shadow_metrics())
    failed_run["paper_shadow_evidence"]["qualified"] = False  # type: ignore[index]
    invalid_cases.append(("failed-evidence", failed_run, True))

    invalid_cases.append(("disabled-effective-path", _qualified_shadow_metrics(), False))

    for index, (name, metrics, enabled) in enumerate(invalid_cases):
        _add_shadow_run(
            state,
            run_id=f"invalid-shadow-{name}",
            started_at=run_at + timedelta(minutes=index),
            metrics=metrics,
            auto_execution_enabled=enabled,
        )

    gate = state.get_paper_shadow_gate(required_trading_days=1, as_of=run_at + timedelta(days=1))
    assert gate["passed"] is False
    assert gate["observed_trading_days"] == 0


def test_live_readiness_requires_twenty_distinct_successful_paper_trading_days(
    monkeypatch,
) -> None:
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "0")
    init_db()
    state = AppState(provider=MockMarketDataProvider())
    trading_dates = _trading_datetimes(20)

    for index, started_at in enumerate(trading_dates[:19]):
        _add_shadow_run(state, run_id=f"shadow-{index}", started_at=started_at)

    _add_shadow_run(
        state,
        run_id="shadow-duplicate-date",
        started_at=trading_dates[0] + timedelta(hours=1),
    )
    blocked = state.get_paper_shadow_gate(
        required_trading_days=20,
        as_of=trading_dates[-1] + timedelta(days=1),
    )
    assert blocked["passed"] is False
    assert blocked["observed_trading_days"] == 19
    assert blocked["remaining_trading_days"] == 1

    _add_shadow_run(state, run_id="shadow-20", started_at=trading_dates[-1])
    ready = state.get_paper_shadow_gate(
        required_trading_days=20,
        as_of=trading_dates[-1] + timedelta(days=1),
    )
    assert ready["passed"] is True
    assert ready["observed_trading_days"] == 20
    assert ready["remaining_trading_days"] == 0


def test_paper_shadow_readiness_endpoint_reports_progress_while_autopilot_is_off(
    monkeypatch,
) -> None:
    state = get_app_state()
    state.reset()
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "0")
    client = TestClient(app)

    anonymous = client.get("/execution/paper-shadow-readiness")
    assert anonymous.status_code == 401

    empty = client.get("/execution/paper-shadow-readiness", headers=AUTH_HEADERS)
    assert empty.status_code == 200
    assert empty.json()["passed"] is False
    assert empty.json()["required_trading_days"] == 20
    assert empty.json()["observed_trading_days"] == 0
    assert empty.json()["remaining_trading_days"] == 20
    assert empty.json()["bypassed_for_test"] is False
    assert state.get_autopilot_policy().enabled is False

    trading_dates = _trading_datetimes(20)
    for index, started_at in enumerate(trading_dates):
        _add_shadow_run(state, run_id=f"endpoint-shadow-{index}", started_at=started_at)
    _add_shadow_run(
        state,
        run_id="endpoint-shadow-duplicate",
        started_at=trading_dates[0] + timedelta(hours=1),
    )

    complete = client.get(
        "/execution/paper-shadow-readiness",
        params={"as_of": (trading_dates[-1] + timedelta(days=1)).isoformat()},
        headers=AUTH_HEADERS,
    )
    assert complete.status_code == 200
    assert complete.json()["passed"] is True
    assert complete.json()["observed_trading_days"] == 20
    assert complete.json()["remaining_trading_days"] == 0
    assert complete.json()["first_trading_date"]
    assert complete.json()["last_trading_date"]
    assert state.get_autopilot_policy().enabled is False
