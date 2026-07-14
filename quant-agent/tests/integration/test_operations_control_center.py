from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app

AUTH_HEADERS = {"x-access-password": "test-access-password"}


def _research_payload(source_snapshot_id: str) -> dict:
    return {
        "run_type": "research_batch",
        "objective": "operation control center test",
        "as_of": "2026-04-10T09:30:00Z",
        "source_snapshot_id": source_snapshot_id,
        "publication": {"top_n": 1, "output_channels": ["api"]},
        "risk_policy": {
            "min_confidence": 0.0,
            "earnings_blackout_minutes": 0,
            "max_name_weight": 0.10,
            "max_sector_weight": 0.30,
            "max_gross_exposure": 1.0,
            "max_correlated_cluster_weight": 0.35,
            "reject_on_material_evidence_conflict": False,
            "event_trading_enabled": True,
        },
        "universe_rules": {
            "min_price": 1,
            "min_avg_dollar_volume": 1000000,
            "max_spread_bps": 100,
            "min_market_cap_usd": 100000000,
            "allowed_sectors": [],
            "max_candidates_after_filter": 50,
        },
    }


def test_operations_control_center_surfaces_next_actions() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json=_research_payload(f"control-center-{uuid4().hex}"),
        headers=AUTH_HEADERS,
    )
    assert run_response.status_code == 200
    recommendation = run_response.json()["recommendations"][0]
    ticker = recommendation["ticker"]
    client.post(f"/portfolio/holdings/{ticker}/close", headers=AUTH_HEADERS)

    pending_response = client.get("/operations/control-center", headers=AUTH_HEADERS)
    assert pending_response.status_code == 200
    pending = pending_response.json()
    assert pending["latest_source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert pending["pending_approval_count"] == 1
    assert pending["approved_ready_to_buy_count"] == 0
    assert pending["pending_approvals"][0]["recommendation_id"] == recommendation["id"]
    assert any(
        action["action_type"] == "approve_recommendation"
        and action["recommendation_id"] == recommendation["id"]
        for action in pending["actions"]
    )

    approval_response = client.post(
        f"/recommendations/{recommendation['id']}/approval",
        json={"decision": "approved", "approver": "ops", "notes": "ready for route"},
        headers=AUTH_HEADERS,
    )
    assert approval_response.status_code == 200

    ready_response = client.get("/operations/control-center", headers=AUTH_HEADERS)
    assert ready_response.status_code == 200
    ready = ready_response.json()
    assert ready["pending_approval_count"] == 0
    assert ready["approved_ready_to_buy_count"] == 1
    assert ready["ready_to_buy"][0]["recommendation_id"] == recommendation["id"]
    assert any(
        action["action_type"] == "route_buy_order"
        and action["endpoint"] == "/paper-orders"
        and action["recommendation_id"] == recommendation["id"]
        for action in ready["actions"]
    )

    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": ticker,
            "qty": 10,
            "buy_price": recommendation["entry_zone_high"],
            "source_recommendation_id": recommendation["id"],
            "note": "control center monitored buy",
            "stop_loss": 99999999,
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200

    alert_response = client.get("/operations/control-center", headers=AUTH_HEADERS)
    assert alert_response.status_code == 200
    alert_state = alert_response.json()
    assert alert_state["sell_alert_count"] >= 1
    assert alert_state["urgent_sell_alert_count"] >= 1
    assert any(alert["ticker"] == ticker for alert in alert_state["sell_alerts"])
    assert any(
        action["action_type"] == "sell_alert"
        and action["ticker"] == ticker
        and action["endpoint"] == f"/portfolio/alerts/{ticker}/execute"
        for action in alert_state["actions"]
    )

    kill_response = client.post(
        "/execution/kill-switch",
        json={"enabled": True, "reason": "control-center-test", "updated_by": "ops"},
        headers=AUTH_HEADERS,
    )
    assert kill_response.status_code == 200

    blocked_response = client.get("/operations/control-center", headers=AUTH_HEADERS)
    assert blocked_response.status_code == 200
    blocked = blocked_response.json()
    assert blocked["kill_switch"]["enabled"] is True
    assert any(action["action_type"] == "execution_blocked" for action in blocked["actions"])

    client.post(
        "/execution/kill-switch",
        json={"enabled": False, "reason": "control-center-test-cleanup", "updated_by": "ops"},
        headers=AUTH_HEADERS,
    )
    client.post(f"/portfolio/holdings/{ticker}/close", headers=AUTH_HEADERS)


def test_operations_system_cycle_endpoint_runs_with_autopilot_policy() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    response = client.post(
        "/operations/system-cycle",
        json={
            "top_n": 1,
            "min_confidence": 0.0,
            "consume_events": False,
            "use_autopilot_policy": True,
            "as_of": "2026-04-10T09:30:00Z",
        },
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["job"] == "system_cycle"
    assert payload["system_cycle_run_id"]
    assert payload["use_autopilot_policy"] is True
    assert payload["autopilot_policy"]["enabled"] is False
    assert payload["autopilot_preflight"]["status"] == "off"
    assert payload["auto_approval"]["enabled"] is False
    assert payload["auto_execution"]["enabled"] is False
    assert payload["recommendation_count"] >= 1
    latest_run = state.list_system_cycle_runs(limit=1)[0]
    assert latest_run.id == payload["system_cycle_run_id"]
