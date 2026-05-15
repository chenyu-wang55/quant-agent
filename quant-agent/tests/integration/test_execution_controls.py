from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app


AUTH_HEADERS = {"x-access-password": "test-access-password"}


def test_kill_switch_blocks_paper_orders() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "kill-switch-integration",
            "as_of": "2026-04-10T09:30:00Z",
            "snapshot_mode": "point_in_time",
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
        },
        headers=AUTH_HEADERS,
    )
    assert run_response.status_code == 200
    recommendation_id = run_response.json()["recommendations"][0]["id"]

    approval_response = client.post(
        f"/recommendations/{recommendation_id}/approval",
        json={"decision": "approved", "approver": "qa", "notes": "ok"},
        headers=AUTH_HEADERS,
    )
    assert approval_response.status_code == 200

    switch_on = client.post(
        "/execution/kill-switch",
        json={"enabled": True, "reason": "maintenance", "updated_by": "qa"},
        headers=AUTH_HEADERS,
    )
    assert switch_on.status_code == 200
    assert switch_on.json()["enabled"] is True

    blocked_order = client.post(
        "/paper-orders",
        json={"recommendation_id": recommendation_id, "side": "BUY", "qty": 10, "limit_price": None},
        headers=AUTH_HEADERS,
    )
    assert blocked_order.status_code == 423

    switch_off = client.post(
        "/execution/kill-switch",
        json={"enabled": False, "reason": "resume", "updated_by": "qa"},
        headers=AUTH_HEADERS,
    )
    assert switch_off.status_code == 200
    assert switch_off.json()["enabled"] is False

    order_response = client.post(
        "/paper-orders",
        json={"recommendation_id": recommendation_id, "side": "BUY", "qty": 10, "limit_price": None},
        headers=AUTH_HEADERS,
    )
    assert order_response.status_code == 200
    assert order_response.json()["status"] == "filled"
