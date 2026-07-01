from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app


AUTH_HEADERS = {"x-access-password": "test-access-password"}


def test_autopilot_policy_api_persists_latest_policy() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    default_response = client.get("/execution/autopilot-policy", headers=AUTH_HEADERS)
    assert default_response.status_code == 200
    default_policy = default_response.json()
    assert default_policy["enabled"] is False
    assert default_policy["auto_approve_recommendations"] is False
    assert default_policy["auto_execute_approved"] is False
    assert default_policy["restrict_auto_execution_to_regular_hours"] is False
    assert default_policy["auto_execution_mode"] == "paper"
    assert default_policy["max_daily_auto_approvals"] == 3
    assert default_policy["max_daily_auto_buys"] == 3
    assert default_policy["max_daily_auto_sells"] == 10
    assert default_policy["order_dedupe_minutes"] == 1440
    assert default_policy["sell_alert_cooldown_minutes"] == 60
    assert default_policy["rebuy_cooldown_minutes"] == 240
    assert default_policy["min_snapshot_bar_coverage"] == 1.0
    assert default_policy["min_snapshot_fundamental_coverage"] == 1.0
    assert default_policy["max_snapshot_bar_age_minutes"] == 4320
    assert default_policy["max_open_risk_pct"] == 0.06
    assert default_policy["max_daily_realized_loss_pct"] == 0.03
    assert default_policy["max_auto_buy_price_drift_pct"] == 0.03
    assert default_policy["require_position_reconciliation"] is False
    assert default_policy["max_position_reconciliation_age_minutes"] == 1440

    update_response = client.post(
        "/execution/autopilot-policy",
        json={
            "enabled": True,
            "auto_approve_recommendations": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "live_dry_run",
            "auto_approve_min_confidence": 0.81,
            "auto_approve_min_composite": 0.35,
            "max_auto_approvals": 2,
            "max_auto_buys": 1,
            "max_auto_sells": 3,
            "max_daily_auto_approvals": 4,
            "max_daily_auto_buys": 5,
            "max_daily_auto_sells": 6,
            "order_dedupe_minutes": 90,
            "sell_alert_cooldown_minutes": 45,
            "rebuy_cooldown_minutes": 120,
            "min_snapshot_bar_coverage": 0.95,
            "min_snapshot_fundamental_coverage": 0.9,
            "max_snapshot_bar_age_minutes": 720,
            "max_open_risk_pct": 0.04,
            "max_daily_realized_loss_pct": 0.025,
            "max_auto_buy_price_drift_pct": 0.015,
            "require_position_reconciliation": False,
            "max_position_reconciliation_age_minutes": 720,
            "account_equity": 10000000,
            "risk_per_trade_pct": 0.005,
            "max_position_pct": 0.08,
            "max_gross_exposure_pct": 0.75,
            "max_sector_exposure_pct": 0.22,
            "reason": "integration-test",
            "updated_by": "qa",
        },
        headers=AUTH_HEADERS,
    )
    assert update_response.status_code == 200
    updated_policy = update_response.json()
    assert updated_policy["policy_id"] > default_policy["policy_id"]
    assert updated_policy["enabled"] is True
    assert updated_policy["auto_approve_recommendations"] is True
    assert updated_policy["auto_execute_approved"] is True
    assert updated_policy["auto_execution_mode"] == "live_dry_run"
    assert updated_policy["auto_approve_min_confidence"] == 0.81
    assert updated_policy["max_auto_approvals"] == 2
    assert updated_policy["max_daily_auto_approvals"] == 4
    assert updated_policy["max_daily_auto_buys"] == 5
    assert updated_policy["max_daily_auto_sells"] == 6
    assert updated_policy["order_dedupe_minutes"] == 90
    assert updated_policy["sell_alert_cooldown_minutes"] == 45
    assert updated_policy["rebuy_cooldown_minutes"] == 120
    assert updated_policy["min_snapshot_bar_coverage"] == 0.95
    assert updated_policy["min_snapshot_fundamental_coverage"] == 0.9
    assert updated_policy["max_snapshot_bar_age_minutes"] == 720
    assert updated_policy["max_open_risk_pct"] == 0.04
    assert updated_policy["max_daily_realized_loss_pct"] == 0.025
    assert updated_policy["max_auto_buy_price_drift_pct"] == 0.015
    assert updated_policy["require_position_reconciliation"] is False
    assert updated_policy["max_position_reconciliation_age_minutes"] == 720
    assert updated_policy["updated_by"] == "qa"

    latest_response = client.get("/execution/autopilot-policy", headers=AUTH_HEADERS)
    assert latest_response.status_code == 200
    assert latest_response.json()["policy_id"] == updated_policy["policy_id"]

    control_center = client.get("/operations/control-center?refresh_alerts=false", headers=AUTH_HEADERS)
    assert control_center.status_code == 200
    assert control_center.json()["autopilot_policy"]["policy_id"] == updated_policy["policy_id"]
    assert control_center.json()["autopilot_preflight"]["status"] == "ready"
    assert control_center.json()["autopilot_preflight"]["can_auto_approve"] is True
    assert control_center.json()["autopilot_preflight"]["can_auto_execute"] is True
    assert control_center.json()["autopilot_preflight"]["daily_usage"]["remaining_buys"] == 5
    preflight_check_names = {
        item["name"] for item in control_center.json()["autopilot_preflight"]["checks"]
    }
    assert "snapshot_quality_policy" in preflight_check_names
    assert "portfolio_open_risk" in preflight_check_names
    assert "daily_realized_loss" in preflight_check_names
    assert "order_dedupe" in preflight_check_names
    assert "sell_alert_cooldown" in preflight_check_names
    assert "auto_buy_price_drift" in preflight_check_names
    assert "position_reconciliation" in preflight_check_names

    realtime = client.get("/dashboard/realtime-data?refresh_alerts=false", headers=AUTH_HEADERS)
    assert realtime.status_code == 200
    assert realtime.json()["autopilot_policy"]["policy_id"] == updated_policy["policy_id"]
    assert realtime.json()["autopilot_preflight"]["status"] == "ready"


def test_autopilot_preflight_blocks_auto_execute_without_required_reconciliation() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    response = client.post(
        "/execution/autopilot-policy",
        json={
            "enabled": True,
            "auto_execute_approved": True,
            "max_auto_buys": 1,
            "max_auto_sells": 1,
            "require_position_reconciliation": True,
            "max_position_reconciliation_age_minutes": 1440,
            "reason": "require-reconciliation-test",
            "updated_by": "qa",
        },
        headers=AUTH_HEADERS,
    )
    assert response.status_code == 200

    control_center = client.get("/operations/control-center?refresh_alerts=false", headers=AUTH_HEADERS)
    assert control_center.status_code == 200
    preflight = control_center.json()["autopilot_preflight"]
    assert preflight["status"] == "blocked"
    assert preflight["can_auto_execute"] is False
    assert "position_reconciliation_missing" in preflight["reasons"]
    reconciliation_check = next(
        item for item in preflight["checks"] if item["name"] == "position_reconciliation"
    )
    assert reconciliation_check["status"] == "fail"
    assert reconciliation_check["details"]["reason"] == "position_reconciliation_missing"


def test_market_session_endpoint_reports_regular_hours() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    open_response = client.get(
        "/execution/market-session?as_of=2026-04-10T13:30:00Z",
        headers=AUTH_HEADERS,
    )
    assert open_response.status_code == 200
    open_session = open_response.json()
    assert open_session["status"] == "regular"
    assert open_session["is_regular_session"] is True
    assert open_session["timezone"] == "America/New_York"

    closed_response = client.get(
        "/execution/market-session?as_of=2026-04-11T13:30:00Z",
        headers=AUTH_HEADERS,
    )
    assert closed_response.status_code == 200
    closed_session = closed_response.json()
    assert closed_session["status"] == "closed"
    assert closed_session["is_regular_session"] is False


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
    recommendation = run_response.json()["recommendations"][0]
    recommendation_id = recommendation["id"]
    cleanup_response = client.post(f"/portfolio/holdings/{recommendation['ticker']}/close", headers=AUTH_HEADERS)
    assert cleanup_response.status_code in {200, 404}

    risk_plan_response = client.post(
        "/paper-orders/risk-plan",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10,
            "limit_price": None,
            "account_equity": 100000,
            "risk_per_trade_pct": 0.01,
            "max_position_pct": 0.10,
        },
        headers=AUTH_HEADERS,
    )
    assert risk_plan_response.status_code == 200
    risk_plan = risk_plan_response.json()
    assert risk_plan["is_within_limits"] is True
    assert risk_plan["recommended_qty"] >= 10
    assert risk_plan["risk_budget"] == 1000
    assert risk_plan["sector"]
    assert risk_plan["max_gross_exposure_value"] == 100000
    assert risk_plan["max_sector_exposure_value"] == 30000
    assert risk_plan["requested_gross_exposure_pct"] > 0
    assert risk_plan["requested_sector_exposure_pct"] > 0

    oversized_plan = client.post(
        "/paper-orders/risk-plan",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10000,
            "limit_price": None,
            "account_equity": 100000,
            "risk_per_trade_pct": 0.01,
            "max_position_pct": 0.10,
        },
        headers=AUTH_HEADERS,
    )
    assert oversized_plan.status_code == 200
    assert oversized_plan.json()["is_within_limits"] is False
    assert "exceeds_per_trade_risk" in oversized_plan.json()["violations"]

    exposure_limited_plan = client.post(
        "/paper-orders/risk-plan",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10,
            "limit_price": None,
            "account_equity": 100000,
            "risk_per_trade_pct": 0.01,
            "max_position_pct": 0.10,
            "max_gross_exposure_pct": 0.001,
            "max_sector_exposure_pct": 0.001,
        },
        headers=AUTH_HEADERS,
    )
    assert exposure_limited_plan.status_code == 200
    exposure_limited = exposure_limited_plan.json()
    assert exposure_limited["is_within_limits"] is False
    assert "exceeds_gross_exposure" in exposure_limited["violations"]
    assert "exceeds_sector_exposure" in exposure_limited["violations"]
    assert exposure_limited["recommended_qty"] == min(
        exposure_limited["max_risk_qty"],
        exposure_limited["max_position_qty"],
        exposure_limited["max_gross_qty"],
        exposure_limited["max_sector_qty"],
    )

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

    oversized_order = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10000,
            "limit_price": None,
            "account_equity": 100000,
            "risk_per_trade_pct": 0.01,
            "max_position_pct": 0.10,
        },
        headers=AUTH_HEADERS,
    )
    assert oversized_order.status_code == 409
    assert oversized_order.json()["detail"]["is_within_limits"] is False

    exposure_limited_order = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10,
            "limit_price": None,
            "account_equity": 100000,
            "risk_per_trade_pct": 0.01,
            "max_position_pct": 0.10,
            "max_gross_exposure_pct": 0.001,
            "max_sector_exposure_pct": 0.001,
        },
        headers=AUTH_HEADERS,
    )
    assert exposure_limited_order.status_code == 409
    assert "exceeds_gross_exposure" in exposure_limited_order.json()["detail"]["violations"]
    assert "exceeds_sector_exposure" in exposure_limited_order.json()["detail"]["violations"]

    blocked_live_order = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10,
            "limit_price": None,
            "execution_mode": "live",
            "dry_run": False,
            "confirm_live": False,
        },
        headers=AUTH_HEADERS,
    )
    assert blocked_live_order.status_code == 409
    assert "Live execution requires" in blocked_live_order.json()["detail"]

    live_dry_run = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10,
            "limit_price": None,
            "execution_mode": "live",
            "dry_run": True,
        },
        headers=AUTH_HEADERS,
    )
    assert live_dry_run.status_code == 200
    live_dry_run_data = live_dry_run.json()
    assert live_dry_run_data["execution_mode"] == "live"
    assert live_dry_run_data["dry_run"] is True
    assert live_dry_run_data["status"] == "submitted"
    assert live_dry_run_data["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert live_dry_run_data["strategy_config_id"] == recommendation["strategy_config_id"]
    assert live_dry_run_data["broker_order_id"].startswith("live_dryrun_")
    assert "not sent to a broker" in live_dry_run_data["adapter_message"]
    holdings_after_dry_run = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_after_dry_run.status_code == 200
    assert all(item["ticker"] != recommendation["ticker"] for item in holdings_after_dry_run.json())

    duplicate_pending_order = client.post(
        "/paper-orders",
        json={"recommendation_id": recommendation_id, "side": "BUY", "qty": 10, "limit_price": None},
        headers=AUTH_HEADERS,
    )
    assert duplicate_pending_order.status_code == 409
    assert duplicate_pending_order.json()["detail"]["reason"] == "same_recommendation_pending_buy_order"

    canceled_live_dry_run = client.post(
        f"/paper-orders/{live_dry_run_data['id']}/cancel",
        json={"reason": "integration cancel dry-run", "canceled_by": "qa"},
        headers=AUTH_HEADERS,
    )
    assert canceled_live_dry_run.status_code == 200
    assert canceled_live_dry_run.json()["status"] == "canceled"
    assert canceled_live_dry_run.json()["cancel_reason"] == "integration cancel dry-run"

    order_response = client.post(
        "/paper-orders",
        json={"recommendation_id": recommendation_id, "side": "BUY", "qty": 10, "limit_price": None},
        headers=AUTH_HEADERS,
    )
    assert order_response.status_code == 200
    assert order_response.json()["status"] == "filled"
    assert order_response.json()["execution_mode"] == "paper"
    assert order_response.json()["dry_run"] is False
    assert order_response.json()["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert order_response.json()["strategy_config_id"] == recommendation["strategy_config_id"]

    cancel_filled_order = client.post(
        f"/paper-orders/{order_response.json()['id']}/cancel",
        json={"reason": "cannot cancel fill", "canceled_by": "qa"},
        headers=AUTH_HEADERS,
    )
    assert cancel_filled_order.status_code == 409
    assert "Only submitted orders can be canceled" in cancel_filled_order.json()["detail"]
