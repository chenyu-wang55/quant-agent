from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from domain.entities.models import Direction, OrderExecutionMode, PaperOrder, PaperOrderStatus
from infra.queue.events import EventType
from services.execution.broker_adapter import BrokerOrderPlacement, BrokerOrderUpdate
from services.execution.router import ExecutionRouter


AUTH_HEADERS = {"x-access-password": "test-access-password"}


class FakeBrokerAdapter:
    name = "fake-broker"

    def __init__(self, *, raw_status: str, fill_price: float | None = None) -> None:
        self.raw_status = raw_status
        self.fill_price = fill_price
        self.placements: list[BrokerOrderPlacement] = []
        self.cancelled_order_ids: list[str] = []

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        self.placements.append(placement)
        return BrokerOrderUpdate(
            broker_order_id="fake_broker_order_123",
            raw_status=self.raw_status,
            client_order_id=placement.client_order_id,
            submitted_at=datetime.now(timezone.utc),
            filled_at=datetime.now(timezone.utc) if self.raw_status == "filled" else None,
            filled_avg_price=self.fill_price,
        )

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        raise AssertionError("not used in this test")

    def cancel_order(self, broker_order_id: str) -> BrokerOrderUpdate:
        self.cancelled_order_ids.append(broker_order_id)
        return BrokerOrderUpdate(
            broker_order_id=broker_order_id,
            raw_status="canceled",
            message="broker cancel accepted",
        )


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


def test_autopilot_live_mode_requires_runtime_allow(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    monkeypatch.delenv("QUANT_ALLOW_AUTOPILOT_LIVE", raising=False)
    client = TestClient(app)

    response = client.post(
        "/execution/autopilot-policy",
        json={
            "enabled": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "live",
            "max_auto_buys": 1,
            "max_auto_sells": 1,
            "reason": "live-mode-runtime-allow-test",
            "updated_by": "qa",
        },
        headers=AUTH_HEADERS,
    )
    assert response.status_code == 200
    assert response.json()["auto_execution_mode"] == "live"

    control_center = client.get("/operations/control-center?refresh_alerts=false", headers=AUTH_HEADERS)
    assert control_center.status_code == 200
    preflight = control_center.json()["autopilot_preflight"]
    assert preflight["status"] == "blocked"
    assert preflight["can_auto_execute"] is False
    assert "auto_live_execution_not_allowed" in preflight["reasons"]
    live_check = next(item for item in preflight["checks"] if item["name"] == "auto_live_execution")
    assert live_check["status"] == "fail"
    assert live_check["details"]["requested"] is True
    assert live_check["details"]["allow_auto_live_execution"] is False

    allowed_preflight = state.build_autopilot_preflight(
        state.get_autopilot_policy(),
        allow_auto_live_execution=True,
    )
    assert allowed_preflight.status == "ready"
    assert allowed_preflight.can_auto_execute is True
    allowed_check = next(item for item in allowed_preflight.checks if item.name == "auto_live_execution")
    assert allowed_check.status == "pass"
    assert allowed_check.details["allow_auto_live_execution"] is True


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

    unconfigured_live_order = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 10,
            "limit_price": None,
            "execution_mode": "live",
            "dry_run": False,
            "confirm_live": True,
        },
        headers=AUTH_HEADERS,
    )
    assert unconfigured_live_order.status_code == 501
    assert "Live broker execution adapter is not configured" in unconfigured_live_order.json()["detail"]

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

    filled_live_dry_run = client.post(
        f"/paper-orders/{live_dry_run_data['id']}/fill",
        json={
            "fill_price": recommendation["entry_zone_high"],
            "filled_by": "qa",
            "apply_to_ledger": False,
        },
        headers=AUTH_HEADERS,
    )
    assert filled_live_dry_run.status_code == 200
    filled_live_dry_run_data = filled_live_dry_run.json()
    assert filled_live_dry_run_data["status"] == "filled"
    assert filled_live_dry_run_data["simulated_fill_price"] == recommendation["entry_zone_high"]
    assert filled_live_dry_run_data["filled_at"] is not None
    assert "filled_by=qa" in filled_live_dry_run_data["adapter_message"]
    holdings_after_fill = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_after_fill.status_code == 200
    assert all(item["ticker"] != recommendation["ticker"] for item in holdings_after_fill.json())

    cancel_filled_dry_run = client.post(
        f"/paper-orders/{live_dry_run_data['id']}/cancel",
        json={"reason": "cannot cancel fill", "canceled_by": "qa"},
        headers=AUTH_HEADERS,
    )
    assert cancel_filled_dry_run.status_code == 409
    assert "Only submitted orders can be canceled" in cancel_filled_dry_run.json()["detail"]

    second_live_dry_run = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "BUY",
            "qty": 5,
            "limit_price": None,
            "execution_mode": "live",
            "dry_run": True,
        },
        headers=AUTH_HEADERS,
    )
    assert second_live_dry_run.status_code == 200
    assert second_live_dry_run.json()["status"] == "submitted"

    canceled_live_dry_run = client.post(
        f"/paper-orders/{second_live_dry_run.json()['id']}/cancel",
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


def test_submitted_paper_order_fill_can_apply_to_ledger() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "submitted-fill-ledger-test",
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
    state.close_holding(recommendation["ticker"])

    recommendation_obj = state.recommendations_by_id[recommendation_id]
    order = PaperOrder(
        id=f"broker_fill_{uuid4().hex[:10]}",
        recommendation_id=recommendation_id,
        source_snapshot_id=recommendation["source_snapshot_id"],
        strategy_config_id=recommendation["strategy_config_id"],
        side=Direction.BUY,
        qty=3,
        limit_price=None,
        execution_mode=OrderExecutionMode.LIVE,
        dry_run=False,
        broker_order_id="broker_order_fill_test",
        adapter_message="broker accepted",
        submitted_at=datetime.now(timezone.utc),
        status=PaperOrderStatus.SUBMITTED,
    )
    state.record_paper_order(order, recommendation=recommendation_obj)

    fill_price = recommendation["entry_zone_high"]
    fill_response = client.post(
        f"/paper-orders/{order.id}/fill",
        json={
            "fill_price": fill_price,
            "filled_by": "broker-webhook",
            "note": "broker fill",
        },
        headers=AUTH_HEADERS,
    )
    assert fill_response.status_code == 200
    filled = fill_response.json()
    assert filled["status"] == "filled"
    assert filled["simulated_fill_price"] == fill_price
    assert filled["filled_at"] is not None
    assert "filled_by=broker-webhook" in filled["adapter_message"]
    assert "apply_to_ledger=true" in filled["adapter_message"]

    persisted = state.get_paper_order(order.id)
    assert persisted is not None
    assert persisted.status == PaperOrderStatus.FILLED
    assert persisted.simulated_fill_price == fill_price

    holdings = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings.status_code == 200
    holding = next(item for item in holdings.json() if item["ticker"] == recommendation["ticker"])
    assert holding["qty"] == 3
    assert holding["avg_buy_price"] == fill_price
    assert holding["source_recommendation_id"] == recommendation_id

    trades = client.get(
        f"/portfolio/trades?ticker={recommendation['ticker']}&side=buy",
        headers=AUTH_HEADERS,
    )
    assert trades.status_code == 200
    assert any(
        item["source_recommendation_id"] == recommendation_id
        and item["price"] == fill_price
        and (item["reason"] or "").startswith(f"paper_order_fill:{order.id}")
        for item in trades.json()
    )


def test_confirmed_live_buy_uses_configured_broker_adapter_and_records_fill() -> None:
    state = get_app_state()
    state.reset()
    original_router = state.execution_router
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "configured-live-broker-test",
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
    state.close_holding(recommendation["ticker"])

    approval_response = client.post(
        f"/recommendations/{recommendation_id}/approval",
        json={"decision": "approved", "approver": "qa", "notes": "ok"},
        headers=AUTH_HEADERS,
    )
    assert approval_response.status_code == 200

    fake_adapter = FakeBrokerAdapter(raw_status="filled", fill_price=recommendation["entry_zone_high"])
    state.execution_router = ExecutionRouter(broker_adapter=fake_adapter)
    try:
        order_response = client.post(
            "/paper-orders",
            json={
                "recommendation_id": recommendation_id,
                "side": "BUY",
                "qty": 1,
                "limit_price": recommendation["entry_zone_high"],
                "execution_mode": "live",
                "dry_run": False,
                "confirm_live": True,
            },
            headers=AUTH_HEADERS,
        )
    finally:
        state.execution_router = original_router

    assert order_response.status_code == 200
    order = order_response.json()
    assert order["execution_mode"] == "live"
    assert order["dry_run"] is False
    assert order["status"] == "filled"
    assert order["broker_order_id"] == "fake_broker_order_123"
    assert order["simulated_fill_price"] == recommendation["entry_zone_high"]
    assert "fake-broker: status=filled" in order["adapter_message"]
    assert len(fake_adapter.placements) == 1
    placement = fake_adapter.placements[0]
    assert placement.symbol == recommendation["ticker"]
    assert placement.side == "BUY"
    assert placement.qty == 1
    assert placement.limit_price == recommendation["entry_zone_high"]
    assert placement.client_order_id.startswith("quant_")

    holdings = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings.status_code == 200
    holding = next(item for item in holdings.json() if item["ticker"] == recommendation["ticker"])
    assert holding["qty"] == 1
    assert holding["avg_buy_price"] == recommendation["entry_zone_high"]
    assert holding["source_recommendation_id"] == recommendation_id

    trades = client.get(
        f"/portfolio/trades?ticker={recommendation['ticker']}&side=buy",
        headers=AUTH_HEADERS,
    )
    assert trades.status_code == 200
    assert any(
        item["source_recommendation_id"] == recommendation_id
        and item["price"] == recommendation["entry_zone_high"]
        and (item["reason"] or "").startswith(f"paper_order_fill:{order['id']}")
        for item in trades.json()
    )


def test_live_submitted_buy_cancel_calls_broker_before_local_cancel() -> None:
    state = get_app_state()
    state.reset()
    original_router = state.execution_router
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "live-broker-cancel-test",
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
    state.close_holding(recommendation["ticker"])

    approval_response = client.post(
        f"/recommendations/{recommendation_id}/approval",
        json={"decision": "approved", "approver": "qa", "notes": "ok"},
        headers=AUTH_HEADERS,
    )
    assert approval_response.status_code == 200

    fake_adapter = FakeBrokerAdapter(raw_status="accepted")
    state.execution_router = ExecutionRouter(broker_adapter=fake_adapter)
    try:
        order_response = client.post(
            "/paper-orders",
            json={
                "recommendation_id": recommendation_id,
                "side": "BUY",
                "qty": 1,
                "limit_price": recommendation["entry_zone_high"],
                "execution_mode": "live",
                "dry_run": False,
                "confirm_live": True,
            },
            headers=AUTH_HEADERS,
        )
        assert order_response.status_code == 200
        order = order_response.json()
        assert order["status"] == "submitted"
        assert order["broker_order_id"] == "fake_broker_order_123"

        cancel_response = client.post(
            f"/paper-orders/{order['id']}/cancel",
            json={
                "reason": "operator cancel live broker order",
                "canceled_by": "qa",
                "skip_broker_cancel": True,
            },
            headers=AUTH_HEADERS,
        )
    finally:
        state.execution_router = original_router

    assert cancel_response.status_code == 200
    canceled = cancel_response.json()
    assert canceled["status"] == "canceled"
    assert canceled["cancel_reason"] == "operator cancel live broker order"
    assert "cancel_status=canceled" in canceled["adapter_message"]
    assert fake_adapter.cancelled_order_ids == ["fake_broker_order_123"]
    holdings = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings.status_code == 200
    assert all(item["ticker"] != recommendation["ticker"] for item in holdings.json())


def test_broker_order_sync_applies_fill_and_reject_snapshots() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "broker-order-sync-test",
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
    state.close_holding(recommendation["ticker"])

    recommendation_obj = state.recommendations_by_id[recommendation_id]
    fill_order = PaperOrder(
        id=f"broker_sync_fill_{uuid4().hex[:8]}",
        recommendation_id=recommendation_id,
        source_snapshot_id=recommendation["source_snapshot_id"],
        strategy_config_id=recommendation["strategy_config_id"],
        side=Direction.BUY,
        qty=4,
        limit_price=None,
        execution_mode=OrderExecutionMode.LIVE,
        dry_run=False,
        broker_order_id=f"broker_fill_{uuid4().hex[:8]}",
        adapter_message="broker accepted fill test",
        submitted_at=datetime.now(timezone.utc),
        status=PaperOrderStatus.SUBMITTED,
    )
    reject_order = PaperOrder(
        id=f"broker_sync_reject_{uuid4().hex[:8]}",
        recommendation_id=recommendation_id,
        source_snapshot_id=recommendation["source_snapshot_id"],
        strategy_config_id=recommendation["strategy_config_id"],
        side=Direction.BUY,
        qty=2,
        limit_price=None,
        execution_mode=OrderExecutionMode.LIVE,
        dry_run=False,
        broker_order_id=f"broker_reject_{uuid4().hex[:8]}",
        adapter_message="broker accepted reject test",
        submitted_at=datetime.now(timezone.utc),
        status=PaperOrderStatus.SUBMITTED,
    )
    state.record_paper_order(fill_order, recommendation=recommendation_obj)
    state.record_paper_order(reject_order, recommendation=recommendation_obj)

    fill_price = recommendation["entry_zone_high"]
    sync_response = client.post(
        "/paper-orders/broker-sync",
        json={
            "broker": "integration-broker",
            "updated_by": "qa",
            "checked_at": "2026-04-10T15:30:00Z",
            "statuses": [
                {
                    "broker_order_id": fill_order.broker_order_id,
                    "status": "filled",
                    "fill_price": fill_price,
                    "broker_message": "avg fill confirmed",
                },
                {
                    "order_id": reject_order.id,
                    "status": "rejected",
                    "reason": "risk rejected",
                    "broker_message": "insufficient buying power",
                },
                {
                    "broker_order_id": "missing_broker_order",
                    "status": "submitted",
                },
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert sync_response.status_code == 200
    sync_result = sync_response.json()
    assert sync_result["broker"] == "integration-broker"
    assert sync_result["total_count"] == 3
    assert sync_result["filled_count"] == 1
    assert sync_result["canceled_count"] == 1
    assert sync_result["missing_count"] == 1
    assert {item["action"] for item in sync_result["items"]} == {"filled", "canceled", "missing"}

    filled = state.get_paper_order(fill_order.id)
    assert filled is not None
    assert filled.status == PaperOrderStatus.FILLED
    assert filled.simulated_fill_price == fill_price
    assert "filled_by=qa:integration-broker" in (filled.adapter_message or "")
    assert "avg fill confirmed" in (filled.adapter_message or "")

    rejected = state.get_paper_order(reject_order.id)
    assert rejected is not None
    assert rejected.status == PaperOrderStatus.CANCELED
    assert rejected.cancel_reason == "risk rejected; insufficient buying power"

    holdings = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings.status_code == 200
    holding = next(item for item in holdings.json() if item["ticker"] == recommendation["ticker"])
    assert holding["qty"] == 4
    assert holding["avg_buy_price"] == fill_price

    events = state.list_pending_events(limit=20)
    assert any(event.event_type == EventType.BROKER_ORDER_SYNC for event in events)
