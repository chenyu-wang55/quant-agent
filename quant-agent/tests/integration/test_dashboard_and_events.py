from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.dependencies import AppState, get_app_state
from apps.api.main import app
from apps.dashboard.main import _select_recommendations_for_dashboard
from infra.queue.events import EventType


AUTH_HEADERS = {"x-access-password": "test-access-password"}


def test_dashboard_and_event_endpoints() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "dashboard-event-test",
            "as_of": "2026-04-10T09:30:00Z",
            "publication": {"top_n": 2, "output_channels": ["api"]},
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
    rec_id = run_response.json()["recommendations"][0]["id"]

    pending_events = client.get("/events/pending", headers=AUTH_HEADERS)
    assert pending_events.status_code == 200
    assert len(pending_events.json()) >= 1

    consumed_events = client.post("/events/consume?limit=10", headers=AUTH_HEADERS)
    assert consumed_events.status_code == 200
    assert len(consumed_events.json()) >= 1

    dashboard_home = client.get("/dashboard", headers=AUTH_HEADERS)
    assert dashboard_home.status_code == 200
    assert "Quant 实时交易看板" in dashboard_home.text
    assert "交易控制" in dashboard_home.text
    assert "Account Equity" in dashboard_home.text
    assert "Risk %" in dashboard_home.text
    assert "Max Position %" in dashboard_home.text
    assert "Max Gross %" in dashboard_home.text
    assert "Max Sector %" in dashboard_home.text
    assert "Exec Mode" in dashboard_home.text
    assert "Live Dry Run" in dashboard_home.text
    assert "行情快照" in dashboard_home.text
    assert "质量" in dashboard_home.text
    assert "策略版本" in dashboard_home.text
    assert "调参建议" in dashboard_home.text
    assert "自动循环历史" in dashboard_home.text
    assert "自动审批" in dashboard_home.text
    assert "自动执行" in dashboard_home.text
    assert "Autopilot Policy" in dashboard_home.text
    assert "autopilotEnabled" in dashboard_home.text
    assert "autopilotRegularHours" in dashboard_home.text
    assert "autopilotDailyBuys" in dashboard_home.text
    assert "autopilotOrderDedupeMinutes" in dashboard_home.text
    assert "autopilotCooldownMinutes" in dashboard_home.text
    assert "autopilotMinSnapshotBars" in dashboard_home.text
    assert "autopilotMinSnapshotFundamentals" in dashboard_home.text
    assert "autopilotMaxSnapshotBarAge" in dashboard_home.text
    assert "autopilotMaxOpenRiskPct" in dashboard_home.text
    assert "autopilotMaxDailyLossPct" in dashboard_home.text
    assert "saveAutopilotPolicy" in dashboard_home.text
    assert "/execution/autopilot-policy" in dashboard_home.text
    assert "runSystemCycle" in dashboard_home.text
    assert "/operations/system-cycle" in dashboard_home.text
    assert "交易流水" in dashboard_home.text
    assert "持仓风控审计" in dashboard_home.text
    assert "卖出执行审计" in dashboard_home.text
    assert "卖出提醒历史" in dashboard_home.text
    assert "纸单记录" in dashboard_home.text
    assert "交易复盘" in dashboard_home.text
    assert "推荐归因" in dashboard_home.text
    assert "renderPaperOrders" in dashboard_home.text
    assert "renderTrades" in dashboard_home.text
    assert "renderHoldingControlAudits" in dashboard_home.text
    assert "renderSellExecutions" in dashboard_home.text
    assert "renderAlertHistory" in dashboard_home.text
    assert "renderPerformance" in dashboard_home.text
    assert "renderAttribution" in dashboard_home.text
    assert "renderSourceSnapshots" in dashboard_home.text
    assert "renderStrategyConfigs" in dashboard_home.text
    assert "renderStrategyTuning" in dashboard_home.text
    assert "renderSystemRuns" in dashboard_home.text
    assert "autoApprovalCell" in dashboard_home.text
    assert "autoExecutionCell" in dashboard_home.text
    assert "snapshotScoreCell" in dashboard_home.text
    assert "performance_score" in dashboard_home.text
    assert "replaySnapshot" in dashboard_home.text
    assert "executeAlert" in dashboard_home.text
    assert "执行建议" in dashboard_home.text
    assert "runResearch" in dashboard_home.text
    assert "建议股数" in dashboard_home.text
    assert "planBuyRecommendation" in dashboard_home.text
    assert "reject_on_material_evidence_conflict: false" in dashboard_home.text
    assert "event_trading_enabled: true" in dashboard_home.text
    assert "buyRecommendation" in dashboard_home.text
    assert "updateHoldingControls" in dashboard_home.text
    assert "sellHolding" in dashboard_home.text
    assert "executionModePayload" in dashboard_home.text
    assert "...executionModePayload()" in dashboard_home.text
    assert "Dry-run完成" in dashboard_home.text
    assert "/paper-orders" in dashboard_home.text
    assert "/paper-orders/risk-plan" in dashboard_home.text
    assert "adapter_message" in dashboard_home.text
    assert "holdingControlAuditCount" in dashboard_home.text
    assert "sellExecutionCount" in dashboard_home.text
    assert "alertHistoryCount" in dashboard_home.text
    assert "systemRunCount" in dashboard_home.text
    assert "autoApprovalCount" in dashboard_home.text
    assert "autoActionCount" in dashboard_home.text

    realtime = client.get("/dashboard/realtime-data?refresh_alerts=false", headers=AUTH_HEADERS)
    assert realtime.status_code == 200
    payload = realtime.json()
    assert "recommendations" in payload
    assert "summary" in payload
    assert "autopilot_policy" in payload
    assert "autopilot_preflight" in payload
    assert "market_session" in payload
    assert "source_snapshots" in payload
    assert "strategy_configs" in payload
    assert "strategy_tuning" in payload
    assert "recent_paper_orders" in payload
    assert "recent_holding_control_audits" in payload
    assert "recent_sell_executions" in payload
    assert "recent_system_runs" in payload
    assert "recent_alert_history" in payload
    assert "recommendation_attribution" in payload
    assert payload["summary"]["recommendation_count"] >= 1
    assert payload["autopilot_policy"]["enabled"] is False
    assert payload["autopilot_policy"]["order_dedupe_minutes"] == 1440
    assert payload["autopilot_policy"]["rebuy_cooldown_minutes"] == 240
    assert payload["autopilot_policy"]["min_snapshot_bar_coverage"] == 1.0
    assert payload["autopilot_policy"]["min_snapshot_fundamental_coverage"] == 1.0
    assert payload["autopilot_policy"]["max_snapshot_bar_age_minutes"] == 4320
    assert payload["autopilot_policy"]["max_open_risk_pct"] == 0.06
    assert payload["autopilot_policy"]["max_daily_realized_loss_pct"] == 0.03
    assert payload["autopilot_preflight"]["status"] == "off"
    assert "daily_usage" in payload["autopilot_preflight"]
    assert payload["market_session"]["timezone"] == "America/New_York"
    assert payload["summary"]["source_snapshot_count"] >= 1
    assert payload["summary"]["strategy_config_count"] >= 1
    assert payload["summary"]["strategy_tuning_count"] >= 1
    assert payload["source_snapshots"][0]["data_quality"]["bar_coverage"] >= 0
    assert payload["source_snapshots"][0]["data_quality"]["fundamental_coverage"] >= 0
    assert "holding_control_audit_count" in payload["summary"]
    assert "latest_auto_approval_count" in payload["summary"]
    assert "latest_auto_action_count" in payload["summary"]
    assert payload["strategy_configs"][0]["strategy_config_id"]
    assert payload["strategy_tuning"]["items"][0]["strategy_config_id"]

    rec = state.latest_run.recommendations[0]
    stale_rec = rec.model_copy(
        update={
            "entry_zone_low": rec.entry_zone_low * 10,
            "entry_zone_high": rec.entry_zone_high * 10,
        }
    )
    assert _select_recommendations_for_dashboard([stale_rec], {rec.ticker: rec.entry_zone_low}) == []

    dashboard_detail = client.get(f"/dashboard/recommendations/{rec_id}", headers=AUTH_HEADERS)
    assert dashboard_detail.status_code == 200
    assert rec_id in dashboard_detail.text


def test_system_events_are_durable_across_app_state_instances() -> None:
    state = get_app_state()
    state.reset()

    state.publish_event(EventType.MODEL_EVALUATION, {"run_id": "durable-event-test"})

    restarted_state = AppState()
    pending = restarted_state.list_pending_events(limit=10)
    assert any(
        event.event_type == EventType.MODEL_EVALUATION
        and event.payload["run_id"] == "durable-event-test"
        for event in pending
    )
    assert restarted_state.pending_event_count() >= 1

    consumed = restarted_state.consume_events(limit=10)
    assert any(event.payload.get("run_id") == "durable-event-test" for event in consumed)
    assert all(event.status == "consumed" for event in consumed)

    second_restart = AppState()
    consumed_after_restart = second_restart.list_consumed_events(limit=10)
    assert any(event.payload.get("run_id") == "durable-event-test" for event in consumed_after_restart)
    assert not any(
        event.payload.get("run_id") == "durable-event-test"
        for event in second_restart.list_pending_events(limit=10)
    )
