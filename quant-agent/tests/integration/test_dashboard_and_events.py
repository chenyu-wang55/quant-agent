from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from apps.dashboard.main import _select_recommendations_for_dashboard


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
    assert "行情快照" in dashboard_home.text
    assert "策略版本" in dashboard_home.text
    assert "调参建议" in dashboard_home.text
    assert "交易流水" in dashboard_home.text
    assert "纸单记录" in dashboard_home.text
    assert "交易复盘" in dashboard_home.text
    assert "推荐归因" in dashboard_home.text
    assert "renderPaperOrders" in dashboard_home.text
    assert "renderTrades" in dashboard_home.text
    assert "renderPerformance" in dashboard_home.text
    assert "renderAttribution" in dashboard_home.text
    assert "renderSourceSnapshots" in dashboard_home.text
    assert "renderStrategyConfigs" in dashboard_home.text
    assert "renderStrategyTuning" in dashboard_home.text
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
    assert "sellHolding" in dashboard_home.text
    assert "/paper-orders" in dashboard_home.text
    assert "/paper-orders/risk-plan" in dashboard_home.text

    realtime = client.get("/dashboard/realtime-data?refresh_alerts=false", headers=AUTH_HEADERS)
    assert realtime.status_code == 200
    payload = realtime.json()
    assert "recommendations" in payload
    assert "summary" in payload
    assert "source_snapshots" in payload
    assert "strategy_configs" in payload
    assert "strategy_tuning" in payload
    assert "recent_paper_orders" in payload
    assert "recommendation_attribution" in payload
    assert payload["summary"]["recommendation_count"] >= 1
    assert payload["summary"]["source_snapshot_count"] >= 1
    assert payload["summary"]["strategy_config_count"] >= 1
    assert payload["summary"]["strategy_tuning_count"] >= 1
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
