from __future__ import annotations

from apps.api.dependencies import get_app_state
from apps.worker.main import system_cycle
from domain.entities.models import HoldingStatus, ManualBuyRequest


def test_system_cycle_generates_recommendations_and_monitors_without_auto_execution() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    state.close_holding("AAPL")
    state.record_manual_buy(
        ManualBuyRequest(
            ticker="AAPL",
            qty=5,
            buy_price=180,
            stop_loss=99999999,
            note="worker cycle alert setup",
        )
    )

    result = system_cycle(top_n=2, min_confidence=0.0, consume_events=False)

    assert result["job"] == "system_cycle"
    assert result["recommendation_count"] >= 1
    assert len(result["top_recommendations"]) <= 2
    assert result["source_snapshot_id"]
    assert result["strategy_config_id"]
    assert result["auto_execution_enabled"] is False
    assert result["sell_alert_count"] >= 1
    assert any(alert["ticker"] == "AAPL" for alert in result["sell_alerts"])
    assert result["pending_event_count"] >= 1
    assert result["metrics"]["counters"]["research_runs"] >= 1

    holding = state.holding_watch_repo.get("AAPL")
    assert holding is not None
    assert holding.status == HoldingStatus.OPEN
    assert holding.qty == 5
    assert state.trade_ledger_repo.list_recent(limit=10, ticker="AAPL")[0].side.value == "buy"

    consumed_result = system_cycle(top_n=1, min_confidence=0.0, consume_events=True)
    assert consumed_result["consumed_event_count"] >= 1
    assert consumed_result["consumed_event_type_counts"]["recommendation_ready"] >= 1
    assert consumed_result["pending_event_count"] == 0
