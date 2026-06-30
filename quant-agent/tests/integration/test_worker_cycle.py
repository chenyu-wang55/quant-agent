from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from apps.worker.main import system_cycle, system_cycle_loop
from domain.entities.models import HoldingStatus, ManualBuyRequest
from domain.policies.approval import ApprovalDecisionRequest


AUTH_HEADERS = {"x-access-password": "test-access-password"}


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

    result = system_cycle(
        top_n=2,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
    )

    assert result["job"] == "system_cycle"
    assert result["system_cycle_run_id"]
    assert result["recommendation_count"] >= 1
    assert len(result["top_recommendations"]) <= 2
    assert result["source_snapshot_id"]
    assert result["strategy_config_id"]
    assert result["auto_execution_enabled"] is False
    assert result["auto_execution"]["enabled"] is False
    assert result["auto_execution"]["action_count"] == 0
    assert result["sell_alert_count"] >= 1
    assert any(alert["ticker"] == "AAPL" for alert in result["sell_alerts"])
    assert result["pending_event_count"] >= 1
    assert result["metrics"]["counters"]["research_runs"] >= 1
    run_history = state.list_system_cycle_runs(limit=1)
    assert run_history
    assert run_history[0].id == result["system_cycle_run_id"]
    assert run_history[0].recommendation_count == result["recommendation_count"]
    assert run_history[0].auto_execution_enabled is False

    client = TestClient(app)
    run_response = client.get("/operations/system-runs?limit=1", headers=AUTH_HEADERS)
    assert run_response.status_code == 200
    run_rows = run_response.json()
    assert run_rows[0]["id"] == result["system_cycle_run_id"]
    assert run_rows[0]["status"] == "success"
    alert_history = client.get(
        f"/portfolio/alert-history?monitor_run_id={result['system_cycle_run_id']}",
        headers=AUTH_HEADERS,
    )
    assert alert_history.status_code == 200
    alert_rows = alert_history.json()
    assert alert_rows
    assert any(item["ticker"] == "AAPL" for item in alert_rows)
    assert all(item["monitor_run_id"] == result["system_cycle_run_id"] for item in alert_rows)

    holding = state.holding_watch_repo.get("AAPL")
    assert holding is not None
    assert holding.status == HoldingStatus.OPEN
    assert holding.qty == 5
    assert state.trade_ledger_repo.list_recent(limit=10, ticker="AAPL")[0].side.value == "buy"

    consumed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=True,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
    )
    assert consumed_result["consumed_event_count"] >= 1
    assert consumed_result["consumed_event_type_counts"]["recommendation_ready"] >= 1
    assert consumed_result["pending_event_count"] == 0


def test_system_cycle_auto_executes_approved_buy() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    ticker = seed_result["top_recommendations"][0]["ticker"]
    state.close_holding(ticker)
    state.decide_recommendation(
        ApprovalDecisionRequest(
            recommendation_id=recommendation_id,
            decision="approved",
            approver="worker-test",
            notes="allow auto paper execution",
        )
    )

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
    )

    assert result["auto_execution_enabled"] is True
    assert result["auto_execution"]["enabled"] is True
    assert result["auto_execution"]["buy_order_count"] == 1
    assert result["auto_execution"]["sell_order_count"] == 0
    buy_action = next(
        item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "executed"
    assert buy_action["recommendation_id"] == recommendation_id
    assert buy_action["ticker"] == ticker
    assert buy_action["qty"] > 0
    assert state.list_paper_orders(limit=1, recommendation_id=recommendation_id)[0].id == buy_action["order_id"]
    holding = state.holding_watch_repo.get(ticker)
    assert holding is not None
    assert holding.status == HoldingStatus.OPEN
    assert holding.source_recommendation_id == recommendation_id
    run_history = state.list_system_cycle_runs(limit=1)
    assert run_history[0].auto_execution_enabled is True
    assert run_history[0].metrics["auto_execution"]["buy_order_count"] == 1


def test_system_cycle_auto_executes_sell_alert_without_buying_same_ticker() -> None:
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
            note="worker cycle auto sell setup",
        )
    )

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=1,
    )

    assert result["auto_execution"]["sell_order_count"] == 1
    assert result["auto_execution"]["buy_order_count"] == 0
    sell_action = next(item for item in result["auto_execution"]["actions"] if item["action"] == "sell_alert")
    assert sell_action["status"] == "executed"
    assert sell_action["ticker"] == "AAPL"
    assert sell_action["sold_qty"] == 5
    buy_actions = [item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"]
    assert any(item["reason"] == "sell_alert_same_cycle" for item in buy_actions)
    holding = state.holding_watch_repo.get("AAPL")
    assert holding is not None
    assert holding.status == HoldingStatus.CLOSED
    assert state.list_sell_execution_audits(limit=1, ticker="AAPL")[0].id == sell_action["sell_execution_id"]


def test_system_cycle_loop_runs_bounded_cycles_without_sleeping() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)

    report = system_cycle_loop(
        interval_seconds=0,
        max_cycles=2,
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        sleep_fn=lambda _seconds: None,
    )

    assert report["job"] == "system_cycle_loop"
    assert report["cycle_count"] == 2
    assert report["success_count"] == 2
    assert report["error_count"] == 0
    assert report["last_system_cycle_run_id"]
    assert all(item["system_cycle_run_id"] for item in report["cycles"])
    assert len(state.list_system_cycle_runs(limit=5)) >= 2
