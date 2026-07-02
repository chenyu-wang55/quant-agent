from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from apps.worker import main as worker_main
from apps.api.dependencies import get_app_state
from apps.api.main import app
from apps.worker.main import system_cycle, system_cycle_loop
from domain.entities.models import (
    Direction,
    HoldingStatus,
    ManualBuyRequest,
    ManualSellRequest,
    OrderExecutionMode,
    PaperOrder,
    PaperOrderCancelRequest,
    PaperOrderStatus,
    SellExecutionAudit,
    SystemCycleRun,
)
from domain.policies.approval import ApprovalDecisionRequest
from services.execution.broker_adapter import BrokerOrderPlacement, BrokerOrderUpdate
from services.execution.router import ExecutionRouter


AUTH_HEADERS = {"x-access-password": "test-access-password"}


class SyncOnlyBrokerAdapter:
    name = "sync-only-broker"

    def __init__(self, updates: dict[str, BrokerOrderUpdate]) -> None:
        self.updates = updates
        self.queried_order_ids: list[str] = []

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        raise AssertionError("submit_order is not used by broker sync tests")

    def get_order_by_id(self, broker_order_id: str) -> BrokerOrderUpdate:
        self.queried_order_ids.append(broker_order_id)
        try:
            return self.updates[broker_order_id]
        except KeyError as exc:
            raise AssertionError(f"unexpected broker order id: {broker_order_id}") from exc

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        raise AssertionError("client order lookup is not used by broker sync tests")


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
    assert result["auto_approval"]["enabled"] is False
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
    source_snapshot_id = seed_result["top_recommendations"][0]["source_snapshot_id"]
    strategy_config_id = seed_result["top_recommendations"][0]["strategy_config_id"]
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
        rebuy_cooldown_minutes=0,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
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
    assert buy_action["source_snapshot_id"] == source_snapshot_id
    assert buy_action["strategy_config_id"] == strategy_config_id
    assert buy_action["qty"] > 0
    order = state.list_paper_orders(limit=1, recommendation_id=recommendation_id)[0]
    assert order.id == buy_action["order_id"]
    assert order.source_snapshot_id == source_snapshot_id
    assert order.strategy_config_id == strategy_config_id
    holding = state.holding_watch_repo.get(ticker)
    assert holding is not None
    assert holding.status == HoldingStatus.OPEN
    assert holding.source_recommendation_id == recommendation_id
    run_history = state.list_system_cycle_runs(limit=1)
    assert run_history[0].auto_execution_enabled is True
    assert run_history[0].metrics["auto_execution"]["buy_order_count"] == 1


def test_system_cycle_auto_syncs_live_buy_broker_fill() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    run_at = datetime(2026, 7, 5, 9, 30, tzinfo=timezone.utc)
    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=run_at,
        auto_sync_broker_statuses=False,
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    recommendation = state.recommendations_by_id[recommendation_id]
    state.close_holding(recommendation.ticker)

    broker_order_id = "worker_buy_fill_001"
    submitted_order = PaperOrder(
        id="worker_buy_order_001",
        recommendation_id=recommendation_id,
        source_snapshot_id=recommendation.source_snapshot_id,
        strategy_config_id=recommendation.strategy_config_id,
        side=Direction.BUY,
        qty=3,
        limit_price=recommendation.entry_zone_high,
        execution_mode=OrderExecutionMode.LIVE,
        dry_run=False,
        broker_order_id=broker_order_id,
        adapter_message="accepted by broker",
        submitted_at=run_at - timedelta(minutes=20),
        status=PaperOrderStatus.SUBMITTED,
    )
    state.record_paper_order(submitted_order, recommendation=recommendation)

    fill_price = recommendation.entry_zone_high
    fake_adapter = SyncOnlyBrokerAdapter(
        {
            broker_order_id: BrokerOrderUpdate(
                broker_order_id=broker_order_id,
                raw_status="filled",
                client_order_id="quant_worker_buy_order_001",
                submitted_at=submitted_order.submitted_at,
                filled_at=run_at - timedelta(minutes=5),
                filled_avg_price=fill_price,
                filled_qty=3,
                message="filled by test broker",
            )
        }
    )
    original_router = state.execution_router
    state.execution_router = ExecutionRouter(broker_adapter=fake_adapter)
    try:
        result = system_cycle(
            top_n=1,
            min_confidence=0.0,
            consume_events=False,
            as_of=run_at,
            auto_sync_broker_statuses=True,
        )
    finally:
        state.execution_router = original_router

    broker_sync = result["broker_order_sync"]
    assert broker_sync["broker"] == "sync-only-broker"
    assert broker_sync["queried_count"] == 1
    assert broker_sync["buy_order_sync"]["filled_count"] == 1
    assert broker_sync["buy_order_sync"]["items"][0]["order_id"] == submitted_order.id
    assert fake_adapter.queried_order_ids == [broker_order_id]
    updated_order = state.get_paper_order(submitted_order.id)
    assert updated_order is not None
    assert updated_order.status == PaperOrderStatus.FILLED
    holding = state.holding_watch_repo.get(recommendation.ticker)
    assert holding is not None
    assert holding.qty == 3
    assert holding.source_recommendation_id == recommendation_id
    run_history = state.list_system_cycle_runs(limit=1)
    assert run_history[0].metrics["broker_order_sync"]["buy_order_sync"]["filled_count"] == 1


def test_system_cycle_auto_syncs_live_sell_broker_fill() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    run_at = datetime(2026, 7, 5, 9, 30, tzinfo=timezone.utc)
    ticker = "MSFT"
    state.record_manual_buy(
        ManualBuyRequest(
            ticker=ticker,
            qty=5,
            buy_price=100,
            bought_at=run_at - timedelta(hours=2),
            stop_loss=50,
            take_profit1=10_000,
            take_profit2=12_000,
            note="worker broker sell sync setup",
        )
    )
    broker_order_id = "worker_sell_fill_001"
    audit = SellExecutionAudit(
        id="worker_sell_audit_001",
        ticker=ticker,
        qty=2,
        sell_price=125,
        submitted_at=run_at - timedelta(minutes=30),
        execution_mode=OrderExecutionMode.LIVE,
        dry_run=False,
        broker_order_id=broker_order_id,
        adapter_message="sell accepted by broker",
        applied_to_ledger=False,
        status="submitted",
        reason="worker broker sell sync test",
        remaining_qty=5,
        holding_status_after=HoldingStatus.OPEN,
    )
    state.sell_execution_audit_repo.add(audit)

    fake_adapter = SyncOnlyBrokerAdapter(
        {
            broker_order_id: BrokerOrderUpdate(
                broker_order_id=broker_order_id,
                raw_status="filled",
                client_order_id="quant_sell_worker_sell_audit_001",
                submitted_at=audit.submitted_at,
                filled_at=run_at - timedelta(minutes=5),
                filled_avg_price=125,
                filled_qty=2,
                message="sell filled by test broker",
            )
        }
    )
    original_router = state.execution_router
    state.execution_router = ExecutionRouter(broker_adapter=fake_adapter)
    try:
        result = system_cycle(
            top_n=1,
            min_confidence=0.0,
            consume_events=False,
            as_of=run_at,
            auto_sync_broker_statuses=True,
        )
    finally:
        state.execution_router = original_router

    broker_sync = result["broker_order_sync"]
    assert broker_sync["broker"] == "sync-only-broker"
    assert broker_sync["queried_count"] == 1
    assert broker_sync["sell_execution_sync"]["filled_count"] == 1
    assert broker_sync["sell_execution_sync"]["items"][0]["order_id"] == audit.id
    assert fake_adapter.queried_order_ids == [broker_order_id]
    holding = state.holding_watch_repo.get(ticker)
    assert holding is not None
    assert holding.qty == 3
    assert holding.last_sell_price == 125
    updated_audit = state.sell_execution_audit_repo.get(audit.id)
    assert updated_audit is not None
    assert updated_audit.status == "filled"
    assert updated_audit.applied_to_ledger is True
    assert updated_audit.remaining_qty == 3
    sell_trades = state.list_trade_ledger(limit=10, ticker=ticker)
    assert sell_trades[0].side.value == "sell"
    assert sell_trades[0].qty == 2
    run_history = state.list_system_cycle_runs(limit=1)
    assert run_history[0].metrics["broker_order_sync"]["sell_execution_sync"]["filled_count"] == 1


def test_system_cycle_blocks_auto_buy_when_latest_price_drifts_from_entry_zone(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    seed_at = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    ticker = seed_result["top_recommendations"][0]["ticker"]
    entry_high = seed_result["top_recommendations"][0]["entry_zone"][1]
    state.close_holding(ticker)
    state.decide_recommendation(
        ApprovalDecisionRequest(
            recommendation_id=recommendation_id,
            decision="approved",
            approver="worker-test",
            notes="approval should be blocked by price drift gate",
        )
    )
    existing_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }
    original_get_latest_price = state.provider.get_latest_price

    def drifted_latest_price(candidate_ticker: str, as_of: datetime):
        if candidate_ticker.upper() == ticker.upper():
            return entry_high * 1.10
        return original_get_latest_price(candidate_ticker, as_of)

    monkeypatch.setattr(state.provider, "get_latest_price", drifted_latest_price)

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        max_auto_buy_price_drift_pct=0.03,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
    )

    assert result["auto_execution"]["buy_order_count"] == 0
    buy_action = next(
        item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "skipped"
    assert buy_action["reason"] == "price_drift_gate_failed"
    price_gate = buy_action["price_drift_gate"]
    assert price_gate["passed"] is False
    assert price_gate["reason"] == "latest_price_above_entry_zone"
    assert price_gate["latest_price"] > price_gate["entry_zone_high"]
    assert price_gate["drift_pct"] > price_gate["max_drift_pct"]
    current_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }
    assert current_order_ids == existing_order_ids
    holding = state.holding_watch_repo.get(ticker)
    assert holding is None or holding.status != HoldingStatus.OPEN


def test_system_cycle_blocks_auto_execution_when_required_reconciliation_is_missing() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    seed_at = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    ticker = seed_result["top_recommendations"][0]["ticker"]
    state.close_holding(ticker)
    state.decide_recommendation(
        ApprovalDecisionRequest(
            recommendation_id=recommendation_id,
            decision="approved",
            approver="worker-test",
            notes="approval should be blocked by missing reconciliation",
        )
    )
    existing_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        require_position_reconciliation=True,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
    )

    assert result["auto_execution_enabled"] is False
    assert result["auto_execution"]["enabled"] is False
    assert result["auto_execution"]["buy_order_count"] == 0
    action = result["auto_execution"]["actions"][0]
    assert action["status"] == "skipped"
    assert action["reason"] == "position_reconciliation_gate_failed"
    gate = action["position_reconciliation_gate"]
    assert gate["passed"] is False
    assert gate["reason"] == "position_reconciliation_missing"
    assert result["position_reconciliation_gate"] == gate
    current_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }
    assert current_order_ids == existing_order_ids


def test_system_cycle_auto_approves_and_executes_same_cycle() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        auto_approve_recommendations=True,
        auto_approve_min_confidence=0.5,
        auto_approve_min_composite=0.0,
        max_auto_approvals=1,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert result["auto_approval"]["enabled"] is True
    assert result["auto_approval"]["approved_count"] == 1
    assert result["auto_execution"]["buy_order_count"] == 1
    approved_action = next(
        item for item in result["auto_approval"]["actions"] if item["status"] == "approved"
    )
    buy_action = next(
        item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert approved_action["recommendation_id"] == buy_action["recommendation_id"]
    assert buy_action["source_snapshot_id"] == result["source_snapshot_id"]
    assert buy_action["strategy_config_id"] == result["strategy_config_id"]
    approval = state.get_latest_approval(approved_action["recommendation_id"])
    assert approval is not None
    assert approval.approver == "system_cycle:auto_approval"
    assert state.list_paper_orders(limit=1, recommendation_id=buy_action["recommendation_id"])


def test_system_cycle_blocks_auto_actions_when_snapshot_quality_gate_fails(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)

    original_get_summary = state.source_snapshot_repo.get_summary

    def degraded_summary(source_snapshot_id: str):
        summary = original_get_summary(source_snapshot_id)
        if summary is None:
            return None
        quality = dict(summary.data_quality)
        quality.update(
            {
                "status": "partial",
                "bar_coverage": 0.5,
                "missing_bar_count": 1,
                "missing_bar_tickers": [summary.tickers[0]],
            }
        )
        return summary.model_copy(update={"data_quality": quality})

    monkeypatch.setattr(state.source_snapshot_repo, "get_summary", degraded_summary)

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        auto_approve_recommendations=True,
        auto_approve_min_confidence=0.5,
        auto_approve_min_composite=0.0,
        max_auto_approvals=1,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        min_snapshot_bar_coverage=1.0,
        min_snapshot_fundamental_coverage=1.0,
    )

    assert result["snapshot_quality_gate"]["passed"] is False
    assert "snapshot_bar_coverage_below_threshold" in result["snapshot_quality_gate"]["reasons"]
    assert result["auto_execution_enabled"] is False
    assert result["auto_approval"]["enabled"] is False
    assert result["auto_approval"]["approved_count"] == 0
    assert result["auto_approval"]["actions"][0]["reason"] == "snapshot_quality_gate_failed"
    assert result["auto_execution"]["enabled"] is False
    assert result["auto_execution"]["buy_order_count"] == 0
    assert result["auto_execution"]["actions"][0]["reason"] == "snapshot_quality_gate_failed"
    latest_run = state.list_system_cycle_runs(limit=1)[0]
    assert latest_run.auto_execution_enabled is False
    assert latest_run.metrics["snapshot_quality_gate"]["passed"] is False


def test_system_cycle_blocks_auto_actions_when_snapshot_bars_are_stale(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)

    original_get_summary = state.source_snapshot_repo.get_summary

    def stale_summary(source_snapshot_id: str):
        summary = original_get_summary(source_snapshot_id)
        if summary is None:
            return None
        quality = dict(summary.data_quality)
        quality.update(
            {
                "latest_bar_age_minutes": 600,
            }
        )
        return summary.model_copy(update={"data_quality": quality})

    monkeypatch.setattr(state.source_snapshot_repo, "get_summary", stale_summary)

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        auto_approve_recommendations=True,
        auto_approve_min_confidence=0.5,
        auto_approve_min_composite=0.0,
        max_auto_approvals=1,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=60,
    )

    assert result["snapshot_quality_gate"]["passed"] is False
    assert "snapshot_bar_age_above_threshold" in result["snapshot_quality_gate"]["reasons"]
    assert result["snapshot_quality_gate"]["latest_bar_age_minutes"] == 600
    assert result["snapshot_quality_gate"]["max_bar_age_minutes"] == 60
    assert result["auto_execution_enabled"] is False
    assert result["auto_approval"]["enabled"] is False
    assert result["auto_execution"]["enabled"] is False


def test_system_cycle_blocks_auto_buy_when_open_risk_exceeds_policy_limit() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)

    seed_at = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    ticker = seed_result["top_recommendations"][0]["ticker"]
    risk_ticker = "MSFT" if ticker != "MSFT" else "AAPL"
    state.close_holding(ticker)
    state.close_holding(risk_ticker)
    state.record_manual_buy(
        ManualBuyRequest(
            ticker=risk_ticker,
            qty=1000,
            buy_price=100,
            bought_at=seed_at,
            stop_loss=1,
            note="open risk gate setup",
        )
    )
    state.decide_recommendation(
        ApprovalDecisionRequest(
            recommendation_id=recommendation_id,
            decision="approved",
            approver="worker-test",
            notes="approval should still be blocked by portfolio risk gate",
        )
    )
    existing_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        account_equity=100_000,
        max_open_risk_pct=0.01,
        max_daily_realized_loss_pct=1.0,
    )

    gate = result["auto_execution"]["portfolio_risk_gate"]
    assert gate["passed"] is False
    assert gate["reason"] == "open_risk_above_policy_limit"
    assert gate["open_risk_pct"] > gate["max_open_risk_pct"]
    assert result["auto_execution"]["buy_order_count"] == 0
    buy_action = next(
        item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "skipped"
    assert buy_action["reason"] == "portfolio_open_risk_gate_failed"
    current_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }
    assert current_order_ids == existing_order_ids


def test_system_cycle_blocks_auto_buy_when_daily_realized_loss_exceeds_policy_limit() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)

    seed_at = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    ticker = seed_result["top_recommendations"][0]["ticker"]
    loss_ticker = "MSFT" if ticker != "MSFT" else "AAPL"
    state.close_holding(ticker)
    state.close_holding(loss_ticker)
    baseline_gate = state.get_autopilot_daily_loss_gate(
        as_of=seed_at,
        account_equity=100_000,
        max_daily_realized_loss_pct=1.0,
    )
    state.record_manual_buy(
        ManualBuyRequest(
            ticker=loss_ticker,
            qty=10,
            buy_price=100,
            bought_at=seed_at,
            stop_loss=1,
            note="daily loss gate setup buy",
        )
    )
    state.sell_holding(
        loss_ticker,
        ManualSellRequest(
            sell_price=50,
            sold_at=seed_at,
            reason="daily loss gate setup sell",
        ),
    )
    state.decide_recommendation(
        ApprovalDecisionRequest(
            recommendation_id=recommendation_id,
            decision="approved",
            approver="worker-test",
            notes="approval should still be blocked by daily loss gate",
        )
    )
    existing_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=0,
        account_equity=100_000,
        max_daily_realized_loss_pct=0.001,
    )

    gate = result["auto_execution"]["daily_loss_gate"]
    assert gate["passed"] is False
    assert gate["reason"] == "daily_realized_loss_above_policy_limit"
    assert gate["daily_realized_pnl"] == baseline_gate["daily_realized_pnl"] - 500
    assert gate["sell_trade_count"] == baseline_gate["sell_trade_count"] + 1
    assert gate["daily_realized_loss_pct"] > gate["max_daily_realized_loss_pct"]
    assert result["daily_loss_gate"]["passed"] is False
    assert result["auto_execution"]["buy_order_count"] == 0
    buy_action = next(
        item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "skipped"
    assert buy_action["reason"] == "daily_loss_gate_failed"
    current_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }
    assert current_order_ids == existing_order_ids


def test_system_cycle_skips_pending_duplicate_buy_order() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)
    run_at = datetime(2026, 7, 5, 9, 30, tzinfo=timezone.utc)

    first = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=run_at,
        auto_approve_recommendations=True,
        auto_approve_min_confidence=0.0,
        auto_approve_min_composite=0.0,
        max_auto_approvals=1,
        auto_execute_approved=True,
        auto_execution_mode="live_dry_run",
        max_auto_buys=1,
        max_auto_sells=0,
        order_dedupe_minutes=0,
        rebuy_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert first["auto_approval"]["approved_count"] == 1
    assert first["auto_execution"]["buy_order_count"] == 1
    first_buy_action = next(
        item for item in first["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    first_order = state.list_paper_orders(
        limit=1,
        recommendation_id=first_buy_action["recommendation_id"],
    )[0]
    assert first_order.id == first_buy_action["order_id"]
    assert first_order.execution_mode == OrderExecutionMode.LIVE
    assert first_order.status == PaperOrderStatus.SUBMITTED
    existing_order_ids = {order.id for order in state.list_paper_orders(limit=1000)}

    second = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=run_at,
        auto_execute_approved=True,
        auto_execution_mode="live_dry_run",
        max_auto_buys=1,
        max_auto_sells=0,
        order_dedupe_minutes=0,
        rebuy_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert second["auto_execution"]["enabled"] is True
    assert second["auto_execution"]["buy_order_count"] == 0
    buy_action = next(
        item for item in second["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "skipped"
    assert buy_action["reason"] == "pending_buy_order_gate_failed"
    assert buy_action["pending_buy_order_gate"]["passed"] is False
    assert buy_action["pending_buy_order_gate"]["pending_order_id"] == first_order.id
    current_order_ids = {order.id for order in state.list_paper_orders(limit=1000)}
    assert current_order_ids == existing_order_ids

    canceled = state.cancel_paper_order(
        first_order.id,
        PaperOrderCancelRequest(reason="release pending gate", canceled_by="worker-test"),
    )
    assert canceled.status == PaperOrderStatus.CANCELED

    third = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=run_at,
        auto_approve_recommendations=True,
        auto_approve_min_confidence=0.0,
        auto_approve_min_composite=0.0,
        max_auto_approvals=1,
        auto_execute_approved=True,
        auto_execution_mode="live_dry_run",
        max_auto_buys=1,
        max_auto_sells=0,
        order_dedupe_minutes=0,
        rebuy_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert third["auto_execution"]["buy_order_count"] == 1
    third_buy_action = next(
        item for item in third["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert third_buy_action["status"] == "executed"
    assert third_buy_action["order_id"] != first_order.id


def test_system_cycle_skips_recent_duplicate_filled_buy_order() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)
    run_at = datetime(2026, 7, 5, 9, 30, tzinfo=timezone.utc)

    first = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=run_at,
        auto_approve_recommendations=True,
        auto_approve_min_confidence=0.0,
        auto_approve_min_composite=0.0,
        max_auto_approvals=1,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        order_dedupe_minutes=0,
        rebuy_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert first["auto_execution"]["buy_order_count"] == 1
    first_buy_action = next(
        item for item in first["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    first_order = state.list_paper_orders(
        limit=1,
        recommendation_id=first_buy_action["recommendation_id"],
    )[0]
    assert first_order.status == PaperOrderStatus.FILLED
    state.close_holding(first_buy_action["ticker"])
    existing_order_ids = {order.id for order in state.list_paper_orders(limit=1000)}

    second = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=run_at,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        order_dedupe_minutes=10080,
        rebuy_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert second["auto_execution"]["enabled"] is True
    assert second["auto_execution"]["buy_order_count"] == 0
    buy_action = next(
        item for item in second["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "skipped"
    assert buy_action["reason"] == "recent_buy_order_gate_failed"
    assert buy_action["recent_buy_order_gate"]["passed"] is False
    assert buy_action["recent_buy_order_gate"]["recent_order_id"] == first_order.id
    current_order_ids = {order.id for order in state.list_paper_orders(limit=1000)}
    assert current_order_ids == existing_order_ids


def test_system_cycle_uses_persisted_autopilot_policy() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    for holding in state.list_holdings(status=HoldingStatus.OPEN, limit=100):
        state.close_holding(holding.ticker)
    policy = state.update_autopilot_policy(
        {
            "enabled": True,
            "auto_approve_recommendations": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "paper",
            "auto_approve_min_confidence": 0.5,
            "auto_approve_min_composite": 0.0,
            "max_auto_approvals": 1,
            "max_auto_buys": 1,
            "max_auto_sells": 0,
            "account_equity": 100_000_000,
            "max_daily_realized_loss_pct": 1.0,
            "max_auto_buy_price_drift_pct": 1.0,
            "rebuy_cooldown_minutes": 0,
            "updated_by": "worker-test",
            "reason": "policy-driven-cycle",
        }
    )

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        use_autopilot_policy=True,
    )

    assert result["use_autopilot_policy"] is True
    assert result["autopilot_policy"]["policy_id"] == policy.policy_id
    assert result["autopilot_preflight"]["status"] == "ready"
    assert result["autopilot_preflight"]["can_auto_approve"] is True
    assert result["autopilot_preflight"]["can_auto_execute"] is True
    assert result["auto_approval"]["enabled"] is True
    assert result["auto_approval"]["approved_count"] == 1
    assert result["auto_execution"]["enabled"] is True
    assert result["auto_execution"]["buy_order_count"] == 1
    latest_run = state.list_system_cycle_runs(limit=1)[0]
    assert latest_run.metrics["autopilot_policy"]["policy_id"] == policy.policy_id
    assert latest_run.metrics["autopilot_preflight"]["status"] == "ready"
    assert latest_run.metrics["auto_approval"]["approved_count"] == 1
    assert latest_run.metrics["auto_execution"]["buy_order_count"] == 1


def test_system_cycle_autopilot_preflight_blocks_on_kill_switch() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    state.update_autopilot_policy(
        {
            "enabled": True,
            "auto_approve_recommendations": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "paper",
            "auto_approve_min_confidence": 0.5,
            "max_auto_approvals": 1,
            "max_auto_buys": 1,
            "max_auto_sells": 1,
            "account_equity": 100_000_000,
            "max_daily_realized_loss_pct": 1.0,
            "updated_by": "worker-test",
            "reason": "kill switch preflight",
        }
    )
    state.set_kill_switch(True, "maintenance", "worker-test")

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        use_autopilot_policy=True,
    )

    assert result["autopilot_preflight"]["status"] == "blocked"
    assert "kill_switch_enabled" in result["autopilot_preflight"]["reasons"]
    assert result["auto_approval"]["enabled"] is False
    assert result["auto_execution"]["enabled"] is False
    assert result["auto_execution_enabled"] is False
    latest_run = state.list_system_cycle_runs(limit=1)[0]
    assert latest_run.metrics["autopilot_preflight"]["status"] == "blocked"


def test_system_cycle_autopilot_market_hours_gate_blocks_auto_execution() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    policy = state.update_autopilot_policy(
        {
            "enabled": True,
            "auto_approve_recommendations": False,
            "auto_execute_approved": True,
            "restrict_auto_execution_to_regular_hours": True,
            "auto_execution_mode": "paper",
            "max_auto_approvals": 0,
            "max_auto_buys": 1,
            "max_auto_sells": 1,
            "account_equity": 100_000_000,
            "max_daily_realized_loss_pct": 1.0,
            "updated_by": "worker-test",
            "reason": "market hours gate",
        }
    )
    open_preflight = state.build_autopilot_preflight(
        policy,
        as_of=datetime(2026, 4, 10, 13, 30, tzinfo=timezone.utc),
    )
    assert open_preflight.status == "ready"
    assert open_preflight.can_auto_execute is True

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 11, 13, 30, tzinfo=timezone.utc),
        use_autopilot_policy=True,
    )

    assert result["autopilot_policy"]["restrict_auto_execution_to_regular_hours"] is True
    assert result["autopilot_preflight"]["status"] == "blocked"
    assert result["autopilot_preflight"]["can_auto_execute"] is False
    assert "market_session_closed" in result["autopilot_preflight"]["reasons"]
    assert result["auto_execution"]["enabled"] is False
    assert result["auto_execution_enabled"] is False
    latest_run = state.list_system_cycle_runs(limit=1)[0]
    assert latest_run.metrics["autopilot_preflight"]["reasons"] == ["market_session_closed"]


def test_system_cycle_autopilot_daily_budget_blocks_after_usage() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    budget_day = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    state.record_system_cycle_run(
        SystemCycleRun(
            id="daily-budget-used",
            started_at=budget_day,
            finished_at=budget_day,
            status="success",
            recommendation_count=1,
            sell_alert_count=0,
            auto_execution_enabled=True,
            metrics={
                "auto_approval": {"approved_count": 1},
                "auto_execution": {"buy_order_count": 1, "sell_order_count": 0},
            },
        )
    )
    state.update_autopilot_policy(
        {
            "enabled": True,
            "auto_approve_recommendations": True,
            "auto_execute_approved": True,
            "auto_execution_mode": "paper",
            "auto_approve_min_confidence": 0.5,
            "max_auto_approvals": 1,
            "max_auto_buys": 1,
            "max_auto_sells": 0,
            "max_daily_auto_approvals": 1,
            "max_daily_auto_buys": 1,
            "max_daily_auto_sells": 0,
            "account_equity": 100_000_000,
            "max_daily_realized_loss_pct": 1.0,
            "updated_by": "worker-test",
            "reason": "daily budget gate",
        }
    )

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=datetime(2026, 4, 10, 15, 0, tzinfo=timezone.utc),
        use_autopilot_policy=True,
    )

    assert result["autopilot_preflight"]["status"] == "blocked"
    assert "daily_auto_approval_budget_exhausted" in result["autopilot_preflight"]["reasons"]
    assert "daily_auto_execution_budget_exhausted" in result["autopilot_preflight"]["reasons"]
    assert result["autopilot_preflight"]["daily_usage"]["used_buys"] == 1
    assert result["autopilot_preflight"]["daily_usage"]["remaining_buys"] == 0
    assert result["auto_approval"]["enabled"] is False
    assert result["auto_execution"]["enabled"] is False


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
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
    )

    assert result["auto_execution"]["sell_order_count"] == 1
    assert result["auto_execution"]["buy_order_count"] == 0
    sell_action = next(item for item in result["auto_execution"]["actions"] if item["action"] == "sell_alert")
    assert sell_action["status"] == "executed"
    assert sell_action["ticker"] == "AAPL"
    assert sell_action["source_snapshot_id"] is None
    assert sell_action["strategy_config_id"] is None
    assert sell_action["sold_qty"] == 5
    buy_actions = [item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"]
    assert any(item["reason"] == "sell_alert_same_cycle" for item in buy_actions)
    holding = state.holding_watch_repo.get("AAPL")
    assert holding is not None
    assert holding.status == HoldingStatus.CLOSED
    assert state.list_sell_execution_audits(limit=1, ticker="AAPL")[0].id == sell_action["sell_execution_id"]


def test_system_cycle_skips_repeated_sell_alert_during_cooldown() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)
    state.close_holding("AAPL")
    state.record_manual_buy(
        ManualBuyRequest(
            ticker="AAPL",
            qty=8,
            buy_price=100,
            stop_loss=1,
            take_profit1=2,
            take_profit2=999999,
            note="worker cycle sell cooldown setup",
        )
    )

    first = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=0,
        max_auto_sells=1,
        sell_alert_cooldown_minutes=0,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
    )

    assert first["auto_execution"]["sell_order_count"] == 1
    first_sell = next(item for item in first["auto_execution"]["actions"] if item["action"] == "sell_alert")
    assert first_sell["status"] == "executed"
    assert first_sell["reason_code"] == "take_profit1_hit"
    assert first_sell["source_snapshot_id"] is None
    assert first_sell["strategy_config_id"] is None
    assert first_sell["sold_qty"] == 4
    assert first_sell["control_adjustment"]["status"] == "updated"
    assert first_sell["control_adjustment"]["old_stop_loss"] == 1
    assert first_sell["control_adjustment"]["new_stop_loss"] == 1.998
    holding_after_first = state.holding_watch_repo.get("AAPL")
    assert holding_after_first is not None
    assert holding_after_first.status == HoldingStatus.OPEN
    assert holding_after_first.qty == 4
    assert holding_after_first.stop_loss == 1.998
    audit = state.list_holding_control_audits(limit=1, ticker="AAPL")[0]
    assert audit.id == first_sell["control_adjustment"]["audit_id"]
    assert audit.updated_by == "system_cycle:auto_sell"

    second = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=0,
        max_auto_sells=1,
        sell_alert_cooldown_minutes=60,
        max_snapshot_bar_age_minutes=999999,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
    )

    assert second["auto_execution"]["sell_order_count"] == 0
    second_sell = next(item for item in second["auto_execution"]["actions"] if item["action"] == "sell_alert")
    assert second_sell["status"] == "skipped"
    assert second_sell["reason"] == "sell_alert_cooldown_active"
    assert second_sell["cooldown"]["last_sell_execution_id"] == first_sell["sell_execution_id"]
    holding_after_second = state.holding_watch_repo.get("AAPL")
    assert holding_after_second is not None
    assert holding_after_second.status == HoldingStatus.OPEN
    assert holding_after_second.qty == 4


def test_system_cycle_skips_rebuy_during_cooldown_after_recent_sell() -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    seed_at = datetime(2026, 7, 5, 9, 30, tzinfo=timezone.utc)
    seed_result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
    )
    recommendation_id = seed_result["top_recommendations"][0]["id"]
    recommendation = state.recommendations_by_id[recommendation_id]
    ticker = recommendation.ticker
    state.close_holding(ticker)
    state.record_manual_buy(
        ManualBuyRequest(
            ticker=ticker,
            qty=1,
            buy_price=recommendation.entry_zone_high,
            bought_at=seed_at - timedelta(minutes=90),
            source_recommendation_id=recommendation_id,
            note="cooldown setup buy",
        )
    )
    sold_at = seed_at - timedelta(minutes=60)
    sell_result = state.sell_holding(
        ticker,
        ManualSellRequest(
            sell_price=recommendation.entry_zone_high,
            sold_at=sold_at,
            reason="cooldown setup sell",
        ),
    )
    state.decide_recommendation(
        ApprovalDecisionRequest(
            recommendation_id=recommendation_id,
            decision="approved",
            approver="worker-test",
            notes="approval should still be blocked by rebuy cooldown",
        )
    )
    existing_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }

    result = system_cycle(
        top_n=1,
        min_confidence=0.0,
        consume_events=False,
        as_of=seed_at,
        auto_execute_approved=True,
        auto_execution_mode="paper",
        max_auto_buys=1,
        max_auto_sells=0,
        rebuy_cooldown_minutes=240,
        account_equity=100_000_000,
        max_daily_realized_loss_pct=1.0,
        max_auto_buy_price_drift_pct=1.0,
    )

    assert sell_result.holding.status == HoldingStatus.CLOSED
    assert result["auto_execution"]["enabled"] is True
    assert result["auto_execution"]["buy_order_count"] == 0
    buy_action = next(
        item for item in result["auto_execution"]["actions"] if item["action"] == "buy_recommendation"
    )
    assert buy_action["status"] == "skipped"
    assert buy_action["ticker"] == ticker
    assert buy_action["recommendation_id"] == recommendation_id
    assert buy_action["reason"] == "rebuy_cooldown_active"
    assert buy_action["cooldown"]["last_sell_trade_id"]
    assert buy_action["cooldown"]["cooldown_until"] == "2026-07-05T12:30:00+00:00"
    assert buy_action["cooldown"]["minutes_remaining"] == 180
    current_order_ids = {
        order.id for order in state.list_paper_orders(limit=100, recommendation_id=recommendation_id)
    }
    assert current_order_ids == existing_order_ids


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


def test_system_cycle_loop_records_errors_and_activates_kill_switch(monkeypatch) -> None:
    state = get_app_state()
    state.reset()
    state.consume_events(limit=1000)

    def failing_cycle(**_kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(worker_main, "system_cycle", failing_cycle)

    report = worker_main.system_cycle_loop(
        interval_seconds=0,
        max_cycles=5,
        max_consecutive_errors=2,
        sleep_fn=lambda _seconds: None,
    )

    assert report["job"] == "system_cycle_loop"
    assert report["cycle_count"] == 2
    assert report["success_count"] == 0
    assert report["error_count"] == 2
    assert report["consecutive_error_count"] == 2
    assert report["kill_switch_activated"] is True
    assert report["stopped_reason"] == "max_consecutive_errors"
    assert all(item["system_cycle_run_id"] for item in report["errors"])
    assert state.kill_switch.enabled is True
    assert "provider unavailable" in (state.kill_switch.reason or "")
    error_runs = state.list_system_cycle_runs(limit=5, status="error")
    assert len(error_runs) >= 2
    assert error_runs[0].error_message == "provider unavailable"
    assert error_runs[0].metrics["loop_error"]["error_type"] == "RuntimeError"
