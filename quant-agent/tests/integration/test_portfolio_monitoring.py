from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from services.execution.broker_adapter import BrokerOrderPlacement, BrokerOrderUpdate
from services.execution.router import ExecutionRouter


AUTH_HEADERS = {"x-access-password": "test-access-password"}


class FakeBrokerAdapter:
    name = "fake-broker"

    def __init__(
        self,
        *,
        raw_status: str,
        fill_price: float | None = None,
        filled_qty: float | None = None,
        broker_order_id: str = "fake_sell_broker_order",
    ) -> None:
        self.raw_status = raw_status
        self.fill_price = fill_price
        self.filled_qty = filled_qty
        self.broker_order_id = broker_order_id
        self.placements: list[BrokerOrderPlacement] = []

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        self.placements.append(placement)
        return BrokerOrderUpdate(
            broker_order_id=self.broker_order_id,
            raw_status=self.raw_status,
            client_order_id=placement.client_order_id,
            submitted_at=datetime.now(timezone.utc),
            filled_at=datetime.now(timezone.utc) if self.raw_status in {"filled", "partially_filled"} else None,
            filled_avg_price=self.fill_price,
            filled_qty=self.filled_qty,
        )

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        raise AssertionError("not used in this test")


def test_manual_buy_record_and_sell_alerts() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "portfolio monitoring test",
            "as_of": "2026-04-10T09:30:00Z",
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
    cleanup_response = client.post(f"/portfolio/holdings/{recommendation['ticker']}/close", headers=AUTH_HEADERS)
    assert cleanup_response.status_code in {200, 404}
    baseline_summary = client.get("/portfolio/summary", headers=AUTH_HEADERS)
    assert baseline_summary.status_code == 200
    baseline_summary_data = baseline_summary.json()
    baseline_realized = baseline_summary_data["total_realized_pnl"]
    baseline_closed_count = baseline_summary_data["closed_holding_count"]
    baseline_performance = client.get("/portfolio/performance", headers=AUTH_HEADERS)
    assert baseline_performance.status_code == 200
    baseline_ticker_rows = [
        item for item in baseline_performance.json()["by_ticker"] if item["ticker"] == recommendation["ticker"]
    ]
    baseline_ticker_realized = baseline_ticker_rows[0]["total_realized_pnl"] if baseline_ticker_rows else 0.0
    baseline_attribution = client.get("/portfolio/recommendation-attribution", headers=AUTH_HEADERS)
    assert baseline_attribution.status_code == 200
    baseline_attr_rows = [
        item
        for item in baseline_attribution.json()["by_recommendation"]
        if item["recommendation_id"] == recommendation["id"]
    ]
    baseline_attr_realized = baseline_attr_rows[0]["total_realized_pnl"] if baseline_attr_rows else 0.0
    baseline_attr_sell_count = baseline_attr_rows[0]["sell_trade_count"] if baseline_attr_rows else 0

    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": recommendation["ticker"],
            "qty": 100,
            "buy_price": recommendation["entry_zone_high"],
            "source_recommendation_id": recommendation["id"],
            "note": "manual buy for monitoring",
            "stop_loss": 99999999,
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200
    assert buy_response.json()["ticker"] == recommendation["ticker"]

    trades_after_buy = client.get(f"/portfolio/trades?ticker={recommendation['ticker']}", headers=AUTH_HEADERS)
    assert trades_after_buy.status_code == 200
    buy_trades = trades_after_buy.json()
    assert len(buy_trades) >= 1
    assert buy_trades[0]["side"] == "buy"
    assert buy_trades[0]["qty"] == 100
    assert buy_trades[0]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert buy_trades[0]["strategy_config_id"] == recommendation["strategy_config_id"]

    holdings_response = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_response.status_code == 200
    assert len(holdings_response.json()) == 1

    alert_response = client.get("/portfolio/alerts", headers=AUTH_HEADERS)
    assert alert_response.status_code == 200
    alerts = alert_response.json()
    assert len(alerts) >= 1
    assert alerts[0]["ticker"] == recommendation["ticker"]
    assert alerts[0]["reason_code"] == "stop_loss_breach"
    assert alerts[0]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert alerts[0]["strategy_config_id"] == recommendation["strategy_config_id"]
    assert "止损" in alerts[0]["message_cn"]

    first_sell_price = recommendation["entry_zone_high"] + 5
    partial_sell = client.post(
        f"/portfolio/holdings/{recommendation['ticker']}/sell",
        json={"qty": 40, "sell_price": first_sell_price, "reason": "trim_at_target"},
        headers=AUTH_HEADERS,
    )
    assert partial_sell.status_code == 200
    partial_data = partial_sell.json()
    assert partial_data["sold_qty"] == 40
    assert partial_data["remaining_qty"] == 60
    assert partial_data["holding"]["status"] == "open"
    assert partial_data["realized_pnl_delta"] == pytest.approx(40 * 5)

    summary_after_partial = client.get("/portfolio/summary", headers=AUTH_HEADERS)
    assert summary_after_partial.status_code == 200
    partial_summary = summary_after_partial.json()
    assert partial_summary["open_holding_count"] == 1
    assert partial_summary["sell_trade_count"] >= 1
    assert partial_summary["total_realized_pnl"] - baseline_realized == pytest.approx(40 * 5)

    oversized_sell = client.post(
        f"/portfolio/holdings/{recommendation['ticker']}/sell",
        json={"qty": 1000, "sell_price": first_sell_price, "reason": "too_much"},
        headers=AUTH_HEADERS,
    )
    assert oversized_sell.status_code == 400

    final_sell_price = recommendation["entry_zone_high"] - 2
    final_sell = client.post(
        f"/portfolio/holdings/{recommendation['ticker']}/sell",
        json={"sell_price": final_sell_price, "reason": "exit_remaining"},
        headers=AUTH_HEADERS,
    )
    assert final_sell.status_code == 200
    final_data = final_sell.json()
    assert final_data["remaining_qty"] == 0
    assert final_data["holding"]["status"] == "closed"
    assert final_data["total_realized_pnl"] == pytest.approx((40 * 5) + (60 * -2))

    holdings_after_sell = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_after_sell.status_code == 200
    assert holdings_after_sell.json() == []

    closed_holdings = client.get("/portfolio/holdings?status=closed", headers=AUTH_HEADERS)
    assert closed_holdings.status_code == 200
    assert any(item["ticker"] == recommendation["ticker"] for item in closed_holdings.json())

    sell_trades = client.get(f"/portfolio/trades?ticker={recommendation['ticker']}&side=sell", headers=AUTH_HEADERS)
    assert sell_trades.status_code == 200
    sell_rows = sell_trades.json()
    assert len(sell_rows) >= 2
    assert sell_rows[0]["holding_status_after"] == "closed"
    assert sell_rows[0]["realized_pnl_delta"] == pytest.approx(60 * -2)
    assert sell_rows[0]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert sell_rows[0]["strategy_config_id"] == recommendation["strategy_config_id"]

    final_summary = client.get("/portfolio/summary", headers=AUTH_HEADERS)
    assert final_summary.status_code == 200
    summary_data = final_summary.json()
    assert summary_data["open_holding_count"] == 0
    assert summary_data["closed_holding_count"] >= baseline_closed_count + 1
    assert summary_data["total_realized_pnl"] - baseline_realized == pytest.approx((40 * 5) + (60 * -2))

    performance_response = client.get("/portfolio/performance", headers=AUTH_HEADERS)
    assert performance_response.status_code == 200
    performance = performance_response.json()
    assert performance["sell_trade_count"] >= 2
    assert performance["win_count"] >= 1
    assert performance["loss_count"] >= 1
    assert performance["profit_factor"] is not None
    ticker_rows = [item for item in performance["by_ticker"] if item["ticker"] == recommendation["ticker"]]
    assert ticker_rows
    assert ticker_rows[0]["total_realized_pnl"] - baseline_ticker_realized == pytest.approx((40 * 5) + (60 * -2))
    assert ticker_rows[0]["win_rate"] > 0

    attribution_response = client.get("/portfolio/recommendation-attribution", headers=AUTH_HEADERS)
    assert attribution_response.status_code == 200
    attribution = attribution_response.json()
    recommendation_rows = [
        item for item in attribution["by_recommendation"] if item["recommendation_id"] == recommendation["id"]
    ]
    assert recommendation_rows
    row = recommendation_rows[0]
    assert row["ticker"] == recommendation["ticker"]
    assert row["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert row["strategy_config_id"] == recommendation["strategy_config_id"]
    assert row["sell_trade_count"] >= baseline_attr_sell_count + 2
    assert row["total_realized_pnl"] - baseline_attr_realized == pytest.approx((40 * 5) + (60 * -2))
    assert row["win_rate"] > 0
    snapshot_rows = [
        item for item in attribution["by_snapshot"] if item["source_snapshot_id"] == recommendation["source_snapshot_id"]
    ]
    assert snapshot_rows
    assert snapshot_rows[0]["sell_trade_count"] >= 2
    assert snapshot_rows[0]["closed_trade_count"] >= 1
    assert snapshot_rows[0]["expectancy_per_sell"] != 0
    assert 0 <= snapshot_rows[0]["performance_score"] <= 100
    assert snapshot_rows[0]["quality_grade"] in {
        "outperforming",
        "positive",
        "neutral",
        "weak",
        "negative",
    }
    assert snapshot_rows[0]["avg_confidence"] is not None
    assert snapshot_rows[0]["avg_composite"] is not None
    strategy_rows = [
        item
        for item in attribution["by_strategy_config"]
        if item["strategy_config_id"] == recommendation["strategy_config_id"]
    ]
    assert strategy_rows
    assert strategy_rows[0]["sell_trade_count"] >= 2
    assert 0 <= strategy_rows[0]["performance_score"] <= 100
    assert strategy_rows[0]["quality_grade"] in {
        "outperforming",
        "positive",
        "neutral",
        "weak",
        "negative",
    }

    tuning_response = client.get("/strategy-configs/tuning-report", headers=AUTH_HEADERS)
    assert tuning_response.status_code == 200
    tuning_report = tuning_response.json()
    tuning_items = [
        item
        for item in tuning_report["items"]
        if item["strategy_config_id"] == recommendation["strategy_config_id"]
    ]
    assert tuning_items
    tuning_item = tuning_items[0]
    assert tuning_item["action"] in {
        "collect_more_data",
        "keep",
        "tighten",
        "relax",
        "review",
    }
    assert tuning_item["metric_snapshot"]["sell_trade_count"] >= 2
    assert tuning_item["current_parameters"]["min_confidence"] == 0.0
    assert tuning_item["rationale_cn"]
    if tuning_item["action"] == "tighten":
        assert "risk_policy.min_confidence" in tuning_item["recommended_changes"]


def test_execute_sell_alert_closes_holding_and_records_trade() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "alert execution test",
            "as_of": "2026-04-10T09:30:00Z",
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
    ticker = recommendation["ticker"]
    cleanup_response = client.post(f"/portfolio/holdings/{ticker}/close", headers=AUTH_HEADERS)
    assert cleanup_response.status_code in {200, 404}

    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": ticker,
            "qty": 25,
            "buy_price": recommendation["entry_zone_high"],
            "source_recommendation_id": recommendation["id"],
            "note": "alert execution setup",
            "stop_loss": 99999999,
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200

    alert_response = client.get("/portfolio/alerts", headers=AUTH_HEADERS)
    assert alert_response.status_code == 200
    alerts = alert_response.json()
    assert any(item["ticker"] == ticker and item["reason_code"] == "stop_loss_breach" for item in alerts)

    blocked_live_sell = client.post(
        f"/portfolio/holdings/{ticker}/sell",
        json={
            "sell_price": recommendation["entry_zone_high"],
            "execution_mode": "live",
            "dry_run": False,
            "confirm_live": False,
        },
        headers=AUTH_HEADERS,
    )
    assert blocked_live_sell.status_code == 400
    assert "Live sell execution requires" in blocked_live_sell.json()["detail"]

    unconfigured_live_sell = client.post(
        f"/portfolio/holdings/{ticker}/sell",
        json={
            "sell_price": recommendation["entry_zone_high"],
            "execution_mode": "live",
            "dry_run": False,
            "confirm_live": True,
        },
        headers=AUTH_HEADERS,
    )
    assert unconfigured_live_sell.status_code == 501
    assert "Live broker sell adapter is not configured" in unconfigured_live_sell.json()["detail"]

    dry_run_baseline_trades = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert dry_run_baseline_trades.status_code == 200
    dry_run_baseline_sell_count = len(dry_run_baseline_trades.json())
    dry_run_response = client.post(
        f"/portfolio/alerts/{ticker}/execute",
        json={"reason_code": "stop_loss_breach", "execution_mode": "live", "dry_run": True},
        headers=AUTH_HEADERS,
    )
    assert dry_run_response.status_code == 200
    dry_run_execution = dry_run_response.json()["execution"]
    assert dry_run_execution["execution_mode"] == "live"
    assert dry_run_execution["dry_run"] is True
    assert dry_run_execution["applied_to_ledger"] is False
    assert dry_run_execution["broker_order_id"].startswith("live_sell_dryrun_")
    assert "not sent to a broker" in dry_run_execution["adapter_message"]
    assert dry_run_execution["holding"]["status"] == "open"
    assert dry_run_execution["remaining_qty"] == 25
    assert dry_run_execution["realized_pnl_delta"] == 0
    assert dry_run_execution["estimated_realized_pnl_delta"] is not None

    holdings_after_dry_run = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_after_dry_run.status_code == 200
    dry_run_holding = next(item for item in holdings_after_dry_run.json() if item["ticker"] == ticker)
    assert dry_run_holding["qty"] == 25
    assert dry_run_holding["status"] == "open"
    trades_after_dry_run = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert trades_after_dry_run.status_code == 200
    assert len(trades_after_dry_run.json()) == dry_run_baseline_sell_count
    dry_run_audits = client.get(
        f"/portfolio/sell-executions?ticker={ticker}&dry_run=true",
        headers=AUTH_HEADERS,
    )
    assert dry_run_audits.status_code == 200
    dry_run_audit_rows = dry_run_audits.json()
    assert dry_run_audit_rows
    assert dry_run_audit_rows[0]["id"] == dry_run_execution["sell_execution_id"]
    assert dry_run_audit_rows[0]["execution_mode"] == "live"
    assert dry_run_audit_rows[0]["dry_run"] is True
    assert dry_run_audit_rows[0]["applied_to_ledger"] is False
    assert dry_run_audit_rows[0]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert dry_run_audit_rows[0]["strategy_config_id"] == recommendation["strategy_config_id"]

    dry_run_events = client.post("/events/consume?limit=100", headers=AUTH_HEADERS)
    assert dry_run_events.status_code == 200
    assert any(
        event["event_type"] == "sell_routed"
        and event["payload"]["ticker"] == ticker
        and event["payload"]["dry_run"] is True
        and event["payload"]["applied_to_ledger"] is False
        for event in dry_run_events.json()
    )

    execute_response = client.post(
        f"/portfolio/alerts/{ticker}/execute",
        json={"reason_code": "stop_loss_breach"},
        headers=AUTH_HEADERS,
    )
    assert execute_response.status_code == 200
    execution = execute_response.json()["execution"]
    assert execution["sold_qty"] == 25
    assert execution["remaining_qty"] == 0
    assert execution["holding"]["status"] == "closed"
    assert execution["execution_mode"] == "paper"
    assert execution["dry_run"] is False
    assert execution["applied_to_ledger"] is True
    assert execution["broker_order_id"].startswith("paper_sell_")
    paper_audits = client.get(
        f"/portfolio/sell-executions?ticker={ticker}&applied_to_ledger=true",
        headers=AUTH_HEADERS,
    )
    assert paper_audits.status_code == 200
    paper_audit_rows = paper_audits.json()
    assert paper_audit_rows
    assert paper_audit_rows[0]["id"] == execution["sell_execution_id"]
    assert paper_audit_rows[0]["status"] == "filled"
    assert paper_audit_rows[0]["holding_status_after"] == "closed"
    assert paper_audit_rows[0]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert paper_audit_rows[0]["strategy_config_id"] == recommendation["strategy_config_id"]

    sell_trades = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert sell_trades.status_code == 200
    sell_rows = sell_trades.json()
    assert sell_rows
    assert sell_rows[0]["source_recommendation_id"] == recommendation["id"]
    assert sell_rows[0]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert sell_rows[0]["strategy_config_id"] == recommendation["strategy_config_id"]
    assert sell_rows[0]["reason"] == "alert:stop_loss_breach"


def test_confirmed_live_sell_uses_configured_broker_adapter_and_records_fill() -> None:
    state = get_app_state()
    state.reset()
    original_router = state.execution_router
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "configured-live-sell-broker-test",
            "as_of": "2026-04-10T09:30:00Z",
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
    ticker = recommendation["ticker"]
    state.close_holding(ticker)

    buy_price = recommendation["entry_zone_high"]
    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": ticker,
            "qty": 10,
            "buy_price": buy_price,
            "source_recommendation_id": recommendation["id"],
            "note": "live sell setup",
            "stop_loss": recommendation["stop_loss"],
            "take_profit1": recommendation["tp1"],
            "take_profit2": recommendation["tp2"],
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200

    sell_price = round(buy_price + 5.0, 6)
    fake_adapter = FakeBrokerAdapter(raw_status="filled", fill_price=sell_price, filled_qty=4)
    state.execution_router = ExecutionRouter(broker_adapter=fake_adapter)
    try:
        sell_response = client.post(
            f"/portfolio/holdings/{ticker}/sell",
            json={
                "qty": 4,
                "sell_price": sell_price,
                "execution_mode": "live",
                "dry_run": False,
                "confirm_live": True,
                "reason": "live broker sell test",
            },
            headers=AUTH_HEADERS,
        )
    finally:
        state.execution_router = original_router

    assert sell_response.status_code == 200
    execution = sell_response.json()
    assert execution["execution_mode"] == "live"
    assert execution["dry_run"] is False
    assert execution["applied_to_ledger"] is True
    assert execution["broker_order_id"] == "fake_sell_broker_order"
    assert execution["sold_qty"] == 4
    assert execution["sell_price"] == sell_price
    assert execution["remaining_qty"] == 6
    assert execution["holding"]["status"] == "open"
    assert execution["holding"]["qty"] == 6
    assert "fake-broker: status=filled" in execution["adapter_message"]

    assert len(fake_adapter.placements) == 1
    placement = fake_adapter.placements[0]
    assert placement.symbol == ticker
    assert placement.side == "sell"
    assert placement.qty == 4
    assert placement.limit_price == sell_price
    assert placement.client_order_id.startswith("quant_sell_")

    holdings = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings.status_code == 200
    holding = next(item for item in holdings.json() if item["ticker"] == ticker)
    assert holding["qty"] == 6
    assert holding["last_sell_price"] == sell_price
    assert holding["last_sell_reason"] == "live broker sell test"

    sell_trades = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert sell_trades.status_code == 200
    sell_rows = sell_trades.json()
    assert sell_rows
    assert sell_rows[0]["price"] == sell_price
    assert sell_rows[0]["qty"] == 4
    assert sell_rows[0]["reason"] == "live broker sell test"
    assert sell_rows[0]["source_recommendation_id"] == recommendation["id"]

    sell_audits = client.get(
        f"/portfolio/sell-executions?ticker={ticker}&applied_to_ledger=true",
        headers=AUTH_HEADERS,
    )
    assert sell_audits.status_code == 200
    audit = next(item for item in sell_audits.json() if item["id"] == execution["sell_execution_id"])
    assert audit["execution_mode"] == "live"
    assert audit["status"] == "filled"
    assert audit["broker_order_id"] == "fake_sell_broker_order"
    assert audit["remaining_qty"] == 6


def test_broker_sell_sync_applies_delayed_fill_once() -> None:
    state = get_app_state()
    state.reset()
    original_router = state.execution_router
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "broker-sell-sync-test",
            "as_of": "2026-04-10T09:30:00Z",
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
    ticker = recommendation["ticker"]
    state.close_holding(ticker)

    buy_price = recommendation["entry_zone_high"]
    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": ticker,
            "qty": 10,
            "buy_price": buy_price,
            "source_recommendation_id": recommendation["id"],
            "note": "delayed live sell setup",
            "stop_loss": recommendation["stop_loss"],
            "take_profit1": recommendation["tp1"],
            "take_profit2": recommendation["tp2"],
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200

    sell_price = round(buy_price + 4.0, 6)
    broker_order_id = f"fake_delayed_sell_{uuid4().hex[:8]}"
    sell_reason = f"delayed broker sell {uuid4().hex[:8]}"
    fake_adapter = FakeBrokerAdapter(
        raw_status="accepted",
        broker_order_id=broker_order_id,
    )
    state.execution_router = ExecutionRouter(broker_adapter=fake_adapter)
    try:
        submitted_sell = client.post(
            f"/portfolio/holdings/{ticker}/sell",
            json={
                "qty": 4,
                "sell_price": sell_price,
                "execution_mode": "live",
                "dry_run": False,
                "confirm_live": True,
                "reason": sell_reason,
            },
            headers=AUTH_HEADERS,
        )
    finally:
        state.execution_router = original_router

    assert submitted_sell.status_code == 200
    submitted_execution = submitted_sell.json()
    assert submitted_execution["applied_to_ledger"] is False
    assert submitted_execution["broker_order_id"] == broker_order_id
    assert submitted_execution["remaining_qty"] == 10

    holdings_before_sync = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_before_sync.status_code == 200
    assert next(item for item in holdings_before_sync.json() if item["ticker"] == ticker)["qty"] == 10

    sync_response = client.post(
        "/portfolio/sell-executions/broker-sync",
        json={
            "broker": "integration-broker",
            "updated_by": "qa",
            "statuses": [
                {
                    "broker_order_id": broker_order_id,
                    "status": "filled",
                    "fill_price": sell_price,
                    "filled_qty": 4,
                    "broker_message": "sell filled later",
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert sync_response.status_code == 200
    sync_result = sync_response.json()
    assert sync_result["filled_count"] == 1
    assert sync_result["items"][0]["action"] == "filled"
    assert sync_result["items"][0]["order_id"] == submitted_execution["sell_execution_id"]

    holdings_after_sync = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_after_sync.status_code == 200
    holding = next(item for item in holdings_after_sync.json() if item["ticker"] == ticker)
    assert holding["qty"] == 6
    assert holding["last_sell_price"] == sell_price

    sell_trades = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert sell_trades.status_code == 200
    sell_rows = [
        row for row in sell_trades.json()
        if row["reason"] == sell_reason and row["price"] == sell_price
    ]
    assert len(sell_rows) == 1
    assert sell_rows[0]["qty"] == 4

    sell_audits = client.get(
        f"/portfolio/sell-executions?ticker={ticker}&applied_to_ledger=true",
        headers=AUTH_HEADERS,
    )
    assert sell_audits.status_code == 200
    audit = next(item for item in sell_audits.json() if item["id"] == submitted_execution["sell_execution_id"])
    assert audit["status"] == "filled"
    assert audit["applied_to_ledger"] is True
    assert audit["remaining_qty"] == 6

    repeat_sync = client.post(
        "/portfolio/sell-executions/broker-sync",
        json={
            "broker": "integration-broker",
            "updated_by": "qa",
            "statuses": [
                {
                    "broker_order_id": broker_order_id,
                    "status": "filled",
                    "fill_price": sell_price,
                    "filled_qty": 4,
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert repeat_sync.status_code == 200
    assert repeat_sync.json()["unchanged_count"] == 1
    trades_after_repeat = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert trades_after_repeat.status_code == 200
    repeated_rows = [
        row for row in trades_after_repeat.json()
        if row["reason"] == sell_reason and row["price"] == sell_price
    ]
    assert len(repeated_rows) == 1


def test_update_holding_controls_records_audit_and_drives_alerts() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    run_response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": "holding controls audit test",
            "as_of": "2026-04-10T09:30:00Z",
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
    ticker = recommendation["ticker"]
    client.post(f"/portfolio/holdings/{ticker}/close", headers=AUTH_HEADERS)

    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": ticker,
            "qty": 25,
            "buy_price": recommendation["entry_zone_high"],
            "source_recommendation_id": recommendation["id"],
            "note": "initial controls",
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200
    original = buy_response.json()

    invalid_response = client.patch(
        f"/portfolio/holdings/{ticker}/controls",
        json={
            "stop_loss": original["take_profit1"] + 1,
            "reason": "bad control order",
            "updated_by": "qa",
        },
        headers=AUTH_HEADERS,
    )
    assert invalid_response.status_code == 400

    control_response = client.patch(
        f"/portfolio/holdings/{ticker}/controls",
        json={
            "stop_loss": 99999999,
            "take_profit1": 100000000,
            "take_profit2": 100000001,
            "note": "tightened for control audit",
            "reason": "force alert in test",
            "updated_by": "qa",
        },
        headers=AUTH_HEADERS,
    )
    assert control_response.status_code == 200
    updated = control_response.json()
    assert updated["holding"]["stop_loss"] == 99999999
    assert updated["holding"]["take_profit1"] == 100000000
    assert updated["audit"]["old_stop_loss"] == original["stop_loss"]
    assert updated["audit"]["new_stop_loss"] == 99999999
    assert updated["audit"]["source_snapshot_id"] == recommendation["source_snapshot_id"]
    assert updated["audit"]["strategy_config_id"] == recommendation["strategy_config_id"]
    assert updated["audit"]["reason"] == "force alert in test"
    assert updated["audit"]["updated_by"] == "qa"

    audit_response = client.get(f"/portfolio/holding-control-audits?ticker={ticker}", headers=AUTH_HEADERS)
    assert audit_response.status_code == 200
    audits = audit_response.json()
    assert audits
    assert audits[0]["id"] == updated["audit"]["id"]
    assert audits[0]["new_take_profit2"] == 100000001

    event_response = client.get("/events/pending", headers=AUTH_HEADERS)
    assert event_response.status_code == 200
    assert any(event["event_type"] == "holding_controls_updated" for event in event_response.json())

    alert_response = client.get("/portfolio/alerts", headers=AUTH_HEADERS)
    assert alert_response.status_code == 200
    alerts = [item for item in alert_response.json() if item["ticker"] == ticker]
    assert alerts
    assert alerts[0]["reason_code"] == "stop_loss_breach"
    assert alerts[0]["stop_loss"] == 99999999

    cleanup_response = client.post(f"/portfolio/holdings/{ticker}/close", headers=AUTH_HEADERS)
    assert cleanup_response.status_code == 200
