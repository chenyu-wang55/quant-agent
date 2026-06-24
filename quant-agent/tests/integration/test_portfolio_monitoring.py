from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app


AUTH_HEADERS = {"x-access-password": "test-access-password"}


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

    holdings_response = client.get("/portfolio/holdings", headers=AUTH_HEADERS)
    assert holdings_response.status_code == 200
    assert len(holdings_response.json()) == 1

    alert_response = client.get("/portfolio/alerts", headers=AUTH_HEADERS)
    assert alert_response.status_code == 200
    alerts = alert_response.json()
    assert len(alerts) >= 1
    assert alerts[0]["ticker"] == recommendation["ticker"]
    assert alerts[0]["reason_code"] == "stop_loss_breach"
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

    sell_trades = client.get(f"/portfolio/trades?ticker={ticker}&side=sell", headers=AUTH_HEADERS)
    assert sell_trades.status_code == 200
    sell_rows = sell_trades.json()
    assert sell_rows
    assert sell_rows[0]["source_recommendation_id"] == recommendation["id"]
    assert sell_rows[0]["reason"] == "alert:stop_loss_breach"
