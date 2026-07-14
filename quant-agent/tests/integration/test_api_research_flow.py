from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app

AUTH_HEADERS = {"x-access-password": "test-access-password"}


def test_api_research_recommendation_and_paper_order_flow() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    payload = {
        "run_type": "research_batch",
        "objective": "integration-test-run",
        "as_of": "2026-04-10T09:30:00Z",
        "snapshot_mode": "point_in_time",
        "source_snapshot_id": f"api-flow-{uuid4().hex}",
        "universe": "SP500",
        "universe_rules": {
            "min_price": 5,
            "min_avg_dollar_volume": 5000000,
            "max_spread_bps": 100,
            "min_market_cap_usd": 1000000000,
            "allowed_sectors": [],
            "max_candidates_after_filter": 100,
        },
        "risk_policy": {
            "min_confidence": 0.0,
            "earnings_blackout_minutes": 15,
            "max_name_weight": 0.10,
            "max_sector_weight": 0.30,
            "max_gross_exposure": 1.0,
            "max_correlated_cluster_weight": 0.35,
            "reject_on_material_evidence_conflict": False,
            "event_trading_enabled": True,
        },
        "publication": {"top_n": 3, "output_channels": ["api"]},
        "execution_mode": "research_only",
    }

    run_response = client.post("/research/run", json=payload, headers=AUTH_HEADERS)
    assert run_response.status_code == 200
    run_data = run_response.json()
    assert "recommendations" in run_data
    assert len(run_data["recommendations"]) > 0
    assert run_data["recommendations"][0]["analysis"]["summary"] != ""
    assert run_data["recommendations"][0]["analysis"]["report_cn"] != ""
    assert len(run_data["recommendations"][0]["analysis"]["why_to_buy_cn"]) > 0
    assert len(run_data["recommendations"][0]["analysis"]["why_to_sell_cn"]) > 0
    snapshot_id = run_data["source_snapshot_id"]
    strategy_config_id = run_data["strategy_config_id"]
    assert strategy_config_id
    assert run_data["recommendations"][0]["strategy_config_id"] == strategy_config_id
    ticker = run_data["recommendations"][0]["ticker"]

    strategy_configs_response = client.get("/strategy-configs?limit=5", headers=AUTH_HEADERS)
    assert strategy_configs_response.status_code == 200
    assert any(item["strategy_config_id"] == strategy_config_id for item in strategy_configs_response.json())

    strategy_config_response = client.get(f"/strategy-configs/{strategy_config_id}", headers=AUTH_HEADERS)
    assert strategy_config_response.status_code == 200
    strategy_config = strategy_config_response.json()
    assert strategy_config["strategy_config_id"] == strategy_config_id
    assert strategy_config["risk_policy"]["min_confidence"] == 0.0
    assert strategy_config["publication"]["top_n"] == 3

    snapshots_response = client.get("/source-snapshots?limit=5", headers=AUTH_HEADERS)
    assert snapshots_response.status_code == 200
    assert any(item["source_snapshot_id"] == snapshot_id for item in snapshots_response.json())
    snapshot_summary = next(item for item in snapshots_response.json() if item["source_snapshot_id"] == snapshot_id)
    assert snapshot_summary["data_quality"]["status"] == "complete"
    assert snapshot_summary["data_quality"]["bar_coverage"] == 1.0
    assert snapshot_summary["data_quality"]["fundamental_coverage"] == 1.0

    snapshot_detail_response = client.get(f"/source-snapshots/{snapshot_id}?event_limit=5", headers=AUTH_HEADERS)
    assert snapshot_detail_response.status_code == 200
    snapshot_detail = snapshot_detail_response.json()
    assert snapshot_detail["source_snapshot_id"] == snapshot_id
    assert snapshot_detail["ticker_count"] > 0
    assert snapshot_detail["bar_count"] > 0
    assert snapshot_detail["recommendation_count"] >= 1
    assert snapshot_detail["data_quality"]["captured_ticker_count"] > 0
    assert snapshot_detail["data_quality"]["missing_bar_count"] == 0
    assert snapshot_detail["data_quality"]["missing_fundamental_count"] == 0

    snapshot_export_response = client.get(f"/source-snapshots/{snapshot_id}/export", headers=AUTH_HEADERS)
    assert snapshot_export_response.status_code == 200
    snapshot_export = snapshot_export_response.json()
    assert snapshot_export["source_snapshot_id"] == snapshot_id
    assert snapshot_export["metadata"]["data_quality"]["status"] == "complete"
    assert snapshot_export["event_count"] == len(snapshot_export["events"])
    assert ticker in snapshot_export["bars_by_ticker"]
    assert len(snapshot_export["bars_by_ticker"][ticker]) >= 3
    assert ticker in snapshot_export["fundamentals_by_ticker"]
    assert snapshot_export["fundamentals_by_ticker"][ticker]["ticker"] == ticker

    snapshot_bars_response = client.get(
        f"/source-snapshots/{snapshot_id}/bars/{ticker}?limit=3",
        headers=AUTH_HEADERS,
    )
    assert snapshot_bars_response.status_code == 200
    assert len(snapshot_bars_response.json()) == 3

    compare_response = client.post(
        f"/source-snapshots/{snapshot_id}/replay/compare",
        json={
            "objective": "api snapshot replay compare",
            "universe_rules": payload["universe_rules"],
            "risk_policy": payload["risk_policy"],
            "publication": payload["publication"],
            "include_unchanged": False,
        },
        headers=AUTH_HEADERS,
    )
    assert compare_response.status_code == 200
    compare_data = compare_response.json()
    assert compare_data["source_snapshot_id"] == snapshot_id
    assert compare_data["replay_operation"] == "replayed"
    assert compare_data["deterministic"] is True
    assert compare_data["matched_count"] == len(run_data["recommendations"])
    assert compare_data["changed_count"] == 0
    assert compare_data["missing_in_replay_count"] == 0
    assert compare_data["new_in_replay_count"] == 0
    assert compare_data["diffs"] == []

    non_mutating_compare_response = client.post(
        f"/source-snapshots/{snapshot_id}/replay/compare",
        json={
            "objective": "api snapshot replay compare with smaller publication",
            "universe_rules": payload["universe_rules"],
            "risk_policy": payload["risk_policy"],
            "publication": {"top_n": 1, "output_channels": ["api"]},
            "include_unchanged": False,
        },
        headers=AUTH_HEADERS,
    )
    assert non_mutating_compare_response.status_code == 200
    non_mutating_compare = non_mutating_compare_response.json()
    assert non_mutating_compare["deterministic"] is False
    assert non_mutating_compare["missing_in_replay_count"] >= 1
    stored_recommendation = state.recommendation_repo.get(run_data["recommendations"][0]["id"])
    assert stored_recommendation is not None
    assert stored_recommendation.strategy_config_id == strategy_config_id

    replay_response = client.post(
        f"/source-snapshots/{snapshot_id}/replay",
        json={
            "objective": "api snapshot replay",
            "universe_rules": payload["universe_rules"],
            "risk_policy": payload["risk_policy"],
            "publication": {"top_n": 2, "output_channels": ["api"]},
        },
        headers=AUTH_HEADERS,
    )
    assert replay_response.status_code == 200
    replay_data = replay_response.json()
    assert replay_data["source_snapshot_id"] == snapshot_id
    assert replay_data["universe_summary"]["snapshot"]["operation"] == "replayed"
    assert replay_data["strategy_config_id"]
    assert len(replay_data["recommendations"]) > 0
    assert replay_data["recommendations"][0]["strategy_config_id"] == replay_data["strategy_config_id"]

    recommendations_response = client.get("/recommendations/latest", headers=AUTH_HEADERS)
    assert recommendations_response.status_code == 200
    recommendations = recommendations_response.json()
    assert len(recommendations) > 0

    recommendation = recommendations[0]
    recommendation_id = recommendation["id"]
    assert recommendation["strategy_config_id"] == replay_data["strategy_config_id"]
    ticker = recommendation["ticker"]
    baseline_holdings = client.get("/portfolio/holdings?status=open", headers=AUTH_HEADERS)
    assert baseline_holdings.status_code == 200
    baseline_ticker_rows = [item for item in baseline_holdings.json() if item["ticker"] == ticker]
    baseline_holding_qty = baseline_ticker_rows[0]["qty"] if baseline_ticker_rows else 0.0

    approval_response = client.post(
        f"/recommendations/{recommendation_id}/approval",
        json={
            "decision": "approved",
            "approver": "integration-tester",
            "notes": "approved for paper routing",
        },
        headers=AUTH_HEADERS,
    )
    assert approval_response.status_code == 200
    assert approval_response.json()["decision"] == "approved"

    evidence_response = client.get(f"/recommendations/{recommendation_id}/evidence", headers=AUTH_HEADERS)
    assert evidence_response.status_code == 200
    assert evidence_response.json()["recommendation"]["id"] == recommendation_id

    mismatched_side_response = client.post(
        "/paper-orders",
        json={
            "recommendation_id": recommendation_id,
            "side": "SHORT",
            "qty": 10,
            "limit_price": None,
        },
        headers=AUTH_HEADERS,
    )
    assert mismatched_side_response.status_code == 409

    idempotency_key = f"integration:{recommendation_id}:paper-buy"
    paper_payload = {
        "recommendation_id": recommendation_id,
        "idempotency_key": idempotency_key,
        "side": "BUY",
        "qty": 10,
        "limit_price": None,
    }
    paper_response = client.post("/paper-orders", json=paper_payload, headers=AUTH_HEADERS)
    assert paper_response.status_code == 200
    paper_order = paper_response.json()
    assert paper_order["status"] == "filled"
    assert paper_order["idempotency_key"] == idempotency_key
    assert paper_order["client_order_id"].startswith("quant_")

    replay_response = client.post("/paper-orders", json=paper_payload, headers=AUTH_HEADERS)
    assert replay_response.status_code == 200
    assert replay_response.json()["id"] == paper_order["id"]

    conflicting_replay = client.post(
        "/paper-orders",
        json={**paper_payload, "qty": 11},
        headers=AUTH_HEADERS,
    )
    assert conflicting_replay.status_code == 409
    assert conflicting_replay.json()["detail"]["reason"] == "idempotency_key_reused_with_different_request"
    assert "qty" in conflicting_replay.json()["detail"]["conflicting_fields"]

    persisted_by_key = state.paper_order_repo.get_by_idempotency_key(idempotency_key)
    assert persisted_by_key is not None
    assert persisted_by_key.id == paper_order["id"]

    orders_response = client.get(
        f"/paper-orders?recommendation_id={recommendation_id}&status=filled",
        headers=AUTH_HEADERS,
    )
    assert orders_response.status_code == 200
    order_rows = orders_response.json()
    assert order_rows
    assert order_rows[0]["id"] == paper_order["id"]
    assert order_rows[0]["recommendation_id"] == recommendation_id
    assert order_rows[0]["simulated_fill_price"] == paper_order["simulated_fill_price"]

    positions_response = client.get("/positions", headers=AUTH_HEADERS)
    assert positions_response.status_code == 200
    assert len(positions_response.json()) >= 1

    holdings_response = client.get("/portfolio/holdings?status=open", headers=AUTH_HEADERS)
    assert holdings_response.status_code == 200
    holding_rows = [item for item in holdings_response.json() if item["ticker"] == ticker]
    assert holding_rows
    assert holding_rows[0]["qty"] >= baseline_holding_qty + 10
    assert holding_rows[0]["source_recommendation_id"] == recommendation_id
    assert holding_rows[0]["stop_loss"] == recommendation["stop_loss"]

    trade_rows_response = client.get(f"/portfolio/trades?ticker={ticker}&side=buy", headers=AUTH_HEADERS)
    assert trade_rows_response.status_code == 200
    buy_rows = trade_rows_response.json()
    assert buy_rows
    assert buy_rows[0]["source_recommendation_id"] == recommendation_id
    assert buy_rows[0]["reason"] == f"paper_order_fill:{paper_order['id']}"
    assert buy_rows[0]["price"] == paper_order["simulated_fill_price"]

    cleanup_response = client.post(f"/portfolio/holdings/{ticker}/close", headers=AUTH_HEADERS)
    assert cleanup_response.status_code == 200
