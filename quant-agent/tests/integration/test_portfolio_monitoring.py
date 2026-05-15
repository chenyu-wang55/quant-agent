from __future__ import annotations

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

    close_response = client.post(
        f"/portfolio/holdings/{recommendation['ticker']}/close",
        headers=AUTH_HEADERS,
    )
    assert close_response.status_code == 200
    assert close_response.json()["status"] == "closed"
