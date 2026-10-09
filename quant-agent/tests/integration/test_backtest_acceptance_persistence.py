from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app

AUTH_HEADERS = {"x-access-password": "test-access-password"}


def test_backtest_api_persists_complete_acceptance_evidence() -> None:
    state = get_app_state()
    state.reset()
    state.provider.validate_backtest_data = (  # type: ignore[attr-defined]
        lambda _start, _end: {
            "status": "verified",
            "dataset_fingerprint": "d" * 64,
        }
    )
    client = TestClient(app)

    response = client.post(
        "/backtests/runs",
        headers=AUTH_HEADERS,
        json={
            "run_name": "acceptance-persistence",
            "start_date": "2025-04-10T09:30:00Z",
            "end_date": "2026-04-10T09:30:00Z",
            "top_n": 3,
            "rebalance_frequency": "monthly",
            "max_position_pct": 0.10,
            "max_sector_exposure_pct": 0.30,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset_fingerprint"] == "d" * 64
    assert payload["strategy_config_id"].startswith("strat_")
    assert payload["acceptance"]["evaluated_segment"] == "out_of_sample"
    assert "profit_factor" in payload["segments"]["out_of_sample"]
    assert set(payload["market_regimes"]) == {"bull", "sideways", "bear"}

    listed = client.get("/backtests/runs", headers=AUTH_HEADERS)
    assert listed.status_code == 200
    assert [item["run_id"] for item in listed.json()] == [payload["run_id"]]
