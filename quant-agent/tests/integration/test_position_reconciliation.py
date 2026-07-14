from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from infra.queue.events import EventType

AUTH_HEADERS = {"x-access-password": "test-access-password"}


def _broker_positions_from_open_holdings() -> list[dict]:
    state = get_app_state()
    return [
        {"ticker": holding.ticker, "qty": holding.qty, "avg_price": holding.avg_buy_price}
        for holding in state.list_open_holdings()
    ]


def test_position_reconciliation_records_matching_and_mismatched_snapshots() -> None:
    state = get_app_state()
    state.reset()
    client = TestClient(app)
    ticker = "ZZRCN"
    state.close_holding(ticker)

    buy_response = client.post(
        "/portfolio/buys",
        json={
            "ticker": ticker,
            "qty": 10,
            "buy_price": 100,
            "stop_loss": 92,
            "take_profit1": 110,
            "take_profit2": 118,
            "note": "reconciliation-test",
        },
        headers=AUTH_HEADERS,
    )
    assert buy_response.status_code == 200

    matched_response = client.post(
        "/portfolio/reconciliation",
        json={
            "broker": "manual-test-broker",
            "account_id": "paper-account",
            "positions": _broker_positions_from_open_holdings(),
            "note": "matching snapshot",
        },
        headers=AUTH_HEADERS,
    )
    assert matched_response.status_code == 200
    matched = matched_response.json()
    assert matched["status"] == "matched"
    assert matched["blocks_auto_execution"] is False
    assert matched["mismatch_count"] == 0
    assert any(item["ticker"] == ticker and item["status"] == "matched" for item in matched["items"])

    mismatched_positions = []
    for item in _broker_positions_from_open_holdings():
        if item["ticker"] == ticker:
            item = {**item, "qty": 8}
        mismatched_positions.append(item)
    mismatched_positions.append({"ticker": "ZZBRK", "qty": 3, "avg_price": 42})
    mismatch_response = client.post(
        "/portfolio/reconciliation",
        json={
            "broker": "manual-test-broker",
            "account_id": "paper-account",
            "positions": mismatched_positions,
            "note": "mismatch snapshot",
        },
        headers=AUTH_HEADERS,
    )
    assert mismatch_response.status_code == 200
    mismatch = mismatch_response.json()
    assert mismatch["status"] == "mismatch"
    assert mismatch["blocks_auto_execution"] is True
    assert mismatch["mismatch_count"] >= 2
    by_ticker = {item["ticker"]: item for item in mismatch["items"]}
    assert by_ticker[ticker]["status"] == "qty_mismatch"
    assert by_ticker[ticker]["qty_diff"] == -2
    assert by_ticker["ZZBRK"]["status"] == "broker_only"

    recent_response = client.get(
        "/portfolio/reconciliations?limit=2&broker=manual-test-broker",
        headers=AUTH_HEADERS,
    )
    assert recent_response.status_code == 200
    recent = recent_response.json()
    assert [item["status"] for item in recent[:2]] == ["mismatch", "matched"]
    assert recent[0]["items"][0]["ticker"]

    events = state.list_pending_events(limit=10)
    reconciliation_events = [
        event for event in events if event.event_type == EventType.POSITION_RECONCILIATION
    ]
    assert len(reconciliation_events) >= 2
    assert reconciliation_events[-1].payload["blocks_auto_execution"] is True
