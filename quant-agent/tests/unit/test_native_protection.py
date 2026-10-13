from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from domain.entities.models import Direction, OrderExecutionMode
from services.execution.protection import check_native_protection


def _state(quantities=(100,)):
    now = datetime.now(timezone.utc)
    orders = []
    for index, qty in enumerate(quantities):
        parent = f"parent-{index}"
        common = {"parent_order_id": parent, "symbol": "PROT", "side": "sell",
                  "status": "new", "qty": qty, "filled_qty": 0, "observed_at": now.isoformat()}
        orders.append(SimpleNamespace(
            execution_mode=OrderExecutionMode.LIVE, dry_run=False, side=Direction.BUY,
            filled_qty=qty, broker_order_id=parent, recommendation_id="rec",
            broker_protection=[
                {**common, "broker_order_id": f"stop-{index}", "order_type": "stop", "stop_price": 90},
                {**common, "broker_order_id": f"target-{index}", "order_type": "limit", "limit_price": 120},
            ],
        ))
    return SimpleNamespace(
        list_open_holdings=lambda: [SimpleNamespace(ticker="PROT", qty=100, stop_loss=90,
                                                   take_profit2=120, source_recommendation_id="rec")],
        get_autopilot_policy=lambda: SimpleNamespace(max_position_reconciliation_age_minutes=30),
        get_position_reconciliation_gate=lambda **kwargs: {"passed": True},
        list_position_reconciliations=lambda **kwargs: [SimpleNamespace(items=[SimpleNamespace(ticker="PROT", broker_qty=100)])],
        list_paper_orders=lambda **kwargs: orders,
        _get_recommendation_by_id=lambda _: SimpleNamespace(ticker="PROT"),
    ), orders


@pytest.mark.parametrize("patch,reason", [
    ({"qty": 1}, "protection_quantity_mismatch"),
    ({"qty": 101}, "protection_quantity_mismatch"),
    ({"side": "buy"}, "protection_identity_mismatch"),
    ({"symbol": "OTHER"}, "protection_identity_mismatch"),
    ({"parent_order_id": "OTHER"}, "protection_identity_mismatch"),
    ({"qty": None}, "protection_values_missing_or_invalid"),
    ({"qty": float("nan")}, "protection_values_missing_or_invalid"),
    ({"stop_price": 89}, "protection_price_mismatch"),
    ({"observed_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()}, "protection_snapshot_stale"),
    ({"observed_at": "2026-10-10T10:00:00"}, "protection_snapshot_stale"),
    ({"observed_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()}, "protection_snapshot_stale"),
    ({"status": "canceled"}, "active_native_bracket_missing"),
    ({"order_type": "market"}, "unknown_protection_type"),
])
def test_invalid_protection_blocks(patch, reason) -> None:
    state, orders = _state()
    orders[0].broker_protection[0].update(patch)
    ready, details = check_native_protection(state)
    assert not ready
    assert reason in details["failures"][0]["reasons"]


def test_multiple_oco_groups_and_partial_fills_cover_remaining_position() -> None:
    state, orders = _state((60, 40))
    assert check_native_protection(state)[0]
    for leg in orders[0].broker_protection:
        leg.update(qty=70, filled_qty=10, status="partially_filled")
    assert check_native_protection(state)[0]
    orders[1].broker_protection = deepcopy(orders[0].broker_protection)
    assert not check_native_protection(state)[0]


def test_unverified_or_mismatched_broker_positions_block() -> None:
    state, _ = _state()
    state.get_position_reconciliation_gate = lambda **kwargs: {"passed": False}
    assert not check_native_protection(state)[0]
    state.get_position_reconciliation_gate = lambda **kwargs: {"passed": True}
    state.list_position_reconciliations = lambda **kwargs: [SimpleNamespace(items=[SimpleNamespace(ticker="PROT", broker_qty=101)])]
    assert not check_native_protection(state)[0]
