from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from apps.worker.broker_cycles import (
    _auto_broker_sync_cycle,
    _auto_position_reconciliation_cycle,
)
from domain.entities.models import (
    BrokerOrderStatusSnapshot,
    BrokerOrderSyncRequest,
    BrokerOrderSyncStatus,
    Direction,
    OrderExecutionMode,
    PaperOrder,
    PaperOrderStatus,
)
from services.execution.broker_adapter import BrokerOrderUpdate
from services.execution.router import ExecutionRouter

AUTH_HEADERS = {"x-access-password": "test-access-password"}


class NestedProtectionBroker:
    name = "alpaca"

    def __init__(self) -> None:
        self.order_queries: list[str] = []

    def get_order_by_id(self, broker_order_id: str) -> BrokerOrderUpdate:
        self.order_queries.append(broker_order_id)
        protection = _protection("canceled")
        protection[0] = {**protection[0], "status": "filled", "filled_qty": 4.0}
        return BrokerOrderUpdate(
            broker_order_id=broker_order_id,
            raw_status="filled",
            filled_qty=4,
            filled_avg_price=103,
            protection_legs=tuple(protection),
        )

    def list_positions(self):
        return []


def _protection(status: str = "new") -> list[dict]:
    return [
        {
            "broker_order_id": "tp-child-1",
            "status": status,
            "order_type": "limit",
            "side": "sell",
            "qty": 4.0,
            "filled_qty": 0.0,
            "limit_price": 120.0,
            "stop_price": None,
        },
        {
            "broker_order_id": "sl-child-1",
            "status": status,
            "order_type": "stop",
            "side": "sell",
            "qty": 4.0,
            "filled_qty": 0.0,
            "limit_price": None,
            "stop_price": 90.0,
        },
    ]


def _recommendation():
    state = get_app_state()
    state.reset()
    response = TestClient(app).post(
        "/research/run",
        headers=AUTH_HEADERS,
        json={
            "run_type": "research_batch",
            "objective": "alpaca-paper-lifecycle",
            "as_of": "2026-04-10T09:30:00Z",
            "snapshot_mode": "point_in_time",
            "publication": {"top_n": 1, "output_channels": ["api"]},
            "risk_policy": {
                "min_confidence": 0.0,
                "earnings_blackout_minutes": 0,
                "reject_on_material_evidence_conflict": False,
                "event_trading_enabled": True,
            },
            "universe_rules": {
                "min_price": 1,
                "min_avg_dollar_volume": 1_000_000,
                "max_spread_bps": 100,
                "min_market_cap_usd": 100_000_000,
                "max_candidates_after_filter": 50,
            },
        },
    )
    assert response.status_code == 200
    recommendation = state.recommendations_by_id[response.json()["recommendations"][0]["id"]]
    state.close_holding(recommendation.ticker)
    return state, recommendation


def test_partial_fill_cancel_late_fill_and_native_protection_survive_restart() -> None:
    state, recommendation = _recommendation()
    order = PaperOrder(
        id=f"alpaca-paper-{uuid4().hex[:8]}",
        recommendation_id=recommendation.id,
        source_snapshot_id=recommendation.source_snapshot_id,
        strategy_config_id=recommendation.strategy_config_id,
        side=Direction.BUY,
        qty=4,
        limit_price=None,
        execution_mode=OrderExecutionMode.LIVE,
        dry_run=False,
        broker_order_id="alpaca-parent-1",
        adapter_message="alpaca: accepted native bracket",
        submitted_at=datetime.now(timezone.utc),
        status=PaperOrderStatus.SUBMITTED,
    )
    state.record_paper_order(order, recommendation=recommendation)

    first = state.sync_broker_order_statuses(
        BrokerOrderSyncRequest(
            broker="alpaca-paper",
            updated_by="paper-uat",
            statuses=[
                BrokerOrderStatusSnapshot(
                    broker_order_id=order.broker_order_id,
                    status=BrokerOrderSyncStatus.PARTIALLY_FILLED,
                    fill_price=100,
                    filled_qty=1.5,
                    broker_protection=_protection(),
                )
            ],
        )
    )
    assert first.partially_filled_count == 1
    persisted = state.get_paper_order(order.id)
    assert persisted is not None
    assert persisted.status == PaperOrderStatus.SUBMITTED
    assert persisted.filled_qty == 1.5
    assert persisted.broker_protection == _protection()
    assert state.holding_watch_repo.get(recommendation.ticker).qty == 1.5

    # Simulate process-memory loss. The second cumulative broker snapshot must
    # continue from retained database evidence without duplicating the first fill.
    state.paper_orders.clear()
    state.holdings_by_ticker.clear()
    second = state.sync_broker_order_statuses(
        BrokerOrderSyncRequest(
            broker="alpaca-paper",
            updated_by="paper-uat-after-restart",
            statuses=[
                BrokerOrderStatusSnapshot(
                    broker_order_id=order.broker_order_id,
                    status=BrokerOrderSyncStatus.PARTIALLY_FILLED,
                    fill_price=102,
                    filled_qty=3,
                    broker_protection=_protection(),
                )
            ],
        )
    )
    assert second.partially_filled_count == 1
    holding = state.holding_watch_repo.get(recommendation.ticker)
    assert holding is not None
    assert holding.qty == 3
    assert holding.avg_buy_price == 102

    duplicate = state.sync_broker_order_statuses(
        BrokerOrderSyncRequest(
            broker="alpaca-paper",
            statuses=[
                BrokerOrderStatusSnapshot(
                    broker_order_id=order.broker_order_id,
                    status=BrokerOrderSyncStatus.PARTIALLY_FILLED,
                    fill_price=102,
                    filled_qty=3,
                    broker_protection=_protection(),
                )
            ],
        )
    )
    assert duplicate.unchanged_count == 1

    canceled = state.sync_broker_order_statuses(
        BrokerOrderSyncRequest(
            broker="alpaca-paper",
            statuses=[
                BrokerOrderStatusSnapshot(
                    broker_order_id=order.broker_order_id,
                    status=BrokerOrderSyncStatus.CANCELED,
                    fill_price=102,
                    filled_qty=3,
                    reason="remainder canceled",
                    broker_protection=_protection("canceled"),
                )
            ],
        )
    )
    assert canceled.canceled_count == 1
    assert state.get_paper_order(order.id).status == PaperOrderStatus.CANCELED
    assert state.get_paper_order(order.id).broker_protection == _protection("canceled")

    # Broker truth wins over a prior local cancel if a final fill arrives late.
    final = state.sync_broker_order_statuses(
        BrokerOrderSyncRequest(
            broker="alpaca-paper",
            statuses=[
                BrokerOrderStatusSnapshot(
                    broker_order_id=order.broker_order_id,
                    status=BrokerOrderSyncStatus.FILLED,
                    fill_price=103,
                    filled_qty=4,
                    broker_protection=_protection(),
                )
            ],
        )
    )
    assert final.filled_count == 1
    final_order = state.get_paper_order(order.id)
    assert final_order is not None
    assert final_order.status == PaperOrderStatus.FILLED
    assert final_order.filled_qty == 4
    final_holding = state.holding_watch_repo.get(recommendation.ticker)
    assert final_holding is not None
    assert final_holding.qty == 4
    assert final_holding.avg_buy_price == 103
    trades = [
        item
        for item in state.list_trade_ledger(ticker=recommendation.ticker)
        if item.reason == f"paper_order_fill:{order.id}"
    ]
    assert sorted(item.qty for item in trades) == [1.0, 1.5, 1.5]
    assert sum(item.qty for item in trades) == 4

    original_router = state.execution_router
    broker = NestedProtectionBroker()
    state.execution_router = ExecutionRouter(broker_adapter=broker)
    try:
        broker_sync = _auto_broker_sync_cycle(
            enabled=True,
            checked_at=datetime.now(timezone.utc),
            max_items=10,
        )
        reconciliation = _auto_position_reconciliation_cycle(
            enabled=True,
            checked_at=datetime.now(timezone.utc),
            qty_tolerance=1e-6,
        )
    finally:
        state.execution_router = original_router

    assert broker_sync["protected_buy_order_count"] == 1
    assert broker.order_queries == [order.broker_order_id]
    synced_protection = state.get_paper_order(order.id).broker_protection
    assert synced_protection[0]["status"] == "filled"
    assert synced_protection[1]["status"] == "canceled"
    assert reconciliation["reconciliation"]["blocks_auto_execution"] is True
    assert reconciliation["reconciliation"]["missing_in_broker_count"] == 1
