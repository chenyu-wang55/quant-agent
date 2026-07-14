from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event, Lock

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_app_state
from apps.api.main import app
from apps.worker.main import _auto_broker_sync_cycle
from domain.entities.models import (
    ManualBuyRequest,
    ManualSellRequest,
    OrderExecutionMode,
    PaperOrderRequest,
    PaperOrderStatus,
    TradeSide,
)
from infra.db.order_unit_of_work import OrderUnitOfWork
from infra.db.sell_unit_of_work import ConcurrentPortfolioUpdateError, SellUnitOfWork
from services.execution.broker_adapter import (
    BrokerAdapterError,
    BrokerOrderNotFoundError,
    BrokerOrderPlacement,
    BrokerOrderUpdate,
)
from services.execution.router import ExecutionRouter

AUTH_HEADERS = {"x-access-password": "test-access-password"}


def _research_recommendation(objective: str):
    client = TestClient(app)
    response = client.post(
        "/research/run",
        json={
            "run_type": "research_batch",
            "objective": objective,
            "as_of": "2026-04-10T09:30:00Z",
            "snapshot_mode": "point_in_time",
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
                "min_avg_dollar_volume": 1_000_000,
                "max_spread_bps": 100,
                "min_market_cap_usd": 100_000_000,
                "allowed_sectors": [],
                "max_candidates_after_filter": 50,
            },
        },
        headers=AUTH_HEADERS,
    )
    assert response.status_code == 200
    state = get_app_state()
    recommendation_id = response.json()["recommendations"][0]["id"]
    return state, state.recommendations_by_id[recommendation_id]


class BlockingBrokerAdapter:
    name = "blocking-broker"

    def __init__(self) -> None:
        self.placements: list[BrokerOrderPlacement] = []
        self.started = Event()
        self.release = Event()
        self.lock = Lock()

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        with self.lock:
            self.placements.append(placement)
        self.started.set()
        assert self.release.wait(timeout=5)
        return BrokerOrderUpdate(
            broker_order_id="blocking_broker_order",
            raw_status="accepted",
            client_order_id=placement.client_order_id,
            submitted_at=datetime.now(timezone.utc),
        )

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        raise AssertionError("concurrent retry must not query or submit to broker")


class TimeoutThenLookupBrokerAdapter:
    name = "timeout-lookup-broker"

    def __init__(self, *, lookup_succeeds: bool) -> None:
        self.lookup_succeeds = lookup_succeeds
        self.placements: list[BrokerOrderPlacement] = []
        self.lookups: list[str] = []

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        self.placements.append(placement)
        raise BrokerAdapterError("timeout after request body was sent")

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        self.lookups.append(client_order_id)
        if not self.lookup_succeeds:
            raise BrokerAdapterError("broker lookup unavailable")
        return BrokerOrderUpdate(
            broker_order_id="recovered_broker_order",
            raw_status="accepted",
            client_order_id=client_order_id,
            submitted_at=datetime.now(timezone.utc),
        )


class NotFoundThenAcceptedBrokerAdapter:
    name = "not-found-then-accepted-broker"

    def __init__(self) -> None:
        self.placements: list[BrokerOrderPlacement] = []

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        self.placements.append(placement)
        if len(self.placements) == 1:
            raise BrokerAdapterError("initial submit timed out")
        return BrokerOrderUpdate(
            broker_order_id="resubmitted_broker_order",
            raw_status="accepted",
            client_order_id=placement.client_order_id,
            submitted_at=datetime.now(timezone.utc),
        )

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        raise BrokerOrderNotFoundError("broker confirmed no matching client id")


def _live_request(recommendation_id: str, idempotency_key: str) -> PaperOrderRequest:
    return PaperOrderRequest(
        recommendation_id=recommendation_id,
        idempotency_key=idempotency_key,
        qty=1,
        execution_mode=OrderExecutionMode.LIVE,
        confirm_live=True,
        enforce_risk_limits=False,
    )


def test_concurrent_retry_reserves_one_intent_and_calls_broker_once() -> None:
    state = get_app_state()
    state.reset()
    state, recommendation = _research_recommendation("concurrent-order-intent")
    original_router = state.execution_router
    adapter = BlockingBrokerAdapter()
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = _live_request(recommendation.id, "concurrent-order-intent-key")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(
                state.submit_order,
                recommendation=recommendation,
                request=request,
            )
            assert adapter.started.wait(timeout=5)
            retry = state.submit_order(recommendation=recommendation, request=request)
            adapter.release.set()
            first = first_future.result(timeout=5)
    finally:
        adapter.release.set()
        state.execution_router = original_router

    assert first.id == retry.id
    assert retry.status == PaperOrderStatus.PENDING_SUBMIT
    assert len(adapter.placements) == 1
    persisted = state.get_paper_order(first.id)
    assert persisted is not None
    assert persisted.status == PaperOrderStatus.SUBMITTED
    assert persisted.client_order_id == adapter.placements[0].client_order_id


def test_ambiguous_submit_is_recovered_by_client_order_id_without_resubmit() -> None:
    state = get_app_state()
    state.reset()
    state, recommendation = _research_recommendation("ambiguous-order-recovery")
    original_router = state.execution_router
    adapter = TimeoutThenLookupBrokerAdapter(lookup_succeeds=True)
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = _live_request(recommendation.id, "ambiguous-order-recovery-key")

    try:
        order = state.submit_order(recommendation=recommendation, request=request)
    finally:
        state.execution_router = original_router

    assert order.status == PaperOrderStatus.SUBMITTED
    assert order.broker_order_id == "recovered_broker_order"
    assert len(adapter.placements) == 1
    assert adapter.lookups == [order.client_order_id]
    assert state.get_paper_order(order.id) == order


def test_ambiguous_submit_stays_recoverable_when_broker_lookup_is_unavailable() -> None:
    state = get_app_state()
    state.reset()
    state, recommendation = _research_recommendation("unknown-order-outcome")
    original_router = state.execution_router
    adapter = TimeoutThenLookupBrokerAdapter(lookup_succeeds=False)
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = _live_request(recommendation.id, "unknown-order-outcome-key")

    try:
        with pytest.raises(BrokerAdapterError, match="timeout after request body was sent"):
            state.submit_order(recommendation=recommendation, request=request)
    finally:
        state.execution_router = original_router

    persisted = state.get_paper_order_by_idempotency_key(request.idempotency_key)
    assert persisted is not None
    assert persisted.status == PaperOrderStatus.SUBMIT_UNKNOWN
    assert persisted.broker_order_id is None
    assert len(adapter.placements) == 1
    assert adapter.lookups == [persisted.client_order_id]


def test_definitive_not_found_reuses_same_client_id_for_safe_resubmit() -> None:
    state = get_app_state()
    state.reset()
    state, recommendation = _research_recommendation("not-found-safe-resubmit")
    original_router = state.execution_router
    adapter = NotFoundThenAcceptedBrokerAdapter()
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = _live_request(recommendation.id, "not-found-safe-resubmit-key")

    try:
        order = state.submit_order(recommendation=recommendation, request=request)
    finally:
        state.execution_router = original_router

    assert order.status == PaperOrderStatus.SUBMITTED
    assert len(adapter.placements) == 2
    assert adapter.placements[0].client_order_id == adapter.placements[1].client_order_id
    assert order.client_order_id == adapter.placements[0].client_order_id


def test_order_fill_transaction_rolls_back_all_ledger_effects_and_recovers() -> None:
    state = get_app_state()
    state.reset()
    state, recommendation = _research_recommendation("atomic-order-fill")
    original_uow = state.order_unit_of_work

    def fail_before_commit() -> None:
        raise RuntimeError("injected failure before transaction commit")

    state.order_unit_of_work = OrderUnitOfWork(before_commit=fail_before_commit)
    request = PaperOrderRequest(
        recommendation_id=recommendation.id,
        idempotency_key="atomic-order-fill-key",
        qty=1,
        enforce_risk_limits=False,
    )
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            state.submit_order(recommendation=recommendation, request=request)
    finally:
        state.order_unit_of_work = original_uow

    intent = state.get_paper_order_by_idempotency_key(request.idempotency_key)
    assert intent is not None
    assert intent.status == PaperOrderStatus.PENDING_SUBMIT
    assert recommendation.ticker not in state.positions
    assert all(item.ticker != recommendation.ticker for item in state.list_holdings())
    assert all((item.reason or "") != f"paper_order_fill:{intent.id}" for item in state.list_trade_ledger())
    assert all(event.payload.get("order_id") != intent.id for event in state.list_pending_events(limit=1000))

    recovered = state.recover_order_submission(intent, recommendation=recommendation)
    assert recovered.status == PaperOrderStatus.FILLED
    holding = next(item for item in state.list_holdings() if item.ticker == recommendation.ticker)
    assert holding.qty == 1
    matching_trades = [
        item
        for item in state.list_trade_ledger()
        if item.reason == f"paper_order_fill:{intent.id}"
    ]
    assert len(matching_trades) == 1
    assert state.get_paper_order(intent.id).status == PaperOrderStatus.FILLED


def _seed_holding(ticker: str = "IDEM"):
    state = get_app_state()
    state.reset()
    holding = state.record_manual_buy(
        ManualBuyRequest(
            ticker=ticker,
            qty=10,
            buy_price=100,
            stop_loss=90,
            take_profit1=110,
            take_profit2=120,
            note="sell idempotency setup",
        )
    )
    return state, holding


def test_paper_sell_idempotency_replay_does_not_reduce_holding_twice() -> None:
    state, holding = _seed_holding("SIDEM")
    request = ManualSellRequest(
        idempotency_key="paper-sell-idempotency-key",
        qty=4,
        sell_price=110,
        reason="idempotent paper sell",
    )

    first = state.sell_holding(holding.ticker, request)
    replay = state.sell_holding(holding.ticker, request)

    assert replay.sell_execution_id == first.sell_execution_id
    assert replay.client_order_id == first.client_order_id
    assert state.holding_watch_repo.get(holding.ticker).qty == 6
    sell_trades = state.list_trade_ledger(ticker=holding.ticker, side=TradeSide.SELL)
    assert len(sell_trades) == 1


def test_concurrent_live_sell_retry_calls_broker_once() -> None:
    state, holding = _seed_holding("SCONC")
    original_router = state.execution_router
    adapter = BlockingBrokerAdapter()
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = ManualSellRequest(
        idempotency_key="concurrent-live-sell-key",
        qty=4,
        sell_price=110,
        execution_mode=OrderExecutionMode.LIVE,
        confirm_live=True,
    )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(state.sell_holding, holding.ticker, request)
            assert adapter.started.wait(timeout=5)
            replay = state.sell_holding(holding.ticker, request)
            adapter.release.set()
            first = first_future.result(timeout=5)
    finally:
        adapter.release.set()
        state.execution_router = original_router

    assert replay.sell_execution_id == first.sell_execution_id
    assert replay.applied_to_ledger is False
    assert len(adapter.placements) == 1
    assert adapter.placements[0].side == "sell"
    persisted = state.sell_execution_audit_repo.get(first.sell_execution_id)
    assert persisted is not None
    assert persisted.status == "submitted"


def test_sell_transaction_rolls_back_and_stale_intent_recovers_once() -> None:
    state, holding = _seed_holding("SATOM")
    original_uow = state.sell_unit_of_work

    def fail_before_commit() -> None:
        raise RuntimeError("injected sell failure before commit")

    state.sell_unit_of_work = SellUnitOfWork(before_commit=fail_before_commit)
    request = ManualSellRequest(
        idempotency_key="atomic-paper-sell-key",
        qty=4,
        sell_price=110,
        reason="atomic sell",
    )
    try:
        with pytest.raises(RuntimeError, match="injected sell failure"):
            state.sell_holding(holding.ticker, request)
    finally:
        state.sell_unit_of_work = original_uow

    pending = state.sell_execution_audit_repo.get_by_idempotency_key(request.idempotency_key)
    assert pending is not None
    assert pending.status == "pending_submit"
    assert state.holding_watch_repo.get(holding.ticker).qty == 10
    assert state.list_trade_ledger(ticker=holding.ticker, side=TradeSide.SELL) == []

    stale = pending.model_copy(update={"submitted_at": pending.submitted_at - timedelta(seconds=31)})
    state.sell_execution_audit_repo.add(stale)
    recovered = state.sell_holding(holding.ticker, request)
    assert recovered.applied_to_ledger is True
    assert state.holding_watch_repo.get(holding.ticker).qty == 6
    assert len(state.list_trade_ledger(ticker=holding.ticker, side=TradeSide.SELL)) == 1


def test_ambiguous_live_sell_recovers_by_client_order_id_on_retry() -> None:
    state, holding = _seed_holding("SRECV")
    original_router = state.execution_router
    adapter = TimeoutThenLookupBrokerAdapter(lookup_succeeds=True)
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = ManualSellRequest(
        idempotency_key="ambiguous-live-sell-key",
        qty=4,
        sell_price=110,
        execution_mode=OrderExecutionMode.LIVE,
        confirm_live=True,
    )

    try:
        with pytest.raises(BrokerAdapterError, match="timeout after request body was sent"):
            state.sell_holding(holding.ticker, request)
        recovered = state.sell_holding(holding.ticker, request)
    finally:
        state.execution_router = original_router

    assert recovered.applied_to_ledger is False
    assert recovered.broker_order_id == "recovered_broker_order"
    assert len(adapter.placements) == 1
    assert adapter.lookups == [recovered.client_order_id]


def test_concurrent_distinct_sells_use_optimistic_holding_guard() -> None:
    state, holding = _seed_holding("SLOCK")
    original_uow = state.sell_unit_of_work
    first_in_transaction = Event()
    release_first = Event()
    hook_lock = Lock()
    hook_calls = 0

    def pause_first_commit() -> None:
        nonlocal hook_calls
        with hook_lock:
            hook_calls += 1
            call_number = hook_calls
        if call_number == 1:
            first_in_transaction.set()
            assert release_first.wait(timeout=5)

    state.sell_unit_of_work = SellUnitOfWork(before_commit=pause_first_commit)
    first_request = ManualSellRequest(
        idempotency_key="optimistic-sell-first-key",
        qty=4,
        sell_price=110,
    )
    second_request = ManualSellRequest(
        idempotency_key="optimistic-sell-second-key",
        qty=4,
        sell_price=111,
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(state.sell_holding, holding.ticker, first_request)
            assert first_in_transaction.wait(timeout=5)
            second_future = executor.submit(state.sell_holding, holding.ticker, second_request)
            release_first.set()
            first = first_future.result(timeout=5)
            with pytest.raises(ConcurrentPortfolioUpdateError):
                second_future.result(timeout=5)
    finally:
        release_first.set()
        state.sell_unit_of_work = original_uow

    assert first.applied_to_ledger is True
    assert state.holding_watch_repo.get(holding.ticker).qty == 6
    assert len(state.list_trade_ledger(ticker=holding.ticker, side=TradeSide.SELL)) == 1


def test_worker_broker_sync_automatically_recovers_unknown_sell_submission() -> None:
    state, holding = _seed_holding("SAUTO")
    original_router = state.execution_router
    adapter = TimeoutThenLookupBrokerAdapter(lookup_succeeds=True)
    state.execution_router = ExecutionRouter(broker_adapter=adapter)
    request = ManualSellRequest(
        idempotency_key="worker-auto-recover-sell-key",
        qty=4,
        sell_price=110,
        execution_mode=OrderExecutionMode.LIVE,
        confirm_live=True,
    )
    try:
        with pytest.raises(BrokerAdapterError):
            state.sell_holding(holding.ticker, request)
        report = _auto_broker_sync_cycle(
            enabled=True,
            checked_at=datetime.now(timezone.utc),
            max_items=10,
        )
    finally:
        state.execution_router = original_router

    assert report["recoverable_sell_execution_count"] == 1
    assert report["error_count"] == 0
    assert any(
        action["resource"] == "sell_executions" and action["status"] == "recovered"
        for action in report["actions"]
    )
    recovered = state.sell_execution_audit_repo.get_by_idempotency_key(request.idempotency_key)
    assert recovered is not None
    assert recovered.status == "submitted"
    assert recovered.broker_order_id == "recovered_broker_order"
