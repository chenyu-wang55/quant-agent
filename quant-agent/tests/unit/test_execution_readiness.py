from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from domain.entities.models import SourceSnapshotSummary
from services.execution.broker_adapter import BrokerAccountSnapshot, BrokerOrderPlacement, BrokerOrderUpdate
from services.execution.readiness import ExecutionReadinessGate


class RecordingLiveBroker:
    name = "alpaca"

    def __init__(self) -> None:
        self.account_calls = 0
        self.submit_calls = 0

    def get_account(self) -> BrokerAccountSnapshot:
        self.account_calls += 1
        return BrokerAccountSnapshot(account_id="acct", status="ACTIVE")

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        self.submit_calls += 1
        return BrokerOrderUpdate(broker_order_id="unexpected", raw_status="accepted")


def _snapshot(
    *,
    provider_name: str = "YFinanceProvider",
    provider_status: str = "verified",
    live_execution_allowed: bool = True,
    bar_coverage: float = 1.0,
    fundamental_coverage: float = 1.0,
    age_minutes: int = 5,
    dataset_fingerprint: str | None = "a" * 64,
) -> SourceSnapshotSummary:
    now = datetime.now(timezone.utc)
    return SourceSnapshotSummary(
        source_snapshot_id="trusted-snapshot",
        created_at=now,
        as_of=now - timedelta(minutes=age_minutes),
        universe="SP500",
        provider_name=provider_name,
        tickers=["AAPL"],
        ticker_count=1,
        bar_count=260,
        fundamental_count=1,
        event_count=0,
        recommendation_count=1,
        data_quality={
            "provider_status": provider_status,
            "live_execution_allowed": live_execution_allowed,
            "bar_coverage": bar_coverage,
            "fundamental_coverage": fundamental_coverage,
            "dataset_fingerprint": dataset_fingerprint,
        },
    )


def _evaluate(
    *,
    snapshot: SourceSnapshotSummary | None = None,
    source_snapshot_id: str | None = "trusted-snapshot",
    approval_granted: bool = True,
    kill_switch_enabled: bool = False,
    reconciliation: dict | None = None,
    strategy_validation: dict | None = None,
    automatic: bool = False,
    paper_shadow: dict | None = None,
):
    broker = RecordingLiveBroker()
    result = ExecutionReadinessGate(max_snapshot_age_minutes=60).evaluate(
        source_snapshot_id=source_snapshot_id,
        snapshot=_snapshot() if snapshot is None else snapshot,
        kill_switch_enabled=kill_switch_enabled,
        approval_granted=approval_granted,
        broker_adapter=broker,
        position_reconciliation=reconciliation or {"passed": True, "reason": None},
        strategy_validation=strategy_validation or {"passed": True, "backtest_run_id": "bt-accepted"},
        automatic=automatic,
        paper_shadow=paper_shadow,
        test_mode=False,
    )
    return result, broker


def test_trusted_live_execution_passes_and_checks_broker_account() -> None:
    result, broker = _evaluate()

    assert result.passed is True
    assert result.reasons == []
    assert broker.account_calls == 1
    assert broker.submit_calls == 0


def test_mock_snapshot_is_rejected_before_any_broker_io() -> None:
    result, broker = _evaluate(
        snapshot=_snapshot(
            provider_name="MockMarketDataProvider",
            provider_status="mock_verified",
            dataset_fingerprint=None,
        )
    )

    assert result.passed is False
    assert "mock_provider_not_allowed_for_live_execution" in result.reasons
    assert "snapshot_provider_not_verified" in result.reasons
    assert broker.account_calls == 0
    assert broker.submit_calls == 0


@pytest.mark.parametrize(
    ("snapshot", "expected_reason"),
    [
        (_snapshot(age_minutes=61), "snapshot_stale"),
        (_snapshot(bar_coverage=0.99), "snapshot_bar_coverage_below_threshold"),
        (_snapshot(fundamental_coverage=0.99), "snapshot_fundamental_coverage_below_threshold"),
        (_snapshot(live_execution_allowed=False), "snapshot_live_execution_not_allowed"),
        (_snapshot(dataset_fingerprint=None), "dataset_fingerprint_missing_or_invalid"),
    ],
)
def test_invalid_snapshot_evidence_is_rejected_before_broker_io(
    snapshot: SourceSnapshotSummary,
    expected_reason: str,
) -> None:
    result, broker = _evaluate(snapshot=snapshot)

    assert expected_reason in result.reasons
    assert broker.account_calls == 0


def test_missing_snapshot_is_rejected_before_broker_io() -> None:
    broker = RecordingLiveBroker()
    result = ExecutionReadinessGate(max_snapshot_age_minutes=60).evaluate(
        source_snapshot_id="missing",
        snapshot=None,
        kill_switch_enabled=False,
        approval_granted=True,
        broker_adapter=broker,
        position_reconciliation={"passed": True},
        strategy_validation={"passed": True, "backtest_run_id": "bt-accepted"},
    )

    assert result.reasons == ["source_snapshot_missing"]
    assert broker.account_calls == 0


@pytest.mark.parametrize(
    ("kwargs", "expected_reason"),
    [
        ({"approval_granted": False}, "approval_required"),
        ({"kill_switch_enabled": True}, "kill_switch_enabled"),
        (
            {"reconciliation": {"passed": False, "reason": "position_reconciliation_mismatch"}},
            "position_reconciliation_mismatch",
        ),
        (
            {"automatic": True, "paper_shadow": {"passed": False}},
            "paper_shadow_period_incomplete",
        ),
        (
            {"strategy_validation": {"passed": False, "reason": "accepted_out_of_sample_backtest_missing"}},
            "accepted_out_of_sample_backtest_missing",
        ),
    ],
)
def test_operational_invariants_block_before_broker_io(kwargs: dict, expected_reason: str) -> None:
    result, broker = _evaluate(**kwargs)

    assert expected_reason in result.reasons
    assert broker.account_calls == 0
