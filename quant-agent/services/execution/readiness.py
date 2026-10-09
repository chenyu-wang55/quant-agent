from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from domain.entities.models import SourceSnapshotSummary
from services.execution.broker_adapter import BrokerExecutionAdapter

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACTIVE_BROKER_STATUSES = {"active"}


class ExecutionNotReadyError(ValueError):
    """Raised before broker I/O when a confirmed live execution is not safe."""

    def __init__(self, result: "ExecutionReadinessResult") -> None:
        self.result = result
        super().__init__(f"Live execution readiness gate failed: {','.join(result.reasons)}")


@dataclass(frozen=True)
class ExecutionReadinessResult:
    passed: bool
    reasons: list[str] = field(default_factory=list)
    checks: dict[str, Any] = field(default_factory=dict)

    def require(self) -> None:
        if not self.passed:
            raise ExecutionNotReadyError(self)


@dataclass(frozen=True)
class ExecutionReadinessGate:
    max_snapshot_age_minutes: int
    min_bar_coverage: float = 1.0
    min_fundamental_coverage: float = 1.0

    def evaluate(
        self,
        *,
        source_snapshot_id: str | None,
        snapshot: SourceSnapshotSummary | None,
        kill_switch_enabled: bool,
        approval_granted: bool,
        broker_adapter: BrokerExecutionAdapter | None,
        position_reconciliation: dict[str, Any],
        strategy_validation: dict[str, Any],
        paper_shadow: dict[str, Any] | None = None,
        automatic: bool = False,
        checked_at: datetime | None = None,
        test_mode: bool = False,
    ) -> ExecutionReadinessResult:
        now = checked_at or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        else:
            now = now.astimezone(timezone.utc)

        reasons: list[str] = []
        checks: dict[str, Any] = {}

        checks["kill_switch"] = {"passed": not kill_switch_enabled}
        if kill_switch_enabled:
            reasons.append("kill_switch_enabled")

        checks["approval"] = {"passed": approval_granted}
        if not approval_granted:
            reasons.append("approval_required")

        snapshot_reasons, snapshot_check = self._snapshot_check(
            source_snapshot_id=source_snapshot_id,
            snapshot=snapshot,
            checked_at=now,
            test_mode=test_mode,
            broker_adapter=broker_adapter,
        )
        reasons.extend(snapshot_reasons)
        checks["source_snapshot"] = snapshot_check

        reconciliation_passed = bool(position_reconciliation.get("passed"))
        checks["position_reconciliation"] = dict(position_reconciliation)
        if not reconciliation_passed:
            reasons.append(str(position_reconciliation.get("reason") or "position_reconciliation_failed"))

        strategy_passed = bool(strategy_validation.get("passed"))
        checks["strategy_validation"] = dict(strategy_validation)
        if not strategy_passed:
            reasons.append(str(strategy_validation.get("reason") or "strategy_validation_failed"))

        if automatic:
            paper_shadow = dict(paper_shadow or {})
            checks["paper_shadow"] = paper_shadow
            if not bool(paper_shadow.get("passed")):
                reasons.append("paper_shadow_period_incomplete")

        # Do not touch a broker account until all local/data invariants pass. This
        # is important for both safety and the guarantee that rejected mock data
        # can never cause any broker-side I/O.
        if reasons:
            checks["broker_account"] = {"passed": False, "skipped": True, "reason": "local_gate_failed"}
            return ExecutionReadinessResult(passed=False, reasons=_dedupe(reasons), checks=checks)

        broker_reasons, broker_check = self._broker_check(
            broker_adapter=broker_adapter,
            test_mode=test_mode,
        )
        reasons.extend(broker_reasons)
        checks["broker_account"] = broker_check
        return ExecutionReadinessResult(passed=not reasons, reasons=_dedupe(reasons), checks=checks)

    def _snapshot_check(
        self,
        *,
        source_snapshot_id: str | None,
        snapshot: SourceSnapshotSummary | None,
        checked_at: datetime,
        test_mode: bool,
        broker_adapter: BrokerExecutionAdapter | None,
    ) -> tuple[list[str], dict[str, Any]]:
        reasons: list[str] = []
        if not source_snapshot_id:
            return ["source_snapshot_id_missing"], {"passed": False, "reason": "source_snapshot_id_missing"}
        if snapshot is None:
            return ["source_snapshot_missing"], {
                "passed": False,
                "reason": "source_snapshot_missing",
                "source_snapshot_id": source_snapshot_id,
            }

        quality = dict(snapshot.data_quality or {})
        provider_name = snapshot.provider_name
        provider_status = str(quality.get("provider_status") or "unverified").lower()
        live_allowed = bool(quality.get("live_execution_allowed"))
        bar_coverage = float(quality.get("bar_coverage") or 0.0)
        fundamental_coverage = float(quality.get("fundamental_coverage") or 0.0)
        fingerprint = str(quality.get("dataset_fingerprint") or "").lower()
        as_of = snapshot.as_of
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        else:
            as_of = as_of.astimezone(timezone.utc)
        age_minutes = max(0.0, (checked_at - as_of).total_seconds() / 60.0)

        # Existing integration tests exercise broker recovery with deterministic
        # fake adapters. Test mode may use mock evidence only with a non-Alpaca
        # adapter; production and any Alpaca-shaped adapter remain strict.
        adapter_name = str(getattr(broker_adapter, "name", "") or "").lower()
        fake_test_execution = test_mode and adapter_name not in {"alpaca", ""}
        is_mock = "mock" in provider_name.lower() or provider_status == "mock_verified"
        if is_mock and not fake_test_execution:
            reasons.append("mock_provider_not_allowed_for_live_execution")
        if provider_status != "verified" and not fake_test_execution:
            reasons.append("snapshot_provider_not_verified")
        if not live_allowed and not fake_test_execution:
            reasons.append("snapshot_live_execution_not_allowed")
        if not fake_test_execution:
            if bar_coverage < self.min_bar_coverage:
                reasons.append("snapshot_bar_coverage_below_threshold")
            if fundamental_coverage < self.min_fundamental_coverage:
                reasons.append("snapshot_fundamental_coverage_below_threshold")
            if age_minutes > self.max_snapshot_age_minutes:
                reasons.append("snapshot_stale")
            if not _SHA256_RE.fullmatch(fingerprint):
                reasons.append("dataset_fingerprint_missing_or_invalid")

        return reasons, {
            "passed": not reasons,
            "source_snapshot_id": source_snapshot_id,
            "provider_name": provider_name,
            "provider_status": provider_status,
            "live_execution_allowed": live_allowed,
            "bar_coverage": bar_coverage,
            "min_bar_coverage": self.min_bar_coverage,
            "fundamental_coverage": fundamental_coverage,
            "min_fundamental_coverage": self.min_fundamental_coverage,
            "dataset_fingerprint": fingerprint or None,
            "as_of": as_of.isoformat(),
            "age_minutes": round(age_minutes, 3),
            "max_age_minutes": self.max_snapshot_age_minutes,
            "test_fake_adapter_bypass": fake_test_execution,
            "reasons": reasons,
        }

    @staticmethod
    def _broker_check(
        *,
        broker_adapter: BrokerExecutionAdapter | None,
        test_mode: bool,
    ) -> tuple[list[str], dict[str, Any]]:
        if broker_adapter is None:
            return ["live_broker_not_configured"], {"passed": False, "reason": "live_broker_not_configured"}

        adapter_name = str(getattr(broker_adapter, "name", broker_adapter.__class__.__name__))
        if test_mode and adapter_name.lower() != "alpaca":
            return [], {"passed": True, "broker": adapter_name, "test_fake_adapter_bypass": True}

        try:
            account = broker_adapter.get_account()
        except Exception as exc:
            return ["broker_account_unavailable"], {
                "passed": False,
                "broker": adapter_name,
                "reason": "broker_account_unavailable",
                "error_type": type(exc).__name__,
            }

        status = str(account.status or "").lower()
        blocked = bool(account.trading_blocked or account.account_blocked)
        passed = status in _ACTIVE_BROKER_STATUSES and not blocked
        reason = None
        if blocked:
            reason = "broker_account_blocked"
        elif status not in _ACTIVE_BROKER_STATUSES:
            reason = "broker_account_not_active"
        return ([] if passed else [str(reason)]), {
            "passed": passed,
            "broker": adapter_name,
            "account_id": account.account_id,
            "status": account.status,
            "trading_blocked": account.trading_blocked,
            "account_blocked": account.account_blocked,
            "reason": reason,
        }


def _dedupe(items: list[str]) -> list[str]:
    return list(dict.fromkeys(item for item in items if item))
