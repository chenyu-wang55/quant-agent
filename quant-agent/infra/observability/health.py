from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from infra.config import ObservabilitySettings, env_text
from infra.db.session import SessionLocal, get_database_url
from infra.observability.alerts import AlertSpec, OperationalAlertManager


def _sqlite_path() -> Path | None:
    url = get_database_url()
    if not url.startswith("sqlite:///"):
        return None
    value = url.removeprefix("sqlite:///")
    path = Path(value)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / path
    return path.resolve()


def _check(name: str, status: str, message: str, **details: Any) -> dict[str, Any]:
    return {"name": name, "status": status, "message": message, "details": details}


class HealthEvaluator:
    def __init__(self, state: Any) -> None:
        self.state = state
        self.alert_manager = OperationalAlertManager()

    def evaluate(self, *, external_probe: bool | None = None) -> dict[str, Any]:
        checked_at = datetime.now(timezone.utc)
        settings = ObservabilitySettings.from_env()
        probe_external = settings.external_health_probe if external_probe is None else external_probe
        checks: list[dict[str, Any]] = []
        alerts: list[AlertSpec] = []

        database_ready, database_details = self._database_check()
        checks.append(
            _check(
                "database",
                "pass" if database_ready else "fail",
                "database query succeeded" if database_ready else "database query failed",
                **database_details,
            )
        )
        db_size = int(database_details.get("size_bytes") or 0)
        db_size_limit = settings.database_size_alert_bytes
        if db_size > db_size_limit:
            alerts.append(
                AlertSpec(
                    key="database_size_high",
                    category="database",
                    severity="warning",
                    message=f"database size {db_size} exceeds alert threshold {db_size_limit}",
                    details={"size_bytes": db_size, "threshold_bytes": db_size_limit},
                )
            )

        provider_ready, provider_details = self._provider_check(checked_at, probe_external)
        checks.append(
            _check(
                "data_provider",
                "pass" if provider_ready else "fail",
                "data provider is ready" if provider_ready else "data provider is not ready",
                **provider_details,
            )
        )

        policy = self.state.get_autopilot_policy()
        live_requested = bool(
            policy.enabled
            and policy.auto_execute_approved
            and getattr(policy.auto_execution_mode, "value", policy.auto_execution_mode) == "live"
        )
        broker_ready, broker_details = self._broker_check(live_requested, probe_external)
        checks.append(
            _check(
                "broker",
                "pass" if broker_ready else "fail",
                "broker is ready" if broker_ready else "broker is not ready",
                **broker_details,
            )
        )

        snapshot_ready, snapshot_details = self._snapshot_check(checked_at, policy)
        checks.append(
            _check(
                "source_snapshot",
                "pass" if snapshot_ready else "warn",
                "latest source snapshot is tradable" if snapshot_ready else "latest source snapshot is missing or stale",
                **snapshot_details,
            )
        )
        failure_count = int(snapshot_details.get("provider_failure_count") or 0)
        if failure_count:
            alerts.append(
                AlertSpec(
                    key="provider_failures",
                    category="data_provider",
                    severity="critical",
                    message=f"latest source snapshot recorded {failure_count} provider failures",
                    details=snapshot_details,
                )
            )

        consecutive_errors = self._consecutive_cycle_errors()
        error_threshold = settings.consecutive_error_alert_count
        if consecutive_errors >= error_threshold:
            alerts.append(
                AlertSpec(
                    key="consecutive_cycle_errors",
                    category="worker",
                    severity="critical",
                    message=f"worker has {consecutive_errors} consecutive failed system cycles",
                    details={"count": consecutive_errors, "threshold": error_threshold},
                )
            )

        reconciliation_ready, reconciliation_details = self._reconciliation_check(live_requested)
        checks.append(
            _check(
                "position_reconciliation",
                "pass" if reconciliation_ready else "fail",
                "position reconciliation is ready" if reconciliation_ready else "position reconciliation blocks live trading",
                **reconciliation_details,
            )
        )
        if not reconciliation_ready:
            alerts.append(
                AlertSpec(
                    key="position_reconciliation_mismatch",
                    category="reconciliation",
                    severity="critical",
                    message="latest broker position reconciliation blocks live execution",
                    details=reconciliation_details,
                )
            )

        active_alerts = self.alert_manager.sync(alerts)
        self.state.metrics_store.set_gauge("health_database_ready", 1.0 if database_ready else 0.0)
        self.state.metrics_store.set_gauge("health_provider_ready", 1.0 if provider_ready else 0.0)
        self.state.metrics_store.set_gauge("health_snapshot_ready", 1.0 if snapshot_ready else 0.0)
        self.state.metrics_store.set_gauge("health_broker_ready", 1.0 if broker_ready else 0.0)
        self.state.metrics_store.set_gauge("database_size_bytes", db_size)
        self.state.metrics_store.set_gauge("operational_alerts_active", len(active_alerts))
        self.state.metrics_store.set_gauge("worker_consecutive_errors", consecutive_errors)

        core_ready = database_ready and provider_ready and broker_ready
        trading_blockers: list[str] = []
        if not snapshot_ready:
            trading_blockers.append("source_snapshot_not_ready")
        if self.state.kill_switch.enabled:
            trading_blockers.append("kill_switch_enabled")
        if not policy.enabled:
            trading_blockers.append("autopilot_disabled")
        if live_requested and not reconciliation_ready:
            trading_blockers.append("position_reconciliation_not_ready")
        trading_ready = core_ready and snapshot_ready and not trading_blockers
        return {
            "status": "ready" if core_ready else "not_ready",
            "service": "quant-agent-api",
            "checked_at": checked_at.isoformat(),
            "ready": core_ready,
            "trading_ready": trading_ready,
            "trading_blockers": trading_blockers,
            "checks": checks,
            "active_alerts": active_alerts,
        }

    @staticmethod
    def _database_check() -> tuple[bool, dict[str, Any]]:
        path = _sqlite_path()
        details: dict[str, Any] = {
            "url_kind": "sqlite" if path is not None else "database",
            "path": str(path) if path is not None else None,
            "size_bytes": path.stat().st_size if path is not None and path.exists() else 0,
        }
        try:
            with SessionLocal() as session:
                session.execute(text("SELECT 1"))
            return True, details
        except Exception as exc:
            details["error_type"] = type(exc).__name__
            details["error"] = str(exc)
            return False, details

    def _provider_check(self, checked_at: datetime, external_probe: bool) -> tuple[bool, dict[str, Any]]:
        provider = self.state.provider
        provider_name = provider.__class__.__name__
        details: dict[str, Any] = {"provider": provider_name, "external_probe": external_probe}
        if provider_name == "ExternalProviderPlaceholder":
            details["reason"] = "unsupported_data_provider"
            return False, details
        if provider_name == "YFinanceProvider":
            universe_path = env_text("POINT_IN_TIME_UNIVERSE_CSV")
            path = Path(universe_path).expanduser() if universe_path else None
            if path is None or not path.is_file():
                details["reason"] = "point_in_time_universe_missing"
                details["required_env"] = "POINT_IN_TIME_UNIVERSE_CSV"
                return False, details
            try:
                details["point_in_time"] = provider.validate_point_in_time_configuration(
                    checked_at
                )
            except Exception as exc:
                details["reason"] = "point_in_time_universe_invalid"
                details["error_type"] = type(exc).__name__
                details["error"] = str(exc)
                return False, details
        quality_report: dict[str, Any] = dict(
            getattr(provider, "get_quality_report", lambda: {})()
        )
        details["quality"] = quality_report
        if str(quality_report.get("status") or "verified") == "blocked":
            details["reason"] = "provider_quality_blocked"
            return False, details
        if external_probe:
            try:
                price = provider.get_latest_price("SPY", checked_at)
                details["probe_ticker"] = "SPY"
                details["probe_price"] = price
                if price is None or float(price) <= 0:
                    details["reason"] = "provider_probe_no_price"
                    return False, details
            except Exception as exc:
                details["reason"] = "provider_probe_failed"
                details["error_type"] = type(exc).__name__
                details["error"] = str(exc)
                return False, details
        return True, details

    def _broker_check(self, live_requested: bool, external_probe: bool) -> tuple[bool, dict[str, Any]]:
        adapter = self.state.execution_router.broker_adapter
        if adapter is None:
            return (not live_requested), {
                "configured": False,
                "required": live_requested,
                "reason": "not_required" if not live_requested else "live_broker_not_configured",
            }
        details: dict[str, Any] = {
            "configured": True,
            "required": live_requested,
            "broker": getattr(adapter, "name", adapter.__class__.__name__),
            "external_probe": external_probe or live_requested,
        }
        if external_probe or live_requested:
            try:
                account = adapter.get_account()
                details.update(
                    {
                        "account_status": account.status,
                        "trading_blocked": account.trading_blocked,
                        "account_blocked": account.account_blocked,
                    }
                )
                if account.trading_blocked or account.account_blocked:
                    details["reason"] = "broker_account_blocked"
                    return False, details
            except Exception as exc:
                details["reason"] = "broker_probe_failed"
                details["error_type"] = type(exc).__name__
                details["error"] = str(exc)
                return False, details
        return True, details

    def _snapshot_check(self, checked_at: datetime, policy: Any) -> tuple[bool, dict[str, Any]]:
        snapshots = self.state.list_source_snapshots(limit=1)
        if not snapshots:
            return False, {"reason": "source_snapshot_missing"}
        snapshot = snapshots[0]
        quality = dict(snapshot.data_quality or {})
        as_of = snapshot.as_of
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        age_minutes = max(0.0, (checked_at - as_of.astimezone(timezone.utc)).total_seconds() / 60.0)
        max_age = int(policy.max_snapshot_bar_age_minutes)
        live_allowed = bool(quality.get("live_execution_allowed"))
        details = {
            "source_snapshot_id": snapshot.source_snapshot_id,
            "as_of": as_of.isoformat(),
            "age_minutes": round(age_minutes, 3),
            "max_age_minutes": max_age,
            "live_execution_allowed": live_allowed,
            "provider_status": quality.get("provider_status"),
            "provider_failure_count": int(quality.get("provider_failure_count") or 0),
        }
        return live_allowed and age_minutes <= max_age, details

    def _consecutive_cycle_errors(self) -> int:
        count = 0
        for run in self.state.list_system_cycle_runs(limit=100):
            if run.status != "error":
                break
            count += 1
        return count

    def _reconciliation_check(self, live_requested: bool) -> tuple[bool, dict[str, Any]]:
        if not live_requested:
            return True, {"required": False, "reason": "not_required"}
        reports = self.state.list_position_reconciliations(limit=1)
        if not reports:
            return False, {"required": True, "reason": "position_reconciliation_missing"}
        latest = reports[0]
        return (not latest.blocks_auto_execution), {
            "required": True,
            "reconciliation_id": latest.reconciliation_id,
            "reconciliation_status": latest.status,
            "blocks_auto_execution": latest.blocks_auto_execution,
            "checked_at": latest.checked_at.isoformat(),
        }
