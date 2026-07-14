from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from apps.api.dependencies import get_app_state
from apps.worker.automation import (
    _auto_approval_report,
    _auto_approve_cycle,
    _auto_execute_cycle,
    _auto_execution_report,
)
from apps.worker.broker_cycles import (
    _auto_broker_sync_cycle,
    _auto_position_reconciliation_cycle,
)
from domain.entities.models import (
    AutopilotPolicy,
    AutopilotPreflight,
    BacktestRunRequest,
    MarketSessionStatus,
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
    SnapshotMode,
    SystemCycleRun,
)
from infra.observability.health import HealthEvaluator
from infra.observability.logging import configure_logging
from infra.queue.events import EventType

configure_logging()
logger = logging.getLogger(__name__)


def _run_research_job(job_name: str) -> None:
    state = get_app_state()
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective=f"Scheduled {job_name} recommendation generation",
        as_of=datetime.now(timezone.utc),
    )
    output = state.pipeline.run(request)
    state.ingest_run_output(request, output)
    print(f"{job_name}: generated {len(output.result.recommendations)} recommendations")


def pre_market_ingestion() -> None:
    _run_research_job("pre_market_ingestion")


def intraday_refresh() -> None:
    _run_research_job("intraday_refresh")


def end_of_day_reconciliation() -> None:
    _run_research_job("end_of_day_reconciliation")


def nightly_backtest_batch() -> None:
    state = get_app_state()
    now = datetime.now(timezone.utc)
    request = BacktestRunRequest(
        run_name="nightly_backtest",
        start_date=now.replace(year=max(2000, now.year - 1)),
        end_date=now,
        top_n=10,
    )
    template = state.last_research_request or ResearchRunRequest(
        run_type=RunType.BACKTEST_EVALUATION,
        objective="Nightly backtest template",
        as_of=now,
    )
    result = state.backtest_engine.run(request, state.pipeline, template)
    state.backtest_runs.append(result)
    state.publish_event(
        EventType.MODEL_EVALUATION,
        {
            "run_id": result.run_id,
            "config_hash": result.config_hash,
            "metrics": result.metrics,
        },
    )
    print(f"nightly_backtest_batch: run_id={result.run_id}")


def daily_metrics_aggregation() -> None:
    state = get_app_state()
    metrics: dict[str, Any] = dict(state.metrics_store.dump())
    print(f"daily_metrics_aggregation: {metrics}")


def process_event_queue() -> None:
    state = get_app_state()
    events = state.consume_events(limit=1000)
    type_counts: dict[str, int] = {}
    for event in events:
        key = event.event_type.value
        type_counts[key] = type_counts.get(key, 0) + 1
    print(f"process_event_queue: consumed={len(events)} by_type={type_counts}")


def monitor_positions_alerts() -> None:
    state = get_app_state()
    alerts = state.monitor_sell_alerts()
    print(f"monitor_positions_alerts: alert_count={len(alerts)}")
    for alert in alerts:
        print(f"{alert.ticker} | {alert.level.value} | {alert.reason_code} | {alert.message_cn}")


def _event_type_counts(events: list[Any]) -> dict[str, int]:
    type_counts: dict[str, int] = {}
    for event in events:
        key = event.event_type.value
        type_counts[key] = type_counts.get(key, 0) + 1
    return type_counts


def _snapshot_quality_gate(
    *,
    source_snapshot_id: str,
    min_bar_coverage: float,
    min_fundamental_coverage: float,
    max_bar_age_minutes: int,
) -> dict[str, Any]:
    state = get_app_state()
    summary = state.source_snapshot_repo.get_summary(source_snapshot_id)
    if summary is None:
        return {
            "passed": False,
            "source_snapshot_id": source_snapshot_id,
            "reason": "source_snapshot_missing",
            "reasons": ["source_snapshot_missing"],
            "min_bar_coverage": min_bar_coverage,
            "min_fundamental_coverage": min_fundamental_coverage,
            "max_bar_age_minutes": max_bar_age_minutes,
            "data_quality": {},
        }

    quality = summary.data_quality or {}
    bar_coverage = float(quality.get("bar_coverage") or 0.0)
    fundamental_coverage = float(quality.get("fundamental_coverage") or 0.0)
    latest_bar_age_minutes = quality.get("latest_bar_age_minutes")
    reasons: list[str] = []
    if not bool(quality.get("live_execution_allowed")):
        reasons.append("snapshot_provider_quality_blocked")
    if bar_coverage < min_bar_coverage:
        reasons.append("snapshot_bar_coverage_below_threshold")
    if fundamental_coverage < min_fundamental_coverage:
        reasons.append("snapshot_fundamental_coverage_below_threshold")
    if latest_bar_age_minutes is None:
        reasons.append("snapshot_latest_bar_missing")
    elif int(latest_bar_age_minutes) > max_bar_age_minutes:
        reasons.append("snapshot_bar_age_above_threshold")

    return {
        "passed": not reasons,
        "source_snapshot_id": source_snapshot_id,
        "reason": reasons[0] if reasons else None,
        "reasons": reasons,
        "min_bar_coverage": min_bar_coverage,
        "min_fundamental_coverage": min_fundamental_coverage,
        "max_bar_age_minutes": max_bar_age_minutes,
        "bar_coverage": bar_coverage,
        "fundamental_coverage": fundamental_coverage,
        "latest_bar_age_minutes": latest_bar_age_minutes,
        "data_quality": quality,
    }


def _apply_autopilot_policy(
    *,
    policy: AutopilotPolicy,
    preflight: AutopilotPreflight,
    current: dict[str, Any],
) -> dict[str, Any]:
    updates = dict(current)
    updates["autopilot_policy"] = policy.model_dump(mode="json")
    updates["autopilot_preflight"] = preflight.model_dump(mode="json")

    daily_usage = preflight.daily_usage or {}
    updates.update(
        {
            "auto_approve_recommendations": preflight.can_auto_approve,
            "auto_execute_approved": preflight.can_auto_execute,
            "auto_approve_min_confidence": policy.auto_approve_min_confidence,
            "auto_approve_min_composite": policy.auto_approve_min_composite,
            "max_auto_approvals": min(
                policy.max_auto_approvals,
                int(daily_usage.get("remaining_approvals", policy.max_auto_approvals)),
            ),
            "auto_execution_mode": policy.auto_execution_mode.value,
            "restrict_auto_execution_to_regular_hours": policy.restrict_auto_execution_to_regular_hours,
            "max_auto_buys": min(
                policy.max_auto_buys,
                int(daily_usage.get("remaining_buys", policy.max_auto_buys)),
            ),
            "max_auto_sells": min(
                policy.max_auto_sells,
                int(daily_usage.get("remaining_sells", policy.max_auto_sells)),
            ),
            "order_dedupe_minutes": policy.order_dedupe_minutes,
            "sell_alert_cooldown_minutes": policy.sell_alert_cooldown_minutes,
            "rebuy_cooldown_minutes": policy.rebuy_cooldown_minutes,
            "min_snapshot_bar_coverage": policy.min_snapshot_bar_coverage,
            "min_snapshot_fundamental_coverage": policy.min_snapshot_fundamental_coverage,
            "max_snapshot_bar_age_minutes": policy.max_snapshot_bar_age_minutes,
            "max_open_risk_pct": policy.max_open_risk_pct,
            "max_daily_realized_loss_pct": policy.max_daily_realized_loss_pct,
            "max_auto_buy_price_drift_pct": policy.max_auto_buy_price_drift_pct,
            "require_position_reconciliation": policy.require_position_reconciliation,
            "max_position_reconciliation_age_minutes": policy.max_position_reconciliation_age_minutes,
            "min_paper_shadow_trading_days": policy.min_paper_shadow_trading_days,
            "account_equity": policy.account_equity,
            "risk_per_trade_pct": policy.risk_per_trade_pct,
            "max_position_pct": policy.max_position_pct,
            "max_gross_exposure_pct": policy.max_gross_exposure_pct,
            "max_sector_exposure_pct": policy.max_sector_exposure_pct,
        }
    )
    return updates


def _paper_shadow_evidence(
    *,
    use_autopilot_policy: bool,
    autopilot_policy: dict[str, Any] | None,
    autopilot_preflight: dict[str, Any] | None,
    auto_execution: dict[str, Any],
    auto_execution_enabled: bool,
    snapshot_quality_gate: dict[str, Any],
    daily_loss_gate: dict[str, Any],
    position_reconciliation_gate: dict[str, Any],
    market_session: MarketSessionStatus,
) -> dict[str, Any]:
    """Build explicit, fail-closed evidence for one paper-shadow trading day."""

    policy = autopilot_policy or {}
    preflight = autopilot_preflight or {}
    mode = str(auto_execution.get("mode") or policy.get("auto_execution_mode") or "").lower()
    reasons: list[str] = []
    checks = {
        "autopilot_policy_used": use_autopilot_policy,
        "autopilot_policy_enabled": policy.get("enabled") is True,
        "policy_auto_execution_enabled": policy.get("auto_execute_approved") is True,
        "paper_mode": mode == "paper",
        "preflight_can_auto_execute": preflight.get("can_auto_execute") is True,
        "auto_execution_path_enabled": auto_execution.get("enabled") is True,
        "effective_auto_execution_enabled": auto_execution_enabled,
        "snapshot_quality_passed": snapshot_quality_gate.get("passed") is True,
        "daily_loss_gate_passed": daily_loss_gate.get("passed") is True,
        "position_reconciliation_passed": position_reconciliation_gate.get("passed") is True,
        "regular_xnys_session": market_session.is_regular_session,
        "execution_completed_without_errors": int(auto_execution.get("error_count") or 0) == 0,
    }
    for name, passed in checks.items():
        if not passed:
            reasons.append(name)
    return {
        "schema_version": 1,
        "qualified": not reasons,
        "mode": mode or None,
        "checks": checks,
        "reasons": reasons,
        "action_count": int(auto_execution.get("action_count") or 0),
        "buy_order_count": int(auto_execution.get("buy_order_count") or 0),
        "sell_order_count": int(auto_execution.get("sell_order_count") or 0),
        "error_count": int(auto_execution.get("error_count") or 0),
        "xnys_session_date": (market_session.as_of.astimezone(ZoneInfo("America/New_York")).date().isoformat()),
    }


def system_cycle(
    top_n: int = 8,
    min_confidence: float | None = None,
    consume_events: bool = False,
    as_of: datetime | None = None,
    auto_sync_broker_statuses: bool = True,
    max_broker_sync_items: int = 50,
    auto_reconcile_broker_positions: bool = True,
    position_reconciliation_qty_tolerance: float = 1e-6,
    use_autopilot_policy: bool = False,
    auto_execute_approved: bool = False,
    auto_approve_recommendations: bool = False,
    auto_approve_min_confidence: float = 0.72,
    auto_approve_min_composite: float = 0.0,
    max_auto_approvals: int = 1,
    auto_execution_mode: str = "paper",
    restrict_auto_execution_to_regular_hours: bool = True,
    allow_auto_live_execution: bool | None = None,
    max_auto_buys: int = 1,
    max_auto_sells: int = 10,
    order_dedupe_minutes: int = 1440,
    sell_alert_cooldown_minutes: int = 60,
    rebuy_cooldown_minutes: int = 240,
    min_snapshot_bar_coverage: float = 1.0,
    min_snapshot_fundamental_coverage: float = 1.0,
    max_snapshot_bar_age_minutes: int = 4320,
    account_equity: float = 100_000.0,
    max_open_risk_pct: float = 0.06,
    max_daily_realized_loss_pct: float = 0.03,
    max_auto_buy_price_drift_pct: float = 0.03,
    require_position_reconciliation: bool = False,
    max_position_reconciliation_age_minutes: int = 1440,
    min_paper_shadow_trading_days: int = 20,
    risk_per_trade_pct: float = 0.01,
    max_position_pct: float = 0.10,
    max_gross_exposure_pct: float = 1.0,
    max_sector_exposure_pct: float = 0.30,
) -> dict[str, Any]:
    state = get_app_state()
    started_at = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    run_id = uuid4().hex[:16]
    logger.info(
        "system cycle started",
        extra={"event": "system_cycle_started", "run_id": run_id, "started_at": started_at.isoformat()},
    )
    broker_order_sync = _auto_broker_sync_cycle(
        enabled=auto_sync_broker_statuses,
        checked_at=started_at,
        max_items=max_broker_sync_items,
    )
    broker_position_reconciliation = _auto_position_reconciliation_cycle(
        enabled=auto_reconcile_broker_positions,
        checked_at=started_at,
        qty_tolerance=position_reconciliation_qty_tolerance,
    )
    autopilot_policy: dict[str, Any] | None = None
    autopilot_preflight: dict[str, Any] | None = None
    if use_autopilot_policy:
        policy = state.get_autopilot_policy()
        preflight = state.build_autopilot_preflight(
            policy,
            as_of=started_at,
            allow_auto_live_execution=allow_auto_live_execution,
        )
        policy_values = _apply_autopilot_policy(
            policy=policy,
            preflight=preflight,
            current={
                "auto_execute_approved": auto_execute_approved,
                "auto_approve_recommendations": auto_approve_recommendations,
                "auto_approve_min_confidence": auto_approve_min_confidence,
                "auto_approve_min_composite": auto_approve_min_composite,
                "max_auto_approvals": max_auto_approvals,
                "auto_execution_mode": auto_execution_mode,
                "restrict_auto_execution_to_regular_hours": restrict_auto_execution_to_regular_hours,
                "max_auto_buys": max_auto_buys,
                "max_auto_sells": max_auto_sells,
                "order_dedupe_minutes": order_dedupe_minutes,
                "sell_alert_cooldown_minutes": sell_alert_cooldown_minutes,
                "rebuy_cooldown_minutes": rebuy_cooldown_minutes,
                "min_snapshot_bar_coverage": min_snapshot_bar_coverage,
                "min_snapshot_fundamental_coverage": min_snapshot_fundamental_coverage,
                "max_snapshot_bar_age_minutes": max_snapshot_bar_age_minutes,
                "account_equity": account_equity,
                "max_open_risk_pct": max_open_risk_pct,
                "max_daily_realized_loss_pct": max_daily_realized_loss_pct,
                "max_auto_buy_price_drift_pct": max_auto_buy_price_drift_pct,
                "require_position_reconciliation": require_position_reconciliation,
                "max_position_reconciliation_age_minutes": max_position_reconciliation_age_minutes,
                "min_paper_shadow_trading_days": min_paper_shadow_trading_days,
                "risk_per_trade_pct": risk_per_trade_pct,
                "max_position_pct": max_position_pct,
                "max_gross_exposure_pct": max_gross_exposure_pct,
                "max_sector_exposure_pct": max_sector_exposure_pct,
            },
        )
        autopilot_policy = policy_values.pop("autopilot_policy")
        autopilot_preflight = policy_values.pop("autopilot_preflight")
        auto_execute_approved = policy_values["auto_execute_approved"]
        auto_approve_recommendations = policy_values["auto_approve_recommendations"]
        auto_approve_min_confidence = policy_values["auto_approve_min_confidence"]
        auto_approve_min_composite = policy_values["auto_approve_min_composite"]
        max_auto_approvals = policy_values["max_auto_approvals"]
        auto_execution_mode = policy_values["auto_execution_mode"]
        restrict_auto_execution_to_regular_hours = policy_values["restrict_auto_execution_to_regular_hours"]
        max_auto_buys = policy_values["max_auto_buys"]
        max_auto_sells = policy_values["max_auto_sells"]
        order_dedupe_minutes = policy_values["order_dedupe_minutes"]
        sell_alert_cooldown_minutes = policy_values["sell_alert_cooldown_minutes"]
        rebuy_cooldown_minutes = policy_values["rebuy_cooldown_minutes"]
        min_snapshot_bar_coverage = policy_values["min_snapshot_bar_coverage"]
        min_snapshot_fundamental_coverage = policy_values["min_snapshot_fundamental_coverage"]
        max_snapshot_bar_age_minutes = policy_values["max_snapshot_bar_age_minutes"]
        account_equity = policy_values["account_equity"]
        max_open_risk_pct = policy_values["max_open_risk_pct"]
        max_daily_realized_loss_pct = policy_values["max_daily_realized_loss_pct"]
        max_auto_buy_price_drift_pct = policy_values["max_auto_buy_price_drift_pct"]
        require_position_reconciliation = policy_values["require_position_reconciliation"]
        max_position_reconciliation_age_minutes = policy_values["max_position_reconciliation_age_minutes"]
        min_paper_shadow_trading_days = policy_values["min_paper_shadow_trading_days"]
        risk_per_trade_pct = policy_values["risk_per_trade_pct"]
        max_position_pct = policy_values["max_position_pct"]
        max_gross_exposure_pct = policy_values["max_gross_exposure_pct"]
        max_sector_exposure_pct = policy_values["max_sector_exposure_pct"]

    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="Scheduled full system cycle",
        as_of=started_at,
        snapshot_mode=SnapshotMode.LATEST,
        publication=PublicationConfig(top_n=top_n, output_channels=["api", "worker"]),
        risk_policy=RiskPolicy(min_confidence=min_confidence) if min_confidence is not None else RiskPolicy(),
    )
    output = state.pipeline.run(request)
    state.ingest_run_output(request, output)
    snapshot_quality_gate = _snapshot_quality_gate(
        source_snapshot_id=output.result.source_snapshot_id,
        min_bar_coverage=min_snapshot_bar_coverage,
        min_fundamental_coverage=min_snapshot_fundamental_coverage,
        max_bar_age_minutes=max_snapshot_bar_age_minutes,
    )
    snapshot_quality_passed = bool(snapshot_quality_gate.get("passed"))
    daily_loss_gate = state.get_autopilot_daily_loss_gate(
        as_of=started_at,
        account_equity=account_equity,
        max_daily_realized_loss_pct=max_daily_realized_loss_pct,
    )
    daily_loss_passed = bool(daily_loss_gate.get("passed"))
    live_auto_execution_requested = auto_execute_approved and auto_execution_mode.lower() == "live"
    paper_shadow_gate = state.get_paper_shadow_gate(
        required_trading_days=min_paper_shadow_trading_days,
        as_of=started_at,
    )
    market_session = state.get_market_session_status(as_of=started_at)
    position_reconciliation_gate = state.get_position_reconciliation_gate(
        require_position_reconciliation=require_position_reconciliation or live_auto_execution_requested,
        max_age_minutes=max_position_reconciliation_age_minutes,
        as_of=started_at,
    )
    position_reconciliation_passed = bool(position_reconciliation_gate.get("passed"))
    auto_approval_block: dict[str, Any] | None = None
    if not snapshot_quality_passed:
        auto_approval_block = {
            "action": "approve_recommendation",
            "status": "skipped",
            "reason": "snapshot_quality_gate_failed",
            "snapshot_quality_gate": snapshot_quality_gate,
        }
    elif not daily_loss_passed:
        auto_approval_block = {
            "action": "approve_recommendation",
            "status": "skipped",
            "reason": "daily_loss_gate_failed",
            "daily_loss_gate": daily_loss_gate,
        }
    auto_approval = (
        _auto_approve_cycle(
            recommendations=output.result.recommendations[:top_n],
            max_auto_approvals=max_auto_approvals,
            min_confidence=auto_approve_min_confidence,
            min_composite=auto_approve_min_composite,
            order_dedupe_minutes=order_dedupe_minutes,
            as_of=started_at,
        )
        if auto_approve_recommendations and auto_approval_block is None
        else _auto_approval_report(
            enabled=False,
            actions=[auto_approval_block],
        )
        if auto_approve_recommendations and auto_approval_block is not None
        else _auto_approval_report(enabled=False, actions=[])
    )
    alerts = state.monitor_sell_alerts(as_of=started_at)
    state.record_sell_alert_audits(alerts, monitor_run_id=run_id)
    auto_execution_block: dict[str, Any] | None = None
    if not snapshot_quality_passed:
        auto_execution_block = {
            "action": "auto_execution",
            "status": "skipped",
            "reason": "snapshot_quality_gate_failed",
            "snapshot_quality_gate": snapshot_quality_gate,
        }
    elif live_auto_execution_requested and not paper_shadow_gate["passed"]:
        auto_execution_block = {
            "action": "auto_execution",
            "status": "skipped",
            "reason": "paper_shadow_period_incomplete",
            "paper_shadow_gate": paper_shadow_gate,
        }
    elif not position_reconciliation_passed:
        auto_execution_block = {
            "action": "auto_execution",
            "status": "skipped",
            "reason": "position_reconciliation_gate_failed",
            "position_reconciliation_gate": position_reconciliation_gate,
        }
    elif (
        live_auto_execution_requested
        and restrict_auto_execution_to_regular_hours
        and not market_session.is_regular_session
    ):
        auto_execution_block = {
            "action": "auto_execution",
            "status": "skipped",
            "reason": "market_session_closed",
            "market_session": market_session.model_dump(mode="json"),
        }
    auto_execution = (
        _auto_execute_cycle(
            recommendations=output.result.recommendations[:top_n],
            alerts=alerts,
            run_id=run_id,
            execution_mode=auto_execution_mode,
            max_auto_buys=max_auto_buys,
            max_auto_sells=max_auto_sells,
            order_dedupe_minutes=order_dedupe_minutes,
            sell_alert_cooldown_minutes=sell_alert_cooldown_minutes,
            rebuy_cooldown_minutes=rebuy_cooldown_minutes,
            as_of=started_at,
            account_equity=account_equity,
            max_open_risk_pct=max_open_risk_pct,
            max_daily_realized_loss_pct=max_daily_realized_loss_pct,
            max_auto_buy_price_drift_pct=max_auto_buy_price_drift_pct,
            allow_auto_live_execution=allow_auto_live_execution,
            risk_per_trade_pct=risk_per_trade_pct,
            max_position_pct=max_position_pct,
            max_gross_exposure_pct=max_gross_exposure_pct,
            max_sector_exposure_pct=max_sector_exposure_pct,
        )
        if auto_execute_approved and auto_execution_block is None
        else _auto_execution_report(
            enabled=False,
            mode=auto_execution_mode,
            actions=[auto_execution_block],
        )
        if auto_execute_approved and auto_execution_block is not None
        else _auto_execution_report(enabled=False, mode=auto_execution_mode, actions=[])
    )
    auto_execution_live_gate = auto_execution.get("live_execution_gate") or {}
    auto_execution_account_gate = auto_execution.get("broker_account_gate") or {}
    auto_execution_equity_gate = auto_execution.get("account_equity_gate") or {}
    effective_auto_execute_approved = (
        auto_execute_approved
        and auto_execution_block is None
        and snapshot_quality_passed
        and position_reconciliation_passed
        and bool(auto_execution_live_gate.get("passed", True))
        and bool(auto_execution_account_gate.get("passed", True))
        and bool(auto_execution_equity_gate.get("passed", True))
    )
    paper_shadow_evidence = _paper_shadow_evidence(
        use_autopilot_policy=use_autopilot_policy,
        autopilot_policy=autopilot_policy,
        autopilot_preflight=autopilot_preflight,
        auto_execution=auto_execution,
        auto_execution_enabled=effective_auto_execute_approved,
        snapshot_quality_gate=snapshot_quality_gate,
        daily_loss_gate=daily_loss_gate,
        position_reconciliation_gate=position_reconciliation_gate,
        market_session=market_session,
    )
    consumed_events = state.consume_events(limit=1000) if consume_events else []
    pending_event_count = state.pending_event_count()
    finished_at = datetime.now(timezone.utc)
    top_recommendations = [
        {
            "id": rec.id,
            "ticker": rec.ticker,
            "source_snapshot_id": rec.source_snapshot_id,
            "strategy_config_id": rec.strategy_config_id,
            "confidence": rec.confidence,
            "entry_zone": rec.entry_zone,
            "stop_loss": rec.stop_loss,
            "tp1": rec.tp1,
            "tp2": rec.tp2,
        }
        for rec in output.result.recommendations[:top_n]
    ]
    sell_alerts = [
        {
            "ticker": alert.ticker,
            "level": alert.level.value,
            "reason_code": alert.reason_code,
            "current_price": alert.current_price,
            "message_cn": alert.message_cn,
            "suggested_action_cn": alert.suggested_action_cn,
        }
        for alert in alerts
    ]
    consumed_type_counts = _event_type_counts(consumed_events)
    metrics: dict[str, Any] = dict(state.metrics_store.dump())
    metrics["auto_approval"] = auto_approval
    metrics["auto_execution"] = auto_execution
    metrics["autopilot_policy"] = autopilot_policy
    metrics["autopilot_preflight"] = autopilot_preflight
    metrics["broker_order_sync"] = broker_order_sync
    metrics["broker_position_reconciliation"] = broker_position_reconciliation
    metrics["snapshot_quality_gate"] = snapshot_quality_gate
    metrics["daily_loss_gate"] = daily_loss_gate
    metrics["position_reconciliation_gate"] = position_reconciliation_gate
    metrics["paper_shadow_gate"] = paper_shadow_gate
    metrics["paper_shadow_evidence"] = paper_shadow_evidence
    run = SystemCycleRun(
        id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        source_snapshot_id=output.result.source_snapshot_id,
        strategy_config_id=output.result.strategy_config_id,
        recommendation_count=len(output.result.recommendations),
        sell_alert_count=len(alerts),
        consumed_event_count=len(consumed_events),
        pending_event_count=pending_event_count,
        auto_execution_enabled=effective_auto_execute_approved,
        top_recommendations=top_recommendations,
        sell_alerts=sell_alerts,
        consumed_event_type_counts=consumed_type_counts,
        metrics=metrics,
    )
    state.record_system_cycle_run(run)
    operational_health = HealthEvaluator(state).evaluate(external_probe=False)
    summary = {
        "system_cycle_run_id": run.id,
        "job": "system_cycle",
        "generated_at": finished_at.isoformat(),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": run.status,
        "source_snapshot_id": output.result.source_snapshot_id,
        "strategy_config_id": output.result.strategy_config_id,
        "recommendation_count": len(output.result.recommendations),
        "top_recommendations": top_recommendations,
        "sell_alert_count": len(alerts),
        "sell_alerts": sell_alerts,
        "auto_execution_enabled": effective_auto_execute_approved,
        "use_autopilot_policy": use_autopilot_policy,
        "autopilot_policy": autopilot_policy,
        "autopilot_preflight": autopilot_preflight,
        "broker_order_sync": broker_order_sync,
        "broker_position_reconciliation": broker_position_reconciliation,
        "snapshot_quality_gate": snapshot_quality_gate,
        "daily_loss_gate": daily_loss_gate,
        "position_reconciliation_gate": position_reconciliation_gate,
        "auto_approval": auto_approval,
        "auto_execution": auto_execution,
        "paper_shadow_evidence": paper_shadow_evidence,
        "consumed_event_count": len(consumed_events),
        "consumed_event_type_counts": consumed_type_counts,
        "pending_event_count": pending_event_count,
        "metrics": run.metrics,
        "operational_health": {
            "ready": operational_health["ready"],
            "trading_ready": operational_health["trading_ready"],
            "trading_blockers": operational_health["trading_blockers"],
            "active_alert_count": len(operational_health["active_alerts"]),
        },
    }
    logger.info(
        "system cycle completed",
        extra={
            "event": "system_cycle_completed",
            "run_id": run.id,
            "status": run.status,
            "recommendation_count": len(output.result.recommendations),
            "sell_alert_count": len(alerts),
        },
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def system_cycle_loop(
    *,
    interval_seconds: float = 300.0,
    max_cycles: int | None = None,
    stop_on_error: bool = False,
    max_consecutive_errors: int = 0,
    sleep_fn: Callable[[float], None] = time.sleep,
    **cycle_kwargs: Any,
) -> dict[str, Any]:
    state = get_app_state()
    started_at = datetime.now(timezone.utc)
    cycles: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    cycle_index = 0
    consecutive_error_count = 0
    kill_switch_activated = False
    stopped_reason: str | None = None

    while max_cycles is None or cycle_index < max_cycles:
        cycle_index += 1
        cycle_started_at = datetime.now(timezone.utc)
        try:
            summary = system_cycle(**cycle_kwargs)
            consecutive_error_count = 0
            cycles.append(
                {
                    "cycle": cycle_index,
                    "status": summary.get("status", "success"),
                    "system_cycle_run_id": summary.get("system_cycle_run_id"),
                    "recommendation_count": summary.get("recommendation_count", 0),
                    "sell_alert_count": summary.get("sell_alert_count", 0),
                    "auto_approval": summary.get("auto_approval", {}),
                    "auto_execution": summary.get("auto_execution", {}),
                    "broker_order_sync": summary.get("broker_order_sync", {}),
                    "broker_position_reconciliation": summary.get("broker_position_reconciliation", {}),
                    "pending_event_count": summary.get("pending_event_count", 0),
                }
            )
        except Exception as exc:
            cycle_finished_at = datetime.now(timezone.utc)
            error_run_id = uuid4().hex[:16]
            error_message = str(exc)
            state.record_system_cycle_run(
                SystemCycleRun(
                    id=error_run_id,
                    started_at=cycle_started_at,
                    finished_at=cycle_finished_at,
                    status="error",
                    auto_execution_enabled=False,
                    metrics={
                        "loop_error": {
                            "cycle": cycle_index,
                            "error_type": type(exc).__name__,
                            "error": error_message,
                        }
                    },
                    error_message=error_message,
                )
            )
            HealthEvaluator(state).evaluate(external_probe=False)
            consecutive_error_count += 1
            state.metrics_store.inc("system_cycle_errors")
            state.metrics_store.set_gauge("worker_consecutive_errors", consecutive_error_count)
            logger.exception(
                "system cycle failed",
                extra={
                    "event": "system_cycle_failed",
                    "run_id": error_run_id,
                    "cycle": cycle_index,
                    "consecutive_error_count": consecutive_error_count,
                },
            )
            error = {
                "cycle": cycle_index,
                "status": "error",
                "system_cycle_run_id": error_run_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
                "consecutive_error_count": consecutive_error_count,
            }
            errors.append(error)
            cycles.append(error)
            if max_consecutive_errors > 0 and consecutive_error_count >= max_consecutive_errors:
                reason = (
                    f"system_cycle_loop reached {consecutive_error_count} consecutive errors; "
                    f"last_error={type(exc).__name__}: {error_message}"
                )
                state.set_kill_switch(True, reason, "system_cycle_loop")
                kill_switch_activated = True
                stopped_reason = "max_consecutive_errors"
                break
            if stop_on_error:
                stopped_reason = "stop_on_error"
                break

        if max_cycles is not None and cycle_index >= max_cycles:
            stopped_reason = "max_cycles"
            break
        sleep_fn(max(0.0, interval_seconds))

    finished_at = datetime.now(timezone.utc)
    report = {
        "job": "system_cycle_loop",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "interval_seconds": interval_seconds,
        "max_cycles": max_cycles,
        "max_consecutive_errors": max_consecutive_errors,
        "cycle_count": len(cycles),
        "success_count": sum(1 for item in cycles if item.get("status") == "success"),
        "error_count": len(errors),
        "consecutive_error_count": consecutive_error_count,
        "kill_switch_activated": kill_switch_activated,
        "stopped_reason": stopped_reason,
        "last_system_cycle_run_id": next(
            (item.get("system_cycle_run_id") for item in reversed(cycles) if item.get("system_cycle_run_id")),
            None,
        ),
        "cycles": cycles,
        "errors": errors,
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return report


def main() -> None:
    from apps.worker.cli import main as run_cli

    run_cli()


if __name__ == "__main__":
    main()
