from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from apps.api.dependencies import get_app_state
from domain.entities.models import (
    ApprovalDecision,
    AutopilotPolicy,
    BacktestRunRequest,
    Direction,
    ManualSellRequest,
    OrderExecutionMode,
    PaperOrderRequest,
    PublicationConfig,
    Recommendation,
    ResearchRunRequest,
    RiskPolicy,
    SellAlert,
    RunType,
    SnapshotMode,
    SystemCycleRun,
)
from domain.policies.approval import ApprovalDecisionRequest
from infra.queue.events import EventType


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
    metrics = state.metrics_store.dump()
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


def _auto_execution_mode(mode: str) -> tuple[OrderExecutionMode, bool]:
    normalized = mode.lower()
    if normalized == "paper":
        return OrderExecutionMode.PAPER, False
    if normalized == "live_dry_run":
        return OrderExecutionMode.LIVE, True
    raise ValueError("auto execution mode must be paper or live_dry_run")


def _auto_approve_cycle(
    *,
    recommendations: list[Recommendation],
    max_auto_approvals: int,
    min_confidence: float,
    min_composite: float,
) -> dict[str, Any]:
    state = get_app_state()
    actions: list[dict[str, Any]] = []
    approval_count = 0
    open_tickers = {holding.ticker.upper() for holding in state.list_open_holdings()}

    if state.kill_switch.enabled:
        actions.append(
            {
                "action": "approve_recommendation",
                "status": "skipped",
                "reason": "kill_switch_enabled",
                "message_cn": f"Kill switch 已开启：{state.kill_switch.reason or '未提供原因'}。",
            }
        )
        return _auto_approval_report(enabled=True, actions=actions)

    for recommendation in recommendations:
        ticker = recommendation.ticker.upper()
        composite = float(recommendation.score_vector.get("composite", 0.0))
        if approval_count >= max_auto_approvals:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "max_auto_approvals_reached",
                }
            )
            continue
        if state.get_latest_approval(recommendation.id) is not None:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "already_decided",
                }
            )
            continue
        if ticker in open_tickers:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "already_open_holding",
                }
            )
            continue
        if recommendation.direction != Direction.BUY:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "unsupported_direction",
                }
            )
            continue
        if recommendation.confidence < min_confidence:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "below_min_confidence",
                    "confidence": recommendation.confidence,
                    "min_confidence": min_confidence,
                }
            )
            continue
        if composite < min_composite:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "below_min_composite",
                    "composite": composite,
                    "min_composite": min_composite,
                }
            )
            continue

        approval = state.decide_recommendation(
            ApprovalDecisionRequest(
                recommendation_id=recommendation.id,
                decision=ApprovalDecision.APPROVED.value,
                approver="system_cycle:auto_approval",
                notes=(
                    f"auto-approved confidence={recommendation.confidence:.4f}, "
                    f"composite={composite:.4f}"
                ),
            )
        )
        approval_count += 1
        actions.append(
            {
                "action": "approve_recommendation",
                "status": "approved",
                "ticker": recommendation.ticker,
                "recommendation_id": recommendation.id,
                "decision_id": approval.decision_id,
                "confidence": recommendation.confidence,
                "composite": composite,
            }
        )

    return _auto_approval_report(enabled=True, actions=actions)


def _auto_approval_report(*, enabled: bool, actions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "action_count": len(actions),
        "approved_count": sum(1 for item in actions if item.get("status") == "approved"),
        "skipped_count": sum(1 for item in actions if item.get("status") == "skipped"),
        "error_count": sum(1 for item in actions if item.get("status") == "error"),
        "actions": actions,
    }


def _auto_execute_cycle(
    *,
    recommendations: list[Recommendation],
    alerts: list[SellAlert],
    run_id: str,
    execution_mode: str,
    max_auto_buys: int,
    max_auto_sells: int,
    account_equity: float,
    risk_per_trade_pct: float,
    max_position_pct: float,
    max_gross_exposure_pct: float,
    max_sector_exposure_pct: float,
) -> dict[str, Any]:
    state = get_app_state()
    order_mode, dry_run = _auto_execution_mode(execution_mode)
    actions: list[dict[str, Any]] = []
    sell_blocked_tickers: set[str] = set()

    if state.kill_switch.enabled:
        actions.append(
            {
                "action": "auto_execution",
                "status": "skipped",
                "reason": "kill_switch_enabled",
                "message_cn": f"Kill switch 已开启：{state.kill_switch.reason or '未提供原因'}。",
            }
        )
        return _auto_execution_report(enabled=True, mode=execution_mode, actions=actions)

    for alert in alerts:
        if len([item for item in actions if item.get("action") == "sell_alert"]) >= max_auto_sells:
            actions.append(
                {
                    "action": "sell_alert",
                    "status": "skipped",
                    "ticker": alert.ticker,
                    "reason": "max_auto_sells_reached",
                }
            )
            continue
        try:
            holding = state.holding_watch_repo.get(alert.ticker)
            if holding is None:
                actions.append(
                    {
                        "action": "sell_alert",
                        "status": "skipped",
                        "ticker": alert.ticker,
                        "reason": "open_holding_not_found",
                    }
                )
                continue
            default_qty, default_action_cn = state._alert_default_sell_qty(alert, holding)
            execution = state.sell_holding(
                ticker=alert.ticker,
                request=ManualSellRequest(
                    qty=default_qty,
                    sell_price=alert.current_price,
                    reason=f"system_cycle:{run_id}:alert:{alert.reason_code}",
                    execution_mode=order_mode,
                    dry_run=dry_run,
                    confirm_live=False,
                ),
            )
            sell_blocked_tickers.add(alert.ticker.upper())
            actions.append(
                {
                    "action": "sell_alert",
                    "status": "executed",
                    "ticker": alert.ticker,
                    "reason_code": alert.reason_code,
                    "execution_mode": execution.execution_mode.value,
                    "dry_run": execution.dry_run,
                    "sell_execution_id": execution.sell_execution_id,
                    "sold_qty": execution.sold_qty,
                    "sell_price": execution.sell_price,
                    "remaining_qty": execution.remaining_qty,
                    "message_cn": execution.message_cn,
                    "default_action_cn": default_action_cn,
                }
            )
        except Exception as exc:
            actions.append(
                {
                    "action": "sell_alert",
                    "status": "error",
                    "ticker": alert.ticker,
                    "reason_code": alert.reason_code,
                    "error": str(exc),
                }
            )

    open_tickers = {holding.ticker.upper() for holding in state.list_open_holdings()}
    buy_count = 0
    for recommendation in recommendations:
        ticker = recommendation.ticker.upper()
        if buy_count >= max_auto_buys:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "max_auto_buys_reached",
                }
            )
            continue
        if ticker in sell_blocked_tickers:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "sell_alert_same_cycle",
                }
            )
            continue
        if ticker in open_tickers:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "already_open_holding",
                }
            )
            continue
        if recommendation.direction != Direction.BUY:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "unsupported_direction",
                }
            )
            continue
        approval = state.get_latest_approval(recommendation.id)
        if approval is None or approval.decision != ApprovalDecision.APPROVED:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "approval_required",
                }
            )
            continue

        try:
            probe_request = PaperOrderRequest(
                recommendation_id=recommendation.id,
                side=Direction.BUY,
                qty=1,
                limit_price=recommendation.entry_zone_high,
                execution_mode=order_mode,
                dry_run=dry_run,
                confirm_live=False,
                account_equity=account_equity,
                risk_per_trade_pct=risk_per_trade_pct,
                max_position_pct=max_position_pct,
                max_gross_exposure_pct=max_gross_exposure_pct,
                max_sector_exposure_pct=max_sector_exposure_pct,
            )
            probe_plan = state.build_paper_order_risk_plan(
                recommendation=recommendation,
                request=probe_request,
            )
            qty = int(probe_plan.recommended_qty)
            if qty <= 0:
                actions.append(
                    {
                        "action": "buy_recommendation",
                        "status": "skipped",
                        "ticker": recommendation.ticker,
                        "recommendation_id": recommendation.id,
                        "reason": "risk_plan_zero_qty",
                        "message_cn": probe_plan.message_cn,
                    }
                )
                continue
            request = probe_request.model_copy(update={"qty": float(qty)})
            risk_plan = state.build_paper_order_risk_plan(recommendation=recommendation, request=request)
            if not risk_plan.is_within_limits:
                actions.append(
                    {
                        "action": "buy_recommendation",
                        "status": "skipped",
                        "ticker": recommendation.ticker,
                        "recommendation_id": recommendation.id,
                        "reason": "risk_plan_violation",
                        "violations": risk_plan.violations,
                        "message_cn": risk_plan.message_cn,
                    }
                )
                continue
            order, updated_positions = state.execution_router.submit(
                recommendation=recommendation,
                request=request,
                positions=state.positions,
            )
            state.positions = updated_positions
            state.record_paper_order(order, recommendation=recommendation)
            open_tickers.add(ticker)
            buy_count += 1
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "executed",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "execution_mode": order.execution_mode.value,
                    "dry_run": order.dry_run,
                    "order_id": order.id,
                    "qty": order.qty,
                    "limit_price": order.limit_price,
                    "simulated_fill_price": order.simulated_fill_price,
                    "message_cn": risk_plan.message_cn,
                }
            )
        except Exception as exc:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "error",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "error": str(exc),
                }
            )

    return _auto_execution_report(enabled=True, mode=execution_mode, actions=actions)


def _auto_execution_report(*, enabled: bool, mode: str, actions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "mode": mode,
        "action_count": len(actions),
        "buy_order_count": sum(
            1
            for item in actions
            if item.get("action") == "buy_recommendation" and item.get("status") == "executed"
        ),
        "sell_order_count": sum(
            1
            for item in actions
            if item.get("action") == "sell_alert" and item.get("status") == "executed"
        ),
        "skipped_count": sum(1 for item in actions if item.get("status") == "skipped"),
        "error_count": sum(1 for item in actions if item.get("status") == "error"),
        "actions": actions,
    }


def _apply_autopilot_policy(
    *,
    policy: AutopilotPolicy,
    current: dict[str, Any],
) -> dict[str, Any]:
    updates = dict(current)
    updates["autopilot_policy"] = policy.model_dump(mode="json")
    if not policy.enabled:
        updates["auto_approve_recommendations"] = False
        updates["auto_execute_approved"] = False
        updates["auto_execution_mode"] = policy.auto_execution_mode.value
        return updates

    updates.update(
        {
            "auto_approve_recommendations": policy.auto_approve_recommendations,
            "auto_execute_approved": policy.auto_execute_approved,
            "auto_approve_min_confidence": policy.auto_approve_min_confidence,
            "auto_approve_min_composite": policy.auto_approve_min_composite,
            "max_auto_approvals": policy.max_auto_approvals,
            "auto_execution_mode": policy.auto_execution_mode.value,
            "max_auto_buys": policy.max_auto_buys,
            "max_auto_sells": policy.max_auto_sells,
            "account_equity": policy.account_equity,
            "risk_per_trade_pct": policy.risk_per_trade_pct,
            "max_position_pct": policy.max_position_pct,
            "max_gross_exposure_pct": policy.max_gross_exposure_pct,
            "max_sector_exposure_pct": policy.max_sector_exposure_pct,
        }
    )
    return updates


def system_cycle(
    top_n: int = 8,
    min_confidence: float | None = None,
    consume_events: bool = False,
    as_of: datetime | None = None,
    use_autopilot_policy: bool = False,
    auto_execute_approved: bool = False,
    auto_approve_recommendations: bool = False,
    auto_approve_min_confidence: float = 0.72,
    auto_approve_min_composite: float = 0.0,
    max_auto_approvals: int = 1,
    auto_execution_mode: str = "paper",
    max_auto_buys: int = 1,
    max_auto_sells: int = 10,
    account_equity: float = 100_000.0,
    risk_per_trade_pct: float = 0.01,
    max_position_pct: float = 0.10,
    max_gross_exposure_pct: float = 1.0,
    max_sector_exposure_pct: float = 0.30,
) -> dict[str, Any]:
    state = get_app_state()
    autopilot_policy: dict[str, Any] | None = None
    if use_autopilot_policy:
        policy_values = _apply_autopilot_policy(
            policy=state.get_autopilot_policy(),
            current={
                "auto_execute_approved": auto_execute_approved,
                "auto_approve_recommendations": auto_approve_recommendations,
                "auto_approve_min_confidence": auto_approve_min_confidence,
                "auto_approve_min_composite": auto_approve_min_composite,
                "max_auto_approvals": max_auto_approvals,
                "auto_execution_mode": auto_execution_mode,
                "max_auto_buys": max_auto_buys,
                "max_auto_sells": max_auto_sells,
                "account_equity": account_equity,
                "risk_per_trade_pct": risk_per_trade_pct,
                "max_position_pct": max_position_pct,
                "max_gross_exposure_pct": max_gross_exposure_pct,
                "max_sector_exposure_pct": max_sector_exposure_pct,
            },
        )
        autopilot_policy = policy_values.pop("autopilot_policy")
        auto_execute_approved = policy_values["auto_execute_approved"]
        auto_approve_recommendations = policy_values["auto_approve_recommendations"]
        auto_approve_min_confidence = policy_values["auto_approve_min_confidence"]
        auto_approve_min_composite = policy_values["auto_approve_min_composite"]
        max_auto_approvals = policy_values["max_auto_approvals"]
        auto_execution_mode = policy_values["auto_execution_mode"]
        max_auto_buys = policy_values["max_auto_buys"]
        max_auto_sells = policy_values["max_auto_sells"]
        account_equity = policy_values["account_equity"]
        risk_per_trade_pct = policy_values["risk_per_trade_pct"]
        max_position_pct = policy_values["max_position_pct"]
        max_gross_exposure_pct = policy_values["max_gross_exposure_pct"]
        max_sector_exposure_pct = policy_values["max_sector_exposure_pct"]

    started_at = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    run_id = uuid4().hex[:16]
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
    auto_approval = (
        _auto_approve_cycle(
            recommendations=output.result.recommendations[:top_n],
            max_auto_approvals=max_auto_approvals,
            min_confidence=auto_approve_min_confidence,
            min_composite=auto_approve_min_composite,
        )
        if auto_approve_recommendations
        else _auto_approval_report(enabled=False, actions=[])
    )
    alerts = state.monitor_sell_alerts(as_of=started_at)
    state.record_sell_alert_audits(alerts, monitor_run_id=run_id)
    auto_execution = (
        _auto_execute_cycle(
            recommendations=output.result.recommendations[:top_n],
            alerts=alerts,
            run_id=run_id,
            execution_mode=auto_execution_mode,
            max_auto_buys=max_auto_buys,
            max_auto_sells=max_auto_sells,
            account_equity=account_equity,
            risk_per_trade_pct=risk_per_trade_pct,
            max_position_pct=max_position_pct,
            max_gross_exposure_pct=max_gross_exposure_pct,
            max_sector_exposure_pct=max_sector_exposure_pct,
        )
        if auto_execute_approved
        else _auto_execution_report(enabled=False, mode=auto_execution_mode, actions=[])
    )
    consumed_events = state.consume_events(limit=1000) if consume_events else []
    pending_event_count = state.pending_event_count()
    finished_at = datetime.now(timezone.utc)
    top_recommendations = [
        {
            "id": rec.id,
            "ticker": rec.ticker,
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
    metrics = state.metrics_store.dump()
    metrics["auto_approval"] = auto_approval
    metrics["auto_execution"] = auto_execution
    metrics["autopilot_policy"] = autopilot_policy
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
        auto_execution_enabled=auto_execute_approved,
        top_recommendations=top_recommendations,
        sell_alerts=sell_alerts,
        consumed_event_type_counts=consumed_type_counts,
        metrics=metrics,
    )
    state.record_system_cycle_run(run)
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
        "auto_execution_enabled": auto_execute_approved,
        "use_autopilot_policy": use_autopilot_policy,
        "autopilot_policy": autopilot_policy,
        "auto_approval": auto_approval,
        "auto_execution": auto_execution,
        "consumed_event_count": len(consumed_events),
        "consumed_event_type_counts": consumed_type_counts,
        "pending_event_count": pending_event_count,
        "metrics": run.metrics,
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def system_cycle_loop(
    *,
    interval_seconds: float = 300.0,
    max_cycles: int | None = None,
    stop_on_error: bool = False,
    sleep_fn: Callable[[float], None] = time.sleep,
    **cycle_kwargs: Any,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    cycles: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    cycle_index = 0

    while max_cycles is None or cycle_index < max_cycles:
        cycle_index += 1
        try:
            summary = system_cycle(**cycle_kwargs)
            cycles.append(
                {
                    "cycle": cycle_index,
                    "status": summary.get("status", "success"),
                    "system_cycle_run_id": summary.get("system_cycle_run_id"),
                    "recommendation_count": summary.get("recommendation_count", 0),
                    "sell_alert_count": summary.get("sell_alert_count", 0),
                    "auto_approval": summary.get("auto_approval", {}),
                    "auto_execution": summary.get("auto_execution", {}),
                    "pending_event_count": summary.get("pending_event_count", 0),
                }
            )
        except Exception as exc:
            error = {
                "cycle": cycle_index,
                "status": "error",
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
            errors.append(error)
            cycles.append(error)
            if stop_on_error:
                break

        if max_cycles is not None and cycle_index >= max_cycles:
            break
        sleep_fn(max(0.0, interval_seconds))

    finished_at = datetime.now(timezone.utc)
    report = {
        "job": "system_cycle_loop",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "interval_seconds": interval_seconds,
        "max_cycles": max_cycles,
        "cycle_count": len(cycles),
        "success_count": sum(1 for item in cycles if item.get("status") == "success"),
        "error_count": len(errors),
        "last_system_cycle_run_id": next(
            (
                item.get("system_cycle_run_id")
                for item in reversed(cycles)
                if item.get("system_cycle_run_id")
            ),
            None,
        ),
        "cycles": cycles,
        "errors": errors,
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Quant agent worker jobs")
    parser.add_argument(
        "job",
        choices=[
            "pre_market_ingestion",
            "intraday_refresh",
            "end_of_day_reconciliation",
            "nightly_backtest_batch",
            "daily_metrics_aggregation",
            "process_event_queue",
            "monitor_positions_alerts",
            "system_cycle",
            "system_cycle_loop",
        ],
    )
    parser.add_argument("--top-n", type=int, default=8, help="system_cycle recommendation publication size")
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="optional system_cycle risk-policy min confidence override",
    )
    parser.add_argument(
        "--consume-events",
        action="store_true",
        help="system_cycle should consume pending events after publishing its summary inputs",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="optional ISO timestamp for deterministic system_cycle replay, defaults to now",
    )
    parser.add_argument(
        "--use-autopilot-policy",
        action="store_true",
        help="load the latest persisted autopilot policy and use it for auto approval/execution controls",
    )
    parser.add_argument(
        "--auto-execute-approved",
        action="store_true",
        help="system_cycle should auto-route approved buys and active sell alerts through existing execution gates",
    )
    parser.add_argument(
        "--auto-approve-recommendations",
        action="store_true",
        help="system_cycle should auto-approve recommendations that pass the configured thresholds",
    )
    parser.add_argument(
        "--auto-approve-min-confidence",
        type=float,
        default=0.72,
        help="minimum recommendation confidence for automatic approval",
    )
    parser.add_argument(
        "--auto-approve-min-composite",
        type=float,
        default=0.0,
        help="minimum composite score for automatic approval",
    )
    parser.add_argument("--max-auto-approvals", type=int, default=1, help="maximum auto approvals per cycle")
    parser.add_argument(
        "--auto-execution-mode",
        choices=["paper", "live_dry_run"],
        default="paper",
        help="execution mode for automatic actions",
    )
    parser.add_argument("--max-auto-buys", type=int, default=1, help="maximum approved buys per cycle")
    parser.add_argument("--max-auto-sells", type=int, default=10, help="maximum sell alerts to execute per cycle")
    parser.add_argument("--account-equity", type=float, default=100_000.0, help="account equity for auto buy risk sizing")
    parser.add_argument(
        "--risk-per-trade-pct",
        type=float,
        default=0.01,
        help="fractional per-trade risk budget for auto buy sizing",
    )
    parser.add_argument(
        "--max-position-pct",
        type=float,
        default=0.10,
        help="fractional per-ticker position cap for auto buy sizing",
    )
    parser.add_argument(
        "--max-gross-exposure-pct",
        type=float,
        default=1.0,
        help="fractional gross exposure cap for auto buy sizing",
    )
    parser.add_argument(
        "--max-sector-exposure-pct",
        type=float,
        default=0.30,
        help="fractional sector exposure cap for auto buy sizing",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=300.0,
        help="system_cycle_loop sleep interval between cycles",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="optional system_cycle_loop cycle cap; omit for continuous operation",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="system_cycle_loop should stop after the first cycle error",
    )
    args = parser.parse_args()
    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    if as_of is not None and as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

    dispatch = {
        "pre_market_ingestion": pre_market_ingestion,
        "intraday_refresh": intraday_refresh,
        "end_of_day_reconciliation": end_of_day_reconciliation,
        "nightly_backtest_batch": nightly_backtest_batch,
        "daily_metrics_aggregation": daily_metrics_aggregation,
        "process_event_queue": process_event_queue,
        "monitor_positions_alerts": monitor_positions_alerts,
        "system_cycle": lambda: system_cycle(
            top_n=args.top_n,
            min_confidence=args.min_confidence,
            consume_events=args.consume_events,
            as_of=as_of,
            use_autopilot_policy=args.use_autopilot_policy,
            auto_execute_approved=args.auto_execute_approved,
            auto_approve_recommendations=args.auto_approve_recommendations,
            auto_approve_min_confidence=args.auto_approve_min_confidence,
            auto_approve_min_composite=args.auto_approve_min_composite,
            max_auto_approvals=args.max_auto_approvals,
            auto_execution_mode=args.auto_execution_mode,
            max_auto_buys=args.max_auto_buys,
            max_auto_sells=args.max_auto_sells,
            account_equity=args.account_equity,
            risk_per_trade_pct=args.risk_per_trade_pct,
            max_position_pct=args.max_position_pct,
            max_gross_exposure_pct=args.max_gross_exposure_pct,
            max_sector_exposure_pct=args.max_sector_exposure_pct,
        ),
        "system_cycle_loop": lambda: system_cycle_loop(
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
            stop_on_error=args.stop_on_error,
            top_n=args.top_n,
            min_confidence=args.min_confidence,
            consume_events=args.consume_events,
            as_of=as_of,
            use_autopilot_policy=args.use_autopilot_policy,
            auto_execute_approved=args.auto_execute_approved,
            auto_approve_recommendations=args.auto_approve_recommendations,
            auto_approve_min_confidence=args.auto_approve_min_confidence,
            auto_approve_min_composite=args.auto_approve_min_composite,
            max_auto_approvals=args.max_auto_approvals,
            auto_execution_mode=args.auto_execution_mode,
            max_auto_buys=args.max_auto_buys,
            max_auto_sells=args.max_auto_sells,
            account_equity=args.account_equity,
            risk_per_trade_pct=args.risk_per_trade_pct,
            max_position_pct=args.max_position_pct,
            max_gross_exposure_pct=args.max_gross_exposure_pct,
            max_sector_exposure_pct=args.max_sector_exposure_pct,
        ),
    }
    dispatch[args.job]()


if __name__ == "__main__":
    main()
