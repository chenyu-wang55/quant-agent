from __future__ import annotations

from datetime import datetime
from typing import Any

from apps.api.dependencies import get_app_state
from apps.worker.automation_helpers import (
    _auto_buy_price_drift_gate,
    _auto_execution_report,
    _post_sell_control_adjustment,
)
from apps.worker.broker_cycles import (
    _auto_live_account_equity_gate,
    _auto_live_broker_account_gate,
    _auto_live_buying_power_gate,
)
from domain.entities.models import (
    ApprovalDecision,
    Direction,
    ManualSellRequest,
    OrderExecutionMode,
    PaperOrderRequest,
    Recommendation,
    SellAlert,
)
from domain.policies.approval import ApprovalDecisionRequest
from infra.config import env_flag


def _truthy_env_flag(name: str) -> bool:
    return env_flag(name)


def _auto_execution_mode(mode: str) -> tuple[OrderExecutionMode, bool, bool]:
    normalized = mode.lower()
    if normalized == "paper":
        return OrderExecutionMode.PAPER, False, False
    if normalized == "live_dry_run":
        return OrderExecutionMode.LIVE, True, False
    if normalized == "live":
        return OrderExecutionMode.LIVE, False, True
    raise ValueError("auto execution mode must be paper, live_dry_run, or live")


def _auto_approve_cycle(
    *,
    recommendations: list[Recommendation],
    max_auto_approvals: int,
    min_confidence: float,
    min_composite: float,
    order_dedupe_minutes: int,
    as_of: datetime,
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
        recent_buy_order_gate = state.get_recent_buy_order_gate(
            ticker=recommendation.ticker,
            recommendation_id=recommendation.id,
            order_dedupe_minutes=order_dedupe_minutes,
            as_of=as_of,
        )
        if not recent_buy_order_gate["passed"]:
            actions.append(
                {
                    "action": "approve_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "recent_buy_order_gate_failed",
                    "recent_buy_order_gate": recent_buy_order_gate,
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
                notes=(f"auto-approved confidence={recommendation.confidence:.4f}, composite={composite:.4f}"),
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
    order_dedupe_minutes: int,
    sell_alert_cooldown_minutes: int,
    rebuy_cooldown_minutes: int,
    as_of: datetime,
    account_equity: float,
    max_open_risk_pct: float,
    max_daily_realized_loss_pct: float,
    max_auto_buy_price_drift_pct: float,
    allow_auto_live_execution: bool | None,
    risk_per_trade_pct: float,
    max_position_pct: float,
    max_gross_exposure_pct: float,
    max_sector_exposure_pct: float,
) -> dict[str, Any]:
    state = get_app_state()
    order_mode, dry_run, confirm_live = _auto_execution_mode(execution_mode)
    live_allowed = (
        _truthy_env_flag("QUANT_ALLOW_AUTOPILOT_LIVE")
        if allow_auto_live_execution is None
        else allow_auto_live_execution
    )
    live_execution_requested = order_mode == OrderExecutionMode.LIVE and not dry_run
    live_execution_gate = {
        "passed": (not live_execution_requested) or live_allowed,
        "requested": live_execution_requested,
        "allow_auto_live_execution": live_allowed,
        "reason": None if (not live_execution_requested or live_allowed) else "auto_live_execution_not_allowed",
        "required_runtime_flag": "--allow-auto-live-execution",
        "required_env_var": "QUANT_ALLOW_AUTOPILOT_LIVE=1",
    }
    actions: list[dict[str, Any]] = []
    sell_blocked_tickers: set[str] = set()
    broker_account_gate: dict[str, Any] | None = None
    account_equity_gate: dict[str, Any] | None = None

    if state.kill_switch.enabled:
        actions.append(
            {
                "action": "auto_execution",
                "status": "skipped",
                "reason": "kill_switch_enabled",
                "message_cn": f"Kill switch 已开启：{state.kill_switch.reason or '未提供原因'}。",
            }
        )
        return _auto_execution_report(
            enabled=True,
            mode=execution_mode,
            actions=actions,
            live_execution_gate=live_execution_gate,
        )
    if not live_execution_gate["passed"]:
        actions.append(
            {
                "action": "auto_execution",
                "status": "skipped",
                "reason": "auto_live_execution_not_allowed",
                "live_execution_gate": live_execution_gate,
                "message_cn": "自动实盘执行需要显式传入 allow_auto_live_execution 或设置 QUANT_ALLOW_AUTOPILOT_LIVE=1。",
            }
        )
        return _auto_execution_report(
            enabled=True,
            mode=execution_mode,
            actions=actions,
            live_execution_gate=live_execution_gate,
        )

    broker_account_gate = _auto_live_broker_account_gate(
        required=live_execution_requested,
        checked_at=as_of,
    )
    if live_execution_requested and not broker_account_gate["passed"]:
        actions.append(
            {
                "action": "auto_execution",
                "status": "skipped",
                "reason": "broker_account_gate_failed",
                "broker_account_gate": broker_account_gate,
                "message_cn": "自动实盘执行前的 broker 账户快照门禁未通过，本轮不会提交 broker 订单。",
            }
        )
        return _auto_execution_report(
            enabled=True,
            mode=execution_mode,
            actions=actions,
            live_execution_gate=live_execution_gate,
            broker_account_gate=broker_account_gate,
        )

    account_equity_gate = _auto_live_account_equity_gate(
        broker_account_gate=broker_account_gate,
        configured_account_equity=account_equity,
    )
    effective_account_equity = float(account_equity_gate.get("effective_account_equity") or account_equity)

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
            pending_sell_order_gate = state.get_pending_sell_order_gate(
                ticker=alert.ticker,
                reason_code=alert.reason_code,
            )
            if not pending_sell_order_gate["passed"]:
                actions.append(
                    {
                        "action": "sell_alert",
                        "status": "skipped",
                        "ticker": alert.ticker,
                        "reason_code": alert.reason_code,
                        "reason": "pending_sell_order_gate_failed",
                        "pending_sell_order_gate": pending_sell_order_gate,
                    }
                )
                continue
            cooldown = state.get_sell_alert_cooldown(
                ticker=alert.ticker,
                reason_code=alert.reason_code,
                cooldown_minutes=sell_alert_cooldown_minutes,
                as_of=as_of,
            )
            if cooldown.get("active"):
                actions.append(
                    {
                        "action": "sell_alert",
                        "status": "skipped",
                        "ticker": alert.ticker,
                        "reason_code": alert.reason_code,
                        "reason": "sell_alert_cooldown_active",
                        "cooldown": cooldown,
                    }
                )
                continue
            default_qty, default_action_cn = state._alert_default_sell_qty(alert, holding)
            execution = state.sell_holding(
                ticker=alert.ticker,
                request=ManualSellRequest(
                    idempotency_key=f"system-cycle:{run_id}:sell:{alert.ticker}:{alert.reason_code}",
                    qty=default_qty,
                    sell_price=alert.current_price,
                    reason=f"system_cycle:{run_id}:alert:{alert.reason_code}",
                    execution_mode=order_mode,
                    dry_run=dry_run,
                    confirm_live=confirm_live,
                ),
            )
            control_adjustment = _post_sell_control_adjustment(
                alert=alert,
                execution=execution,
                run_id=run_id,
            )
            sell_blocked_tickers.add(alert.ticker.upper())
            actions.append(
                {
                    "action": "sell_alert",
                    "status": "executed",
                    "ticker": alert.ticker,
                    "reason_code": alert.reason_code,
                    "source_recommendation_id": alert.source_recommendation_id,
                    "source_snapshot_id": alert.source_snapshot_id,
                    "strategy_config_id": alert.strategy_config_id,
                    "execution_mode": execution.execution_mode.value,
                    "dry_run": execution.dry_run,
                    "sell_execution_id": execution.sell_execution_id,
                    "sold_qty": execution.sold_qty,
                    "sell_price": execution.sell_price,
                    "remaining_qty": execution.remaining_qty,
                    "message_cn": execution.message_cn,
                    "default_action_cn": default_action_cn,
                    "control_adjustment": control_adjustment,
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

    portfolio_summary = state.get_portfolio_summary(as_of=as_of)
    open_risk_pct = (
        round(portfolio_summary.open_risk_to_stop / effective_account_equity, 6)
        if effective_account_equity > 0
        else 1.0
    )
    portfolio_risk_gate = {
        "passed": open_risk_pct <= max_open_risk_pct,
        "reason": None if open_risk_pct <= max_open_risk_pct else "open_risk_above_policy_limit",
        "open_risk_to_stop": portfolio_summary.open_risk_to_stop,
        "open_risk_pct": open_risk_pct,
        "max_open_risk_pct": max_open_risk_pct,
        "account_equity": effective_account_equity,
        "configured_account_equity": account_equity,
        "account_equity_gate": account_equity_gate,
        "open_holding_count": portfolio_summary.open_holding_count,
    }
    daily_loss_gate = state.get_autopilot_daily_loss_gate(
        as_of=as_of,
        account_equity=effective_account_equity,
        max_daily_realized_loss_pct=max_daily_realized_loss_pct,
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
        if live_execution_requested and account_equity_gate and not account_equity_gate["passed"]:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "broker_account_equity_gate_failed",
                    "account_equity_gate": account_equity_gate,
                    "broker_account_gate": broker_account_gate,
                    "message_cn": "Broker account equity/portfolio value 不可用，自动实盘买入已跳过。",
                }
            )
            continue
        if not daily_loss_gate["passed"]:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "daily_loss_gate_failed",
                    "daily_loss_gate": daily_loss_gate,
                }
            )
            continue
        if not portfolio_risk_gate["passed"]:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "portfolio_open_risk_gate_failed",
                    "portfolio_risk_gate": portfolio_risk_gate,
                }
            )
            continue
        pending_buy_order_gate = state.get_pending_buy_order_gate(
            ticker=recommendation.ticker,
            recommendation_id=recommendation.id,
        )
        if not pending_buy_order_gate["passed"]:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "pending_buy_order_gate_failed",
                    "pending_buy_order_gate": pending_buy_order_gate,
                }
            )
            continue
        recent_buy_order_gate = state.get_recent_buy_order_gate(
            ticker=recommendation.ticker,
            recommendation_id=recommendation.id,
            order_dedupe_minutes=order_dedupe_minutes,
            as_of=as_of,
        )
        if not recent_buy_order_gate["passed"]:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "recent_buy_order_gate_failed",
                    "recent_buy_order_gate": recent_buy_order_gate,
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
        cooldown = state.get_rebuy_cooldown(
            ticker=recommendation.ticker,
            cooldown_minutes=rebuy_cooldown_minutes,
            as_of=as_of,
        )
        if cooldown.get("active"):
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "rebuy_cooldown_active",
                    "cooldown": cooldown,
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
        price_drift_gate = _auto_buy_price_drift_gate(
            recommendation=recommendation,
            max_drift_pct=max_auto_buy_price_drift_pct,
            as_of=as_of,
        )
        if not price_drift_gate["passed"]:
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "skipped",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "reason": "price_drift_gate_failed",
                    "price_drift_gate": price_drift_gate,
                }
            )
            continue

        try:
            probe_request = PaperOrderRequest(
                recommendation_id=recommendation.id,
                idempotency_key=f"system-cycle:{run_id}:buy:{recommendation.id}",
                side=Direction.BUY,
                qty=1,
                limit_price=recommendation.entry_zone_high,
                execution_mode=order_mode,
                dry_run=dry_run,
                confirm_live=confirm_live,
                account_equity=effective_account_equity,
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
            buying_power_gate = _auto_live_buying_power_gate(
                broker_account_gate=broker_account_gate,
                requested_notional=risk_plan.requested_notional,
            )
            if not buying_power_gate["passed"]:
                actions.append(
                    {
                        "action": "buy_recommendation",
                        "status": "skipped",
                        "ticker": recommendation.ticker,
                        "recommendation_id": recommendation.id,
                        "reason": buying_power_gate["reason"],
                        "broker_buying_power_gate": buying_power_gate,
                        "broker_account_gate": broker_account_gate,
                        "message_cn": "Broker buying power 不足或不可用，自动实盘买入已跳过。",
                    }
                )
                continue
            order = state.submit_order(
                recommendation=recommendation,
                request=request,
            )
            open_tickers.add(ticker)
            buy_count += 1
            actions.append(
                {
                    "action": "buy_recommendation",
                    "status": "executed",
                    "ticker": recommendation.ticker,
                    "recommendation_id": recommendation.id,
                    "source_snapshot_id": recommendation.source_snapshot_id,
                    "strategy_config_id": recommendation.strategy_config_id,
                    "execution_mode": order.execution_mode.value,
                    "dry_run": order.dry_run,
                    "order_id": order.id,
                    "qty": order.qty,
                    "limit_price": order.limit_price,
                    "simulated_fill_price": order.simulated_fill_price,
                    "account_equity_gate": account_equity_gate,
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

    return _auto_execution_report(
        enabled=True,
        mode=execution_mode,
        actions=actions,
        portfolio_risk_gate=portfolio_risk_gate,
        daily_loss_gate=daily_loss_gate,
        live_execution_gate=live_execution_gate,
        broker_account_gate=broker_account_gate,
        account_equity_gate=account_equity_gate,
    )
