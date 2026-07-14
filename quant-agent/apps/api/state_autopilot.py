from __future__ import annotations

import hashlib
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from domain.entities.models import (
    AlertExecutionResult,
    AlertSellRequest,
    ApprovalDecision,
    AutoExecutionMode,
    AutopilotPolicy,
    AutopilotPreflight,
    AutopilotPreflightCheck,
    BacktestRunResult,
    BrokerOrderStatusSnapshot,
    BrokerOrderSyncItemResult,
    BrokerOrderSyncRequest,
    BrokerOrderSyncResult,
    BrokerOrderSyncStatus,
    Direction,
    FeatureSnapshot,
    HoldingControlAudit,
    HoldingControlUpdateRequest,
    HoldingControlUpdateResult,
    HoldingStatus,
    HoldingWatch,
    KillSwitchState,
    ManualBuyRequest,
    ManualSellRequest,
    MarketSessionStatus,
    OperationAction,
    OperationControlCenter,
    OperationRecommendationCandidate,
    OrderExecutionMode,
    PaperOrder,
    PaperOrderCancelRequest,
    PaperOrderFillRequest,
    PaperOrderRequest,
    PaperOrderRiskPlan,
    PaperOrderStatus,
    PaperShadowReadiness,
    PortfolioPerformance,
    PortfolioSummary,
    PositionReconciliationItem,
    PositionReconciliationReport,
    PositionReconciliationRequest,
    PositionState,
    Recommendation,
    RecommendationApproval,
    RecommendationAttribution,
    RecommendationAttributionReport,
    ResearchRunRequest,
    ResearchRunResult,
    SellAlert,
    SellAlertAudit,
    SellAlertLevel,
    SellExecutionAudit,
    SellExecutionResult,
    SignalSnapshot,
    SnapshotAttribution,
    SnapshotMode,
    SourceSnapshotDetail,
    SourceSnapshotExport,
    SourceSnapshotReplayCompareRequest,
    SourceSnapshotReplayComparison,
    SourceSnapshotReplayDiff,
    SourceSnapshotReplayRequest,
    SourceSnapshotSummary,
    StrategyConfigAttribution,
    StrategyConfigSnapshot,
    StrategyTuningAction,
    StrategyTuningRecommendation,
    StrategyTuningReport,
    SystemCycleRun,
    TickerPerformance,
    TradeLedgerEntry,
    TradeSide,
)
from domain.policies.approval import ApprovalDecisionRequest, ApprovalPolicy
from infra.config import env_flag
from infra.db.init_db import init_db
from infra.db.order_unit_of_work import OrderUnitOfWork
from infra.db.portfolio_risk_reservation import PortfolioRiskReservationRepository
from infra.db.repositories import (
    ApprovalRepository,
    AutopilotPolicyRepository,
    ExecutionControlRepository,
    FeatureRepository,
    HoldingControlAuditRepository,
    HoldingWatchRepository,
    PaperOrderRepository,
    PositionReconciliationRepository,
    PositionRepository,
    RecommendationRepository,
    SellAlertAuditRepository,
    SellExecutionAuditRepository,
    SignalRepository,
    SourceSnapshotRepository,
    StrategyConfigRepository,
    SystemCycleRunRepository,
    SystemEventRepository,
    TradeLedgerRepository,
)
from infra.db.sell_unit_of_work import SellUnitOfWork
from infra.observability.alerts import OperationalAlertManager
from infra.observability.metrics import MetricsStore
from infra.queue.events import EventStatus, EventType, SystemEvent
from infra.queue.in_memory import InMemoryEventQueue
from services.execution.broker_adapter import (
    BrokerAdapterError,
    BrokerOrderNotFoundError,
    BrokerOrderPlacement,
)
from services.execution.market_calendar import xnys_session
from services.execution.router import ExecutionRouter
from services.ingestion.interfaces import DataProvider
from services.ingestion.provider_factory import build_data_provider
from services.ranking.pipeline import PipelineOutput, ResearchPipeline
from services.research.backtest_engine import BacktestEngine
from services.risk.position_monitor import PositionMonitor

logger = logging.getLogger(__name__)


class AutopilotStateMixin:
    def decide_recommendation(self, request: ApprovalDecisionRequest) -> RecommendationApproval:
        issues = self.approval_policy.validate(request)
        if issues:
            raise ValueError("; ".join(issues))

        recommendation = self.recommendations_by_id.get(request.recommendation_id)
        if recommendation is None:
            recommendation = self.recommendation_repo.get(request.recommendation_id)
        if recommendation is None:
            raise KeyError("recommendation not found")

        decision = RecommendationApproval(
            decision_id=uuid4().hex[:16],
            recommendation_id=request.recommendation_id,
            decision=ApprovalDecision(request.decision),
            approver=request.approver,
            notes=request.notes,
            decided_at=datetime.now(timezone.utc),
        )
        self.approvals_by_recommendation_id[request.recommendation_id] = decision
        self.approval_repo.add(decision)
        self.metrics_store.inc("approvals")
        return decision

    def set_kill_switch(self, enabled: bool, reason: str | None, updated_by: str) -> KillSwitchState:
        self.kill_switch = self.execution_control_repo.set_kill_switch(enabled, reason, updated_by)
        self.metrics_store.set_gauge("kill_switch_enabled", 1.0 if enabled else 0.0)
        return self.kill_switch

    def get_autopilot_policy(self) -> AutopilotPolicy:
        self.autopilot_policy = self.autopilot_policy_repo.get_latest()
        return self.autopilot_policy

    def get_paper_shadow_gate(
        self,
        *,
        required_trading_days: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        checked_at = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
        eastern = ZoneInfo("America/New_York")
        runs = self.system_cycle_run_repo.list_successful_paper_shadow_runs(checked_at)
        trading_dates = sorted(
            {
                run.astimezone(eastern).date().isoformat()
                for run in runs
                if xnys_session(run.astimezone(eastern).date()).is_trading_day
            }
        )
        test_bypass = env_flag("QUANT_AGENT_TEST_MODE")
        observed = len(trading_dates)
        return {
            "passed": test_bypass or observed >= required_trading_days,
            "required_trading_days": required_trading_days,
            "observed_trading_days": observed,
            "remaining_trading_days": max(0, required_trading_days - observed),
            "first_trading_date": trading_dates[0] if trading_dates else None,
            "last_trading_date": trading_dates[-1] if trading_dates else None,
            "bypassed_for_test": test_bypass,
            "checked_at": checked_at.isoformat(),
        }

    def build_paper_shadow_readiness(
        self,
        *,
        as_of: datetime | None = None,
    ) -> PaperShadowReadiness:
        policy = self.get_autopilot_policy()
        return PaperShadowReadiness.model_validate(
            self.get_paper_shadow_gate(
                required_trading_days=policy.min_paper_shadow_trading_days,
                as_of=as_of,
            )
        )

    def build_autopilot_preflight(
        self,
        policy: AutopilotPolicy | None = None,
        as_of: datetime | None = None,
        allow_auto_live_execution: bool | None = None,
    ) -> AutopilotPreflight:
        policy = policy or self.get_autopilot_policy()
        live_allowed = (
            env_flag("QUANT_ALLOW_AUTOPILOT_LIVE") if allow_auto_live_execution is None else allow_auto_live_execution
        )
        checks: list[AutopilotPreflightCheck] = []
        reasons: list[str] = []
        market_session = self.get_market_session_status(as_of=as_of)
        daily_usage = self.get_autopilot_daily_usage(policy=policy, as_of=as_of)
        daily_loss_gate = self.get_autopilot_daily_loss_gate(policy=policy, as_of=as_of)

        if not policy.enabled:
            checks.append(
                AutopilotPreflightCheck(
                    name="policy_enabled",
                    status="fail",
                    message_cn="Autopilot policy 未开启，自动审批和自动执行保持关闭。",
                )
            )
            return AutopilotPreflight(
                status="off",
                reasons=["policy_disabled"],
                daily_usage=daily_usage,
                checks=checks,
            )

        checks.append(
            AutopilotPreflightCheck(
                name="policy_enabled",
                status="pass",
                message_cn="Autopilot policy 已开启。",
            )
        )

        if self.kill_switch.enabled:
            reasons.append("kill_switch_enabled")
            checks.append(
                AutopilotPreflightCheck(
                    name="kill_switch",
                    status="fail",
                    message_cn=f"Kill switch 已开启：{self.kill_switch.reason or '未提供原因'}。",
                    details={"updated_by": self.kill_switch.updated_by},
                )
            )
        else:
            checks.append(
                AutopilotPreflightCheck(
                    name="kill_switch",
                    status="pass",
                    message_cn="Kill switch 未开启。",
                )
            )

        if not daily_loss_gate["passed"]:
            reasons.append("daily_realized_loss_limit_exceeded")
        checks.append(
            AutopilotPreflightCheck(
                name="daily_realized_loss",
                status="pass" if daily_loss_gate["passed"] else "warn",
                message_cn=(
                    "今日已实现亏损在 policy 阈值内。"
                    if daily_loss_gate["passed"]
                    else "今日已实现亏损达到熔断线；自动审批和自动买入会被跳过，自动卖出仍可运行。"
                ),
                details=daily_loss_gate,
            )
        )

        can_auto_approve = (
            policy.auto_approve_recommendations
            and policy.max_auto_approvals > 0
            and daily_usage["remaining_approvals"] > 0
            and daily_loss_gate["passed"]
            and not self.kill_switch.enabled
        )
        if not policy.auto_approve_recommendations:
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_approval",
                    status="warn",
                    message_cn="自动审批未启用。",
                )
            )
        elif policy.max_auto_approvals <= 0:
            reasons.append("max_auto_approvals_zero")
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_approval",
                    status="fail",
                    message_cn="自动审批已启用，但 max_auto_approvals 为 0。",
                )
            )
        elif daily_usage["remaining_approvals"] <= 0:
            reasons.append("daily_auto_approval_budget_exhausted")
            checks.append(
                AutopilotPreflightCheck(
                    name="daily_auto_approval_budget",
                    status="fail",
                    message_cn="今日自动审批预算已用完。",
                    details=daily_usage,
                )
            )
        elif not daily_loss_gate["passed"]:
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_approval",
                    status="fail",
                    message_cn="自动审批已启用，但今日已实现亏损达到 policy 熔断线。",
                    details=daily_loss_gate,
                )
            )
        else:
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_approval",
                    status="pass",
                    message_cn=(
                        f"自动审批可用，本轮最多 "
                        f"{min(policy.max_auto_approvals, daily_usage['remaining_approvals'])} 条。"
                    ),
                    details={
                        "min_confidence": policy.auto_approve_min_confidence,
                        "min_composite": policy.auto_approve_min_composite,
                        **daily_usage,
                    },
                )
            )

        buy_slots_remaining = min(policy.max_auto_buys, daily_usage["remaining_buys"])
        sell_slots_remaining = min(policy.max_auto_sells, daily_usage["remaining_sells"])
        execution_capacity = policy.max_auto_buys > 0 or policy.max_auto_sells > 0
        execution_daily_budget = (buy_slots_remaining > 0 and daily_loss_gate["passed"]) or sell_slots_remaining > 0
        market_hours_allowed = not policy.restrict_auto_execution_to_regular_hours or market_session.is_regular_session
        live_execution_requested = policy.auto_execution_mode == AutoExecutionMode.LIVE
        paper_shadow_gate = self.get_paper_shadow_gate(
            required_trading_days=policy.min_paper_shadow_trading_days,
            as_of=as_of,
        )
        checks.append(
            AutopilotPreflightCheck(
                name="paper_shadow_readiness",
                status=("pass" if paper_shadow_gate["passed"] else "fail" if live_execution_requested else "warn"),
                message_cn=(
                    "纸交易影子运行天数已达到自动实盘要求。"
                    if paper_shadow_gate["passed"]
                    else f"自动实盘前必须先完成至少 {policy.min_paper_shadow_trading_days} 个交易日的纸交易影子运行。"
                ),
                details=paper_shadow_gate,
            )
        )
        if live_execution_requested and not paper_shadow_gate["passed"]:
            reasons.append("paper_shadow_period_incomplete")
        live_position_reconciliation_required = policy.auto_execute_approved and live_execution_requested
        position_reconciliation_gate = self.get_position_reconciliation_gate(
            require_position_reconciliation=(
                policy.require_position_reconciliation or live_position_reconciliation_required
            ),
            max_age_minutes=policy.max_position_reconciliation_age_minutes,
            as_of=as_of,
        )
        live_execution_gate = {
            "passed": (not live_execution_requested) or live_allowed,
            "requested": live_execution_requested,
            "allow_auto_live_execution": live_allowed,
            "reason": None if (not live_execution_requested or live_allowed) else "auto_live_execution_not_allowed",
            "required_runtime_flag": "--allow-auto-live-execution",
            "required_env_var": "QUANT_ALLOW_AUTOPILOT_LIVE=1",
        }
        can_auto_execute = (
            policy.auto_execute_approved
            and execution_capacity
            and execution_daily_budget
            and market_hours_allowed
            and live_execution_gate["passed"]
            and ((not live_execution_requested) or paper_shadow_gate["passed"])
            and position_reconciliation_gate["passed"]
            and not self.kill_switch.enabled
        )
        if not policy.auto_execute_approved:
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_execution",
                    status="warn",
                    message_cn="自动执行未启用。",
                )
            )
        elif not execution_capacity:
            reasons.append("auto_execution_capacity_zero")
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_execution",
                    status="fail",
                    message_cn="自动执行已启用，但 max_auto_buys 和 max_auto_sells 都为 0。",
                )
            )
        elif not execution_daily_budget:
            if not daily_loss_gate["passed"] and sell_slots_remaining <= 0:
                reason = "daily_realized_loss_limit_exceeded"
                message_cn = "今日已实现亏损达到熔断线，且当前没有可用自动卖出预算。"
            else:
                reason = "daily_auto_execution_budget_exhausted"
                message_cn = "今日自动买入/卖出预算已用完。"
            if reason not in reasons:
                reasons.append(reason)
            checks.append(
                AutopilotPreflightCheck(
                    name="daily_auto_execution_budget",
                    status="fail",
                    message_cn=message_cn,
                    details={**daily_usage, "daily_loss_gate": daily_loss_gate},
                )
            )
        elif not market_hours_allowed:
            reasons.append("market_session_closed")
            checks.append(
                AutopilotPreflightCheck(
                    name="market_session",
                    status="fail",
                    message_cn="自动执行限制在美股常规交易时段，但当前不在 09:30-16:00 ET。",
                    details=market_session.model_dump(mode="json"),
                )
            )
        elif not live_execution_gate["passed"]:
            reason = str(live_execution_gate["reason"])
            if reason not in reasons:
                reasons.append(reason)
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_live_execution",
                    status="fail",
                    message_cn="Autopilot 已选择 live 实盘执行，但本轮未显式允许自动实盘下单。",
                    details=live_execution_gate,
                )
            )
        elif live_execution_requested and not paper_shadow_gate["passed"]:
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_execution",
                    status="fail",
                    message_cn="纸交易影子运行尚未达到自动实盘所需交易日数。",
                    details=paper_shadow_gate,
                )
            )
        elif not position_reconciliation_gate["passed"]:
            reason = str(position_reconciliation_gate["reason"])
            if reason not in reasons:
                reasons.append(reason)
            checks.append(
                AutopilotPreflightCheck(
                    name="position_reconciliation",
                    status="fail",
                    message_cn="自动执行要求最近一次仓位核对通过，但当前核对缺失、过期或存在差异。",
                    details=position_reconciliation_gate,
                )
            )
        else:
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_execution",
                    status="pass",
                    message_cn=(
                        f"自动执行可用，本轮最多买入 "
                        f"{buy_slots_remaining if daily_loss_gate['passed'] else 0} 条、"
                        f"卖出 {sell_slots_remaining} 条。"
                    ),
                    details={
                        "execution_mode": policy.auto_execution_mode.value,
                        "live_execution_gate": live_execution_gate,
                        **daily_usage,
                        "daily_loss_gate": daily_loss_gate,
                        "position_reconciliation_gate": position_reconciliation_gate,
                    },
                )
            )
            checks.append(
                AutopilotPreflightCheck(
                    name="market_session",
                    status="pass" if market_session.is_regular_session else "warn",
                    message_cn=(
                        "美股常规交易时段门禁已通过。"
                        if policy.restrict_auto_execution_to_regular_hours
                        else "未强制美股常规交易时段门禁。"
                    ),
                    details=market_session.model_dump(mode="json"),
                )
            )
            checks.append(
                AutopilotPreflightCheck(
                    name="auto_live_execution",
                    status="pass" if live_execution_gate["passed"] else "warn",
                    message_cn=(
                        "自动实盘执行已显式允许。"
                        if live_execution_requested and live_execution_gate["passed"]
                        else "当前自动执行模式不是 live 实盘。"
                    ),
                    details=live_execution_gate,
                )
            )

        if not policy.auto_execute_approved or position_reconciliation_gate["passed"]:
            position_reconciliation_required = bool(position_reconciliation_gate.get("required"))
            checks.append(
                AutopilotPreflightCheck(
                    name="position_reconciliation",
                    status=(
                        "pass"
                        if position_reconciliation_required and position_reconciliation_gate["passed"]
                        else "warn"
                    ),
                    message_cn=(
                        "自动执行前要求最近一次仓位核对通过，当前门禁已通过。"
                        if position_reconciliation_required and position_reconciliation_gate["passed"]
                        else "自动执行前要求最近一次仓位核对通过；当前未通过，但自动执行未启用。"
                        if position_reconciliation_required
                        else "未强制要求自动执行前仓位核对。"
                    ),
                    details=position_reconciliation_gate,
                )
            )

        checks.append(
            AutopilotPreflightCheck(
                name="rebuy_cooldown",
                status="pass" if policy.rebuy_cooldown_minutes > 0 else "warn",
                message_cn=(
                    f"卖出后 {policy.rebuy_cooldown_minutes} 分钟内禁止自动买回同一股票。"
                    if policy.rebuy_cooldown_minutes > 0
                    else "未启用卖出后买回冷却期。"
                ),
                details={"rebuy_cooldown_minutes": policy.rebuy_cooldown_minutes},
            )
        )
        checks.append(
            AutopilotPreflightCheck(
                name="sell_alert_cooldown",
                status="pass" if policy.sell_alert_cooldown_minutes > 0 else "warn",
                message_cn=(
                    f"同一股票同一卖出提醒在 {policy.sell_alert_cooldown_minutes} 分钟内禁止自动重复卖出。"
                    if policy.sell_alert_cooldown_minutes > 0
                    else "未启用自动卖出提醒冷却期。"
                ),
                details={"sell_alert_cooldown_minutes": policy.sell_alert_cooldown_minutes},
            )
        )
        checks.append(
            AutopilotPreflightCheck(
                name="order_dedupe",
                status="pass" if policy.order_dedupe_minutes > 0 else "warn",
                message_cn=(
                    f"同一推荐或同一股票在 {policy.order_dedupe_minutes} 分钟内已有买单时禁止自动重复买入。"
                    if policy.order_dedupe_minutes > 0
                    else "未启用自动买入订单去重窗口。"
                ),
                details={"order_dedupe_minutes": policy.order_dedupe_minutes},
            )
        )
        checks.append(
            AutopilotPreflightCheck(
                name="auto_buy_price_drift",
                status="pass" if policy.max_auto_buy_price_drift_pct > 0 else "warn",
                message_cn=(
                    "自动买入前会重新检查最新价，若偏离推荐入场区间超过 "
                    f"{policy.max_auto_buy_price_drift_pct:.1%} 则跳过买入。"
                    if policy.max_auto_buy_price_drift_pct > 0
                    else "未启用自动买入价格漂移门禁。"
                ),
                details={"max_auto_buy_price_drift_pct": policy.max_auto_buy_price_drift_pct},
            )
        )
        checks.append(
            AutopilotPreflightCheck(
                name="snapshot_quality_policy",
                status="pass",
                message_cn=(
                    "自动审批/执行前要求本轮行情快照 bars 覆盖率 "
                    f">= {policy.min_snapshot_bar_coverage:.0%}，基本面覆盖率 "
                    f">= {policy.min_snapshot_fundamental_coverage:.0%}，"
                    f"最新 bar 年龄 <= {policy.max_snapshot_bar_age_minutes} 分钟。"
                ),
                details={
                    "min_snapshot_bar_coverage": policy.min_snapshot_bar_coverage,
                    "min_snapshot_fundamental_coverage": policy.min_snapshot_fundamental_coverage,
                    "max_snapshot_bar_age_minutes": policy.max_snapshot_bar_age_minutes,
                },
            )
        )
        portfolio_summary = self.get_portfolio_summary(as_of=as_of)
        open_risk_pct = (
            round(portfolio_summary.open_risk_to_stop / policy.account_equity, 6) if policy.account_equity > 0 else 1.0
        )
        checks.append(
            AutopilotPreflightCheck(
                name="portfolio_open_risk",
                status="pass" if open_risk_pct <= policy.max_open_risk_pct else "warn",
                message_cn=(
                    "组合 open risk 在自动买入阈值内。"
                    if open_risk_pct <= policy.max_open_risk_pct
                    else "组合 open risk 已超过阈值；自动卖出仍可运行，但自动买入会被跳过。"
                ),
                details={
                    "open_risk_to_stop": portfolio_summary.open_risk_to_stop,
                    "open_risk_pct": open_risk_pct,
                    "max_open_risk_pct": policy.max_open_risk_pct,
                    "account_equity": policy.account_equity,
                    "open_holding_count": portfolio_summary.open_holding_count,
                },
            )
        )

        if can_auto_approve or can_auto_execute:
            status = "ready"
        else:
            if not reasons:
                reasons.append("no_auto_actions_enabled")
            status = "blocked"

        return AutopilotPreflight(
            status=status,
            can_auto_approve=can_auto_approve,
            can_auto_execute=can_auto_execute,
            reasons=reasons,
            daily_usage=daily_usage,
            checks=checks,
        )
