from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from functools import lru_cache
import math
import os
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from domain.entities.models import (
    AlertExecutionResult,
    AlertSellRequest,
    ApprovalDecision,
    AutoExecutionMode,
    AutopilotPreflight,
    AutopilotPreflightCheck,
    AutopilotPolicy,
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
    OrderExecutionMode,
    OperationAction,
    OperationControlCenter,
    OperationRecommendationCandidate,
    PaperOrder,
    PaperOrderCancelRequest,
    PaperOrderFillRequest,
    PaperOrderRequest,
    PaperOrderRiskPlan,
    PaperOrderStatus,
    PortfolioPerformance,
    PortfolioSummary,
    PositionReconciliationItem,
    PositionReconciliationReport,
    PositionReconciliationRequest,
    PositionState,
    Recommendation,
    RecommendationAttribution,
    RecommendationAttributionReport,
    RecommendationApproval,
    ResearchRunRequest,
    ResearchRunResult,
    SellAlert,
    SellAlertAudit,
    SellAlertLevel,
    SellExecutionAudit,
    SellExecutionResult,
    SignalSnapshot,
    SnapshotMode,
    SnapshotAttribution,
    SourceSnapshotReplayCompareRequest,
    SourceSnapshotReplayComparison,
    SourceSnapshotReplayDiff,
    SourceSnapshotDetail,
    SourceSnapshotExport,
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
from infra.db.init_db import init_db
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
from infra.observability.metrics import MetricsStore
from infra.queue.events import EventStatus, EventType, SystemEvent
from infra.queue.in_memory import InMemoryEventQueue
from services.execution.router import ExecutionRouter
from services.execution.broker_adapter import BrokerAdapterError, BrokerOrderPlacement
from services.ingestion.interfaces import DataProvider
from services.ingestion.provider_factory import build_data_provider
from services.ranking.pipeline import PipelineOutput, ResearchPipeline
from services.research.backtest_engine import BacktestEngine
from services.risk.position_monitor import PositionMonitor


def _truthy_env_flag(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class AppState:
    provider: DataProvider = field(default_factory=build_data_provider)
    pipeline: ResearchPipeline = field(init=False)
    execution_router: ExecutionRouter = field(default_factory=ExecutionRouter)
    backtest_engine: BacktestEngine = field(default_factory=BacktestEngine)
    metrics_store: MetricsStore = field(default_factory=MetricsStore)
    event_queue: InMemoryEventQueue = field(default_factory=InMemoryEventQueue)
    approval_policy: ApprovalPolicy = field(default_factory=ApprovalPolicy)
    position_monitor: PositionMonitor = field(default_factory=PositionMonitor)

    recommendation_repo: RecommendationRepository = field(default_factory=RecommendationRepository)
    signal_repo: SignalRepository = field(default_factory=SignalRepository)
    feature_repo: FeatureRepository = field(default_factory=FeatureRepository)
    paper_order_repo: PaperOrderRepository = field(default_factory=PaperOrderRepository)
    position_repo: PositionRepository = field(default_factory=PositionRepository)
    position_reconciliation_repo: PositionReconciliationRepository = field(
        default_factory=PositionReconciliationRepository
    )
    holding_watch_repo: HoldingWatchRepository = field(default_factory=HoldingWatchRepository)
    holding_control_audit_repo: HoldingControlAuditRepository = field(default_factory=HoldingControlAuditRepository)
    trade_ledger_repo: TradeLedgerRepository = field(default_factory=TradeLedgerRepository)
    sell_execution_audit_repo: SellExecutionAuditRepository = field(default_factory=SellExecutionAuditRepository)
    sell_alert_audit_repo: SellAlertAuditRepository = field(default_factory=SellAlertAuditRepository)
    approval_repo: ApprovalRepository = field(default_factory=ApprovalRepository)
    execution_control_repo: ExecutionControlRepository = field(default_factory=ExecutionControlRepository)
    autopilot_policy_repo: AutopilotPolicyRepository = field(default_factory=AutopilotPolicyRepository)
    source_snapshot_repo: SourceSnapshotRepository = field(default_factory=SourceSnapshotRepository)
    strategy_config_repo: StrategyConfigRepository = field(default_factory=StrategyConfigRepository)
    system_cycle_run_repo: SystemCycleRunRepository = field(default_factory=SystemCycleRunRepository)
    system_event_repo: SystemEventRepository = field(default_factory=SystemEventRepository)

    latest_run: ResearchRunResult | None = None
    last_research_request: ResearchRunRequest | None = None
    recommendations_by_id: dict[str, Recommendation] = field(default_factory=dict)
    signals_by_ticker: dict[str, SignalSnapshot] = field(default_factory=dict)
    features_by_ticker: dict[str, FeatureSnapshot] = field(default_factory=dict)
    paper_orders: list[PaperOrder] = field(default_factory=list)
    positions: dict[str, PositionState] = field(default_factory=dict)
    holdings_by_ticker: dict[str, HoldingWatch] = field(default_factory=dict)
    backtest_runs: list[BacktestRunResult] = field(default_factory=list)
    approvals_by_recommendation_id: dict[str, RecommendationApproval] = field(default_factory=dict)
    kill_switch: KillSwitchState = field(default_factory=lambda: KillSwitchState(enabled=False))
    autopilot_policy: AutopilotPolicy = field(default_factory=AutopilotPolicy)
    recent_sell_alerts: list[SellAlert] = field(default_factory=list)

    _alert_priority: dict[str, int] = field(
        default_factory=lambda: {
            "stop_loss_breach": 0,
            "take_profit2_hit": 1,
            "regime_risk_off": 2,
            "take_profit1_hit": 3,
        }
    )

    def _record_trade(self, entry: TradeLedgerEntry) -> None:
        self.trade_ledger_repo.add(entry)
        self.metrics_store.inc(f"trade_ledger_{entry.side.value}")

    def _record_sell_execution_audit(self, item: SellExecutionAudit) -> None:
        self.sell_execution_audit_repo.add(item)
        self.metrics_store.inc("sell_execution_audits")

    def _get_recommendation_by_id(self, recommendation_id: str | None) -> Recommendation | None:
        if not recommendation_id:
            return None
        recommendation = self.recommendations_by_id.get(recommendation_id)
        if recommendation is None:
            recommendation = self.recommendation_repo.get(recommendation_id)
        return recommendation

    def _recommendation_provenance(
        self,
        recommendation_id: str | None,
        recommendation: Recommendation | None = None,
    ) -> dict[str, str | None]:
        recommendation = recommendation or self._get_recommendation_by_id(recommendation_id)
        return {
            "source_snapshot_id": recommendation.source_snapshot_id if recommendation is not None else None,
            "strategy_config_id": recommendation.strategy_config_id if recommendation is not None else None,
        }

    def record_system_cycle_run(self, item: SystemCycleRun) -> None:
        self.system_cycle_run_repo.add(item)
        self.metrics_store.inc("system_cycle_runs")

    def list_system_cycle_runs(self, limit: int = 100, status: str | None = None) -> list[SystemCycleRun]:
        return self.system_cycle_run_repo.list_recent(limit=limit, status=status)

    def build_operation_control_center(
        self,
        recommendation_limit: int = 20,
        refresh_alerts: bool = True,
    ) -> OperationControlCenter:
        recommendations = (
            list(self.latest_run.recommendations)
            if self.latest_run is not None
            else self.recommendation_repo.list_latest(limit=recommendation_limit)
        )[:recommendation_limit]
        open_holdings = self.list_open_holdings()
        open_tickers = {holding.ticker.upper() for holding in open_holdings}
        alerts = self.monitor_sell_alerts() if refresh_alerts else list(self.recent_sell_alerts)
        pending_approvals: list[OperationRecommendationCandidate] = []
        ready_to_buy: list[OperationRecommendationCandidate] = []
        actions: list[OperationAction] = []

        if self.kill_switch.enabled:
            actions.append(
                OperationAction(
                    action_type="execution_blocked",
                    priority="urgent",
                    message_cn=f"Kill switch 已开启：{self.kill_switch.reason or '未提供原因'}。买入和卖出执行会被阻断。",
                    endpoint="/execution/kill-switch",
                    method="POST",
                    details={"updated_by": self.kill_switch.updated_by},
                )
            )

        for alert in alerts:
            priority = "urgent" if alert.level == SellAlertLevel.URGENT else "high"
            actions.append(
                OperationAction(
                    action_type="sell_alert",
                    priority=priority,
                    message_cn=alert.message_cn,
                    endpoint=f"/portfolio/alerts/{alert.ticker}/execute",
                    method="POST",
                    ticker=alert.ticker,
                    recommendation_id=alert.source_recommendation_id,
                    source_snapshot_id=alert.source_snapshot_id,
                    details={
                        "reason_code": alert.reason_code,
                        "suggested_action_cn": alert.suggested_action_cn,
                        "current_price": alert.current_price,
                        "strategy_config_id": alert.strategy_config_id,
                    },
                )
            )

        for recommendation in recommendations:
            approval = self.get_latest_approval(recommendation.id)
            approval_status = approval.decision.value if approval else "pending"
            candidate = self._operation_candidate(recommendation, approval_status)
            if approval is None:
                pending_approvals.append(candidate)
                actions.append(
                    OperationAction(
                        action_type="approve_recommendation",
                        priority="medium",
                        message_cn=(
                            f"{recommendation.ticker} 推荐尚未审批；审批后才允许进入买入执行。"
                        ),
                        endpoint=f"/recommendations/{recommendation.id}/approval",
                        method="POST",
                        ticker=recommendation.ticker,
                        recommendation_id=recommendation.id,
                        source_snapshot_id=recommendation.source_snapshot_id,
                    )
                )
            elif approval.decision == ApprovalDecision.APPROVED and recommendation.ticker.upper() not in open_tickers:
                ready_to_buy.append(candidate)
                actions.append(
                    OperationAction(
                        action_type="route_buy_order",
                        priority="medium",
                        message_cn=(
                            f"{recommendation.ticker} 已审批且当前无监控持仓；可先查 risk-plan 再提交买入单。"
                        ),
                        endpoint="/paper-orders",
                        method="POST",
                        ticker=recommendation.ticker,
                        recommendation_id=recommendation.id,
                        source_snapshot_id=recommendation.source_snapshot_id,
                        details={
                            "risk_plan_endpoint": "/paper-orders/risk-plan",
                            "entry_zone": recommendation.entry_zone,
                            "stop_loss": recommendation.stop_loss,
                        },
                    )
                )

        pending_event_count = self.pending_event_count()
        if pending_event_count > 0:
            actions.append(
                OperationAction(
                    action_type="inspect_pending_events",
                    priority="low",
                    message_cn=f"事件队列还有 {pending_event_count} 条待处理事件，可检查或消费。",
                    endpoint="/events/pending",
                    method="GET",
                    details={"pending_event_count": pending_event_count},
                )
            )

        priority_rank = {"urgent": 0, "high": 1, "medium": 2, "low": 3}
        actions.sort(key=lambda item: (priority_rank.get(item.priority, 9), item.action_type, item.ticker or ""))
        latest_source_snapshot_id = self.latest_run.source_snapshot_id if self.latest_run else None
        latest_strategy_config_id = self.latest_run.strategy_config_id if self.latest_run else None
        autopilot_policy = self.get_autopilot_policy()
        return OperationControlCenter(
            kill_switch=self.kill_switch,
            autopilot_policy=autopilot_policy,
            autopilot_preflight=self.build_autopilot_preflight(autopilot_policy),
            latest_source_snapshot_id=latest_source_snapshot_id,
            latest_strategy_config_id=latest_strategy_config_id,
            latest_recommendation_count=len(recommendations),
            pending_approval_count=len(pending_approvals),
            approved_ready_to_buy_count=len(ready_to_buy),
            open_holding_count=len(open_holdings),
            sell_alert_count=len(alerts),
            urgent_sell_alert_count=sum(1 for alert in alerts if alert.level == SellAlertLevel.URGENT),
            pending_event_count=pending_event_count,
            recent_order_count=len(self.list_paper_orders(limit=10)),
            recent_sell_execution_count=len(self.list_sell_execution_audits(limit=10)),
            pending_approvals=pending_approvals,
            ready_to_buy=ready_to_buy,
            sell_alerts=alerts,
            actions=actions,
        )

    @staticmethod
    def _operation_candidate(
        recommendation: Recommendation,
        approval_status: str,
    ) -> OperationRecommendationCandidate:
        return OperationRecommendationCandidate(
            recommendation_id=recommendation.id,
            ticker=recommendation.ticker,
            approval_status=approval_status,
            confidence=recommendation.confidence,
            composite_score=round(float(recommendation.score_vector.get("composite", 0.0)), 6),
            entry_zone_low=recommendation.entry_zone_low,
            entry_zone_high=recommendation.entry_zone_high,
            stop_loss=recommendation.stop_loss,
            tp1=recommendation.tp1,
            tp2=recommendation.tp2,
            source_snapshot_id=recommendation.source_snapshot_id,
            strategy_config_id=recommendation.strategy_config_id,
        )

    def record_sell_alert_audits(self, alerts: list[SellAlert], monitor_run_id: str | None = None) -> list[SellAlertAudit]:
        items = [
            SellAlertAudit(
                id=uuid4().hex[:16],
                ticker=alert.ticker,
                level=alert.level,
                reason_code=alert.reason_code,
                current_price=alert.current_price,
                stop_loss=alert.stop_loss,
                take_profit1=alert.take_profit1,
                take_profit2=alert.take_profit2,
                source_recommendation_id=alert.source_recommendation_id,
                source_snapshot_id=alert.source_snapshot_id,
                strategy_config_id=alert.strategy_config_id,
                message_cn=alert.message_cn,
                suggested_action_cn=alert.suggested_action_cn,
                generated_at=alert.generated_at,
                monitor_run_id=monitor_run_id,
            )
            for alert in alerts
        ]
        if items:
            self.sell_alert_audit_repo.add_many(items)
            self.metrics_store.inc("sell_alert_audits", len(items))
        return items

    def list_sell_alert_audits(
        self,
        limit: int = 100,
        ticker: str | None = None,
        reason_code: str | None = None,
        level: SellAlertLevel | None = None,
        monitor_run_id: str | None = None,
    ) -> list[SellAlertAudit]:
        return self.sell_alert_audit_repo.list_recent(
            limit=limit,
            ticker=ticker,
            reason_code=reason_code,
            level=level,
            monitor_run_id=monitor_run_id,
        )

    def __post_init__(self) -> None:
        init_db()
        self.pipeline = ResearchPipeline(provider=self.provider, snapshot_repository=self.source_snapshot_repo)
        self.kill_switch = self.execution_control_repo.get_kill_switch()
        self.autopilot_policy = self.autopilot_policy_repo.get_latest()

    def ingest_run_output(self, request: ResearchRunRequest, output: PipelineOutput) -> None:
        self.last_research_request = request
        self.latest_run = output.result
        self.signals_by_ticker = dict(output.signals_by_ticker)
        self.features_by_ticker = dict(output.features_by_ticker)
        self.recommendations_by_id = {rec.id: rec for rec in output.result.recommendations}
        self.signal_repo.upsert_many(self.signals_by_ticker.values())
        self.feature_repo.upsert_many(self.features_by_ticker.values())
        self.strategy_config_repo.upsert(output.strategy_config)
        self.recommendation_repo.upsert_many(output.result.recommendations)
        self.metrics_store.inc("research_runs")
        self.metrics_store.set_gauge("latest_recommendation_count", len(output.result.recommendations))
        self.metrics_store.set_gauge("latest_rejection_rate", output.result.run_metrics.rejection_rate)
        self.publish_event(
            EventType.RECOMMENDATION_READY,
            {
                "run_type": str(request.run_type),
                "source_snapshot_id": output.result.source_snapshot_id,
                "recommendation_count": len(output.result.recommendations),
                "rejection_rate": output.result.run_metrics.rejection_rate,
            },
        )

    def record_paper_order(self, order: PaperOrder, recommendation: Recommendation | None = None) -> None:
        self.paper_orders.append(order)
        self.paper_order_repo.add(order)
        self.position_repo.replace_all(self.positions.values())
        self.metrics_store.inc("paper_orders")
        self.metrics_store.set_gauge("open_positions", sum(1 for p in self.positions.values() if p.qty > 0))
        self.publish_event(
            EventType.ORDER_ROUTED,
            {
                "order_id": order.id,
                "recommendation_id": order.recommendation_id,
                "source_snapshot_id": order.source_snapshot_id,
                "strategy_config_id": order.strategy_config_id,
                "execution_mode": order.execution_mode.value,
                "dry_run": order.dry_run,
                "status": order.status.value,
                "broker_order_id": order.broker_order_id,
                "adapter_message": order.adapter_message,
            },
        )
        if (
            recommendation is not None
            and order.status == PaperOrderStatus.FILLED
            and order.side == Direction.BUY
            and order.simulated_fill_price is not None
        ):
            self.record_manual_buy(
                ManualBuyRequest(
                    ticker=recommendation.ticker,
                    qty=order.qty,
                    buy_price=order.simulated_fill_price,
                    source_recommendation_id=recommendation.id,
                    note=f"paper_order_fill:{order.id}",
                    stop_loss=recommendation.stop_loss,
                    take_profit1=recommendation.tp1,
                    take_profit2=recommendation.tp2,
                    bought_at=order.filled_at or order.submitted_at,
                )
            )
            self.publish_event(
                EventType.PAPER_FILL,
                {
                    "order_id": order.id,
                    "recommendation_id": order.recommendation_id,
                    "status": order.status.value,
                    "fill_price": order.simulated_fill_price,
                },
            )

    def list_paper_orders(
        self,
        limit: int = 100,
        recommendation_id: str | None = None,
        side: Direction | None = None,
        status: PaperOrderStatus | None = None,
    ) -> list[PaperOrder]:
        orders = self.paper_order_repo.list_recent(
            limit=limit,
            recommendation_id=recommendation_id,
            side=side,
            status=status,
        )
        known_ids = {order.id for order in orders}
        memory_only = [
            order
            for order in self.paper_orders
            if order.id not in known_ids
            and (recommendation_id is None or order.recommendation_id == recommendation_id)
            and (side is None or order.side == side)
            and (status is None or order.status == status)
        ]
        merged = sorted([*orders, *memory_only], key=lambda order: order.submitted_at, reverse=True)
        return merged[:limit]

    def get_paper_order(self, order_id: str) -> PaperOrder | None:
        order = self.paper_order_repo.get(order_id)
        if order is not None:
            return order
        return next((item for item in self.paper_orders if item.id == order_id), None)

    def get_paper_order_by_broker_order_id(self, broker_order_id: str | None) -> PaperOrder | None:
        if not broker_order_id:
            return None
        order = self.paper_order_repo.get_by_broker_order_id(broker_order_id)
        if order is not None:
            return order
        return next((item for item in self.paper_orders if item.broker_order_id == broker_order_id), None)

    def cancel_paper_order(self, order_id: str, request: PaperOrderCancelRequest) -> PaperOrder:
        order = self.get_paper_order(order_id)
        if order is None:
            raise KeyError("paper order not found")
        if order.status == PaperOrderStatus.CANCELED:
            return order
        if order.status != PaperOrderStatus.SUBMITTED:
            raise ValueError("Only submitted orders can be canceled")

        reason = request.reason or f"canceled_by:{request.canceled_by}"
        updated = order.model_copy(
            update={
                "status": PaperOrderStatus.CANCELED,
                "cancel_reason": reason,
                "adapter_message": (
                    f"{order.adapter_message}; canceled_by={request.canceled_by}"
                    if order.adapter_message
                    else f"canceled_by={request.canceled_by}"
                ),
            }
        )
        self.paper_order_repo.add(updated)
        for index, item in enumerate(self.paper_orders):
            if item.id == updated.id:
                self.paper_orders[index] = updated
                break
        else:
            self.paper_orders.append(updated)
        self.metrics_store.inc("paper_orders_canceled")
        self.publish_event(
            EventType.ORDER_CANCELED,
            {
                "order_id": updated.id,
                "recommendation_id": updated.recommendation_id,
                "source_snapshot_id": updated.source_snapshot_id,
                "strategy_config_id": updated.strategy_config_id,
                "execution_mode": updated.execution_mode.value,
                "dry_run": updated.dry_run,
                "broker_order_id": updated.broker_order_id,
                "cancel_reason": updated.cancel_reason,
                "canceled_by": request.canceled_by,
            },
        )
        return updated

    def fill_paper_order(self, order_id: str, request: PaperOrderFillRequest) -> PaperOrder:
        order = self.get_paper_order(order_id)
        if order is None:
            raise KeyError("paper order not found")
        if order.status == PaperOrderStatus.FILLED:
            return order
        if order.status == PaperOrderStatus.CANCELED:
            raise ValueError("Canceled orders cannot be filled")
        if order.status != PaperOrderStatus.SUBMITTED:
            raise ValueError("Only submitted orders can be filled")

        apply_to_ledger = (not order.dry_run) if request.apply_to_ledger is None else request.apply_to_ledger
        recommendation = self._get_recommendation_by_id(order.recommendation_id)
        if apply_to_ledger:
            if order.side != Direction.BUY:
                raise ValueError("Only BUY fills can be applied to the holding ledger")
            if recommendation is None:
                raise ValueError("Recommendation id not found for ledger application")

        filled_at = request.filled_at or datetime.now(timezone.utc)
        if filled_at.tzinfo is None:
            filled_at = filled_at.replace(tzinfo=timezone.utc)
        else:
            filled_at = filled_at.astimezone(timezone.utc)

        fill_note_parts = [
            f"filled_by={request.filled_by}",
            f"apply_to_ledger={str(apply_to_ledger).lower()}",
        ]
        if request.note:
            fill_note_parts.append(f"note={request.note}")
        fill_message = "; ".join(fill_note_parts)

        updated = order.model_copy(
            update={
                "status": PaperOrderStatus.FILLED,
                "simulated_fill_price": round(request.fill_price, 6),
                "filled_at": filled_at,
                "cancel_reason": None,
                "adapter_message": (
                    f"{order.adapter_message}; {fill_message}"
                    if order.adapter_message
                    else fill_message
                ),
            }
        )
        self.paper_order_repo.add(updated)
        for index, item in enumerate(self.paper_orders):
            if item.id == updated.id:
                self.paper_orders[index] = updated
                break
        else:
            self.paper_orders.append(updated)

        if apply_to_ledger and recommendation is not None:
            ledger_note = f"paper_order_fill:{updated.id}"
            if request.note:
                ledger_note = f"{ledger_note}; {request.note}"
            self.record_manual_buy(
                ManualBuyRequest(
                    ticker=recommendation.ticker,
                    qty=updated.qty,
                    buy_price=updated.simulated_fill_price or request.fill_price,
                    source_recommendation_id=recommendation.id,
                    note=ledger_note,
                    stop_loss=recommendation.stop_loss,
                    take_profit1=recommendation.tp1,
                    take_profit2=recommendation.tp2,
                    bought_at=updated.filled_at,
                )
            )

        self.metrics_store.inc("paper_order_fills")
        self.publish_event(
            EventType.PAPER_FILL,
            {
                "order_id": updated.id,
                "recommendation_id": updated.recommendation_id,
                "source_snapshot_id": updated.source_snapshot_id,
                "strategy_config_id": updated.strategy_config_id,
                "execution_mode": updated.execution_mode.value,
                "dry_run": updated.dry_run,
                "status": updated.status.value,
                "fill_price": updated.simulated_fill_price,
                "filled_at": updated.filled_at.isoformat() if updated.filled_at else None,
                "filled_by": request.filled_by,
                "apply_to_ledger": apply_to_ledger,
                "broker_order_id": updated.broker_order_id,
            },
        )
        return updated

    def _resolve_broker_sync_order(self, snapshot: BrokerOrderStatusSnapshot) -> PaperOrder | None:
        if snapshot.order_id:
            order = self.get_paper_order(snapshot.order_id)
            if order is not None:
                return order
        return self.get_paper_order_by_broker_order_id(snapshot.broker_order_id)

    def sync_broker_order_statuses(self, request: BrokerOrderSyncRequest) -> BrokerOrderSyncResult:
        checked_at = request.checked_at or datetime.now(timezone.utc)
        if checked_at.tzinfo is None:
            checked_at = checked_at.replace(tzinfo=timezone.utc)
        else:
            checked_at = checked_at.astimezone(timezone.utc)

        items: list[BrokerOrderSyncItemResult] = []
        filled_count = 0
        canceled_count = 0
        unchanged_count = 0
        skipped_count = 0
        missing_count = 0

        for snapshot in request.statuses:
            order = self._resolve_broker_sync_order(snapshot)
            if order is None:
                missing_count += 1
                items.append(
                    BrokerOrderSyncItemResult(
                        order_id=snapshot.order_id,
                        broker_order_id=snapshot.broker_order_id,
                        broker_status=snapshot.status,
                        action="missing",
                        message_cn="未找到匹配的本地订单。",
                    )
                )
                continue

            before_status = order.status
            apply_to_ledger = (
                snapshot.apply_to_ledger
                if snapshot.apply_to_ledger is not None
                else request.apply_fills_to_ledger
            )
            broker_note = snapshot.broker_message or snapshot.reason
            try:
                if snapshot.status == BrokerOrderSyncStatus.FILLED:
                    if order.status == PaperOrderStatus.CANCELED:
                        skipped_count += 1
                        items.append(
                            BrokerOrderSyncItemResult(
                                order_id=order.id,
                                broker_order_id=order.broker_order_id,
                                broker_status=snapshot.status,
                                action="skipped",
                                before_status=before_status,
                                after_status=order.status,
                                apply_to_ledger=apply_to_ledger,
                                message_cn="本地订单已取消，跳过 broker fill。",
                            )
                        )
                        continue
                    updated = self.fill_paper_order(
                        order.id,
                        PaperOrderFillRequest(
                            fill_price=float(snapshot.fill_price or 0.0),
                            filled_at=snapshot.filled_at or checked_at,
                            filled_by=f"{request.updated_by}:{request.broker}",
                            apply_to_ledger=apply_to_ledger,
                            note=broker_note,
                        ),
                    )
                    action = "unchanged" if before_status == PaperOrderStatus.FILLED else "filled"
                    if action == "filled":
                        filled_count += 1
                    else:
                        unchanged_count += 1
                    items.append(
                        BrokerOrderSyncItemResult(
                            order_id=updated.id,
                            broker_order_id=updated.broker_order_id,
                            broker_status=snapshot.status,
                            action=action,
                            before_status=before_status,
                            after_status=updated.status,
                            apply_to_ledger=apply_to_ledger,
                            message_cn="已回写 broker 成交。" if action == "filled" else "订单已是 filled。",
                        )
                    )
                elif snapshot.status in {BrokerOrderSyncStatus.CANCELED, BrokerOrderSyncStatus.REJECTED}:
                    if order.status == PaperOrderStatus.FILLED:
                        skipped_count += 1
                        items.append(
                            BrokerOrderSyncItemResult(
                                order_id=order.id,
                                broker_order_id=order.broker_order_id,
                                broker_status=snapshot.status,
                                action="skipped",
                                before_status=before_status,
                                after_status=order.status,
                                message_cn="本地订单已成交，跳过 broker cancel/reject。",
                            )
                        )
                        continue
                    reason = snapshot.reason or f"{snapshot.status.value}_by_{request.broker}"
                    if snapshot.broker_message:
                        reason = f"{reason}; {snapshot.broker_message}"
                    updated = self.cancel_paper_order(
                        order.id,
                        PaperOrderCancelRequest(
                            reason=reason,
                            canceled_by=f"{request.updated_by}:{request.broker}",
                        ),
                    )
                    action = "unchanged" if before_status == PaperOrderStatus.CANCELED else "canceled"
                    if action == "canceled":
                        canceled_count += 1
                    else:
                        unchanged_count += 1
                    items.append(
                        BrokerOrderSyncItemResult(
                            order_id=updated.id,
                            broker_order_id=updated.broker_order_id,
                            broker_status=snapshot.status,
                            action=action,
                            before_status=before_status,
                            after_status=updated.status,
                            message_cn="已回写 broker 取消/拒单。" if action == "canceled" else "订单已是 canceled。",
                        )
                    )
                else:
                    unchanged_count += 1
                    items.append(
                        BrokerOrderSyncItemResult(
                            order_id=order.id,
                            broker_order_id=order.broker_order_id,
                            broker_status=snapshot.status,
                            action="unchanged",
                            before_status=before_status,
                            after_status=order.status,
                            message_cn="broker 仍显示 submitted，本地保持不变。",
                        )
                    )
            except ValueError as exc:
                current_order = self.get_paper_order(order.id)
                skipped_count += 1
                items.append(
                    BrokerOrderSyncItemResult(
                        order_id=order.id,
                        broker_order_id=order.broker_order_id,
                        broker_status=snapshot.status,
                        action="skipped",
                        before_status=before_status,
                        after_status=current_order.status if current_order is not None else before_status,
                        apply_to_ledger=apply_to_ledger,
                        message_cn=str(exc),
                    )
                )

        result = BrokerOrderSyncResult(
            broker=request.broker,
            checked_at=checked_at,
            total_count=len(request.statuses),
            filled_count=filled_count,
            canceled_count=canceled_count,
            unchanged_count=unchanged_count,
            skipped_count=skipped_count,
            missing_count=missing_count,
            items=items,
        )
        self.publish_event(
            EventType.BROKER_ORDER_SYNC,
            {
                "broker": result.broker,
                "checked_at": result.checked_at.isoformat(),
                "total_count": result.total_count,
                "filled_count": result.filled_count,
                "canceled_count": result.canceled_count,
                "unchanged_count": result.unchanged_count,
                "skipped_count": result.skipped_count,
                "missing_count": result.missing_count,
            },
        )
        return result

    def list_source_snapshots(self, limit: int = 50) -> list[SourceSnapshotSummary]:
        return self.source_snapshot_repo.list_summaries(limit=limit)

    def get_source_snapshot_detail(
        self,
        source_snapshot_id: str,
        event_limit: int = 20,
    ) -> SourceSnapshotDetail | None:
        return self.source_snapshot_repo.get_detail(
            source_snapshot_id=source_snapshot_id,
            event_limit=event_limit,
        )

    def get_source_snapshot_export(self, source_snapshot_id: str) -> SourceSnapshotExport | None:
        return self.source_snapshot_repo.get_export(source_snapshot_id)

    def replay_source_snapshot(
        self,
        source_snapshot_id: str,
        replay_request: SourceSnapshotReplayRequest,
    ) -> ResearchRunResult:
        summary = self.source_snapshot_repo.get_summary(source_snapshot_id)
        if summary is None:
            raise KeyError("source snapshot not found")

        request = ResearchRunRequest(
            run_type=replay_request.run_type,
            objective=replay_request.objective,
            as_of=summary.as_of,
            snapshot_mode=SnapshotMode.POINT_IN_TIME,
            source_snapshot_id=source_snapshot_id,
            universe=summary.universe,
            universe_rules=replay_request.universe_rules,
            signal_config=replay_request.signal_config,
            price_plan_config=replay_request.price_plan_config,
            risk_policy=replay_request.risk_policy,
            publication=replay_request.publication,
            execution_mode=replay_request.execution_mode,
        )
        output = self.pipeline.run(request)
        self.ingest_run_output(request, output)
        return output.result

    def compare_source_snapshot_replay(
        self,
        source_snapshot_id: str,
        compare_request: SourceSnapshotReplayCompareRequest,
    ) -> SourceSnapshotReplayComparison:
        summary = self.source_snapshot_repo.get_summary(source_snapshot_id)
        if summary is None:
            raise KeyError("source snapshot not found")

        baseline = self.recommendation_repo.list_by_source_snapshot(
            source_snapshot_id=source_snapshot_id,
            strategy_config_id=compare_request.baseline_strategy_config_id,
        )
        request = ResearchRunRequest(
            run_type=compare_request.run_type,
            objective=compare_request.objective,
            as_of=summary.as_of,
            snapshot_mode=SnapshotMode.POINT_IN_TIME,
            source_snapshot_id=source_snapshot_id,
            universe=summary.universe,
            universe_rules=compare_request.universe_rules,
            signal_config=compare_request.signal_config,
            price_plan_config=compare_request.price_plan_config,
            risk_policy=compare_request.risk_policy,
            publication=compare_request.publication,
            execution_mode=compare_request.execution_mode,
        )
        output = self.pipeline.run(request)
        return self._build_replay_comparison(
            source_snapshot_id=source_snapshot_id,
            baseline=baseline,
            replay=output.result,
            baseline_strategy_config_id=compare_request.baseline_strategy_config_id,
            include_unchanged=compare_request.include_unchanged,
        )

    def _build_replay_comparison(
        self,
        source_snapshot_id: str,
        baseline: list[Recommendation],
        replay: ResearchRunResult,
        baseline_strategy_config_id: str | None,
        include_unchanged: bool,
    ) -> SourceSnapshotReplayComparison:
        baseline_by_ticker = self._recommendations_by_ticker_for_compare(baseline)
        replay_by_ticker = self._recommendations_by_ticker_for_compare(replay.recommendations)
        all_tickers = sorted(set(baseline_by_ticker) | set(replay_by_ticker))
        diffs: list[SourceSnapshotReplayDiff] = []
        matched_count = 0
        changed_count = 0
        missing_count = 0
        new_count = 0

        for ticker in all_tickers:
            baseline_rec = baseline_by_ticker.get(ticker)
            replay_rec = replay_by_ticker.get(ticker)
            baseline_values = (
                self._recommendation_compare_values(baseline_rec) if baseline_rec is not None else {}
            )
            replay_values = (
                self._recommendation_compare_values(replay_rec) if replay_rec is not None else {}
            )

            if baseline_rec is None:
                status = "new_in_replay"
                changed_fields = sorted(replay_values)
                new_count += 1
            elif replay_rec is None:
                status = "missing_in_replay"
                changed_fields = sorted(baseline_values)
                missing_count += 1
            else:
                changed_fields = [
                    field
                    for field in sorted(set(baseline_values) | set(replay_values))
                    if baseline_values.get(field) != replay_values.get(field)
                ]
                if changed_fields:
                    status = "changed"
                    changed_count += 1
                else:
                    status = "matched"
                    matched_count += 1

            if include_unchanged or status != "matched":
                diffs.append(
                    SourceSnapshotReplayDiff(
                        ticker=ticker,
                        status=status,
                        baseline_recommendation_id=baseline_rec.id if baseline_rec else None,
                        replay_recommendation_id=replay_rec.id if replay_rec else None,
                        changed_fields=changed_fields,
                        baseline_values=baseline_values,
                        replay_values=replay_values,
                    )
                )

        deterministic = (
            changed_count == 0
            and missing_count == 0
            and new_count == 0
            and matched_count == len(baseline_by_ticker)
            and matched_count == len(replay_by_ticker)
        )
        snapshot_info = replay.universe_summary.get("snapshot", {})
        replay_operation = str(snapshot_info.get("operation") or "unknown")
        return SourceSnapshotReplayComparison(
            source_snapshot_id=source_snapshot_id,
            compared_at=datetime.now(timezone.utc),
            baseline_strategy_config_id=baseline_strategy_config_id,
            replay_strategy_config_id=replay.strategy_config_id,
            replay_operation=replay_operation,
            baseline_count=len(baseline_by_ticker),
            replay_count=len(replay_by_ticker),
            matched_count=matched_count,
            changed_count=changed_count,
            missing_in_replay_count=missing_count,
            new_in_replay_count=new_count,
            deterministic=deterministic,
            diffs=diffs,
        )

    @staticmethod
    def _recommendations_by_ticker_for_compare(
        recommendations: list[Recommendation],
    ) -> dict[str, Recommendation]:
        ranked = sorted(
            recommendations,
            key=lambda rec: (
                float(rec.score_vector.get("composite", 0.0)),
                rec.generated_at,
                rec.id,
            ),
            reverse=True,
        )
        result: dict[str, Recommendation] = {}
        for recommendation in ranked:
            result.setdefault(recommendation.ticker.upper(), recommendation)
        return result

    @staticmethod
    def _recommendation_compare_values(recommendation: Recommendation) -> dict[str, Any]:
        return {
            "recommendation_id": recommendation.id,
            "direction": recommendation.direction.value,
            "entry_zone_low": round(recommendation.entry_zone_low, 6),
            "entry_zone_high": round(recommendation.entry_zone_high, 6),
            "stop_loss": round(recommendation.stop_loss, 6),
            "tp1": round(recommendation.tp1, 6),
            "tp2": round(recommendation.tp2, 6),
            "confidence": round(recommendation.confidence, 6),
            "risk_grade": recommendation.risk_grade.value,
            "pattern_template": recommendation.pattern_template.value,
            "composite_score": round(float(recommendation.score_vector.get("composite", 0.0)), 6),
        }

    def list_strategy_configs(self, limit: int = 50) -> list[StrategyConfigSnapshot]:
        return self.strategy_config_repo.list_recent(limit=limit)

    def get_strategy_config(self, strategy_config_id: str) -> StrategyConfigSnapshot | None:
        return self.strategy_config_repo.get(strategy_config_id)

    def _current_position_value(self, ticker: str, as_of: datetime | None = None) -> float:
        ticker_upper = ticker.upper()
        holding = self.holdings_by_ticker.get(ticker_upper)
        if holding is None:
            holding = self.holding_watch_repo.get(ticker_upper)
        if holding is None or holding.status != HoldingStatus.OPEN:
            return 0.0
        return self._holding_market_value(holding, as_of=as_of)

    def _holding_market_value(self, holding: HoldingWatch, as_of: datetime | None = None) -> float:
        try:
            mark = self.provider.get_latest_price(holding.ticker, as_of or datetime.now(timezone.utc))
        except Exception:
            mark = None
        price = float(mark) if mark is not None else holding.avg_buy_price
        return round(max(0.0, price * holding.qty), 6)

    def _sector_for_ticker(self, ticker: str, source_snapshot_id: str | None = None) -> str:
        ticker_upper = ticker.upper()
        if source_snapshot_id:
            try:
                for security in self.source_snapshot_repo.get_securities(source_snapshot_id):
                    if security.ticker.upper() == ticker_upper:
                        return security.sector or "Unknown"
            except Exception:
                pass
        return "Unknown"

    def _holding_sector(self, holding: HoldingWatch, fallback_snapshot_id: str | None = None) -> str:
        snapshot_id = fallback_snapshot_id
        if holding.source_recommendation_id:
            recommendation = self.recommendation_repo.get(holding.source_recommendation_id)
            if recommendation is not None:
                snapshot_id = recommendation.source_snapshot_id
        return self._sector_for_ticker(holding.ticker, snapshot_id)

    @staticmethod
    def _floor_qty(value: float) -> float:
        if value <= 0:
            return 0.0
        return float(math.floor(value))

    def build_paper_order_risk_plan(
        self,
        recommendation: Recommendation,
        request: PaperOrderRequest,
    ) -> PaperOrderRiskPlan:
        side = Direction(request.side)
        entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
        entry_price = float(request.limit_price if request.limit_price is not None else entry_mid)
        stop_loss = float(recommendation.stop_loss)
        risk_per_share = (
            entry_price - stop_loss
            if side == Direction.BUY
            else stop_loss - entry_price
        )
        risk_per_share = round(max(0.0, risk_per_share), 6)
        risk_budget = round(request.account_equity * request.risk_per_trade_pct, 6)
        max_position_value = round(request.account_equity * request.max_position_pct, 6)
        current_position_value = self._current_position_value(recommendation.ticker)
        remaining_position_value = round(max(0.0, max_position_value - current_position_value), 6)
        open_holdings = self.list_open_holdings()
        current_gross_exposure_value = round(
            sum(self._holding_market_value(holding) for holding in open_holdings),
            6,
        )
        max_gross_exposure_value = round(request.account_equity * request.max_gross_exposure_pct, 6)
        remaining_gross_exposure_value = round(
            max(0.0, max_gross_exposure_value - current_gross_exposure_value),
            6,
        )
        sector = self._sector_for_ticker(recommendation.ticker, recommendation.source_snapshot_id)
        current_sector_exposure_value = round(
            sum(
                self._holding_market_value(holding)
                for holding in open_holdings
                if self._holding_sector(holding, recommendation.source_snapshot_id) == sector
            ),
            6,
        )
        max_sector_exposure_value = round(request.account_equity * request.max_sector_exposure_pct, 6)
        remaining_sector_exposure_value = round(
            max(0.0, max_sector_exposure_value - current_sector_exposure_value),
            6,
        )
        max_risk_qty = (
            self._floor_qty(risk_budget / risk_per_share)
            if risk_per_share > 0
            else 0.0
        )
        max_position_qty = (
            self._floor_qty(remaining_position_value / entry_price)
            if entry_price > 0
            else 0.0
        )
        max_gross_qty = (
            self._floor_qty(remaining_gross_exposure_value / entry_price)
            if entry_price > 0
            else 0.0
        )
        max_sector_qty = (
            self._floor_qty(remaining_sector_exposure_value / entry_price)
            if entry_price > 0
            else 0.0
        )
        recommended_qty = min(max_risk_qty, max_position_qty, max_gross_qty, max_sector_qty)

        requested_notional = round(entry_price * request.qty, 6)
        requested_risk_amount = round(risk_per_share * request.qty, 6)
        requested_position_pct = round(
            (current_position_value + requested_notional) / request.account_equity,
            6,
        )
        requested_gross_exposure_pct = round(
            (current_gross_exposure_value + requested_notional) / request.account_equity,
            6,
        )
        requested_sector_exposure_pct = round(
            (current_sector_exposure_value + requested_notional) / request.account_equity,
            6,
        )
        requested_risk_pct = round(requested_risk_amount / request.account_equity, 6)

        violations: list[str] = []
        if risk_per_share <= 0:
            violations.append("invalid_stop_distance")
        if request.qty > max_risk_qty + 1e-9:
            violations.append("exceeds_per_trade_risk")
        if request.qty > max_position_qty + 1e-9:
            violations.append("exceeds_position_cap")
        if request.qty > max_gross_qty + 1e-9:
            violations.append("exceeds_gross_exposure")
        if request.qty > max_sector_qty + 1e-9:
            violations.append("exceeds_sector_exposure")

        if not violations:
            message_cn = (
                f"风险校验通过。建议股数 {recommended_qty:g}，本次名义本金 "
                f"{requested_notional:.2f}，止损风险 {requested_risk_amount:.2f}，"
                f"组合暴露 {requested_gross_exposure_pct:.1%}，{sector} 行业暴露 "
                f"{requested_sector_exposure_pct:.1%}。"
            )
        else:
            labels = {
                "invalid_stop_distance": "止损距离无效",
                "exceeds_per_trade_risk": "超过单笔风险预算",
                "exceeds_position_cap": "超过单票仓位上限",
                "exceeds_gross_exposure": "超过组合总暴露上限",
                "exceeds_sector_exposure": "超过行业暴露上限",
            }
            reasons = "；".join(labels.get(code, code) for code in violations)
            message_cn = (
                f"风险校验未通过：{reasons}。建议最多 {recommended_qty:g} 股，"
                f"当前请求 {request.qty:g} 股。"
            )

        return PaperOrderRiskPlan(
            recommendation_id=request.recommendation_id,
            ticker=recommendation.ticker,
            side=side,
            entry_price=round(entry_price, 6),
            stop_loss=round(stop_loss, 6),
            risk_per_share=risk_per_share,
            account_equity=round(request.account_equity, 6),
            risk_budget=risk_budget,
            max_position_value=max_position_value,
            current_position_value=current_position_value,
            remaining_position_value=remaining_position_value,
            max_gross_exposure_value=max_gross_exposure_value,
            current_gross_exposure_value=current_gross_exposure_value,
            remaining_gross_exposure_value=remaining_gross_exposure_value,
            sector=sector,
            max_sector_exposure_value=max_sector_exposure_value,
            current_sector_exposure_value=current_sector_exposure_value,
            remaining_sector_exposure_value=remaining_sector_exposure_value,
            max_risk_qty=max_risk_qty,
            max_position_qty=max_position_qty,
            max_gross_qty=max_gross_qty,
            max_sector_qty=max_sector_qty,
            recommended_qty=recommended_qty,
            requested_qty=round(request.qty, 6),
            requested_notional=requested_notional,
            requested_risk_amount=requested_risk_amount,
            requested_position_pct=requested_position_pct,
            requested_gross_exposure_pct=requested_gross_exposure_pct,
            requested_sector_exposure_pct=requested_sector_exposure_pct,
            requested_risk_pct=requested_risk_pct,
            is_within_limits=not violations,
            violations=violations,
            message_cn=message_cn,
        )

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

    def build_autopilot_preflight(
        self,
        policy: AutopilotPolicy | None = None,
        as_of: datetime | None = None,
        allow_auto_live_execution: bool | None = None,
    ) -> AutopilotPreflight:
        policy = policy or self.get_autopilot_policy()
        live_allowed = (
            _truthy_env_flag("QUANT_ALLOW_AUTOPILOT_LIVE")
            if allow_auto_live_execution is None
            else allow_auto_live_execution
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
        execution_daily_budget = (
            (buy_slots_remaining > 0 and daily_loss_gate["passed"])
            or sell_slots_remaining > 0
        )
        market_hours_allowed = (
            not policy.restrict_auto_execution_to_regular_hours
            or market_session.is_regular_session
        )
        position_reconciliation_gate = self.get_position_reconciliation_gate(
            require_position_reconciliation=policy.require_position_reconciliation,
            max_age_minutes=policy.max_position_reconciliation_age_minutes,
            as_of=as_of,
        )
        live_execution_requested = policy.auto_execution_mode == AutoExecutionMode.LIVE
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
            checks.append(
                AutopilotPreflightCheck(
                    name="position_reconciliation",
                    status=(
                        "pass"
                        if policy.require_position_reconciliation and position_reconciliation_gate["passed"]
                        else "warn"
                    ),
                    message_cn=(
                        "自动执行前要求最近一次仓位核对通过，当前门禁已通过。"
                        if policy.require_position_reconciliation and position_reconciliation_gate["passed"]
                        else "自动执行前要求最近一次仓位核对通过；当前未通过，但自动执行未启用。"
                        if policy.require_position_reconciliation
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
            round(portfolio_summary.open_risk_to_stop / policy.account_equity, 6)
            if policy.account_equity > 0
            else 1.0
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

    def get_autopilot_daily_usage(
        self,
        policy: AutopilotPolicy | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        timestamp = as_of or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        eastern = timestamp.astimezone(ZoneInfo("America/New_York"))
        trading_day = eastern.date().isoformat()
        runs = self.list_system_cycle_runs(limit=1000, status="success")
        used_approvals = 0
        used_buys = 0
        used_sells = 0
        for run in runs:
            run_day = run.started_at.astimezone(ZoneInfo("America/New_York")).date().isoformat()
            if run_day != trading_day:
                continue
            auto_approval = run.metrics.get("auto_approval", {})
            auto_execution = run.metrics.get("auto_execution", {})
            used_approvals += int(auto_approval.get("approved_count") or 0)
            used_buys += int(auto_execution.get("buy_order_count") or 0)
            used_sells += int(auto_execution.get("sell_order_count") or 0)

        policy = policy or self.get_autopilot_policy()
        return {
            "trading_day": trading_day,
            "used_approvals": used_approvals,
            "used_buys": used_buys,
            "used_sells": used_sells,
            "max_daily_auto_approvals": policy.max_daily_auto_approvals,
            "max_daily_auto_buys": policy.max_daily_auto_buys,
            "max_daily_auto_sells": policy.max_daily_auto_sells,
            "remaining_approvals": max(0, policy.max_daily_auto_approvals - used_approvals),
            "remaining_buys": max(0, policy.max_daily_auto_buys - used_buys),
            "remaining_sells": max(0, policy.max_daily_auto_sells - used_sells),
        }

    def get_autopilot_daily_loss_gate(
        self,
        policy: AutopilotPolicy | None = None,
        as_of: datetime | None = None,
        account_equity: float | None = None,
        max_daily_realized_loss_pct: float | None = None,
    ) -> dict[str, Any]:
        timestamp = as_of or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        eastern = timestamp.astimezone(ZoneInfo("America/New_York"))
        trading_day = eastern.date().isoformat()
        policy = policy or self.get_autopilot_policy()
        equity = float(account_equity if account_equity is not None else policy.account_equity)
        loss_limit = float(
            max_daily_realized_loss_pct
            if max_daily_realized_loss_pct is not None
            else policy.max_daily_realized_loss_pct
        )
        sell_trades = self.list_trade_ledger(limit=10000, side=TradeSide.SELL)
        daily_sell_trades = []
        daily_realized_pnl = 0.0
        for trade in sell_trades:
            executed_at = trade.executed_at
            if executed_at.tzinfo is None:
                executed_at = executed_at.replace(tzinfo=timezone.utc)
            trade_day = executed_at.astimezone(ZoneInfo("America/New_York")).date().isoformat()
            if trade_day != trading_day:
                continue
            daily_sell_trades.append(trade)
            daily_realized_pnl += trade.realized_pnl_delta

        daily_realized_pnl = round(daily_realized_pnl, 6)
        daily_realized_loss = round(max(0.0, -daily_realized_pnl), 6)
        daily_realized_loss_pct = round(daily_realized_loss / equity, 6) if equity > 0 else 1.0
        passed = daily_realized_loss_pct <= loss_limit
        return {
            "passed": passed,
            "reason": None if passed else "daily_realized_loss_above_policy_limit",
            "trading_day": trading_day,
            "daily_realized_pnl": daily_realized_pnl,
            "daily_realized_loss": daily_realized_loss,
            "daily_realized_loss_pct": daily_realized_loss_pct,
            "max_daily_realized_loss_pct": loss_limit,
            "account_equity": equity,
            "sell_trade_count": len(daily_sell_trades),
        }

    def get_market_session_status(self, as_of: datetime | None = None) -> MarketSessionStatus:
        timestamp = as_of or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        eastern = timestamp.astimezone(ZoneInfo("America/New_York"))
        regular_open = time(hour=9, minute=30)
        regular_close = time(hour=16, minute=0)
        local_time = eastern.time().replace(tzinfo=None)
        is_weekday = eastern.weekday() < 5
        is_regular_session = is_weekday and regular_open <= local_time < regular_close
        return MarketSessionStatus(
            as_of=timestamp,
            local_time=eastern.strftime("%Y-%m-%d %H:%M:%S %Z"),
            is_weekday=is_weekday,
            is_regular_session=is_regular_session,
            status="regular" if is_regular_session else "closed",
        )

    def get_rebuy_cooldown(
        self,
        ticker: str,
        cooldown_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        if cooldown_minutes <= 0:
            return {"active": False, "ticker": ticker.upper(), "cooldown_minutes": cooldown_minutes}
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        recent_sells = self.list_trade_ledger(
            limit=100,
            ticker=ticker.upper(),
            side=TradeSide.SELL,
        )
        last_sell = None
        sold_at = None
        for candidate in recent_sells:
            candidate_sold_at = candidate.executed_at
            if candidate_sold_at.tzinfo is None:
                candidate_sold_at = candidate_sold_at.replace(tzinfo=timezone.utc)
            candidate_sold_at = candidate_sold_at.astimezone(timezone.utc)
            if candidate_sold_at <= now:
                last_sell = candidate
                sold_at = candidate_sold_at
                break
        if last_sell is None or sold_at is None:
            return {"active": False, "ticker": ticker.upper(), "cooldown_minutes": cooldown_minutes}
        cooldown_until = sold_at + timedelta(minutes=cooldown_minutes)
        active = now < cooldown_until
        return {
            "active": active,
            "ticker": ticker.upper(),
            "cooldown_minutes": cooldown_minutes,
            "last_sell_trade_id": last_sell.trade_id,
            "last_sell_at": sold_at.isoformat(),
            "cooldown_until": cooldown_until.isoformat(),
            "minutes_remaining": max(0, int((cooldown_until - now).total_seconds() // 60)),
        }

    def get_sell_alert_cooldown(
        self,
        ticker: str,
        reason_code: str,
        cooldown_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        if cooldown_minutes <= 0:
            return {
                "active": False,
                "ticker": ticker_upper,
                "reason_code": reason_code,
                "cooldown_minutes": cooldown_minutes,
            }
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        alert_marker = f"alert:{reason_code}"
        recent_executions = self.list_sell_execution_audits(limit=1000, ticker=ticker_upper)
        for execution in recent_executions:
            if alert_marker not in (execution.reason or ""):
                continue
            submitted_at = execution.submitted_at
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=timezone.utc)
            submitted_at = submitted_at.astimezone(timezone.utc)
            if submitted_at > now:
                continue
            cooldown_until = submitted_at + timedelta(minutes=cooldown_minutes)
            if now >= cooldown_until:
                continue
            return {
                "active": True,
                "ticker": ticker_upper,
                "reason_code": reason_code,
                "cooldown_minutes": cooldown_minutes,
                "last_sell_execution_id": execution.id,
                "last_sell_reason": execution.reason,
                "last_sell_at": submitted_at.isoformat(),
                "cooldown_until": cooldown_until.isoformat(),
                "minutes_remaining": max(0, int((cooldown_until - now).total_seconds() // 60)),
            }
        return {
            "active": False,
            "ticker": ticker_upper,
            "reason_code": reason_code,
            "cooldown_minutes": cooldown_minutes,
        }

    def get_recent_buy_order_gate(
        self,
        ticker: str,
        recommendation_id: str,
        order_dedupe_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        if order_dedupe_minutes <= 0:
            return {
                "passed": True,
                "ticker": ticker_upper,
                "recommendation_id": recommendation_id,
                "order_dedupe_minutes": order_dedupe_minutes,
            }
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        recent_orders = self.list_paper_orders(limit=1000, side=Direction.BUY)
        for order in recent_orders:
            if order.status == PaperOrderStatus.CANCELED:
                continue
            submitted_at = order.submitted_at
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=timezone.utc)
            submitted_at = submitted_at.astimezone(timezone.utc)
            if submitted_at > now:
                continue
            cooldown_until = submitted_at + timedelta(minutes=order_dedupe_minutes)
            if now >= cooldown_until:
                continue

            match_reason = None
            if order.recommendation_id == recommendation_id:
                match_reason = "same_recommendation_recent_buy_order"
            else:
                source_recommendation = (
                    self.recommendations_by_id.get(order.recommendation_id)
                    or self.recommendation_repo.get(order.recommendation_id)
                )
                if source_recommendation is not None and source_recommendation.ticker.upper() == ticker_upper:
                    match_reason = "same_ticker_recent_buy_order"
            if match_reason is None:
                continue

            return {
                "passed": False,
                "reason": match_reason,
                "ticker": ticker_upper,
                "recommendation_id": recommendation_id,
                "order_dedupe_minutes": order_dedupe_minutes,
                "recent_order_id": order.id,
                "recent_order_recommendation_id": order.recommendation_id,
                "recent_order_status": order.status.value,
                "recent_order_execution_mode": order.execution_mode.value,
                "recent_order_submitted_at": submitted_at.isoformat(),
                "dedupe_until": cooldown_until.isoformat(),
                "minutes_remaining": max(0, int((cooldown_until - now).total_seconds() // 60)),
            }

        return {
            "passed": True,
            "ticker": ticker_upper,
            "recommendation_id": recommendation_id,
            "order_dedupe_minutes": order_dedupe_minutes,
        }

    def get_pending_buy_order_gate(
        self,
        *,
        ticker: str,
        recommendation_id: str,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        pending_orders = self.list_paper_orders(
            limit=1000,
            side=Direction.BUY,
            status=PaperOrderStatus.SUBMITTED,
        )
        for order in pending_orders:
            match_reason = None
            if order.recommendation_id == recommendation_id:
                match_reason = "same_recommendation_pending_buy_order"
            else:
                source_recommendation = (
                    self.recommendations_by_id.get(order.recommendation_id)
                    or self.recommendation_repo.get(order.recommendation_id)
                )
                if source_recommendation is not None and source_recommendation.ticker.upper() == ticker_upper:
                    match_reason = "same_ticker_pending_buy_order"
            if match_reason is None:
                continue

            return {
                "passed": False,
                "reason": match_reason,
                "ticker": ticker_upper,
                "recommendation_id": recommendation_id,
                "pending_order_id": order.id,
                "pending_order_recommendation_id": order.recommendation_id,
                "pending_order_status": order.status.value,
                "pending_order_execution_mode": order.execution_mode.value,
                "pending_order_submitted_at": order.submitted_at.isoformat(),
                "pending_order_broker_order_id": order.broker_order_id,
            }

        return {
            "passed": True,
            "ticker": ticker_upper,
            "recommendation_id": recommendation_id,
        }

    def update_autopilot_policy(self, updates: dict[str, Any]) -> AutopilotPolicy:
        current = self.get_autopilot_policy()
        payload = current.model_dump(mode="python")
        payload.pop("policy_id", None)
        payload.update(updates)
        payload["updated_at"] = datetime.now(timezone.utc)
        payload["updated_by"] = str(payload.get("updated_by") or "operator")
        policy = AutopilotPolicy(**payload)
        self.autopilot_policy = self.autopilot_policy_repo.set_policy(policy)
        self.metrics_store.set_gauge("autopilot_policy_enabled", 1.0 if policy.enabled else 0.0)
        self.metrics_store.set_gauge(
            "autopilot_auto_approval_enabled",
            1.0 if policy.enabled and policy.auto_approve_recommendations else 0.0,
        )
        self.metrics_store.set_gauge(
            "autopilot_auto_execution_enabled",
            1.0 if policy.enabled and policy.auto_execute_approved else 0.0,
        )
        return self.autopilot_policy

    def publish_event(self, event_type: EventType, payload: dict) -> None:
        event = SystemEvent(event_type=event_type, payload=payload)
        self.event_queue.publish(event)
        self.system_event_repo.add(event)
        self.metrics_store.inc(f"event_published_{event_type.value}")
        self.metrics_store.set_gauge("event_queue_pending", self.pending_event_count())

    def record_manual_buy(self, request: ManualBuyRequest) -> HoldingWatch:
        ticker = request.ticker.upper()
        recommendation = None
        if request.source_recommendation_id:
            recommendation = self._get_recommendation_by_id(request.source_recommendation_id)
        provenance = self._recommendation_provenance(
            request.source_recommendation_id,
            recommendation=recommendation,
        )

        stop_loss = (
            request.stop_loss
            if request.stop_loss is not None
            else (recommendation.stop_loss if recommendation is not None else request.buy_price * 0.92)
        )
        take_profit1 = (
            request.take_profit1
            if request.take_profit1 is not None
            else (recommendation.tp1 if recommendation is not None else request.buy_price * 1.10)
        )
        take_profit2 = (
            request.take_profit2
            if request.take_profit2 is not None
            else (recommendation.tp2 if recommendation is not None else request.buy_price * 1.18)
        )

        existing = self.holdings_by_ticker.get(ticker)
        if existing is None:
            existing = self.holding_watch_repo.get(ticker)

        bought_at = request.bought_at or datetime.now(timezone.utc)
        if existing is not None and existing.status == HoldingStatus.OPEN:
            total_qty = existing.qty + request.qty
            avg_buy = (
                (existing.avg_buy_price * existing.qty + request.buy_price * request.qty) / max(total_qty, 1e-9)
            )
            holding = HoldingWatch(
                ticker=ticker,
                qty=round(total_qty, 6),
                avg_buy_price=round(avg_buy, 6),
                bought_at=existing.bought_at,
                source_recommendation_id=request.source_recommendation_id or existing.source_recommendation_id,
                stop_loss=float(stop_loss if request.stop_loss is not None else existing.stop_loss),
                take_profit1=float(take_profit1 if request.take_profit1 is not None else existing.take_profit1),
                take_profit2=float(take_profit2 if request.take_profit2 is not None else existing.take_profit2),
                note=request.note or existing.note,
                status=HoldingStatus.OPEN,
                updated_at=datetime.now(timezone.utc),
                realized_pnl=existing.realized_pnl,
                closed_at=None,
                last_sell_price=existing.last_sell_price,
                last_sell_reason=existing.last_sell_reason,
            )
        else:
            holding = HoldingWatch(
                ticker=ticker,
                qty=round(request.qty, 6),
                avg_buy_price=round(request.buy_price, 6),
                bought_at=bought_at,
                source_recommendation_id=request.source_recommendation_id,
                stop_loss=float(stop_loss),
                take_profit1=float(take_profit1),
                take_profit2=float(take_profit2),
                note=request.note,
                status=HoldingStatus.OPEN,
                updated_at=datetime.now(timezone.utc),
                realized_pnl=0.0,
                closed_at=None,
            )

        self.holdings_by_ticker[ticker] = holding
        self.holding_watch_repo.upsert(holding)
        self._record_trade(
            TradeLedgerEntry(
                trade_id=uuid4().hex[:16],
                ticker=ticker,
                side=TradeSide.BUY,
                qty=round(request.qty, 6),
                price=round(request.buy_price, 6),
                executed_at=bought_at,
                source_recommendation_id=request.source_recommendation_id,
                source_snapshot_id=provenance["source_snapshot_id"],
                strategy_config_id=provenance["strategy_config_id"],
                reason=request.note,
                holding_status_after=HoldingStatus.OPEN,
            )
        )
        self.metrics_store.inc("manual_buys")
        self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        return holding

    def update_holding_controls(
        self,
        ticker: str,
        request: HoldingControlUpdateRequest,
    ) -> HoldingControlUpdateResult:
        ticker_upper = ticker.upper()
        holding = self.holdings_by_ticker.get(ticker_upper)
        if holding is None:
            holding = self.holding_watch_repo.get(ticker_upper)
        if holding is None or holding.status != HoldingStatus.OPEN:
            raise KeyError("open holding not found")

        if (
            request.stop_loss is None
            and request.take_profit1 is None
            and request.take_profit2 is None
            and request.note is None
        ):
            raise ValueError("at least one control field must be provided")
        provenance = self._recommendation_provenance(holding.source_recommendation_id)

        new_stop = float(request.stop_loss if request.stop_loss is not None else holding.stop_loss)
        new_tp1 = float(request.take_profit1 if request.take_profit1 is not None else holding.take_profit1)
        new_tp2 = float(request.take_profit2 if request.take_profit2 is not None else holding.take_profit2)
        new_note = request.note if request.note is not None else holding.note
        if not new_stop < new_tp1 < new_tp2:
            raise ValueError("holding controls must satisfy stop_loss < take_profit1 < take_profit2")

        updated_at = datetime.now(timezone.utc)
        updated = HoldingWatch(
            ticker=holding.ticker,
            qty=holding.qty,
            avg_buy_price=holding.avg_buy_price,
            bought_at=holding.bought_at,
            source_recommendation_id=holding.source_recommendation_id,
            stop_loss=round(new_stop, 6),
            take_profit1=round(new_tp1, 6),
            take_profit2=round(new_tp2, 6),
            note=new_note,
            status=HoldingStatus.OPEN,
            updated_at=updated_at,
            realized_pnl=holding.realized_pnl,
            closed_at=None,
            last_sell_price=holding.last_sell_price,
            last_sell_reason=holding.last_sell_reason,
        )
        audit = HoldingControlAudit(
            id=uuid4().hex[:16],
            ticker=ticker_upper,
            source_recommendation_id=holding.source_recommendation_id,
            source_snapshot_id=provenance["source_snapshot_id"],
            strategy_config_id=provenance["strategy_config_id"],
            old_stop_loss=holding.stop_loss,
            new_stop_loss=updated.stop_loss,
            old_take_profit1=holding.take_profit1,
            new_take_profit1=updated.take_profit1,
            old_take_profit2=holding.take_profit2,
            new_take_profit2=updated.take_profit2,
            old_note=holding.note,
            new_note=updated.note,
            reason=request.reason,
            updated_by=request.updated_by,
            updated_at=updated_at,
        )
        self.holdings_by_ticker[ticker_upper] = updated
        self.holding_watch_repo.upsert(updated)
        self.holding_control_audit_repo.add(audit)
        self.metrics_store.inc("holding_control_updates")
        self.publish_event(
            EventType.HOLDING_CONTROLS_UPDATED,
            {
                "audit_id": audit.id,
                "ticker": ticker_upper,
                "source_recommendation_id": holding.source_recommendation_id,
                "source_snapshot_id": provenance["source_snapshot_id"],
                "strategy_config_id": provenance["strategy_config_id"],
                "old_stop_loss": holding.stop_loss,
                "new_stop_loss": updated.stop_loss,
                "old_take_profit1": holding.take_profit1,
                "new_take_profit1": updated.take_profit1,
                "old_take_profit2": holding.take_profit2,
                "new_take_profit2": updated.take_profit2,
                "reason": request.reason,
                "updated_by": request.updated_by,
            },
        )
        return HoldingControlUpdateResult(
            holding=updated,
            audit=audit,
            message_cn=(
                f"{ticker_upper} 风控参数已更新：止损 {updated.stop_loss:.2f}，"
                f"目标 {updated.take_profit1:.2f}/{updated.take_profit2:.2f}。"
            ),
        )

    def list_holding_control_audits(
        self,
        limit: int = 100,
        ticker: str | None = None,
    ) -> list[HoldingControlAudit]:
        return self.holding_control_audit_repo.list_recent(limit=limit, ticker=ticker)

    def sell_holding(self, ticker: str, request: ManualSellRequest) -> SellExecutionResult:
        ticker_upper = ticker.upper()
        holding = self.holdings_by_ticker.get(ticker_upper)
        if holding is None:
            holding = self.holding_watch_repo.get(ticker_upper)
        if holding is None or holding.status != HoldingStatus.OPEN:
            raise KeyError("open holding not found")
        provenance = self._recommendation_provenance(holding.source_recommendation_id)

        sell_qty = request.qty if request.qty is not None else holding.qty
        if sell_qty <= 0:
            raise ValueError("qty must be greater than 0")
        if sell_qty > holding.qty:
            raise ValueError("sell qty cannot exceed open holding qty")

        execution_mode = OrderExecutionMode(request.execution_mode)
        if execution_mode == OrderExecutionMode.LIVE:
            if not request.dry_run and not request.confirm_live:
                raise ValueError("Live sell execution requires dry_run=true or confirm_live=true")
            if not request.dry_run:
                return self._sell_holding_live_broker(
                    holding=holding,
                    request=request,
                    sell_qty=sell_qty,
                    provenance=provenance,
                )

            estimated_delta = (request.sell_price - holding.avg_buy_price) * sell_qty
            projected_remaining_qty = round(max(0.0, holding.qty - sell_qty), 6)
            broker_order_id = f"live_sell_dryrun_{uuid4().hex[:12]}"
            adapter_message = "live_sell_dry_run_only: order was validated but not sent to a broker"
            submitted_at = datetime.now(timezone.utc)
            audit = SellExecutionAudit(
                id=uuid4().hex[:16],
                ticker=ticker_upper,
                qty=round(sell_qty, 6),
                sell_price=request.sell_price,
                submitted_at=submitted_at,
                execution_mode=OrderExecutionMode.LIVE,
                dry_run=True,
                broker_order_id=broker_order_id,
                adapter_message=adapter_message,
                applied_to_ledger=False,
                status="dry_run",
                reason=request.reason,
                source_recommendation_id=holding.source_recommendation_id,
                source_snapshot_id=provenance["source_snapshot_id"],
                strategy_config_id=provenance["strategy_config_id"],
                realized_pnl_delta=0.0,
                estimated_realized_pnl_delta=round(estimated_delta, 6),
                remaining_qty=holding.qty,
                holding_status_after=holding.status,
            )
            self._record_sell_execution_audit(audit)
            self.publish_event(
                EventType.SELL_ROUTED,
                {
                    "sell_execution_id": audit.id,
                    "ticker": ticker_upper,
                    "side": TradeSide.SELL.value,
                    "qty": round(sell_qty, 6),
                    "sell_price": request.sell_price,
                    "execution_mode": execution_mode.value,
                    "dry_run": True,
                    "broker_order_id": broker_order_id,
                    "adapter_message": adapter_message,
                    "applied_to_ledger": False,
                    "reason": request.reason,
                    "source_recommendation_id": holding.source_recommendation_id,
                    "source_snapshot_id": provenance["source_snapshot_id"],
                    "strategy_config_id": provenance["strategy_config_id"],
                },
            )
            return SellExecutionResult(
                sell_execution_id=audit.id,
                holding=holding,
                sold_qty=round(sell_qty, 6),
                sell_price=request.sell_price,
                realized_pnl_delta=0.0,
                estimated_realized_pnl_delta=round(estimated_delta, 6),
                total_realized_pnl=holding.realized_pnl,
                remaining_qty=holding.qty,
                execution_mode=OrderExecutionMode.LIVE,
                dry_run=True,
                broker_order_id=broker_order_id,
                adapter_message=adapter_message,
                applied_to_ledger=False,
                message_cn=(
                    f"{ticker_upper} live dry-run 已校验卖出 {sell_qty:g} 股，"
                    f"预计盈亏 {estimated_delta:.2f}，若执行后预计剩余 {projected_remaining_qty:g} 股；"
                    "本次未发送券商，持仓和交易流水未改变。"
                ),
            )

        sold_at = request.sold_at or datetime.now(timezone.utc)
        realized_delta = (request.sell_price - holding.avg_buy_price) * sell_qty
        remaining_qty = round(max(0.0, holding.qty - sell_qty), 6)
        is_closed = remaining_qty <= 1e-9
        total_realized = round(holding.realized_pnl + realized_delta, 6)
        broker_order_id = f"paper_sell_{uuid4().hex[:12]}"
        adapter_message = "paper_sell_fill_recorded"

        updated = HoldingWatch(
            ticker=holding.ticker,
            qty=0.0 if is_closed else remaining_qty,
            avg_buy_price=holding.avg_buy_price,
            bought_at=holding.bought_at,
            source_recommendation_id=holding.source_recommendation_id,
            stop_loss=holding.stop_loss,
            take_profit1=holding.take_profit1,
            take_profit2=holding.take_profit2,
            note=holding.note,
            status=HoldingStatus.CLOSED if is_closed else HoldingStatus.OPEN,
            updated_at=sold_at,
            realized_pnl=total_realized,
            closed_at=sold_at if is_closed else None,
            last_sell_price=request.sell_price,
            last_sell_reason=request.reason,
        )

        self.holdings_by_ticker[ticker_upper] = updated
        self.holding_watch_repo.upsert(updated)
        self._record_trade(
            TradeLedgerEntry(
                trade_id=uuid4().hex[:16],
                ticker=ticker_upper,
                side=TradeSide.SELL,
                qty=round(sell_qty, 6),
                price=round(request.sell_price, 6),
                executed_at=sold_at,
                source_recommendation_id=holding.source_recommendation_id,
                source_snapshot_id=provenance["source_snapshot_id"],
                strategy_config_id=provenance["strategy_config_id"],
                reason=request.reason,
                realized_pnl_delta=round(realized_delta, 6),
                holding_status_after=updated.status,
            )
        )
        audit = SellExecutionAudit(
            id=uuid4().hex[:16],
            ticker=ticker_upper,
            qty=round(sell_qty, 6),
            sell_price=request.sell_price,
            submitted_at=sold_at,
            execution_mode=OrderExecutionMode.PAPER,
            dry_run=False,
            broker_order_id=broker_order_id,
            adapter_message=adapter_message,
            applied_to_ledger=True,
            status="filled",
            reason=request.reason,
            source_recommendation_id=holding.source_recommendation_id,
            source_snapshot_id=provenance["source_snapshot_id"],
            strategy_config_id=provenance["strategy_config_id"],
            realized_pnl_delta=round(realized_delta, 6),
            estimated_realized_pnl_delta=round(realized_delta, 6),
            remaining_qty=updated.qty,
            holding_status_after=updated.status,
        )
        self._record_sell_execution_audit(audit)
        self.metrics_store.inc("manual_sells")
        self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        self.publish_event(
            EventType.PORTFOLIO_SELL,
            {
                "sell_execution_id": audit.id,
                "ticker": ticker_upper,
                "sold_qty": round(sell_qty, 6),
                "sell_price": request.sell_price,
                "realized_pnl_delta": round(realized_delta, 6),
                "remaining_qty": updated.qty,
                "status": updated.status.value,
                "reason": request.reason,
                "execution_mode": OrderExecutionMode.PAPER.value,
                "dry_run": False,
                "broker_order_id": broker_order_id,
                "adapter_message": adapter_message,
                "applied_to_ledger": True,
                "source_recommendation_id": holding.source_recommendation_id,
                "source_snapshot_id": provenance["source_snapshot_id"],
                "strategy_config_id": provenance["strategy_config_id"],
            },
        )

        action = "全部卖出并关闭持仓" if is_closed else f"卖出 {sell_qty:g} 股，剩余 {remaining_qty:g} 股"
        return SellExecutionResult(
            sell_execution_id=audit.id,
            holding=updated,
            sold_qty=round(sell_qty, 6),
            sell_price=request.sell_price,
            realized_pnl_delta=round(realized_delta, 6),
            estimated_realized_pnl_delta=round(realized_delta, 6),
            total_realized_pnl=total_realized,
            remaining_qty=updated.qty,
            execution_mode=OrderExecutionMode.PAPER,
            dry_run=False,
            broker_order_id=broker_order_id,
            adapter_message=adapter_message,
            applied_to_ledger=True,
            message_cn=f"{ticker_upper} 已{action}，本次已实现盈亏 {realized_delta:.2f}。",
        )

    def _sell_holding_live_broker(
        self,
        *,
        holding: HoldingWatch,
        request: ManualSellRequest,
        sell_qty: float,
        provenance: dict[str, str | None],
    ) -> SellExecutionResult:
        ticker_upper = holding.ticker.upper()
        broker_adapter = self.execution_router.broker_adapter
        if broker_adapter is None:
            raise NotImplementedError("Live broker sell adapter is not configured")

        client_order_id = f"quant_sell_{uuid4().hex[:16]}"
        try:
            broker_update = broker_adapter.submit_order(
                BrokerOrderPlacement(
                    client_order_id=client_order_id,
                    symbol=ticker_upper,
                    qty=round(sell_qty, 6),
                    side=TradeSide.SELL.value,
                    limit_price=request.sell_price,
                )
            )
        except BrokerAdapterError:
            raise
        except Exception as exc:
            raise BrokerAdapterError(f"Live broker sell submit failed: {exc}") from exc

        raw_status = broker_update.raw_status.lower()
        adapter_message = (
            f"{broker_adapter.name}: status={broker_update.raw_status}; "
            f"client_order_id={broker_update.client_order_id or client_order_id}"
        )
        if broker_update.message:
            adapter_message = f"{adapter_message}; message={broker_update.message}"
        submitted_at = broker_update.submitted_at or datetime.now(timezone.utc)
        estimated_delta = (request.sell_price - holding.avg_buy_price) * sell_qty

        if raw_status in {"filled", "partially_filled"}:
            if broker_update.filled_avg_price is None:
                raise BrokerAdapterError("Live broker returned sell fill without filled_avg_price")
            filled_qty = broker_update.filled_qty if broker_update.filled_qty is not None else sell_qty
            if filled_qty <= 0:
                raise BrokerAdapterError("Live broker returned sell fill without a positive filled quantity")
            if filled_qty > holding.qty + 1e-9:
                raise BrokerAdapterError("Live broker sell fill exceeds open holding quantity")

            actual_qty = round(filled_qty, 6)
            actual_price = round(float(broker_update.filled_avg_price), 6)
            sold_at = broker_update.filled_at or submitted_at
            realized_delta = (actual_price - holding.avg_buy_price) * actual_qty
            remaining_qty = round(max(0.0, holding.qty - actual_qty), 6)
            is_closed = remaining_qty <= 1e-9
            total_realized = round(holding.realized_pnl + realized_delta, 6)
            updated = HoldingWatch(
                ticker=holding.ticker,
                qty=0.0 if is_closed else remaining_qty,
                avg_buy_price=holding.avg_buy_price,
                bought_at=holding.bought_at,
                source_recommendation_id=holding.source_recommendation_id,
                stop_loss=holding.stop_loss,
                take_profit1=holding.take_profit1,
                take_profit2=holding.take_profit2,
                note=holding.note,
                status=HoldingStatus.CLOSED if is_closed else HoldingStatus.OPEN,
                updated_at=sold_at,
                realized_pnl=total_realized,
                closed_at=sold_at if is_closed else None,
                last_sell_price=actual_price,
                last_sell_reason=request.reason,
            )
            self.holdings_by_ticker[ticker_upper] = updated
            self.holding_watch_repo.upsert(updated)
            self._record_trade(
                TradeLedgerEntry(
                    trade_id=uuid4().hex[:16],
                    ticker=ticker_upper,
                    side=TradeSide.SELL,
                    qty=actual_qty,
                    price=actual_price,
                    executed_at=sold_at,
                    source_recommendation_id=holding.source_recommendation_id,
                    source_snapshot_id=provenance["source_snapshot_id"],
                    strategy_config_id=provenance["strategy_config_id"],
                    reason=request.reason,
                    realized_pnl_delta=round(realized_delta, 6),
                    holding_status_after=updated.status,
                )
            )
            audit = SellExecutionAudit(
                id=uuid4().hex[:16],
                ticker=ticker_upper,
                qty=actual_qty,
                sell_price=actual_price,
                submitted_at=sold_at,
                execution_mode=OrderExecutionMode.LIVE,
                dry_run=False,
                broker_order_id=broker_update.broker_order_id,
                adapter_message=adapter_message,
                applied_to_ledger=True,
                status=raw_status,
                reason=request.reason,
                source_recommendation_id=holding.source_recommendation_id,
                source_snapshot_id=provenance["source_snapshot_id"],
                strategy_config_id=provenance["strategy_config_id"],
                realized_pnl_delta=round(realized_delta, 6),
                estimated_realized_pnl_delta=round(estimated_delta, 6),
                remaining_qty=updated.qty,
                holding_status_after=updated.status,
            )
            self._record_sell_execution_audit(audit)
            self.metrics_store.inc("live_sells")
            self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
            self.publish_event(
                EventType.PORTFOLIO_SELL,
                {
                    "sell_execution_id": audit.id,
                    "ticker": ticker_upper,
                    "sold_qty": actual_qty,
                    "sell_price": actual_price,
                    "realized_pnl_delta": round(realized_delta, 6),
                    "remaining_qty": updated.qty,
                    "status": updated.status.value,
                    "reason": request.reason,
                    "execution_mode": OrderExecutionMode.LIVE.value,
                    "dry_run": False,
                    "broker_order_id": broker_update.broker_order_id,
                    "adapter_message": adapter_message,
                    "applied_to_ledger": True,
                    "source_recommendation_id": holding.source_recommendation_id,
                    "source_snapshot_id": provenance["source_snapshot_id"],
                    "strategy_config_id": provenance["strategy_config_id"],
                },
            )
            action = "全部卖出并关闭持仓" if is_closed else f"卖出 {actual_qty:g} 股，剩余 {remaining_qty:g} 股"
            return SellExecutionResult(
                sell_execution_id=audit.id,
                holding=updated,
                sold_qty=actual_qty,
                sell_price=actual_price,
                realized_pnl_delta=round(realized_delta, 6),
                estimated_realized_pnl_delta=round(estimated_delta, 6),
                total_realized_pnl=total_realized,
                remaining_qty=updated.qty,
                execution_mode=OrderExecutionMode.LIVE,
                dry_run=False,
                broker_order_id=broker_update.broker_order_id,
                adapter_message=adapter_message,
                applied_to_ledger=True,
                message_cn=f"{ticker_upper} live broker 已{action}，本次已实现盈亏 {realized_delta:.2f}。",
            )

        terminal_without_fill = raw_status in {"canceled", "expired", "rejected"}
        audit_status = "canceled" if terminal_without_fill else "submitted"
        audit = SellExecutionAudit(
            id=uuid4().hex[:16],
            ticker=ticker_upper,
            qty=round(sell_qty, 6),
            sell_price=request.sell_price,
            submitted_at=submitted_at,
            execution_mode=OrderExecutionMode.LIVE,
            dry_run=False,
            broker_order_id=broker_update.broker_order_id,
            adapter_message=adapter_message,
            applied_to_ledger=False,
            status=audit_status,
            reason=request.reason,
            source_recommendation_id=holding.source_recommendation_id,
            source_snapshot_id=provenance["source_snapshot_id"],
            strategy_config_id=provenance["strategy_config_id"],
            realized_pnl_delta=0.0,
            estimated_realized_pnl_delta=round(estimated_delta, 6),
            remaining_qty=holding.qty,
            holding_status_after=holding.status,
        )
        self._record_sell_execution_audit(audit)
        self.publish_event(
            EventType.SELL_ROUTED,
            {
                "sell_execution_id": audit.id,
                "ticker": ticker_upper,
                "side": TradeSide.SELL.value,
                "qty": round(sell_qty, 6),
                "sell_price": request.sell_price,
                "execution_mode": OrderExecutionMode.LIVE.value,
                "dry_run": False,
                "broker_order_id": broker_update.broker_order_id,
                "adapter_message": adapter_message,
                "applied_to_ledger": False,
                "status": raw_status,
                "reason": request.reason,
                "source_recommendation_id": holding.source_recommendation_id,
                "source_snapshot_id": provenance["source_snapshot_id"],
                "strategy_config_id": provenance["strategy_config_id"],
            },
        )
        status_text = "未成交" if terminal_without_fill else "已提交，等待券商成交回报"
        return SellExecutionResult(
            sell_execution_id=audit.id,
            holding=holding,
            sold_qty=round(sell_qty, 6),
            sell_price=request.sell_price,
            realized_pnl_delta=0.0,
            estimated_realized_pnl_delta=round(estimated_delta, 6),
            total_realized_pnl=holding.realized_pnl,
            remaining_qty=holding.qty,
            execution_mode=OrderExecutionMode.LIVE,
            dry_run=False,
            broker_order_id=broker_update.broker_order_id,
            adapter_message=adapter_message,
            applied_to_ledger=False,
            message_cn=f"{ticker_upper} live broker 卖出订单{status_text}；本地持仓和交易流水未改变。",
        )

    def _get_sell_execution_audit_by_broker_order_id(self, broker_order_id: str | None) -> SellExecutionAudit | None:
        if not broker_order_id:
            return None
        return self.sell_execution_audit_repo.get_by_broker_order_id(broker_order_id)

    def _resolve_broker_sell_sync_audit(self, snapshot: BrokerOrderStatusSnapshot) -> SellExecutionAudit | None:
        if snapshot.order_id:
            audit = self.sell_execution_audit_repo.get(snapshot.order_id)
            if audit is not None:
                return audit
        return self._get_sell_execution_audit_by_broker_order_id(snapshot.broker_order_id)

    def _apply_sell_execution_fill_from_broker(
        self,
        *,
        audit: SellExecutionAudit,
        fill_price: float,
        filled_qty: float | None,
        filled_at: datetime,
        broker_status: BrokerOrderSyncStatus,
        adapter_message: str | None,
    ) -> SellExecutionAudit:
        ticker_upper = audit.ticker.upper()
        holding = self.holdings_by_ticker.get(ticker_upper)
        if holding is None:
            holding = self.holding_watch_repo.get(ticker_upper)
        if holding is None or holding.status != HoldingStatus.OPEN:
            raise ValueError("open holding not found for broker sell fill")

        already_applied_qty = audit.qty if audit.applied_to_ledger else 0.0
        cumulative_filled_qty = filled_qty if filled_qty is not None else audit.qty
        incremental_qty = round(cumulative_filled_qty - already_applied_qty, 6)
        if incremental_qty <= 0:
            return audit
        if incremental_qty > holding.qty + 1e-9:
            raise ValueError("broker sell fill exceeds open holding quantity")

        actual_price = round(fill_price, 6)
        realized_delta = (actual_price - holding.avg_buy_price) * incremental_qty
        remaining_qty = round(max(0.0, holding.qty - incremental_qty), 6)
        is_closed = remaining_qty <= 1e-9
        total_realized = round(holding.realized_pnl + realized_delta, 6)
        updated_holding = HoldingWatch(
            ticker=holding.ticker,
            qty=0.0 if is_closed else remaining_qty,
            avg_buy_price=holding.avg_buy_price,
            bought_at=holding.bought_at,
            source_recommendation_id=holding.source_recommendation_id,
            stop_loss=holding.stop_loss,
            take_profit1=holding.take_profit1,
            take_profit2=holding.take_profit2,
            note=holding.note,
            status=HoldingStatus.CLOSED if is_closed else HoldingStatus.OPEN,
            updated_at=filled_at,
            realized_pnl=total_realized,
            closed_at=filled_at if is_closed else None,
            last_sell_price=actual_price,
            last_sell_reason=audit.reason,
        )
        self.holdings_by_ticker[ticker_upper] = updated_holding
        self.holding_watch_repo.upsert(updated_holding)
        self._record_trade(
            TradeLedgerEntry(
                trade_id=uuid4().hex[:16],
                ticker=ticker_upper,
                side=TradeSide.SELL,
                qty=incremental_qty,
                price=actual_price,
                executed_at=filled_at,
                source_recommendation_id=audit.source_recommendation_id,
                source_snapshot_id=audit.source_snapshot_id,
                strategy_config_id=audit.strategy_config_id,
                reason=audit.reason,
                realized_pnl_delta=round(realized_delta, 6),
                holding_status_after=updated_holding.status,
            )
        )

        cumulative_qty = round(already_applied_qty + incremental_qty, 6)
        updated_audit = SellExecutionAudit(
            id=audit.id,
            ticker=ticker_upper,
            qty=cumulative_qty,
            sell_price=actual_price,
            submitted_at=audit.submitted_at,
            execution_mode=OrderExecutionMode.LIVE,
            dry_run=False,
            broker_order_id=audit.broker_order_id,
            adapter_message=adapter_message or audit.adapter_message,
            applied_to_ledger=True,
            status=broker_status.value,
            reason=audit.reason,
            source_recommendation_id=audit.source_recommendation_id,
            source_snapshot_id=audit.source_snapshot_id,
            strategy_config_id=audit.strategy_config_id,
            realized_pnl_delta=round(audit.realized_pnl_delta + realized_delta, 6),
            estimated_realized_pnl_delta=audit.estimated_realized_pnl_delta,
            remaining_qty=updated_holding.qty,
            holding_status_after=updated_holding.status,
        )
        self.sell_execution_audit_repo.add(updated_audit)
        self.metrics_store.inc("live_sell_sync_fills")
        self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        self.publish_event(
            EventType.PORTFOLIO_SELL,
            {
                "sell_execution_id": updated_audit.id,
                "ticker": ticker_upper,
                "sold_qty": incremental_qty,
                "sell_price": actual_price,
                "realized_pnl_delta": round(realized_delta, 6),
                "remaining_qty": updated_holding.qty,
                "status": updated_holding.status.value,
                "reason": audit.reason,
                "execution_mode": OrderExecutionMode.LIVE.value,
                "dry_run": False,
                "broker_order_id": audit.broker_order_id,
                "adapter_message": updated_audit.adapter_message,
                "applied_to_ledger": True,
                "broker_status": broker_status.value,
                "source_recommendation_id": audit.source_recommendation_id,
                "source_snapshot_id": audit.source_snapshot_id,
                "strategy_config_id": audit.strategy_config_id,
            },
        )
        return updated_audit

    def sync_broker_sell_statuses(self, request: BrokerOrderSyncRequest) -> BrokerOrderSyncResult:
        checked_at = request.checked_at or datetime.now(timezone.utc)
        if checked_at.tzinfo is None:
            checked_at = checked_at.replace(tzinfo=timezone.utc)
        else:
            checked_at = checked_at.astimezone(timezone.utc)

        items: list[BrokerOrderSyncItemResult] = []
        filled_count = 0
        canceled_count = 0
        unchanged_count = 0
        skipped_count = 0
        missing_count = 0

        for snapshot in request.statuses:
            audit = self._resolve_broker_sell_sync_audit(snapshot)
            if audit is None:
                missing_count += 1
                items.append(
                    BrokerOrderSyncItemResult(
                        order_id=snapshot.order_id,
                        broker_order_id=snapshot.broker_order_id,
                        broker_status=snapshot.status,
                        action="missing",
                        message_cn="未找到匹配的本地卖出审计记录。",
                    )
                )
                continue

            before_status = audit.status
            broker_note = snapshot.broker_message or snapshot.reason
            adapter_message = (
                f"{audit.adapter_message}; broker_sync_by={request.updated_by}:{request.broker}"
                if audit.adapter_message
                else f"broker_sync_by={request.updated_by}:{request.broker}"
            )
            if broker_note:
                adapter_message = f"{adapter_message}; message={broker_note}"

            try:
                if snapshot.status in {BrokerOrderSyncStatus.FILLED, BrokerOrderSyncStatus.PARTIALLY_FILLED}:
                    updated = self._apply_sell_execution_fill_from_broker(
                        audit=audit,
                        fill_price=float(snapshot.fill_price or 0.0),
                        filled_qty=snapshot.filled_qty,
                        filled_at=snapshot.filled_at or checked_at,
                        broker_status=snapshot.status,
                        adapter_message=adapter_message,
                    )
                    if updated is audit:
                        unchanged_count += 1
                        action = "unchanged"
                        message = "卖出审计已包含该 broker 成交数量。"
                    else:
                        filled_count += 1
                        action = "filled"
                        message = "已回写 broker 卖出成交。"
                    items.append(
                        BrokerOrderSyncItemResult(
                            order_id=updated.id,
                            broker_order_id=updated.broker_order_id,
                            broker_status=snapshot.status,
                            action=action,
                            before_status=before_status,
                            after_status=updated.status,
                            apply_to_ledger=True,
                            message_cn=message,
                        )
                    )
                elif snapshot.status in {BrokerOrderSyncStatus.CANCELED, BrokerOrderSyncStatus.REJECTED}:
                    if audit.applied_to_ledger:
                        skipped_count += 1
                        items.append(
                            BrokerOrderSyncItemResult(
                                order_id=audit.id,
                                broker_order_id=audit.broker_order_id,
                                broker_status=snapshot.status,
                                action="skipped",
                                before_status=before_status,
                                after_status=audit.status,
                                apply_to_ledger=audit.applied_to_ledger,
                                message_cn="本地卖出审计已落账，跳过 broker cancel/reject。",
                            )
                        )
                        continue
                    reason = audit.reason
                    if snapshot.reason:
                        reason = f"{reason}; {snapshot.reason}" if reason else snapshot.reason
                    updated = audit.model_copy(
                        update={
                            "status": "canceled",
                            "reason": reason,
                            "adapter_message": adapter_message,
                            "applied_to_ledger": False,
                        }
                    )
                    self.sell_execution_audit_repo.add(updated)
                    action = "unchanged" if before_status == "canceled" else "canceled"
                    if action == "canceled":
                        canceled_count += 1
                    else:
                        unchanged_count += 1
                    items.append(
                        BrokerOrderSyncItemResult(
                            order_id=updated.id,
                            broker_order_id=updated.broker_order_id,
                            broker_status=snapshot.status,
                            action=action,
                            before_status=before_status,
                            after_status=updated.status,
                            apply_to_ledger=False,
                            message_cn="已回写 broker 卖出取消/拒单。" if action == "canceled" else "卖出审计已是 canceled。",
                        )
                    )
                else:
                    if audit.applied_to_ledger:
                        skipped_count += 1
                        action = "skipped"
                        message = "本地卖出审计已落账，跳过 submitted 状态。"
                    else:
                        updated = audit.model_copy(update={"status": "submitted", "adapter_message": adapter_message})
                        self.sell_execution_audit_repo.add(updated)
                        audit = updated
                        unchanged_count += 1
                        action = "unchanged"
                        message = "broker 仍显示 submitted，本地保持待成交审计。"
                    items.append(
                        BrokerOrderSyncItemResult(
                            order_id=audit.id,
                            broker_order_id=audit.broker_order_id,
                            broker_status=snapshot.status,
                            action=action,
                            before_status=before_status,
                            after_status=audit.status,
                            apply_to_ledger=audit.applied_to_ledger,
                            message_cn=message,
                        )
                    )
            except ValueError as exc:
                skipped_count += 1
                current = self.sell_execution_audit_repo.get(audit.id) or audit
                items.append(
                    BrokerOrderSyncItemResult(
                        order_id=audit.id,
                        broker_order_id=audit.broker_order_id,
                        broker_status=snapshot.status,
                        action="skipped",
                        before_status=before_status,
                        after_status=current.status,
                        apply_to_ledger=current.applied_to_ledger,
                        message_cn=str(exc),
                    )
                )

        result = BrokerOrderSyncResult(
            broker=request.broker,
            checked_at=checked_at,
            total_count=len(request.statuses),
            filled_count=filled_count,
            canceled_count=canceled_count,
            unchanged_count=unchanged_count,
            skipped_count=skipped_count,
            missing_count=missing_count,
            items=items,
        )
        self.publish_event(
            EventType.BROKER_ORDER_SYNC,
            {
                "broker": result.broker,
                "resource": "sell_executions",
                "checked_at": result.checked_at.isoformat(),
                "total_count": result.total_count,
                "filled_count": result.filled_count,
                "canceled_count": result.canceled_count,
                "unchanged_count": result.unchanged_count,
                "skipped_count": result.skipped_count,
                "missing_count": result.missing_count,
            },
        )
        return result

    def list_open_holdings(self) -> list[HoldingWatch]:
        if self.holdings_by_ticker:
            return [item for item in self.holdings_by_ticker.values() if item.status == HoldingStatus.OPEN]
        holdings = self.holding_watch_repo.list_open()
        self.holdings_by_ticker = {item.ticker: item for item in holdings}
        return holdings

    def list_holdings(self, status: HoldingStatus | None = None, limit: int = 100) -> list[HoldingWatch]:
        if status == HoldingStatus.OPEN:
            return self.list_open_holdings()[:limit]
        holdings = self.holding_watch_repo.list_by_status(status=status, limit=limit)
        for holding in holdings:
            self.holdings_by_ticker[holding.ticker] = holding
        return holdings

    def list_trade_ledger(
        self,
        limit: int = 100,
        ticker: str | None = None,
        side: TradeSide | None = None,
    ) -> list[TradeLedgerEntry]:
        return self.trade_ledger_repo.list_recent(limit=limit, ticker=ticker, side=side)

    def list_sell_execution_audits(
        self,
        limit: int = 100,
        ticker: str | None = None,
        dry_run: bool | None = None,
        applied_to_ledger: bool | None = None,
    ) -> list[SellExecutionAudit]:
        return self.sell_execution_audit_repo.list_recent(
            limit=limit,
            ticker=ticker,
            dry_run=dry_run,
            applied_to_ledger=applied_to_ledger,
        )

    def reconcile_broker_positions(
        self,
        request: PositionReconciliationRequest,
    ) -> PositionReconciliationReport:
        checked_at = datetime.now(timezone.utc)
        as_of = request.as_of or checked_at
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        else:
            as_of = as_of.astimezone(timezone.utc)

        qty_tolerance = float(request.qty_tolerance)
        local_holdings = self.list_open_holdings()
        local_by_ticker = {holding.ticker.upper(): holding for holding in local_holdings}
        broker_by_ticker: dict[str, dict[str, float | None]] = {}
        for position in request.positions:
            ticker = position.ticker.upper().strip()
            if not ticker:
                raise ValueError("broker position ticker cannot be empty")
            qty = round(float(position.qty), 6)
            current = broker_by_ticker.setdefault(
                ticker,
                {"qty": 0.0, "priced_qty": 0.0, "weighted_avg_price": 0.0, "avg_price": None},
            )
            current["qty"] = float(current["qty"] or 0.0) + qty
            if position.avg_price is not None and qty > qty_tolerance:
                current["priced_qty"] = float(current["priced_qty"] or 0.0) + qty
                current["weighted_avg_price"] = (
                    float(current["weighted_avg_price"] or 0.0) + qty * float(position.avg_price)
                )

        for current in broker_by_ticker.values():
            priced_qty = float(current["priced_qty"] or 0.0)
            if priced_qty > qty_tolerance:
                current["avg_price"] = round(float(current["weighted_avg_price"] or 0.0) / priced_qty, 6)

        broker_positive = {
            ticker: item
            for ticker, item in broker_by_ticker.items()
            if float(item["qty"] or 0.0) > qty_tolerance
        }
        tickers = sorted(set(local_by_ticker) | set(broker_positive))
        items: list[PositionReconciliationItem] = []
        matched_count = 0
        missing_in_broker_count = 0
        broker_only_count = 0
        qty_mismatch_count = 0

        for ticker in tickers:
            local = local_by_ticker.get(ticker)
            broker = broker_by_ticker.get(ticker, {})
            local_qty = round(float(local.qty if local is not None else 0.0), 6)
            broker_qty = round(float(broker.get("qty") or 0.0), 6)
            qty_diff = round(broker_qty - local_qty, 6)
            local_present = local is not None and local_qty > qty_tolerance
            broker_present = broker_qty > qty_tolerance

            if local_present and broker_present and abs(qty_diff) <= qty_tolerance:
                status = "matched"
                message_cn = f"{ticker} 本地持仓与券商快照一致。"
                matched_count += 1
            elif local_present and not broker_present:
                status = "missing_in_broker"
                message_cn = f"{ticker} 本地记录有 {local_qty:g} 股，但券商快照没有该持仓。"
                missing_in_broker_count += 1
            elif broker_present and not local_present:
                status = "broker_only"
                message_cn = f"{ticker} 券商快照有 {broker_qty:g} 股，但本地没有 open holding。"
                broker_only_count += 1
            else:
                status = "qty_mismatch"
                message_cn = (
                    f"{ticker} 数量不一致：本地 {local_qty:g} 股，券商 {broker_qty:g} 股，"
                    f"差额 {qty_diff:g} 股。"
                )
                qty_mismatch_count += 1

            items.append(
                PositionReconciliationItem(
                    ticker=ticker,
                    local_qty=local_qty,
                    broker_qty=broker_qty,
                    qty_diff=qty_diff,
                    local_avg_price=round(local.avg_buy_price, 6) if local is not None else None,
                    broker_avg_price=broker.get("avg_price"),
                    local_present=local_present,
                    broker_present=broker_present,
                    status=status,
                    message_cn=message_cn,
                )
            )

        mismatch_count = missing_in_broker_count + broker_only_count + qty_mismatch_count
        if not local_by_ticker and not broker_positive:
            status = "empty"
        else:
            status = "matched" if mismatch_count == 0 else "mismatch"
        report = PositionReconciliationReport(
            reconciliation_id=uuid4().hex[:16],
            broker=request.broker.strip() or "manual",
            account_id=request.account_id,
            checked_at=checked_at,
            as_of=as_of,
            status=status,
            blocks_auto_execution=mismatch_count > 0,
            local_position_count=len(local_by_ticker),
            broker_position_count=len(broker_positive),
            matched_count=matched_count,
            mismatch_count=mismatch_count,
            missing_in_broker_count=missing_in_broker_count,
            broker_only_count=broker_only_count,
            qty_tolerance=qty_tolerance,
            note=request.note,
            items=items,
        )
        self.position_reconciliation_repo.add(report)
        self.metrics_store.inc("position_reconciliations")
        self.metrics_store.set_gauge(
            "position_reconciliation_blocks_auto_execution",
            1.0 if report.blocks_auto_execution else 0.0,
        )
        self.publish_event(
            EventType.POSITION_RECONCILIATION,
            {
                "reconciliation_id": report.reconciliation_id,
                "broker": report.broker,
                "account_id": report.account_id,
                "status": report.status,
                "blocks_auto_execution": report.blocks_auto_execution,
                "mismatch_count": report.mismatch_count,
                "local_position_count": report.local_position_count,
                "broker_position_count": report.broker_position_count,
            },
        )
        return report

    def list_position_reconciliations(
        self,
        limit: int = 100,
        broker: str | None = None,
        status: str | None = None,
    ) -> list[PositionReconciliationReport]:
        return self.position_reconciliation_repo.list_recent(limit=limit, broker=broker, status=status)

    def get_position_reconciliation_gate(
        self,
        *,
        require_position_reconciliation: bool,
        max_age_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        if not require_position_reconciliation:
            return {
                "passed": True,
                "required": False,
                "reason": "not_required",
                "max_age_minutes": max_age_minutes,
            }

        reference_at = as_of or datetime.now(timezone.utc)
        if reference_at.tzinfo is None:
            reference_at = reference_at.replace(tzinfo=timezone.utc)
        else:
            reference_at = reference_at.astimezone(timezone.utc)
        latest = self.list_position_reconciliations(limit=1)
        if not latest:
            return {
                "passed": False,
                "required": True,
                "reason": "position_reconciliation_missing",
                "max_age_minutes": max_age_minutes,
            }

        report = latest[0]
        checked_at = report.checked_at.astimezone(timezone.utc)
        age_seconds = max(0.0, (reference_at - checked_at).total_seconds())
        age_minutes = round(age_seconds / 60.0, 3)
        details = {
            "required": True,
            "reconciliation_id": report.reconciliation_id,
            "broker": report.broker,
            "account_id": report.account_id,
            "status": report.status,
            "blocks_auto_execution": report.blocks_auto_execution,
            "mismatch_count": report.mismatch_count,
            "checked_at": checked_at.isoformat(),
            "age_minutes": age_minutes,
            "max_age_minutes": max_age_minutes,
        }
        if max_age_minutes > 0 and age_minutes > max_age_minutes:
            return {
                "passed": False,
                "reason": "position_reconciliation_stale",
                **details,
            }
        if report.blocks_auto_execution or report.status not in {"matched", "empty"}:
            return {
                "passed": False,
                "reason": "position_reconciliation_mismatch",
                **details,
            }
        return {
            "passed": True,
            "reason": None,
            **details,
        }

    def get_portfolio_summary(self, as_of: datetime | None = None) -> PortfolioSummary:
        now = as_of or datetime.now(timezone.utc)
        open_holdings = self.list_open_holdings()
        trades = self.list_trade_ledger(limit=10_000)

        open_cost_basis = 0.0
        open_market_value = 0.0
        open_unrealized_pnl = 0.0
        open_risk_to_stop = 0.0
        for holding in open_holdings:
            open_cost_basis += holding.avg_buy_price * holding.qty
            try:
                current_price = self.provider.get_latest_price(holding.ticker, now)
            except Exception:
                current_price = None
            mark = float(current_price) if current_price is not None else holding.avg_buy_price
            open_market_value += mark * holding.qty
            open_unrealized_pnl += (mark - holding.avg_buy_price) * holding.qty
            open_risk_to_stop += max(0.0, mark - holding.stop_loss) * holding.qty

        sell_trades = [trade for trade in trades if trade.side == TradeSide.SELL]
        closed_trade_count = sum(1 for trade in sell_trades if trade.holding_status_after == HoldingStatus.CLOSED)
        last_trade_at = max((trade.executed_at for trade in trades), default=None)
        last_closed_at = max(
            (
                trade.executed_at
                for trade in sell_trades
                if trade.holding_status_after == HoldingStatus.CLOSED
            ),
            default=None,
        )

        return PortfolioSummary(
            open_holding_count=len(open_holdings),
            closed_holding_count=closed_trade_count,
            trade_count=len(trades),
            buy_trade_count=sum(1 for trade in trades if trade.side == TradeSide.BUY),
            sell_trade_count=len(sell_trades),
            open_cost_basis=round(open_cost_basis, 6),
            open_market_value=round(open_market_value, 6),
            open_unrealized_pnl=round(open_unrealized_pnl, 6),
            open_risk_to_stop=round(open_risk_to_stop, 6),
            total_realized_pnl=round(sum(trade.realized_pnl_delta for trade in sell_trades), 6),
            last_trade_at=last_trade_at,
            last_closed_at=last_closed_at,
        )

    @staticmethod
    def _performance_from_trades(
        trades: list[TradeLedgerEntry],
        generated_at: datetime | None = None,
    ) -> PortfolioPerformance:
        sell_trades = [trade for trade in trades if trade.side == TradeSide.SELL]
        sell_pnls = [trade.realized_pnl_delta for trade in sell_trades]
        wins = [pnl for pnl in sell_pnls if pnl > 0]
        losses = [pnl for pnl in sell_pnls if pnl < 0]
        flat_count = sum(1 for pnl in sell_pnls if pnl == 0)
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        by_ticker: list[TickerPerformance] = []

        for ticker in sorted({trade.ticker for trade in trades}):
            ticker_trades = [trade for trade in trades if trade.ticker == ticker]
            ticker_sells = [trade for trade in ticker_trades if trade.side == TradeSide.SELL]
            ticker_pnls = [trade.realized_pnl_delta for trade in ticker_sells]
            ticker_wins = [pnl for pnl in ticker_pnls if pnl > 0]
            ticker_losses = [pnl for pnl in ticker_pnls if pnl < 0]
            ticker_gross_loss = abs(sum(ticker_losses))
            by_ticker.append(
                TickerPerformance(
                    ticker=ticker,
                    trade_count=len(ticker_trades),
                    sell_trade_count=len(ticker_sells),
                    total_realized_pnl=round(sum(ticker_pnls), 6),
                    win_count=len(ticker_wins),
                    loss_count=len(ticker_losses),
                    flat_count=sum(1 for pnl in ticker_pnls if pnl == 0),
                    win_rate=round(len(ticker_wins) / len(ticker_pnls), 6) if ticker_pnls else 0.0,
                    avg_win=round(sum(ticker_wins) / len(ticker_wins), 6) if ticker_wins else 0.0,
                    avg_loss=round(sum(ticker_losses) / len(ticker_losses), 6) if ticker_losses else 0.0,
                    profit_factor=round(sum(ticker_wins) / ticker_gross_loss, 6) if ticker_gross_loss > 0 else None,
                    best_trade_pnl=round(max(ticker_pnls), 6) if ticker_pnls else 0.0,
                    worst_trade_pnl=round(min(ticker_pnls), 6) if ticker_pnls else 0.0,
                )
            )

        by_ticker.sort(key=lambda item: item.total_realized_pnl, reverse=True)
        return PortfolioPerformance(
            generated_at=generated_at or datetime.now(timezone.utc),
            trade_count=len(trades),
            sell_trade_count=len(sell_trades),
            closed_trade_count=sum(1 for trade in sell_trades if trade.holding_status_after == HoldingStatus.CLOSED),
            total_realized_pnl=round(sum(sell_pnls), 6),
            win_count=len(wins),
            loss_count=len(losses),
            flat_count=flat_count,
            win_rate=round(len(wins) / len(sell_pnls), 6) if sell_pnls else 0.0,
            avg_win=round(gross_profit / len(wins), 6) if wins else 0.0,
            avg_loss=round(sum(losses) / len(losses), 6) if losses else 0.0,
            profit_factor=round(gross_profit / gross_loss, 6) if gross_loss > 0 else None,
            expectancy_per_sell=round(sum(sell_pnls) / len(sell_pnls), 6) if sell_pnls else 0.0,
            best_trade_pnl=round(max(sell_pnls), 6) if sell_pnls else 0.0,
            worst_trade_pnl=round(min(sell_pnls), 6) if sell_pnls else 0.0,
            by_ticker=by_ticker,
        )

    def get_portfolio_performance(self, limit: int = 10_000) -> PortfolioPerformance:
        return self._performance_from_trades(
            trades=self.list_trade_ledger(limit=limit),
            generated_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _profit_factor(pnls: list[float]) -> float | None:
        gross_profit = sum(pnl for pnl in pnls if pnl > 0)
        gross_loss = abs(sum(pnl for pnl in pnls if pnl < 0))
        return round(gross_profit / gross_loss, 6) if gross_loss > 0 else None

    @staticmethod
    def _snapshot_performance_score(
        total_realized_pnl: float,
        win_rate: float,
        profit_factor: float | None,
        expectancy_per_sell: float,
        recommendation_count: int,
        sell_trade_count: int,
    ) -> float:
        if sell_trade_count <= 0:
            return 0.0
        profit_factor_value = profit_factor
        if profit_factor_value is None:
            profit_factor_value = 3.0 if win_rate > 0 else 0.0
        score = 50.0
        score += (win_rate - 0.5) * 50.0
        score += math.tanh(expectancy_per_sell / 500.0) * 20.0
        pnl_scale = max(500.0, float(max(1, recommendation_count)) * 500.0)
        score += math.tanh(total_realized_pnl / pnl_scale) * 15.0
        score += math.tanh((profit_factor_value - 1.0) / 1.5) * 15.0
        return round(max(0.0, min(100.0, score)), 2)

    @staticmethod
    def _snapshot_quality_grade(score: float, sell_trade_count: int) -> str:
        if sell_trade_count <= 0:
            return "insufficient_history"
        if score >= 70:
            return "outperforming"
        if score >= 55:
            return "positive"
        if score >= 45:
            return "neutral"
        if score >= 30:
            return "weak"
        return "negative"

    @staticmethod
    def _sell_window(items: list[RecommendationAttribution]) -> tuple[datetime | None, datetime | None]:
        first_sell_at = min(
            (item.first_sell_at for item in items if item.first_sell_at is not None),
            default=None,
        )
        last_sell_at = max(
            (item.last_sell_at for item in items if item.last_sell_at is not None),
            default=None,
        )
        return first_sell_at, last_sell_at

    def _group_recommendation_attribution(
        self,
        items: list[RecommendationAttribution],
        pnls_by_recommendation: dict[str, list[float]],
    ) -> dict:
        pnls = [
            pnl
            for item in items
            for pnl in pnls_by_recommendation.get(item.recommendation_id, [])
        ]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        sell_count = sum(item.sell_trade_count for item in items)
        total_realized_pnl = round(sum(pnls), 6)
        win_rate = round(len(wins) / len(pnls), 6) if pnls else 0.0
        profit_factor = self._profit_factor(pnls)
        expectancy_per_sell = round(sum(pnls) / len(pnls), 6) if pnls else 0.0
        confidence_values = [item.confidence for item in items if item.confidence is not None]
        composite_values = [item.composite for item in items if item.composite is not None]
        performance_score = self._snapshot_performance_score(
            total_realized_pnl=total_realized_pnl,
            win_rate=win_rate,
            profit_factor=profit_factor,
            expectancy_per_sell=expectancy_per_sell,
            recommendation_count=len(items),
            sell_trade_count=sell_count,
        )
        first_sell_at, last_sell_at = self._sell_window(items)
        return {
            "recommendation_count": len(items),
            "sell_trade_count": sell_count,
            "closed_trade_count": sum(item.closed_trade_count for item in items),
            "total_realized_pnl": total_realized_pnl,
            "win_count": len(wins),
            "loss_count": len(losses),
            "flat_count": sum(item.flat_count for item in items),
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "expectancy_per_sell": expectancy_per_sell,
            "avg_confidence": (
                round(sum(confidence_values) / len(confidence_values), 6)
                if confidence_values
                else None
            ),
            "avg_composite": (
                round(sum(composite_values) / len(composite_values), 6)
                if composite_values
                else None
            ),
            "performance_score": performance_score,
            "quality_grade": self._snapshot_quality_grade(performance_score, sell_count),
            "first_sell_at": first_sell_at,
            "last_sell_at": last_sell_at,
        }

    def _get_recommendation(self, recommendation_id: str) -> Recommendation | None:
        cached = self.recommendations_by_id.get(recommendation_id)
        if cached is not None:
            return cached
        return self.recommendation_repo.get(recommendation_id)

    @staticmethod
    def _bounded_float(value: Any, default: float, lower: float, upper: float) -> float:
        try:
            candidate = float(value)
        except (TypeError, ValueError):
            candidate = default
        return round(max(lower, min(upper, candidate)), 6)

    @staticmethod
    def _strategy_metric_snapshot(row: StrategyConfigAttribution | None) -> dict[str, Any]:
        if row is None:
            return {
                "recommendation_count": 0,
                "sell_trade_count": 0,
                "closed_trade_count": 0,
                "total_realized_pnl": 0.0,
                "win_rate": 0.0,
                "profit_factor": None,
                "expectancy_per_sell": 0.0,
                "performance_score": 0.0,
                "quality_grade": "insufficient_history",
            }
        return {
            "recommendation_count": row.recommendation_count,
            "sell_trade_count": row.sell_trade_count,
            "closed_trade_count": row.closed_trade_count,
            "total_realized_pnl": row.total_realized_pnl,
            "win_rate": row.win_rate,
            "profit_factor": row.profit_factor,
            "expectancy_per_sell": row.expectancy_per_sell,
            "avg_confidence": row.avg_confidence,
            "avg_composite": row.avg_composite,
            "performance_score": row.performance_score,
            "quality_grade": row.quality_grade,
            "first_sell_at": row.first_sell_at,
            "last_sell_at": row.last_sell_at,
        }

    @staticmethod
    def _strategy_current_parameters(config: StrategyConfigSnapshot | None) -> dict[str, Any]:
        if config is None:
            return {}
        risk_policy = dict(config.risk_policy or {})
        signal_config = dict(config.signal_config or {})
        price_plan_config = dict(config.price_plan_config or {})
        publication = dict(config.publication or {})
        return {
            "universe": config.universe,
            "strategy_pattern": price_plan_config.get("strategy_pattern"),
            "min_confidence": risk_policy.get("min_confidence"),
            "max_entry_gap_pct": risk_policy.get("max_entry_gap_pct"),
            "event_trading_enabled": risk_policy.get("event_trading_enabled"),
            "event_news_weight": signal_config.get("event_news_weight"),
            "technical_weight": signal_config.get("technical_weight"),
            "stop_atr_range": price_plan_config.get("stop_atr_range"),
            "top_n": publication.get("top_n"),
        }

    def _tighten_strategy_changes(self, config: StrategyConfigSnapshot | None) -> dict[str, Any]:
        current = self._strategy_current_parameters(config)
        min_confidence = self._bounded_float(current.get("min_confidence"), 0.72, 0.0, 0.99)
        max_entry_gap_pct = self._bounded_float(current.get("max_entry_gap_pct"), 0.30, 0.0, 1.0)
        event_news_weight = self._bounded_float(current.get("event_news_weight"), 0.25, 0.0, 1.0)
        technical_weight = self._bounded_float(current.get("technical_weight"), 0.30, 0.0, 1.0)
        stop_range = current.get("stop_atr_range")
        if not isinstance(stop_range, list) or len(stop_range) < 2:
            stop_range = [1.2, 1.8]
        stop_low = self._bounded_float(stop_range[0], 1.2, 0.1, 10.0)
        stop_high = self._bounded_float(stop_range[1], 1.8, 0.1, 10.0)
        return {
            "risk_policy.min_confidence": {
                "current": min_confidence,
                "suggested": round(min(min_confidence + 0.05, 0.95), 2),
            },
            "risk_policy.max_entry_gap_pct": {
                "current": max_entry_gap_pct,
                "suggested": round(max(max_entry_gap_pct - 0.05, 0.05), 2),
            },
            "price_plan_config.stop_atr_range": {
                "current": [stop_low, stop_high],
                "suggested": [round(max(stop_low * 0.9, 0.6), 2), round(max(stop_high * 0.9, 0.9), 2)],
            },
            "signal_config.event_news_weight": {
                "current": event_news_weight,
                "suggested": round(max(event_news_weight - 0.03, 0.10), 2),
            },
            "signal_config.technical_weight": {
                "current": technical_weight,
                "suggested": round(min(technical_weight + 0.03, 0.55), 2),
            },
        }

    def _relax_strategy_changes(self, config: StrategyConfigSnapshot | None) -> dict[str, Any]:
        current = self._strategy_current_parameters(config)
        min_confidence = self._bounded_float(current.get("min_confidence"), 0.72, 0.0, 0.99)
        top_n = int(self._bounded_float(current.get("top_n"), 8.0, 1.0, 50.0))
        return {
            "risk_policy.min_confidence": {
                "current": min_confidence,
                "suggested": round(max(min_confidence - 0.03, 0.50), 2),
            },
            "publication.top_n": {
                "current": top_n,
                "suggested": min(top_n + 1, 12),
            },
        }

    def _build_strategy_tuning_recommendation(
        self,
        strategy_config_id: str,
        config: StrategyConfigSnapshot | None,
        row: StrategyConfigAttribution | None,
    ) -> StrategyTuningRecommendation:
        metrics = self._strategy_metric_snapshot(row)
        current_parameters = self._strategy_current_parameters(config)
        sell_count = int(metrics["sell_trade_count"])
        score = float(metrics["performance_score"])
        win_rate = float(metrics["win_rate"])
        expectancy = float(metrics["expectancy_per_sell"])
        total_pnl = float(metrics["total_realized_pnl"])
        profit_factor = metrics["profit_factor"]

        if sell_count < 2:
            action = StrategyTuningAction.COLLECT_MORE_DATA
            priority = 30
            rationale_cn = "卖出归因样本不足，先保留当前参数并继续收集真实卖出结果。"
            recommended_changes: dict[str, Any] = {}
        elif score < 45 or expectancy < 0 or total_pnl < 0:
            action = StrategyTuningAction.TIGHTEN
            priority = 90 if score < 30 or expectancy < 0 else 75
            rationale_cn = "该策略版本卖出归因偏弱，建议先提高入选门槛并收紧入场/止损参数。"
            recommended_changes = self._tighten_strategy_changes(config)
        elif score >= 70 and win_rate >= 0.55 and expectancy > 0:
            action = StrategyTuningAction.KEEP
            priority = 20
            rationale_cn = "该策略版本的收益、胜率和盈亏比表现较好，建议继续作为候选默认参数。"
            recommended_changes = {}
        elif score >= 60 and sell_count >= 5 and win_rate >= 0.55 and (
            profit_factor is None or float(profit_factor) >= 1.3
        ):
            action = StrategyTuningAction.RELAX
            priority = 45
            rationale_cn = "该策略版本表现偏正且样本较充分，可以小幅放宽阈值以扩大候选覆盖。"
            recommended_changes = self._relax_strategy_changes(config)
        else:
            action = StrategyTuningAction.REVIEW
            priority = 55
            rationale_cn = "该策略版本表现接近中性，建议人工复盘样本后再决定是否改参数。"
            recommended_changes = {}

        return StrategyTuningRecommendation(
            strategy_config_id=strategy_config_id,
            action=action,
            priority=priority,
            rationale_cn=rationale_cn,
            metric_snapshot=metrics,
            current_parameters=current_parameters,
            recommended_changes=recommended_changes,
            generated_at=datetime.now(timezone.utc),
        )

    def get_strategy_tuning_report(
        self,
        limit: int = 10_000,
        strategy_limit: int = 100,
        attribution: RecommendationAttributionReport | None = None,
    ) -> StrategyTuningReport:
        attribution_report = attribution or self.get_recommendation_attribution(limit=limit)
        rows_by_config = {row.strategy_config_id: row for row in attribution_report.by_strategy_config}
        configs_by_id = {
            config.strategy_config_id: config
            for config in self.strategy_config_repo.list_recent(limit=strategy_limit)
        }
        for strategy_config_id in rows_by_config:
            if strategy_config_id not in configs_by_id:
                config = self.strategy_config_repo.get(strategy_config_id)
                if config is not None:
                    configs_by_id[strategy_config_id] = config

        strategy_config_ids = sorted(set(configs_by_id) | set(rows_by_config))
        items = [
            self._build_strategy_tuning_recommendation(
                strategy_config_id=strategy_config_id,
                config=configs_by_id.get(strategy_config_id),
                row=rows_by_config.get(strategy_config_id),
            )
            for strategy_config_id in strategy_config_ids
        ]
        items.sort(
            key=lambda item: (
                item.priority,
                item.metric_snapshot.get("performance_score", 0.0),
                item.metric_snapshot.get("total_realized_pnl", 0.0),
            ),
            reverse=True,
        )
        return StrategyTuningReport(
            generated_at=datetime.now(timezone.utc),
            recommendation_count=len(items),
            items=items,
        )

    def get_recommendation_attribution(self, limit: int = 10_000) -> RecommendationAttributionReport:
        trades = self.list_trade_ledger(limit=limit)
        sell_trades = [trade for trade in trades if trade.side == TradeSide.SELL]
        attributed = [trade for trade in sell_trades if trade.source_recommendation_id]
        unattributed_count = len(sell_trades) - len(attributed)

        grouped: dict[str, list[TradeLedgerEntry]] = {}
        for trade in attributed:
            grouped.setdefault(str(trade.source_recommendation_id), []).append(trade)

        by_recommendation: list[RecommendationAttribution] = []
        pnls_by_recommendation: dict[str, list[float]] = {}
        for recommendation_id, rec_trades in grouped.items():
            recommendation = self._get_recommendation(recommendation_id)
            pnls = [trade.realized_pnl_delta for trade in rec_trades]
            pnls_by_recommendation[recommendation_id] = pnls
            wins = [pnl for pnl in pnls if pnl > 0]
            losses = [pnl for pnl in pnls if pnl < 0]
            first_sell_at = min((trade.executed_at for trade in rec_trades), default=None)
            last_sell_at = max((trade.executed_at for trade in rec_trades), default=None)
            by_recommendation.append(
                RecommendationAttribution(
                    recommendation_id=recommendation_id,
                    ticker=recommendation.ticker if recommendation is not None else rec_trades[0].ticker,
                    source_snapshot_id=recommendation.source_snapshot_id if recommendation is not None else None,
                    strategy_config_id=recommendation.strategy_config_id if recommendation is not None else None,
                    generated_at=recommendation.generated_at if recommendation is not None else None,
                    confidence=round(recommendation.confidence, 6) if recommendation is not None else None,
                    composite=(
                        round(float(recommendation.score_vector.get("composite", 0.0)), 6)
                        if recommendation is not None
                        else None
                    ),
                    sell_trade_count=len(rec_trades),
                    closed_trade_count=sum(
                        1 for trade in rec_trades if trade.holding_status_after == HoldingStatus.CLOSED
                    ),
                    total_realized_pnl=round(sum(pnls), 6),
                    win_count=len(wins),
                    loss_count=len(losses),
                    flat_count=sum(1 for pnl in pnls if pnl == 0),
                    win_rate=round(len(wins) / len(pnls), 6) if pnls else 0.0,
                    profit_factor=self._profit_factor(pnls),
                    expectancy_per_sell=round(sum(pnls) / len(pnls), 6) if pnls else 0.0,
                    first_sell_at=first_sell_at,
                    last_sell_at=last_sell_at,
                )
            )

        by_recommendation.sort(key=lambda item: item.total_realized_pnl, reverse=True)

        snapshot_groups: dict[str, list[RecommendationAttribution]] = {}
        for item in by_recommendation:
            if item.source_snapshot_id:
                snapshot_groups.setdefault(item.source_snapshot_id, []).append(item)

        by_snapshot: list[SnapshotAttribution] = []
        for source_snapshot_id, items in snapshot_groups.items():
            group = self._group_recommendation_attribution(items, pnls_by_recommendation)
            by_snapshot.append(
                SnapshotAttribution(
                    source_snapshot_id=source_snapshot_id,
                    **group,
                )
            )

        by_snapshot.sort(key=lambda item: (item.performance_score, item.total_realized_pnl), reverse=True)

        strategy_groups: dict[str, list[RecommendationAttribution]] = {}
        for item in by_recommendation:
            if item.strategy_config_id:
                strategy_groups.setdefault(item.strategy_config_id, []).append(item)

        by_strategy_config: list[StrategyConfigAttribution] = []
        for strategy_config_id, items in strategy_groups.items():
            by_strategy_config.append(
                StrategyConfigAttribution(
                    strategy_config_id=strategy_config_id,
                    **self._group_recommendation_attribution(items, pnls_by_recommendation),
                )
            )

        by_strategy_config.sort(
            key=lambda item: (item.performance_score, item.total_realized_pnl),
            reverse=True,
        )
        return RecommendationAttributionReport(
            generated_at=datetime.now(timezone.utc),
            recommendation_count=len(by_recommendation),
            attributed_sell_trade_count=len(attributed),
            unattributed_sell_trade_count=unattributed_count,
            total_realized_pnl=round(sum(item.total_realized_pnl for item in by_recommendation), 6),
            by_recommendation=by_recommendation,
            by_snapshot=by_snapshot,
            by_strategy_config=by_strategy_config,
        )

    def close_holding(self, ticker: str) -> HoldingWatch | None:
        ticker_upper = ticker.upper()
        holding = self.holding_watch_repo.close(ticker_upper)
        if holding is None:
            return None
        self.holdings_by_ticker[ticker_upper] = holding
        self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        return holding

    def monitor_sell_alerts(self, as_of: datetime | None = None) -> list[SellAlert]:
        holdings = self.list_open_holdings()
        alerts = self.position_monitor.evaluate(
            holdings=holdings,
            provider=self.provider,
            as_of=as_of,
            signal_lookup=lambda ticker: self.signals_by_ticker.get(ticker) or self.signal_repo.get_latest_by_ticker(ticker),
        )
        for alert in alerts:
            provenance = self._recommendation_provenance(alert.source_recommendation_id)
            alert.source_snapshot_id = provenance["source_snapshot_id"]
            alert.strategy_config_id = provenance["strategy_config_id"]
        self.recent_sell_alerts = alerts
        self.metrics_store.set_gauge("sell_alert_count", len(alerts))
        for alert in alerts:
            self.publish_event(
                EventType.SELL_ALERT,
                {
                    "ticker": alert.ticker,
                    "reason_code": alert.reason_code,
                    "level": alert.level.value,
                    "message_cn": alert.message_cn,
                    "source_recommendation_id": alert.source_recommendation_id,
                    "source_snapshot_id": alert.source_snapshot_id,
                    "strategy_config_id": alert.strategy_config_id,
                },
            )
        return alerts

    def _select_sell_alert(self, ticker: str, reason_code: str | None = None) -> SellAlert | None:
        ticker_upper = ticker.upper()
        alerts = self.monitor_sell_alerts()
        candidates = [alert for alert in alerts if alert.ticker.upper() == ticker_upper]
        if reason_code is not None:
            candidates = [alert for alert in candidates if alert.reason_code == reason_code]
        candidates.sort(key=lambda alert: self._alert_priority.get(alert.reason_code, 99))
        return candidates[0] if candidates else None

    @staticmethod
    def _alert_default_sell_qty(alert: SellAlert, holding: HoldingWatch) -> tuple[float | None, str]:
        if alert.reason_code in {"stop_loss_breach", "take_profit2_hit"}:
            return None, "执行建议: 全部卖出并关闭持仓"
        if alert.reason_code == "take_profit1_hit":
            return round(max(holding.qty * 0.5, 0.0), 6), "执行建议: 先卖出一半锁定利润"
        if alert.reason_code == "regime_risk_off":
            return round(max(holding.qty * 0.5, 0.0), 6), "执行建议: 降低一半风险暴露"
        return round(max(holding.qty * 0.5, 0.0), 6), "执行建议: 先减仓观察"

    def execute_sell_alert(self, ticker: str, request: AlertSellRequest) -> AlertExecutionResult:
        ticker_upper = ticker.upper()
        holding = self.holdings_by_ticker.get(ticker_upper) or self.holding_watch_repo.get(ticker_upper)
        if holding is None or holding.status != HoldingStatus.OPEN:
            raise KeyError("open holding not found")

        alert = self._select_sell_alert(ticker=ticker_upper, reason_code=request.reason_code)
        if alert is None:
            raise ValueError("No active sell alert found for holding")

        default_qty, default_action_cn = self._alert_default_sell_qty(alert, holding)
        sell_qty = None if request.sell_all is True else (request.qty if request.qty is not None else default_qty)
        sell_price = request.sell_price if request.sell_price is not None else alert.current_price
        reason = request.note or f"alert:{alert.reason_code}"
        execution = self.sell_holding(
            ticker=ticker_upper,
            request=ManualSellRequest(
                qty=sell_qty,
                sell_price=sell_price,
                reason=reason,
                execution_mode=request.execution_mode,
                dry_run=request.dry_run,
                confirm_live=request.confirm_live,
            ),
        )
        return AlertExecutionResult(alert=alert, execution=execution, default_action_cn=default_action_cn)

    def consume_events(self, limit: int = 100) -> list[SystemEvent]:
        events = self.system_event_repo.consume(limit=limit)
        self.event_queue.consume(limit=limit)
        self.metrics_store.set_gauge("event_queue_pending", self.pending_event_count())
        self.metrics_store.inc("events_consumed", len(events))
        return events

    def list_pending_events(self, limit: int = 100) -> list[SystemEvent]:
        return self.system_event_repo.list_by_status(EventStatus.PENDING, limit=limit)

    def list_consumed_events(self, limit: int = 100) -> list[SystemEvent]:
        return self.system_event_repo.list_by_status(EventStatus.CONSUMED, limit=limit)

    def pending_event_count(self) -> int:
        return self.system_event_repo.count_by_status(EventStatus.PENDING)

    def get_latest_approval(self, recommendation_id: str) -> RecommendationApproval | None:
        cached = self.approvals_by_recommendation_id.get(recommendation_id)
        if cached is not None:
            return cached
        persisted = self.approval_repo.latest_for_recommendation(recommendation_id)
        if persisted is not None:
            self.approvals_by_recommendation_id[recommendation_id] = persisted
        return persisted

    def reset(self) -> None:
        self.latest_run = None
        self.last_research_request = None
        self.recommendations_by_id.clear()
        self.signals_by_ticker.clear()
        self.features_by_ticker.clear()
        self.paper_orders.clear()
        self.positions.clear()
        self.holdings_by_ticker.clear()
        self.backtest_runs.clear()
        self.approvals_by_recommendation_id.clear()
        self.recent_sell_alerts.clear()
        self.event_queue = InMemoryEventQueue()
        self.paper_order_repo.clear_all()
        self.position_repo.clear_all()
        self.holding_watch_repo.clear_all()
        self.trade_ledger_repo.clear_all()
        self.sell_execution_audit_repo.clear_all()
        self.sell_alert_audit_repo.clear_all()
        self.approval_repo.clear_all()
        self.autopilot_policy_repo.clear_all()
        self.system_cycle_run_repo.clear_all()
        self.system_event_repo.clear_all()
        self.position_reconciliation_repo.clear_all()
        self.kill_switch = self.execution_control_repo.set_kill_switch(
            enabled=False,
            reason="state_reset",
            updated_by="test-reset",
        )
        self.autopilot_policy = self.autopilot_policy_repo.set_policy(
            AutopilotPolicy(
                enabled=False,
                reason="state_reset",
                updated_by="test-reset",
            )
        )


@lru_cache(maxsize=1)
def get_app_state() -> AppState:
    return AppState()
