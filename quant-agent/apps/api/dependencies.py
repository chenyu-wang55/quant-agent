from __future__ import annotations

import logging
from dataclasses import dataclass, field
from threading import Lock
from typing import Any
from uuid import uuid4

from apps.api.state_autopilot import AutopilotStateMixin
from apps.api.state_autopilot_gates import AutopilotGateStateMixin
from apps.api.state_order_replay import OrderReplayAndRiskStateMixin
from apps.api.state_orders import OrderStateMixin
from apps.api.state_portfolio import PortfolioAnalyticsMixin
from apps.api.state_sell_sync import SellSyncStateMixin
from apps.api.state_sells import SellExecutionStateMixin
from apps.api.state_trading import TradingStateMixin
from domain.entities.models import (
    ApprovalDecision,
    AutopilotPolicy,
    BacktestRunResult,
    FeatureSnapshot,
    HoldingWatch,
    KillSwitchState,
    OperationAction,
    OperationControlCenter,
    OperationRecommendationCandidate,
    PaperOrder,
    PositionState,
    Recommendation,
    RecommendationApproval,
    ResearchRunRequest,
    ResearchRunResult,
    SellAlert,
    SellAlertAudit,
    SellAlertLevel,
    SellExecutionAudit,
    SignalSnapshot,
    SystemCycleRun,
    TradeLedgerEntry,
)
from domain.policies.approval import ApprovalPolicy
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
from services.execution.router import ExecutionRouter
from services.ingestion.interfaces import DataProvider
from services.ingestion.provider_factory import build_data_provider
from services.ranking.pipeline import PipelineOutput, ResearchPipeline
from services.research.backtest_engine import BacktestEngine
from services.risk.position_monitor import PositionMonitor

logger = logging.getLogger(__name__)


def _truthy_env_flag(name: str) -> bool:
    return env_flag(name)


@dataclass
class AppState(
    OrderStateMixin,
    OrderReplayAndRiskStateMixin,
    AutopilotStateMixin,
    AutopilotGateStateMixin,
    TradingStateMixin,
    SellExecutionStateMixin,
    SellSyncStateMixin,
    PortfolioAnalyticsMixin,
):
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
    order_unit_of_work: OrderUnitOfWork = field(default_factory=OrderUnitOfWork)
    portfolio_risk_reservation_repo: PortfolioRiskReservationRepository = field(
        default_factory=PortfolioRiskReservationRepository
    )
    sell_unit_of_work: SellUnitOfWork = field(default_factory=SellUnitOfWork)

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

    def _commit_sell_execution(
        self,
        *,
        audit: SellExecutionAudit,
        event_type: EventType,
        event_payload: dict[str, Any],
        holding: HoldingWatch | None = None,
        trade: TradeLedgerEntry | None = None,
    ) -> None:
        event_suffix = f"{audit.status}_{audit.qty:.6f}".replace(".", "_")
        event = SystemEvent(
            id=f"{event_type.value}_{audit.id}_{event_suffix}"[:64],
            event_type=event_type,
            payload=event_payload,
        )
        ledger_applied = self.sell_unit_of_work.commit(
            audit=audit,
            holding=holding,
            trade=trade,
            event=event,
        )
        if ledger_applied and holding is not None:
            self.holdings_by_ticker[holding.ticker] = holding
            self.metrics_store.inc("trade_ledger_sell")
            self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        self.metrics_store.inc("sell_execution_audits")
        self.event_queue.publish(event)
        self.metrics_store.inc(f"event_published_{event_type.value}")
        self.metrics_store.set_gauge("event_queue_pending", self.pending_event_count())

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
        self.metrics_store.set_gauge("system_cycle_last_success", 1.0 if item.status == "success" else 0.0)
        logger.info(
            "system cycle persisted",
            extra={
                "event": "system_cycle_persisted",
                "run_id": item.id,
                "status": item.status,
                "source_snapshot_id": item.source_snapshot_id,
            },
        )

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
                        message_cn=(f"{recommendation.ticker} 推荐尚未审批；审批后才允许进入买入执行。"),
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
                        message_cn=(f"{recommendation.ticker} 已审批且当前无监控持仓；可先查 risk-plan 再提交买入单。"),
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

    def record_sell_alert_audits(
        self, alerts: list[SellAlert], monitor_run_id: str | None = None
    ) -> list[SellAlertAudit]:
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
        if not _truthy_env_flag("QUANT_AGENT_TEST_MODE") and not _truthy_env_flag(
            "QUANT_AGENT_ALLOW_DESTRUCTIVE_RESET"
        ):
            raise RuntimeError(
                "AppState.reset() deletes operational records and is disabled outside test mode. "
                "Set QUANT_AGENT_ALLOW_DESTRUCTIVE_RESET=1 only for an intentional reset."
            )
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
        self.portfolio_risk_reservation_repo.clear_all()
        self.system_cycle_run_repo.clear_all()
        self.system_event_repo.clear_all()
        self.position_reconciliation_repo.clear_all()
        self.metrics_store.clear()
        OperationalAlertManager().clear()
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


_APP_STATE_LOCK = Lock()
_APP_STATE: AppState | None = None


def get_app_state() -> AppState:
    global _APP_STATE
    if _APP_STATE is not None:
        return _APP_STATE
    with _APP_STATE_LOCK:
        if _APP_STATE is None:
            _APP_STATE = AppState()
        return _APP_STATE
