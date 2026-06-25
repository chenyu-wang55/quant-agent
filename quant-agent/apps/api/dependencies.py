from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
import math
from typing import Any
from uuid import uuid4

from domain.entities.models import (
    AlertExecutionResult,
    AlertSellRequest,
    ApprovalDecision,
    BacktestRunResult,
    Direction,
    FeatureSnapshot,
    HoldingStatus,
    HoldingWatch,
    KillSwitchState,
    ManualBuyRequest,
    ManualSellRequest,
    OrderExecutionMode,
    OperationAction,
    OperationControlCenter,
    OperationRecommendationCandidate,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderRiskPlan,
    PaperOrderStatus,
    PortfolioPerformance,
    PortfolioSummary,
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
    ExecutionControlRepository,
    FeatureRepository,
    HoldingWatchRepository,
    PaperOrderRepository,
    PositionRepository,
    RecommendationRepository,
    SellAlertAuditRepository,
    SellExecutionAuditRepository,
    SignalRepository,
    SourceSnapshotRepository,
    StrategyConfigRepository,
    SystemCycleRunRepository,
    TradeLedgerRepository,
)
from infra.observability.metrics import MetricsStore
from infra.queue.events import EventType, SystemEvent
from infra.queue.in_memory import InMemoryEventQueue
from services.execution.router import ExecutionRouter
from services.ingestion.interfaces import DataProvider
from services.ingestion.provider_factory import build_data_provider
from services.ranking.pipeline import PipelineOutput, ResearchPipeline
from services.research.backtest_engine import BacktestEngine
from services.risk.position_monitor import PositionMonitor


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
    holding_watch_repo: HoldingWatchRepository = field(default_factory=HoldingWatchRepository)
    trade_ledger_repo: TradeLedgerRepository = field(default_factory=TradeLedgerRepository)
    sell_execution_audit_repo: SellExecutionAuditRepository = field(default_factory=SellExecutionAuditRepository)
    sell_alert_audit_repo: SellAlertAuditRepository = field(default_factory=SellAlertAuditRepository)
    approval_repo: ApprovalRepository = field(default_factory=ApprovalRepository)
    execution_control_repo: ExecutionControlRepository = field(default_factory=ExecutionControlRepository)
    source_snapshot_repo: SourceSnapshotRepository = field(default_factory=SourceSnapshotRepository)
    strategy_config_repo: StrategyConfigRepository = field(default_factory=StrategyConfigRepository)
    system_cycle_run_repo: SystemCycleRunRepository = field(default_factory=SystemCycleRunRepository)

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
                    details={
                        "reason_code": alert.reason_code,
                        "suggested_action_cn": alert.suggested_action_cn,
                        "current_price": alert.current_price,
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

        pending_event_count = self.event_queue.size()
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
        return OperationControlCenter(
            kill_switch=self.kill_switch,
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
        try:
            mark = self.provider.get_latest_price(ticker_upper, as_of or datetime.now(timezone.utc))
        except Exception:
            mark = None
        price = float(mark) if mark is not None else holding.avg_buy_price
        return round(max(0.0, price * holding.qty), 6)

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
        recommended_qty = min(max_risk_qty, max_position_qty)

        requested_notional = round(entry_price * request.qty, 6)
        requested_risk_amount = round(risk_per_share * request.qty, 6)
        requested_position_pct = round(
            (current_position_value + requested_notional) / request.account_equity,
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

        if not violations:
            message_cn = (
                f"风险校验通过。建议股数 {recommended_qty:g}，本次名义本金 "
                f"{requested_notional:.2f}，止损风险 {requested_risk_amount:.2f}。"
            )
        else:
            labels = {
                "invalid_stop_distance": "止损距离无效",
                "exceeds_per_trade_risk": "超过单笔风险预算",
                "exceeds_position_cap": "超过单票仓位上限",
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
            max_risk_qty=max_risk_qty,
            max_position_qty=max_position_qty,
            recommended_qty=recommended_qty,
            requested_qty=round(request.qty, 6),
            requested_notional=requested_notional,
            requested_risk_amount=requested_risk_amount,
            requested_position_pct=requested_position_pct,
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

    def publish_event(self, event_type: EventType, payload: dict) -> None:
        self.event_queue.publish(SystemEvent(event_type=event_type, payload=payload))
        self.metrics_store.inc(f"event_published_{event_type.value}")
        self.metrics_store.set_gauge("event_queue_pending", self.event_queue.size())

    def record_manual_buy(self, request: ManualBuyRequest) -> HoldingWatch:
        ticker = request.ticker.upper()
        recommendation = None
        if request.source_recommendation_id:
            recommendation = self.recommendations_by_id.get(request.source_recommendation_id)
            if recommendation is None:
                recommendation = self.recommendation_repo.get(request.source_recommendation_id)

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
                reason=request.note,
                holding_status_after=HoldingStatus.OPEN,
            )
        )
        self.metrics_store.inc("manual_buys")
        self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        return holding

    def sell_holding(self, ticker: str, request: ManualSellRequest) -> SellExecutionResult:
        ticker_upper = ticker.upper()
        holding = self.holdings_by_ticker.get(ticker_upper)
        if holding is None:
            holding = self.holding_watch_repo.get(ticker_upper)
        if holding is None or holding.status != HoldingStatus.OPEN:
            raise KeyError("open holding not found")

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
                raise NotImplementedError("Live broker sell adapter is not configured")

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
        events = self.event_queue.consume(limit=limit)
        self.metrics_store.set_gauge("event_queue_pending", self.event_queue.size())
        self.metrics_store.inc("events_consumed", len(events))
        return events

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
        self.kill_switch = self.execution_control_repo.set_kill_switch(
            enabled=False,
            reason="state_reset",
            updated_by="test-reset",
        )


@lru_cache(maxsize=1)
def get_app_state() -> AppState:
    return AppState()
