from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from uuid import uuid4

from domain.entities.models import (
    ApprovalDecision,
    BacktestRunResult,
    FeatureSnapshot,
    HoldingStatus,
    HoldingWatch,
    KillSwitchState,
    ManualBuyRequest,
    ManualSellRequest,
    PaperOrder,
    PositionState,
    Recommendation,
    RecommendationApproval,
    ResearchRunRequest,
    ResearchRunResult,
    SellAlert,
    SellExecutionResult,
    SignalSnapshot,
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
    SignalRepository,
    SourceSnapshotRepository,
)
from infra.observability.metrics import MetricsStore
from infra.queue.events import EventType, SystemEvent
from infra.queue.in_memory import InMemoryEventQueue
from services.execution.paper_router import PaperExecutionRouter
from services.ingestion.interfaces import DataProvider
from services.ingestion.provider_factory import build_data_provider
from services.ranking.pipeline import PipelineOutput, ResearchPipeline
from services.research.backtest_engine import BacktestEngine
from services.risk.position_monitor import PositionMonitor


@dataclass
class AppState:
    provider: DataProvider = field(default_factory=build_data_provider)
    pipeline: ResearchPipeline = field(init=False)
    paper_router: PaperExecutionRouter = field(default_factory=PaperExecutionRouter)
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
    approval_repo: ApprovalRepository = field(default_factory=ApprovalRepository)
    execution_control_repo: ExecutionControlRepository = field(default_factory=ExecutionControlRepository)
    source_snapshot_repo: SourceSnapshotRepository = field(default_factory=SourceSnapshotRepository)

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

    def record_paper_order(self, order: PaperOrder) -> None:
        self.paper_orders.append(order)
        self.paper_order_repo.add(order)
        self.position_repo.replace_all(self.positions.values())
        self.metrics_store.inc("paper_orders")
        self.metrics_store.set_gauge("open_positions", sum(1 for p in self.positions.values() if p.qty > 0))
        self.publish_event(
            EventType.PAPER_FILL,
            {
                "order_id": order.id,
                "recommendation_id": order.recommendation_id,
                "status": order.status.value,
                "fill_price": order.simulated_fill_price,
            },
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

        sold_at = request.sold_at or datetime.now(timezone.utc)
        realized_delta = (request.sell_price - holding.avg_buy_price) * sell_qty
        remaining_qty = round(max(0.0, holding.qty - sell_qty), 6)
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
            last_sell_price=request.sell_price,
            last_sell_reason=request.reason,
        )

        self.holdings_by_ticker[ticker_upper] = updated
        self.holding_watch_repo.upsert(updated)
        self.metrics_store.inc("manual_sells")
        self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        self.publish_event(
            EventType.PORTFOLIO_SELL,
            {
                "ticker": ticker_upper,
                "sold_qty": round(sell_qty, 6),
                "sell_price": request.sell_price,
                "realized_pnl_delta": round(realized_delta, 6),
                "remaining_qty": updated.qty,
                "status": updated.status.value,
                "reason": request.reason,
            },
        )

        action = "全部卖出并关闭持仓" if is_closed else f"卖出 {sell_qty:g} 股，剩余 {remaining_qty:g} 股"
        return SellExecutionResult(
            holding=updated,
            sold_qty=round(sell_qty, 6),
            sell_price=request.sell_price,
            realized_pnl_delta=round(realized_delta, 6),
            total_realized_pnl=total_realized,
            remaining_qty=updated.qty,
            message_cn=f"{ticker_upper} 已{action}，本次已实现盈亏 {realized_delta:.2f}。",
        )

    def list_open_holdings(self) -> list[HoldingWatch]:
        if self.holdings_by_ticker:
            return [item for item in self.holdings_by_ticker.values() if item.status == HoldingStatus.OPEN]
        holdings = self.holding_watch_repo.list_open()
        self.holdings_by_ticker = {item.ticker: item for item in holdings}
        return holdings

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
