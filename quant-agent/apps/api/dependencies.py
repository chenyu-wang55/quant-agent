from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
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
    PaperOrder,
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
    SellExecutionResult,
    SignalSnapshot,
    SnapshotMode,
    SnapshotAttribution,
    SourceSnapshotDetail,
    SourceSnapshotReplayRequest,
    SourceSnapshotSummary,
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
    SignalRepository,
    SourceSnapshotRepository,
    TradeLedgerRepository,
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
    trade_ledger_repo: TradeLedgerRepository = field(default_factory=TradeLedgerRepository)
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

    def record_paper_order(self, order: PaperOrder, recommendation: Recommendation | None = None) -> None:
        self.paper_orders.append(order)
        self.paper_order_repo.add(order)
        self.position_repo.replace_all(self.positions.values())
        self.metrics_store.inc("paper_orders")
        self.metrics_store.set_gauge("open_positions", sum(1 for p in self.positions.values() if p.qty > 0))
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

    def _get_recommendation(self, recommendation_id: str) -> Recommendation | None:
        cached = self.recommendations_by_id.get(recommendation_id)
        if cached is not None:
            return cached
        return self.recommendation_repo.get(recommendation_id)

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
            pnls = [
                pnl
                for item in items
                for pnl in pnls_by_recommendation.get(item.recommendation_id, [])
            ]
            wins = [pnl for pnl in pnls if pnl > 0]
            losses = [pnl for pnl in pnls if pnl < 0]
            sell_count = sum(item.sell_trade_count for item in items)
            by_snapshot.append(
                SnapshotAttribution(
                    source_snapshot_id=source_snapshot_id,
                    recommendation_count=len(items),
                    sell_trade_count=sell_count,
                    total_realized_pnl=round(sum(pnls), 6),
                    win_count=len(wins),
                    loss_count=len(losses),
                    win_rate=round(len(wins) / len(pnls), 6) if pnls else 0.0,
                    profit_factor=self._profit_factor(pnls),
                )
            )

        by_snapshot.sort(key=lambda item: item.total_realized_pnl, reverse=True)
        return RecommendationAttributionReport(
            generated_at=datetime.now(timezone.utc),
            recommendation_count=len(by_recommendation),
            attributed_sell_trade_count=len(attributed),
            unattributed_sell_trade_count=unattributed_count,
            total_realized_pnl=round(sum(item.total_realized_pnl for item in by_recommendation), 6),
            by_recommendation=by_recommendation,
            by_snapshot=by_snapshot,
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
            request=ManualSellRequest(qty=sell_qty, sell_price=sell_price, reason=reason),
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
