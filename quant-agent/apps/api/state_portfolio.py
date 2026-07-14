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


class PortfolioAnalyticsMixin:
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
                idempotency_key=request.idempotency_key,
                qty=sell_qty,
                sell_price=sell_price,
                reason=reason,
                execution_mode=request.execution_mode,
                dry_run=request.dry_run,
                confirm_live=request.confirm_live,
            ),
        )
        return AlertExecutionResult(alert=alert, execution=execution, default_action_cn=default_action_cn)
