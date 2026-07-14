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


class OrderReplayAndRiskStateMixin:
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
            baseline_values = self._recommendation_compare_values(baseline_rec) if baseline_rec is not None else {}
            replay_values = self._recommendation_compare_values(replay_rec) if replay_rec is not None else {}

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
        risk_per_share = entry_price - stop_loss if side == Direction.BUY else stop_loss - entry_price
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
        max_risk_qty = self._floor_qty(risk_budget / risk_per_share) if risk_per_share > 0 else 0.0
        max_position_qty = self._floor_qty(remaining_position_value / entry_price) if entry_price > 0 else 0.0
        max_gross_qty = self._floor_qty(remaining_gross_exposure_value / entry_price) if entry_price > 0 else 0.0
        max_sector_qty = self._floor_qty(remaining_sector_exposure_value / entry_price) if entry_price > 0 else 0.0
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
            message_cn = f"风险校验未通过：{reasons}。建议最多 {recommended_qty:g} 股，当前请求 {request.qty:g} 股。"

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
