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


class TradingStateMixin:
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
