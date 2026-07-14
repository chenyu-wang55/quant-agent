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


class SellSyncStateMixin:
    def recover_sell_submission(self, audit: SellExecutionAudit) -> SellExecutionResult:
        return self.sell_holding(
            audit.ticker,
            ManualSellRequest(
                idempotency_key=audit.idempotency_key,
                qty=audit.qty,
                sell_price=audit.sell_price,
                reason=audit.reason,
                execution_mode=audit.execution_mode,
                dry_run=audit.dry_run,
                confirm_live=audit.execution_mode == OrderExecutionMode.LIVE and not audit.dry_run,
            ),
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
        cumulative_qty = round(already_applied_qty + incremental_qty, 6)
        updated_audit = SellExecutionAudit(
            id=audit.id,
            client_order_id=audit.client_order_id,
            idempotency_key=audit.idempotency_key,
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
        trade = TradeLedgerEntry(
            trade_id=f"sell_fill_{audit.id}_{cumulative_qty:.6f}",
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
        self._commit_sell_execution(
            audit=updated_audit,
            holding=updated_holding,
            trade=trade,
            event_type=EventType.PORTFOLIO_SELL,
            event_payload={
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
        self.metrics_store.inc("live_sell_sync_fills")
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
                    self._commit_sell_execution(
                        audit=updated,
                        event_type=EventType.SELL_ROUTED,
                        event_payload={
                            "sell_execution_id": updated.id,
                            "ticker": updated.ticker,
                            "broker_order_id": updated.broker_order_id,
                            "status": updated.status,
                            "adapter_message": updated.adapter_message,
                            "applied_to_ledger": False,
                        },
                    )
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
                            message_cn="已回写 broker 卖出取消/拒单。"
                            if action == "canceled"
                            else "卖出审计已是 canceled。",
                        )
                    )
                else:
                    if audit.applied_to_ledger:
                        skipped_count += 1
                        action = "skipped"
                        message = "本地卖出审计已落账，跳过 submitted 状态。"
                    else:
                        updated = audit.model_copy(update={"status": "submitted", "adapter_message": adapter_message})
                        self._commit_sell_execution(
                            audit=updated,
                            event_type=EventType.SELL_ROUTED,
                            event_payload={
                                "sell_execution_id": updated.id,
                                "ticker": updated.ticker,
                                "broker_order_id": updated.broker_order_id,
                                "status": updated.status,
                                "adapter_message": updated.adapter_message,
                                "applied_to_ledger": False,
                            },
                        )
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
