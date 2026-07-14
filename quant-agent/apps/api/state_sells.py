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


class SellExecutionStateMixin:
    def _sell_result_from_audit(self, audit: SellExecutionAudit, request: ManualSellRequest) -> SellExecutionResult:
        holding = self.holdings_by_ticker.get(audit.ticker) or self.holding_watch_repo.get(audit.ticker)
        if holding is None:
            raise ValueError("Sell execution audit has no matching holding")
        conflicts: list[str] = []
        if request.qty is not None and round(request.qty, 6) != round(audit.qty, 6):
            conflicts.append("qty")
        if round(request.sell_price, 6) != round(audit.sell_price, 6):
            conflicts.append("sell_price")
        if request.execution_mode != audit.execution_mode:
            conflicts.append("execution_mode")
        if request.dry_run != audit.dry_run:
            conflicts.append("dry_run")
        if conflicts:
            raise ValueError("idempotency_key_reused_with_different_sell_request: " + ",".join(conflicts))
        return SellExecutionResult(
            sell_execution_id=audit.id,
            client_order_id=audit.client_order_id,
            idempotency_key=audit.idempotency_key,
            holding=holding,
            sold_qty=audit.qty,
            sell_price=audit.sell_price,
            realized_pnl_delta=audit.realized_pnl_delta,
            estimated_realized_pnl_delta=audit.estimated_realized_pnl_delta,
            total_realized_pnl=holding.realized_pnl,
            remaining_qty=audit.remaining_qty,
            execution_mode=audit.execution_mode,
            dry_run=audit.dry_run,
            broker_order_id=audit.broker_order_id,
            adapter_message=audit.adapter_message,
            applied_to_ledger=audit.applied_to_ledger,
            message_cn=f"{audit.ticker} 卖出请求已按幂等键处理；状态 {audit.status}。",
        )

    def sell_holding(self, ticker: str, request: ManualSellRequest) -> SellExecutionResult:
        ticker_upper = ticker.upper()
        existing_audit: SellExecutionAudit | None = None
        if request.idempotency_key:
            existing_audit = self.sell_execution_audit_repo.get_by_idempotency_key(request.idempotency_key)
            if existing_audit is not None:
                if existing_audit.ticker != ticker_upper:
                    raise ValueError("idempotency_key_reused_for_different_ticker")
                replay = self._sell_result_from_audit(existing_audit, request)
                submitted_at = existing_audit.submitted_at
                if submitted_at.tzinfo is None:
                    submitted_at = submitted_at.replace(tzinfo=timezone.utc)
                fresh_pending = existing_audit.status == "pending_submit" and datetime.now(
                    timezone.utc
                ) - submitted_at < timedelta(seconds=30)
                if existing_audit.status not in {"pending_submit", "submit_unknown"} or fresh_pending:
                    return replay
        if existing_audit is None and self.kill_switch.enabled:
            raise ValueError("Execution is blocked by kill switch")
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
                if not request.idempotency_key:
                    raise ValueError("Live sell execution requires idempotency_key")
                if self.execution_router.broker_adapter is None:
                    raise NotImplementedError("Live broker sell adapter is not configured")

        execution_id = existing_audit.id if existing_audit is not None else uuid4().hex[:16]
        if existing_audit is not None and existing_audit.client_order_id:
            client_order_id = existing_audit.client_order_id
        elif request.idempotency_key:
            digest = hashlib.sha256(request.idempotency_key.encode("utf-8")).hexdigest()[:24]
            client_order_id = f"quant_sell_{digest}"
        else:
            client_order_id = f"quant_sell_{execution_id}"
        intent = SellExecutionAudit(
            id=execution_id,
            client_order_id=client_order_id,
            idempotency_key=request.idempotency_key,
            ticker=ticker_upper,
            qty=round(sell_qty, 6),
            sell_price=request.sell_price,
            submitted_at=datetime.now(timezone.utc),
            execution_mode=execution_mode,
            dry_run=request.dry_run,
            broker_order_id=None,
            adapter_message="sell_submission_intent_reserved",
            applied_to_ledger=False,
            status="pending_submit",
            reason=request.reason,
            source_recommendation_id=holding.source_recommendation_id,
            source_snapshot_id=provenance["source_snapshot_id"],
            strategy_config_id=provenance["strategy_config_id"],
            realized_pnl_delta=0.0,
            estimated_realized_pnl_delta=round(
                (request.sell_price - holding.avg_buy_price) * sell_qty,
                6,
            ),
            remaining_qty=holding.qty,
            holding_status_after=holding.status,
        )
        if existing_audit is None:
            reserved, created = self.sell_execution_audit_repo.reserve_intent(intent)
            if not created:
                return self._sell_result_from_audit(reserved, request)
        else:
            created = False

        if execution_mode == OrderExecutionMode.LIVE:
            if not request.dry_run:
                return self._sell_holding_live_broker(
                    holding=holding,
                    request=request,
                    sell_qty=sell_qty,
                    provenance=provenance,
                    execution_id=execution_id,
                    client_order_id=client_order_id,
                    recovering=not created,
                )

            if not created and self.kill_switch.enabled:
                raise ValueError("Sell recovery resubmission is blocked by kill switch")

            estimated_delta = (request.sell_price - holding.avg_buy_price) * sell_qty
            projected_remaining_qty = round(max(0.0, holding.qty - sell_qty), 6)
            broker_order_id = f"live_sell_dryrun_{uuid4().hex[:12]}"
            adapter_message = "live_sell_dry_run_only: order was validated but not sent to a broker"
            submitted_at = datetime.now(timezone.utc)
            audit = SellExecutionAudit(
                id=execution_id,
                client_order_id=client_order_id,
                idempotency_key=request.idempotency_key,
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
            self._commit_sell_execution(
                audit=audit,
                event_type=EventType.SELL_ROUTED,
                event_payload={
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
                client_order_id=audit.client_order_id,
                idempotency_key=audit.idempotency_key,
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

        audit = SellExecutionAudit(
            id=execution_id,
            client_order_id=client_order_id,
            idempotency_key=request.idempotency_key,
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
        trade = TradeLedgerEntry(
            trade_id=f"sell_fill_{audit.id}_{audit.qty:.6f}",
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
        self._commit_sell_execution(
            audit=audit,
            holding=updated,
            trade=trade,
            event_type=EventType.PORTFOLIO_SELL,
            event_payload={
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
        self.metrics_store.inc("manual_sells")

        action = "全部卖出并关闭持仓" if is_closed else f"卖出 {sell_qty:g} 股，剩余 {remaining_qty:g} 股"
        return SellExecutionResult(
            sell_execution_id=audit.id,
            client_order_id=audit.client_order_id,
            idempotency_key=audit.idempotency_key,
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
        execution_id: str,
        client_order_id: str,
        recovering: bool,
    ) -> SellExecutionResult:
        ticker_upper = holding.ticker.upper()
        broker_adapter = self.execution_router.broker_adapter
        if broker_adapter is None:
            raise NotImplementedError("Live broker sell adapter is not configured")

        broker_update = None
        if recovering:
            try:
                broker_update = broker_adapter.get_order_by_client_order_id(client_order_id)
            except BrokerOrderNotFoundError:
                broker_update = None
            except BrokerAdapterError:
                raise

        if broker_update is None:
            if recovering and self.kill_switch.enabled:
                raise ValueError("Sell recovery resubmission is blocked by kill switch")
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
            except BrokerAdapterError as exc:
                unknown = SellExecutionAudit(
                    id=execution_id,
                    client_order_id=client_order_id,
                    idempotency_key=request.idempotency_key,
                    ticker=ticker_upper,
                    qty=round(sell_qty, 6),
                    sell_price=request.sell_price,
                    submitted_at=datetime.now(timezone.utc),
                    execution_mode=OrderExecutionMode.LIVE,
                    dry_run=False,
                    broker_order_id=None,
                    adapter_message=f"broker_sell_submit_outcome_unknown: {exc}",
                    applied_to_ledger=False,
                    status="submit_unknown",
                    reason=request.reason,
                    source_recommendation_id=holding.source_recommendation_id,
                    source_snapshot_id=provenance["source_snapshot_id"],
                    strategy_config_id=provenance["strategy_config_id"],
                    realized_pnl_delta=0.0,
                    estimated_realized_pnl_delta=round(
                        (request.sell_price - holding.avg_buy_price) * sell_qty,
                        6,
                    ),
                    remaining_qty=holding.qty,
                    holding_status_after=holding.status,
                )
                self.sell_execution_audit_repo.add(unknown)
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
            audit = SellExecutionAudit(
                id=execution_id,
                client_order_id=client_order_id,
                idempotency_key=request.idempotency_key,
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
            trade = TradeLedgerEntry(
                trade_id=f"sell_fill_{audit.id}_{audit.qty:.6f}",
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
            self._commit_sell_execution(
                audit=audit,
                holding=updated,
                trade=trade,
                event_type=EventType.PORTFOLIO_SELL,
                event_payload={
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
            self.metrics_store.inc("live_sells")
            action = "全部卖出并关闭持仓" if is_closed else f"卖出 {actual_qty:g} 股，剩余 {remaining_qty:g} 股"
            return SellExecutionResult(
                sell_execution_id=audit.id,
                client_order_id=audit.client_order_id,
                idempotency_key=audit.idempotency_key,
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
            id=execution_id,
            client_order_id=client_order_id,
            idempotency_key=request.idempotency_key,
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
        self._commit_sell_execution(
            audit=audit,
            event_type=EventType.SELL_ROUTED,
            event_payload={
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
            client_order_id=audit.client_order_id,
            idempotency_key=audit.idempotency_key,
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
