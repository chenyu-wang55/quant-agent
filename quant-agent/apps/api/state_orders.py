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


class OrderStateMixin:
    def record_paper_order(
        self,
        order: PaperOrder,
        recommendation: Recommendation | None = None,
        *,
        positions: dict[str, PositionState] | None = None,
        extra_events: list[SystemEvent] | None = None,
    ) -> None:
        effective_positions = positions if positions is not None else self.positions
        routed_event = SystemEvent(
            id=f"order_routed_{order.id}",
            event_type=EventType.ORDER_ROUTED,
            payload={
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
        events = [routed_event, *(extra_events or [])]
        if order.status == PaperOrderStatus.FILLED and order.simulated_fill_price is not None:
            events.append(
                SystemEvent(
                    id=f"paper_fill_{order.id}",
                    event_type=EventType.PAPER_FILL,
                    payload={
                        "order_id": order.id,
                        "recommendation_id": order.recommendation_id,
                        "status": order.status.value,
                        "fill_price": order.simulated_fill_price,
                    },
                )
            )

        result = self.order_unit_of_work.commit(
            order=order,
            positions=effective_positions,
            recommendation=recommendation,
            events=events,
        )
        self.positions = effective_positions
        self._cache_paper_order(order)
        if result.holding is not None:
            self.holdings_by_ticker[result.holding.ticker] = result.holding
        self.metrics_store.inc("paper_orders")
        self.metrics_store.set_gauge("open_positions", sum(1 for p in self.positions.values() if p.qty > 0))
        if result.ledger_applied:
            self.metrics_store.inc("trade_ledger_buy")
            self.metrics_store.inc("order_buy_fills")
            self.metrics_store.set_gauge("open_holdings", len(self.list_open_holdings()))
        for event in events:
            self.event_queue.publish(event)
            self.metrics_store.inc(f"event_published_{event.event_type.value}")
        self.metrics_store.set_gauge("event_queue_pending", self.pending_event_count())

    def _cache_paper_order(self, order: PaperOrder) -> None:
        for index, item in enumerate(self.paper_orders):
            if item.id == order.id:
                self.paper_orders[index] = order
                return
        self.paper_orders.append(order)

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

    def get_paper_order_by_idempotency_key(self, idempotency_key: str | None) -> PaperOrder | None:
        if not idempotency_key:
            return None
        order = self.paper_order_repo.get_by_idempotency_key(idempotency_key)
        if order is not None:
            return order
        return next((item for item in self.paper_orders if item.idempotency_key == idempotency_key), None)

    def submit_order(
        self,
        *,
        recommendation: Recommendation,
        request: PaperOrderRequest,
    ) -> PaperOrder:
        """Persist the order intent before routing and collapse concurrent retries."""

        intent = self.execution_router.prepare_intent(
            recommendation=recommendation,
            request=request,
        )
        reserved, created = self.paper_order_repo.reserve_intent(intent)
        self._cache_paper_order(reserved)
        logger.info(
            "order intent reserved" if created else "idempotent order intent reused",
            extra={
                "event": "order_intent_reserved" if created else "order_intent_reused",
                "order_id": reserved.id,
                "client_order_id": reserved.client_order_id,
                "ticker": recommendation.ticker,
                "execution_mode": str(request.execution_mode),
            },
        )
        if not created:
            return reserved

        try:
            if request.side == Direction.BUY and request.enforce_risk_limits:
                entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
                entry_price = float(request.limit_price if request.limit_price is not None else entry_mid)
                self.portfolio_risk_reservation_repo.reserve(
                    order_id=intent.id,
                    ticker=recommendation.ticker,
                    requested_notional=entry_price * request.qty,
                    account_equity=request.account_equity,
                    max_gross_exposure_pct=request.max_gross_exposure_pct,
                )
            order, updated_positions = self.execution_router.submit(
                recommendation=recommendation,
                request=request,
                positions=self.positions,
                local_order_id=intent.id,
                client_order_id=intent.client_order_id,
            )
        except BrokerAdapterError as exc:
            logger.exception(
                "broker submission outcome is unknown",
                extra={
                    "event": "order_submit_unknown",
                    "order_id": intent.id,
                    "client_order_id": intent.client_order_id,
                    "ticker": recommendation.ticker,
                },
            )
            unknown = intent.model_copy(
                update={
                    "status": PaperOrderStatus.SUBMIT_UNKNOWN,
                    "adapter_message": f"broker_submit_outcome_unknown: {exc}",
                }
            )
            self.paper_order_repo.add(unknown)
            self._cache_paper_order(unknown)
            self.metrics_store.inc("paper_order_submit_unknown")
            try:
                return self.recover_order_submission(unknown, recommendation=recommendation)
            except BrokerAdapterError:
                raise exc
        except (ValueError, NotImplementedError) as exc:
            logger.warning(
                "order submission failed before confirmation",
                extra={
                    "event": "order_submit_failed",
                    "order_id": intent.id,
                    "client_order_id": intent.client_order_id,
                    "ticker": recommendation.ticker,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            self.portfolio_risk_reservation_repo.release(intent.id)
            failed = intent.model_copy(
                update={
                    "status": PaperOrderStatus.SUBMIT_FAILED,
                    "adapter_message": f"submission_failed_before_confirmation: {exc}",
                }
            )
            self.paper_order_repo.add(failed)
            self._cache_paper_order(failed)
            self.metrics_store.inc("paper_order_submit_failed")
            raise

        self.record_paper_order(
            order,
            recommendation=recommendation,
            positions=updated_positions,
        )
        logger.info(
            "order submission completed",
            extra={
                "event": "order_submit_completed",
                "order_id": order.id,
                "client_order_id": order.client_order_id,
                "broker_order_id": order.broker_order_id,
                "ticker": recommendation.ticker,
                "status": str(order.status),
            },
        )
        return order

    def recover_order_submission(
        self,
        order: PaperOrder,
        *,
        recommendation: Recommendation | None = None,
    ) -> PaperOrder:
        """Reconcile a reserved/unknown order by its stable broker client id."""

        if order.status not in {
            PaperOrderStatus.PENDING_SUBMIT,
            PaperOrderStatus.SUBMIT_UNKNOWN,
        }:
            return order
        recommendation = recommendation or self._get_recommendation_by_id(order.recommendation_id)
        if recommendation is None:
            raise ValueError("Recommendation id not found for order recovery")
        request = PaperOrderRequest(
            recommendation_id=order.recommendation_id,
            idempotency_key=order.idempotency_key,
            side=order.side,
            qty=order.qty,
            limit_price=order.limit_price,
            execution_mode=order.execution_mode,
            dry_run=order.dry_run,
            confirm_live=order.execution_mode == OrderExecutionMode.LIVE and not order.dry_run,
            enforce_risk_limits=False,
        )

        if order.execution_mode == OrderExecutionMode.LIVE and not order.dry_run:
            broker_adapter = self.execution_router.broker_adapter
            if broker_adapter is None:
                raise BrokerAdapterError("Live broker execution adapter is not configured")
            if not order.client_order_id:
                raise BrokerAdapterError("Recoverable live order is missing client_order_id")
            try:
                broker_update = broker_adapter.get_order_by_client_order_id(order.client_order_id)
                recovered = self.execution_router.order_from_broker_update(
                    recommendation=recommendation,
                    request=request,
                    local_order_id=order.id,
                    client_order_id=order.client_order_id,
                    broker_update=broker_update,
                )
                self.record_paper_order(recovered, recommendation=recommendation)
                self.metrics_store.inc("paper_order_submit_recovered")
                return recovered
            except BrokerOrderNotFoundError:
                # The broker definitively has no such client id. Resubmitting with
                # the same client id is safe and closes the crash-before-send gap.
                pass
            except BrokerAdapterError:
                raise

        if self.kill_switch.enabled:
            raise ValueError("Order recovery resubmission is blocked by kill switch")

        recovered, updated_positions = self.execution_router.submit(
            recommendation=recommendation,
            request=request,
            positions=self.positions,
            local_order_id=order.id,
            client_order_id=order.client_order_id,
        )
        self.record_paper_order(
            recovered,
            recommendation=recommendation,
            positions=updated_positions,
        )
        self.metrics_store.inc("paper_order_submit_recovered")
        return recovered

    def cancel_paper_order(self, order_id: str, request: PaperOrderCancelRequest) -> PaperOrder:
        order = self.get_paper_order(order_id)
        if order is None:
            raise KeyError("paper order not found")
        if order.status == PaperOrderStatus.CANCELED:
            return order
        if order.status != PaperOrderStatus.SUBMITTED:
            raise ValueError("Only submitted orders can be canceled")

        broker_cancel_message: str | None = None
        if order.execution_mode == OrderExecutionMode.LIVE and not order.dry_run and not request.skip_broker_cancel:
            if not order.broker_order_id:
                raise ValueError("Live broker order is missing broker_order_id")
            broker_adapter = self.execution_router.broker_adapter
            if broker_adapter is None:
                raise NotImplementedError("Live broker cancel adapter is not configured")
            cancel_order = getattr(broker_adapter, "cancel_order", None)
            if not callable(cancel_order):
                raise NotImplementedError("Live broker cancel adapter does not support order cancellation")
            try:
                broker_update = cancel_order(order.broker_order_id)
            except BrokerAdapterError:
                raise
            except Exception as exc:
                raise BrokerAdapterError(f"Live broker order cancel failed: {exc}") from exc
            broker_status = broker_update.raw_status.lower()
            if broker_status in {"filled", "partially_filled"}:
                raise ValueError("Broker order already filled; sync broker status before canceling locally")
            if broker_status not in {"canceled", "cancelled", "expired", "rejected", "pending_cancel"}:
                raise BrokerAdapterError(f"Broker cancel returned unsupported status: {broker_update.raw_status}")
            broker_cancel_message = (
                f"{broker_adapter.name}: cancel_status={broker_update.raw_status}; "
                f"broker_order_id={broker_update.broker_order_id or order.broker_order_id}"
            )
            if broker_update.message:
                broker_cancel_message = f"{broker_cancel_message}; message={broker_update.message}"

        reason = request.reason or f"canceled_by:{request.canceled_by}"
        cancel_message = f"canceled_by={request.canceled_by}"
        if broker_cancel_message:
            cancel_message = f"{cancel_message}; {broker_cancel_message}"
        updated = order.model_copy(
            update={
                "status": PaperOrderStatus.CANCELED,
                "cancel_reason": reason,
                "adapter_message": (
                    f"{order.adapter_message}; {cancel_message}" if order.adapter_message else cancel_message
                ),
            }
        )
        canceled_event = SystemEvent(
            id=f"order_canceled_{updated.id}",
            event_type=EventType.ORDER_CANCELED,
            payload={
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
        self.record_paper_order(updated, extra_events=[canceled_event])
        self.metrics_store.inc("paper_orders_canceled")
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
                    f"{order.adapter_message}; {fill_message}" if order.adapter_message else fill_message
                ),
            }
        )
        self.record_paper_order(
            updated,
            recommendation=recommendation if apply_to_ledger else None,
        )

        self.metrics_store.inc("paper_order_fills")
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
                snapshot.apply_to_ledger if snapshot.apply_to_ledger is not None else request.apply_fills_to_ledger
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
                            skip_broker_cancel=True,
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
