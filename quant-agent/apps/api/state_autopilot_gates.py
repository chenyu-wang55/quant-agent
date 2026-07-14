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


class AutopilotGateStateMixin:
    def get_autopilot_daily_usage(
        self,
        policy: AutopilotPolicy | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        timestamp = as_of or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        eastern = timestamp.astimezone(ZoneInfo("America/New_York"))
        trading_day = eastern.date().isoformat()
        runs = self.list_system_cycle_runs(limit=1000, status="success")
        used_approvals = 0
        used_buys = 0
        used_sells = 0
        for run in runs:
            run_day = run.started_at.astimezone(ZoneInfo("America/New_York")).date().isoformat()
            if run_day != trading_day:
                continue
            auto_approval = run.metrics.get("auto_approval", {})
            auto_execution = run.metrics.get("auto_execution", {})
            used_approvals += int(auto_approval.get("approved_count") or 0)
            used_buys += int(auto_execution.get("buy_order_count") or 0)
            used_sells += int(auto_execution.get("sell_order_count") or 0)

        policy = policy or self.get_autopilot_policy()
        return {
            "trading_day": trading_day,
            "used_approvals": used_approvals,
            "used_buys": used_buys,
            "used_sells": used_sells,
            "max_daily_auto_approvals": policy.max_daily_auto_approvals,
            "max_daily_auto_buys": policy.max_daily_auto_buys,
            "max_daily_auto_sells": policy.max_daily_auto_sells,
            "remaining_approvals": max(0, policy.max_daily_auto_approvals - used_approvals),
            "remaining_buys": max(0, policy.max_daily_auto_buys - used_buys),
            "remaining_sells": max(0, policy.max_daily_auto_sells - used_sells),
        }

    def get_autopilot_daily_loss_gate(
        self,
        policy: AutopilotPolicy | None = None,
        as_of: datetime | None = None,
        account_equity: float | None = None,
        max_daily_realized_loss_pct: float | None = None,
    ) -> dict[str, Any]:
        timestamp = as_of or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        eastern = timestamp.astimezone(ZoneInfo("America/New_York"))
        trading_day = eastern.date().isoformat()
        policy = policy or self.get_autopilot_policy()
        equity = float(account_equity if account_equity is not None else policy.account_equity)
        loss_limit = float(
            max_daily_realized_loss_pct
            if max_daily_realized_loss_pct is not None
            else policy.max_daily_realized_loss_pct
        )
        sell_trades = self.list_trade_ledger(limit=10000, side=TradeSide.SELL)
        daily_sell_trades = []
        daily_realized_pnl = 0.0
        for trade in sell_trades:
            executed_at = trade.executed_at
            if executed_at.tzinfo is None:
                executed_at = executed_at.replace(tzinfo=timezone.utc)
            trade_day = executed_at.astimezone(ZoneInfo("America/New_York")).date().isoformat()
            if trade_day != trading_day:
                continue
            daily_sell_trades.append(trade)
            daily_realized_pnl += trade.realized_pnl_delta

        daily_realized_pnl = round(daily_realized_pnl, 6)
        daily_realized_loss = round(max(0.0, -daily_realized_pnl), 6)
        daily_realized_loss_pct = round(daily_realized_loss / equity, 6) if equity > 0 else 1.0
        passed = daily_realized_loss_pct <= loss_limit
        return {
            "passed": passed,
            "reason": None if passed else "daily_realized_loss_above_policy_limit",
            "trading_day": trading_day,
            "daily_realized_pnl": daily_realized_pnl,
            "daily_realized_loss": daily_realized_loss,
            "daily_realized_loss_pct": daily_realized_loss_pct,
            "max_daily_realized_loss_pct": loss_limit,
            "account_equity": equity,
            "sell_trade_count": len(daily_sell_trades),
        }

    def get_market_session_status(self, as_of: datetime | None = None) -> MarketSessionStatus:
        timestamp = as_of or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        eastern = timestamp.astimezone(ZoneInfo("America/New_York"))
        regular_open = time(hour=9, minute=30)
        exchange_session = xnys_session(eastern.date())
        close_hour, close_minute = (int(part) for part in exchange_session.close_time.split(":"))
        regular_close = time(hour=close_hour, minute=close_minute)
        local_time = eastern.time().replace(tzinfo=None)
        is_weekday = eastern.weekday() < 5
        is_regular_session = exchange_session.is_trading_day and regular_open <= local_time < regular_close
        return MarketSessionStatus(
            as_of=timestamp,
            local_time=eastern.strftime("%Y-%m-%d %H:%M:%S %Z"),
            regular_close_time=exchange_session.close_time,
            is_weekday=is_weekday,
            is_trading_day=exchange_session.is_trading_day,
            is_early_close=exchange_session.is_early_close,
            holiday_name=exchange_session.holiday_name,
            is_regular_session=is_regular_session,
            status=(
                "early_close"
                if is_regular_session and exchange_session.is_early_close
                else "regular"
                if is_regular_session
                else "closed"
            ),
        )

    def get_rebuy_cooldown(
        self,
        ticker: str,
        cooldown_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        if cooldown_minutes <= 0:
            return {"active": False, "ticker": ticker.upper(), "cooldown_minutes": cooldown_minutes}
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        recent_sells = self.list_trade_ledger(
            limit=100,
            ticker=ticker.upper(),
            side=TradeSide.SELL,
        )
        last_sell = None
        sold_at = None
        for candidate in recent_sells:
            candidate_sold_at = candidate.executed_at
            if candidate_sold_at.tzinfo is None:
                candidate_sold_at = candidate_sold_at.replace(tzinfo=timezone.utc)
            candidate_sold_at = candidate_sold_at.astimezone(timezone.utc)
            if candidate_sold_at <= now:
                last_sell = candidate
                sold_at = candidate_sold_at
                break
        if last_sell is None or sold_at is None:
            return {"active": False, "ticker": ticker.upper(), "cooldown_minutes": cooldown_minutes}
        cooldown_until = sold_at + timedelta(minutes=cooldown_minutes)
        active = now < cooldown_until
        return {
            "active": active,
            "ticker": ticker.upper(),
            "cooldown_minutes": cooldown_minutes,
            "last_sell_trade_id": last_sell.trade_id,
            "last_sell_at": sold_at.isoformat(),
            "cooldown_until": cooldown_until.isoformat(),
            "minutes_remaining": max(0, int((cooldown_until - now).total_seconds() // 60)),
        }

    def get_sell_alert_cooldown(
        self,
        ticker: str,
        reason_code: str,
        cooldown_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        if cooldown_minutes <= 0:
            return {
                "active": False,
                "ticker": ticker_upper,
                "reason_code": reason_code,
                "cooldown_minutes": cooldown_minutes,
            }
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        alert_marker = f"alert:{reason_code}"
        recent_executions = self.list_sell_execution_audits(limit=1000, ticker=ticker_upper)
        for execution in recent_executions:
            if alert_marker not in (execution.reason or ""):
                continue
            submitted_at = execution.submitted_at
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=timezone.utc)
            submitted_at = submitted_at.astimezone(timezone.utc)
            if submitted_at > now:
                continue
            cooldown_until = submitted_at + timedelta(minutes=cooldown_minutes)
            if now >= cooldown_until:
                continue
            return {
                "active": True,
                "ticker": ticker_upper,
                "reason_code": reason_code,
                "cooldown_minutes": cooldown_minutes,
                "last_sell_execution_id": execution.id,
                "last_sell_reason": execution.reason,
                "last_sell_at": submitted_at.isoformat(),
                "cooldown_until": cooldown_until.isoformat(),
                "minutes_remaining": max(0, int((cooldown_until - now).total_seconds() // 60)),
            }
        return {
            "active": False,
            "ticker": ticker_upper,
            "reason_code": reason_code,
            "cooldown_minutes": cooldown_minutes,
        }

    def get_pending_sell_order_gate(
        self,
        *,
        ticker: str,
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        pending_statuses = {
            "accepted",
            "accepted_for_bidding",
            "new",
            "partially_filled",
            "pending_cancel",
            "pending_new",
            "pending_replace",
            "submitted",
        }
        recent_executions = self.list_sell_execution_audits(limit=1000, ticker=ticker_upper)
        for execution in recent_executions:
            status = (execution.status or "").strip().lower()
            if status not in pending_statuses:
                continue
            if execution.execution_mode != OrderExecutionMode.LIVE or execution.dry_run:
                continue
            if not execution.broker_order_id:
                continue
            return {
                "passed": False,
                "reason": "pending_live_sell_order",
                "ticker": ticker_upper,
                "reason_code": reason_code,
                "pending_sell_execution_id": execution.id,
                "pending_broker_order_id": execution.broker_order_id,
                "pending_status": execution.status,
                "pending_qty": execution.qty,
                "pending_remaining_qty": execution.remaining_qty,
                "pending_submitted_at": execution.submitted_at.isoformat(),
                "pending_applied_to_ledger": execution.applied_to_ledger,
                "pending_reason": execution.reason,
            }
        return {
            "passed": True,
            "ticker": ticker_upper,
            "reason_code": reason_code,
        }

    def get_recent_buy_order_gate(
        self,
        ticker: str,
        recommendation_id: str,
        order_dedupe_minutes: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        if order_dedupe_minutes <= 0:
            return {
                "passed": True,
                "ticker": ticker_upper,
                "recommendation_id": recommendation_id,
                "order_dedupe_minutes": order_dedupe_minutes,
            }
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        recent_orders = self.list_paper_orders(limit=1000, side=Direction.BUY)
        for order in recent_orders:
            if order.status in {
                PaperOrderStatus.CANCELED,
                PaperOrderStatus.SUBMIT_FAILED,
            }:
                continue
            submitted_at = order.submitted_at
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=timezone.utc)
            submitted_at = submitted_at.astimezone(timezone.utc)
            effective_submitted_at = submitted_at if submitted_at <= now else now
            cooldown_until = effective_submitted_at + timedelta(minutes=order_dedupe_minutes)
            if now >= cooldown_until:
                continue

            match_reason = None
            if order.recommendation_id == recommendation_id:
                match_reason = "same_recommendation_recent_buy_order"
            else:
                source_recommendation = self.recommendations_by_id.get(
                    order.recommendation_id
                ) or self.recommendation_repo.get(order.recommendation_id)
                if source_recommendation is not None and source_recommendation.ticker.upper() == ticker_upper:
                    match_reason = "same_ticker_recent_buy_order"
            if match_reason is None:
                continue

            return {
                "passed": False,
                "reason": match_reason,
                "ticker": ticker_upper,
                "recommendation_id": recommendation_id,
                "order_dedupe_minutes": order_dedupe_minutes,
                "recent_order_id": order.id,
                "recent_order_recommendation_id": order.recommendation_id,
                "recent_order_status": order.status.value,
                "recent_order_execution_mode": order.execution_mode.value,
                "recent_order_submitted_at": submitted_at.isoformat(),
                "recent_order_effective_submitted_at": effective_submitted_at.isoformat(),
                "recent_order_clock_skew_seconds": max(0, int((submitted_at - now).total_seconds())),
                "dedupe_until": cooldown_until.isoformat(),
                "minutes_remaining": max(0, int((cooldown_until - now).total_seconds() // 60)),
            }

        return {
            "passed": True,
            "ticker": ticker_upper,
            "recommendation_id": recommendation_id,
            "order_dedupe_minutes": order_dedupe_minutes,
        }

    def get_pending_buy_order_gate(
        self,
        *,
        ticker: str,
        recommendation_id: str,
    ) -> dict[str, Any]:
        ticker_upper = ticker.upper()
        pending_orders = [
            order
            for order in self.list_paper_orders(limit=1000, side=Direction.BUY)
            if order.status
            in {
                PaperOrderStatus.PENDING_SUBMIT,
                PaperOrderStatus.SUBMIT_UNKNOWN,
                PaperOrderStatus.SUBMITTED,
            }
        ]
        for order in pending_orders:
            match_reason = None
            if order.recommendation_id == recommendation_id:
                match_reason = "same_recommendation_pending_buy_order"
            else:
                source_recommendation = self.recommendations_by_id.get(
                    order.recommendation_id
                ) or self.recommendation_repo.get(order.recommendation_id)
                if source_recommendation is not None and source_recommendation.ticker.upper() == ticker_upper:
                    match_reason = "same_ticker_pending_buy_order"
            if match_reason is None:
                continue

            return {
                "passed": False,
                "reason": match_reason,
                "ticker": ticker_upper,
                "recommendation_id": recommendation_id,
                "pending_order_id": order.id,
                "pending_order_recommendation_id": order.recommendation_id,
                "pending_order_status": order.status.value,
                "pending_order_execution_mode": order.execution_mode.value,
                "pending_order_submitted_at": order.submitted_at.isoformat(),
                "pending_order_broker_order_id": order.broker_order_id,
            }

        return {
            "passed": True,
            "ticker": ticker_upper,
            "recommendation_id": recommendation_id,
        }

    def update_autopilot_policy(self, updates: dict[str, Any]) -> AutopilotPolicy:
        current = self.get_autopilot_policy()
        payload = current.model_dump(mode="python")
        payload.pop("policy_id", None)
        payload.update(updates)
        payload["updated_at"] = datetime.now(timezone.utc)
        payload["updated_by"] = str(payload.get("updated_by") or "operator")
        policy = AutopilotPolicy(**payload)
        self.autopilot_policy = self.autopilot_policy_repo.set_policy(policy)
        self.metrics_store.set_gauge("autopilot_policy_enabled", 1.0 if policy.enabled else 0.0)
        self.metrics_store.set_gauge(
            "autopilot_auto_approval_enabled",
            1.0 if policy.enabled and policy.auto_approve_recommendations else 0.0,
        )
        self.metrics_store.set_gauge(
            "autopilot_auto_execution_enabled",
            1.0 if policy.enabled and policy.auto_execute_approved else 0.0,
        )
        return self.autopilot_policy
