from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from apps.api.dependencies import get_app_state
from domain.entities.models import (
    BrokerOrderStatusSnapshot,
    BrokerOrderSyncRequest,
    BrokerOrderSyncStatus,
    BrokerPositionSnapshot,
    Direction,
    OrderExecutionMode,
    PaperOrderStatus,
    PositionReconciliationRequest,
)
from services.execution.broker_adapter import (
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerOrderUpdate,
    BrokerPositionUpdate,
)


def _broker_sync_status(raw_status: str) -> BrokerOrderSyncStatus:
    normalized = raw_status.strip().lower().replace(" ", "_")
    if normalized == "filled":
        return BrokerOrderSyncStatus.FILLED
    if normalized == "partially_filled":
        return BrokerOrderSyncStatus.PARTIALLY_FILLED
    if normalized in {"canceled", "cancelled", "expired"}:
        return BrokerOrderSyncStatus.CANCELED
    if normalized == "rejected":
        return BrokerOrderSyncStatus.REJECTED
    return BrokerOrderSyncStatus.SUBMITTED


def _broker_status_snapshot(
    *,
    local_order_id: str,
    fallback_broker_order_id: str,
    update: BrokerOrderUpdate,
) -> BrokerOrderStatusSnapshot:
    status = _broker_sync_status(update.raw_status)
    fill_price = None
    if status in {BrokerOrderSyncStatus.FILLED, BrokerOrderSyncStatus.PARTIALLY_FILLED}:
        if update.filled_avg_price is None:
            raise ValueError("broker fill status is missing filled_avg_price")
        fill_price = float(update.filled_avg_price)
    return BrokerOrderStatusSnapshot(
        order_id=local_order_id,
        broker_order_id=update.broker_order_id or fallback_broker_order_id,
        status=status,
        fill_price=fill_price,
        filled_qty=update.filled_qty,
        filled_at=update.filled_at,
        reason=update.message,
        broker_message=update.message or f"raw_status={update.raw_status}",
    )


def _auto_broker_sync_cycle(
    *,
    enabled: bool,
    checked_at: datetime,
    max_items: int,
) -> dict[str, Any]:
    state = get_app_state()
    item_limit = max(0, max_items)
    recent_buy_orders = state.list_paper_orders(
        limit=max(1, item_limit * 3),
        side=Direction.BUY,
    )
    recoverable_buy_orders = [
        order
        for order in recent_buy_orders
        if order.execution_mode == OrderExecutionMode.LIVE
        and not order.dry_run
        and order.client_order_id
        and (
            order.status == PaperOrderStatus.SUBMIT_UNKNOWN
            or (
                order.status == PaperOrderStatus.PENDING_SUBMIT
                and checked_at - (
                    order.submitted_at
                    if order.submitted_at.tzinfo is not None
                    else order.submitted_at.replace(tzinfo=timezone.utc)
                )
                >= timedelta(seconds=30)
            )
        )
    ][:item_limit]
    pending_buy_orders = [
        order
        for order in recent_buy_orders
        if order.execution_mode == OrderExecutionMode.LIVE
        and not order.dry_run
        and order.broker_order_id
        and order.status == PaperOrderStatus.SUBMITTED
    ][:item_limit]
    pending_sell_audits = [
        audit
        for audit in state.list_sell_execution_audits(limit=max(1, item_limit))
        if audit.execution_mode == OrderExecutionMode.LIVE
        and not audit.dry_run
        and audit.broker_order_id
        and audit.status in {"submitted", "partially_filled"}
    ][:item_limit]
    recoverable_sell_audits = [
        audit
        for audit in state.list_sell_execution_audits(limit=max(1, item_limit * 3))
        if audit.execution_mode == OrderExecutionMode.LIVE
        and not audit.dry_run
        and audit.client_order_id
        and (
            audit.status == "submit_unknown"
            or (
                audit.status == "pending_submit"
                and checked_at - (
                    audit.submitted_at
                    if audit.submitted_at.tzinfo is not None
                    else audit.submitted_at.replace(tzinfo=timezone.utc)
                )
                >= timedelta(seconds=30)
            )
        )
    ][:item_limit]
    report: dict[str, Any] = {
        "enabled": enabled,
        "broker": None,
        "checked_at": checked_at.isoformat(),
        "recoverable_buy_order_count": len(recoverable_buy_orders),
        "pending_buy_order_count": len(pending_buy_orders),
        "recoverable_sell_execution_count": len(recoverable_sell_audits),
        "pending_sell_execution_count": len(pending_sell_audits),
        "queried_count": 0,
        "error_count": 0,
        "buy_order_sync": None,
        "sell_execution_sync": None,
        "actions": [],
    }
    if not enabled:
        report["reason"] = "auto_broker_sync_disabled"
        return report
    if (
        not recoverable_buy_orders
        and not pending_buy_orders
        and not recoverable_sell_audits
        and not pending_sell_audits
    ):
        report["reason"] = "no_pending_live_broker_orders"
        return report

    broker_adapter = state.execution_router.broker_adapter
    if broker_adapter is None:
        report["enabled"] = False
        report["reason"] = "broker_adapter_not_configured"
        return report
    report["broker"] = broker_adapter.name

    buy_snapshots: list[BrokerOrderStatusSnapshot] = []
    sell_snapshots: list[BrokerOrderStatusSnapshot] = []

    for order in recoverable_buy_orders:
        try:
            recovered = state.recover_order_submission(order)
            report["queried_count"] += 1
            report["actions"].append(
                {
                    "resource": "paper_orders",
                    "status": "recovered",
                    "order_id": recovered.id,
                    "client_order_id": recovered.client_order_id,
                    "broker_order_id": recovered.broker_order_id,
                    "broker_status": recovered.status.value,
                }
            )
        except (BrokerAdapterError, ValueError, NotImplementedError) as exc:
            report["error_count"] += 1
            report["actions"].append(
                {
                    "resource": "paper_orders",
                    "status": "recovery_error",
                    "order_id": order.id,
                    "client_order_id": order.client_order_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )

    for audit in recoverable_sell_audits:
        try:
            recovered = state.recover_sell_submission(audit)
            report["queried_count"] += 1
            report["actions"].append(
                {
                    "resource": "sell_executions",
                    "status": "recovered",
                    "order_id": recovered.sell_execution_id,
                    "client_order_id": recovered.client_order_id,
                    "broker_order_id": recovered.broker_order_id,
                    "applied_to_ledger": recovered.applied_to_ledger,
                }
            )
        except (BrokerAdapterError, ValueError, NotImplementedError) as exc:
            report["error_count"] += 1
            report["actions"].append(
                {
                    "resource": "sell_executions",
                    "status": "recovery_error",
                    "order_id": audit.id,
                    "client_order_id": audit.client_order_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )

    def query_order(resource: str, local_order_id: str, broker_order_id: str) -> BrokerOrderStatusSnapshot | None:
        try:
            update = broker_adapter.get_order_by_id(broker_order_id)
            report["queried_count"] += 1
            snapshot = _broker_status_snapshot(
                local_order_id=local_order_id,
                fallback_broker_order_id=broker_order_id,
                update=update,
            )
            report["actions"].append(
                {
                    "resource": resource,
                    "status": "queried",
                    "order_id": local_order_id,
                    "broker_order_id": snapshot.broker_order_id,
                    "broker_status": snapshot.status.value,
                }
            )
            return snapshot
        except (BrokerAdapterError, ValueError) as exc:
            report["error_count"] += 1
            report["actions"].append(
                {
                    "resource": resource,
                    "status": "error",
                    "order_id": local_order_id,
                    "broker_order_id": broker_order_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )
            return None
        except Exception as exc:
            report["error_count"] += 1
            report["actions"].append(
                {
                    "resource": resource,
                    "status": "error",
                    "order_id": local_order_id,
                    "broker_order_id": broker_order_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )
            return None

    for order in pending_buy_orders:
        if order.broker_order_id is None:
            continue
        snapshot = query_order("paper_orders", order.id, order.broker_order_id)
        if snapshot is not None:
            buy_snapshots.append(snapshot)

    for audit in pending_sell_audits:
        if audit.broker_order_id is None:
            continue
        snapshot = query_order("sell_executions", audit.id, audit.broker_order_id)
        if snapshot is not None:
            sell_snapshots.append(snapshot)

    if buy_snapshots:
        try:
            result = state.sync_broker_order_statuses(
                BrokerOrderSyncRequest(
                    broker=broker_adapter.name,
                    checked_at=checked_at,
                    updated_by="system_cycle:auto_broker_sync",
                    statuses=buy_snapshots,
                )
            )
            report["buy_order_sync"] = result.model_dump(mode="json")
        except Exception as exc:
            report["error_count"] += 1
            report["actions"].append(
                {
                    "resource": "paper_orders",
                    "status": "sync_error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )

    if sell_snapshots:
        try:
            result = state.sync_broker_sell_statuses(
                BrokerOrderSyncRequest(
                    broker=broker_adapter.name,
                    checked_at=checked_at,
                    updated_by="system_cycle:auto_broker_sync",
                    statuses=sell_snapshots,
                )
            )
            report["sell_execution_sync"] = result.model_dump(mode="json")
        except Exception as exc:
            report["error_count"] += 1
            report["actions"].append(
                {
                    "resource": "sell_executions",
                    "status": "sync_error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )

    if report["error_count"]:
        report["reason"] = "broker_sync_errors"
    else:
        report["reason"] = "synced"
    return report

def _broker_position_snapshot(update: BrokerPositionUpdate) -> BrokerPositionSnapshot:
    avg_price = update.avg_price if update.avg_price is not None and update.avg_price > 0 else None
    market_price = update.market_price if update.market_price is not None and update.market_price > 0 else None
    return BrokerPositionSnapshot(
        ticker=update.symbol,
        qty=update.qty,
        avg_price=avg_price,
        market_price=market_price,
        broker_position_id=update.broker_position_id,
    )


def _auto_position_reconciliation_cycle(
    *,
    enabled: bool,
    checked_at: datetime,
    qty_tolerance: float,
) -> dict[str, Any]:
    state = get_app_state()
    report: dict[str, Any] = {
        "enabled": enabled,
        "broker": None,
        "checked_at": checked_at.isoformat(),
        "position_count": 0,
        "error_count": 0,
        "reconciliation": None,
        "actions": [],
    }
    if not enabled:
        report["reason"] = "auto_position_reconciliation_disabled"
        return report

    broker_adapter = state.execution_router.broker_adapter
    if broker_adapter is None:
        report["enabled"] = False
        report["reason"] = "broker_adapter_not_configured"
        return report
    report["broker"] = broker_adapter.name

    list_positions = getattr(broker_adapter, "list_positions", None)
    if not callable(list_positions):
        report["enabled"] = False
        report["reason"] = "broker_position_reconciliation_not_supported"
        return report

    try:
        broker_positions = list_positions()
        snapshots = [_broker_position_snapshot(position) for position in broker_positions]
        report["position_count"] = len(snapshots)
        reconciliation = state.reconcile_broker_positions(
            PositionReconciliationRequest(
                broker=broker_adapter.name,
                as_of=checked_at,
                qty_tolerance=qty_tolerance,
                positions=snapshots,
                note="system_cycle:auto_position_reconciliation",
            )
        )
        report["reconciliation"] = reconciliation.model_dump(mode="json")
        report["reason"] = "reconciled"
        report["actions"].append(
            {
                "status": "reconciled",
                "position_count": reconciliation.broker_position_count,
                "reconciliation_id": reconciliation.reconciliation_id,
                "reconciliation_status": reconciliation.status,
                "blocks_auto_execution": reconciliation.blocks_auto_execution,
            }
        )
    except (BrokerAdapterError, ValueError) as exc:
        report["error_count"] += 1
        report["reason"] = "position_reconciliation_error"
        report["actions"].append(
            {
                "status": "error",
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        )
    except Exception as exc:
        report["error_count"] += 1
        report["reason"] = "position_reconciliation_error"
        report["actions"].append(
            {
                "status": "error",
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        )
    return report


def _broker_account_snapshot_payload(snapshot: BrokerAccountSnapshot) -> dict[str, Any]:
    return {
        "account_id": snapshot.account_id,
        "status": snapshot.status,
        "currency": snapshot.currency,
        "cash": snapshot.cash,
        "buying_power": snapshot.buying_power,
        "equity": snapshot.equity,
        "portfolio_value": snapshot.portfolio_value,
        "trading_blocked": snapshot.trading_blocked,
        "account_blocked": snapshot.account_blocked,
        "transfers_blocked": snapshot.transfers_blocked,
        "pattern_day_trader": snapshot.pattern_day_trader,
        "raw_payload_keys": sorted(snapshot.raw_payload.keys()) if snapshot.raw_payload else [],
    }


def _auto_live_broker_account_gate(*, required: bool, checked_at: datetime) -> dict[str, Any]:
    state = get_app_state()
    report: dict[str, Any] = {
        "required": required,
        "passed": True,
        "broker": None,
        "checked_at": checked_at.isoformat(),
        "reason": "not_required",
        "account": None,
        "buying_power_check": None,
    }
    if not required:
        return report

    report["passed"] = False
    broker_adapter = state.execution_router.broker_adapter
    if broker_adapter is None:
        report["reason"] = "broker_adapter_not_configured"
        return report
    report["broker"] = broker_adapter.name

    get_account = getattr(broker_adapter, "get_account", None)
    if not callable(get_account):
        report["reason"] = "broker_account_snapshot_not_supported"
        return report

    try:
        snapshot = get_account()
    except (BrokerAdapterError, ValueError) as exc:
        report["reason"] = "broker_account_snapshot_error"
        report["error"] = str(exc)
        report["error_type"] = type(exc).__name__
        return report
    except Exception as exc:
        report["reason"] = "broker_account_snapshot_error"
        report["error"] = str(exc)
        report["error_type"] = type(exc).__name__
        return report

    account = _broker_account_snapshot_payload(snapshot)
    buying_power = snapshot.buying_power
    if buying_power is None:
        buying_power_check = {"passed": False, "reason": "broker_buying_power_missing"}
    elif buying_power <= 0:
        buying_power_check = {
            "passed": False,
            "reason": "broker_buying_power_empty",
            "buying_power": buying_power,
        }
    else:
        buying_power_check = {
            "passed": True,
            "reason": None,
            "buying_power": buying_power,
        }
    report["account"] = account
    report["buying_power_check"] = buying_power_check

    status = (snapshot.status or "").strip().upper()
    if snapshot.account_blocked:
        report["reason"] = "broker_account_blocked"
        return report
    if snapshot.trading_blocked:
        report["reason"] = "broker_trading_blocked"
        return report
    if status and status != "ACTIVE":
        report["reason"] = "broker_account_not_active"
        return report

    report["passed"] = True
    report["reason"] = "broker_account_ready"
    return report


def _auto_live_buying_power_gate(
    *,
    broker_account_gate: dict[str, Any] | None,
    requested_notional: float,
) -> dict[str, Any]:
    if not broker_account_gate or not broker_account_gate.get("required"):
        return {
            "passed": True,
            "reason": "not_required",
            "requested_notional": round(requested_notional, 6),
        }
    account = broker_account_gate.get("account") or {}
    buying_power = account.get("buying_power")
    gate = {
        "passed": False,
        "requested_notional": round(requested_notional, 6),
        "buying_power": buying_power,
    }
    if buying_power is None:
        gate["reason"] = "broker_buying_power_missing"
        return gate
    if float(buying_power) + 1e-9 < requested_notional:
        gate["reason"] = "broker_buying_power_insufficient"
        return gate
    gate["passed"] = True
    gate["reason"] = None
    return gate


def _positive_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _auto_live_account_equity_gate(
    *,
    broker_account_gate: dict[str, Any] | None,
    configured_account_equity: float,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "required": bool(broker_account_gate and broker_account_gate.get("required")),
        "passed": True,
        "reason": "not_required",
        "configured_account_equity": round(configured_account_equity, 6),
        "effective_account_equity": round(configured_account_equity, 6),
        "source": "configured_account_equity",
    }
    if not report["required"]:
        return report

    account = (broker_account_gate or {}).get("account") or {}
    equity = _positive_float(account.get("equity"))
    portfolio_value = _positive_float(account.get("portfolio_value"))
    if equity is not None:
        report.update(
            {
                "passed": True,
                "reason": None,
                "effective_account_equity": round(equity, 6),
                "source": "broker_equity",
            }
        )
        return report
    if portfolio_value is not None:
        report.update(
            {
                "passed": True,
                "reason": None,
                "effective_account_equity": round(portfolio_value, 6),
                "source": "broker_portfolio_value",
            }
        )
        return report

    report.update(
        {
            "passed": False,
            "reason": "broker_account_equity_missing",
            "effective_account_equity": None,
            "source": None,
        }
    )
    return report
