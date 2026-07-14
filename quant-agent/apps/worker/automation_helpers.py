from __future__ import annotations

from datetime import datetime
from typing import Any

from apps.api.dependencies import get_app_state
from domain.entities.models import HoldingControlUpdateRequest, Recommendation, SellAlert


def _auto_buy_price_drift_gate(
    *,
    recommendation: Recommendation,
    max_drift_pct: float,
    as_of: datetime,
) -> dict[str, Any]:
    ticker = recommendation.ticker.upper()
    if max_drift_pct <= 0:
        return {
            "passed": True,
            "ticker": ticker,
            "recommendation_id": recommendation.id,
            "max_drift_pct": max_drift_pct,
            "reason": "disabled",
        }

    state = get_app_state()
    try:
        latest_price = state.provider.get_latest_price(ticker, as_of)
    except Exception as exc:
        return {
            "passed": False,
            "ticker": ticker,
            "recommendation_id": recommendation.id,
            "reason": "latest_price_unavailable",
            "error": str(exc),
            "max_drift_pct": max_drift_pct,
        }
    if latest_price is None or latest_price <= 0:
        return {
            "passed": False,
            "ticker": ticker,
            "recommendation_id": recommendation.id,
            "reason": "latest_price_missing",
            "latest_price": latest_price,
            "max_drift_pct": max_drift_pct,
        }

    latest = float(latest_price)
    entry_low = float(recommendation.entry_zone_low)
    entry_high = float(recommendation.entry_zone_high)
    if entry_low <= 0 or entry_high <= 0 or entry_low > entry_high:
        return {
            "passed": False,
            "ticker": ticker,
            "recommendation_id": recommendation.id,
            "reason": "invalid_entry_zone",
            "entry_zone_low": entry_low,
            "entry_zone_high": entry_high,
            "latest_price": latest,
            "max_drift_pct": max_drift_pct,
        }

    if entry_low <= latest <= entry_high:
        drift_pct = 0.0
        reason = None
        passed = True
    elif latest > entry_high:
        drift_pct = round((latest - entry_high) / entry_high, 6)
        reason = "latest_price_above_entry_zone"
        passed = drift_pct <= max_drift_pct
    else:
        drift_pct = round((entry_low - latest) / entry_low, 6)
        reason = "latest_price_below_entry_zone"
        passed = drift_pct <= max_drift_pct

    return {
        "passed": passed,
        "ticker": ticker,
        "recommendation_id": recommendation.id,
        "reason": None if passed else reason,
        "latest_price": round(latest, 6),
        "entry_zone_low": entry_low,
        "entry_zone_high": entry_high,
        "drift_pct": drift_pct,
        "max_drift_pct": max_drift_pct,
    }


def _post_sell_control_adjustment(
    *,
    alert: SellAlert,
    execution: Any,
    run_id: str,
) -> dict[str, Any] | None:
    if alert.reason_code != "take_profit1_hit":
        return None
    if not execution.applied_to_ledger or execution.dry_run or execution.remaining_qty <= 0:
        return {
            "status": "skipped",
            "reason": "sell_not_applied_to_open_holding",
        }

    holding = execution.holding
    desired_stop = max(holding.stop_loss, holding.avg_buy_price)
    stop_ceiling = max(0.000001, holding.take_profit1 * 0.999)
    new_stop = round(min(desired_stop, stop_ceiling), 6)
    if new_stop <= holding.stop_loss + 1e-9:
        return {
            "status": "skipped",
            "reason": "stop_already_at_or_above_target",
            "current_stop_loss": holding.stop_loss,
            "target_stop_loss": new_stop,
        }
    if not new_stop < holding.take_profit1:
        return {
            "status": "skipped",
            "reason": "target_stop_not_below_take_profit1",
            "target_stop_loss": new_stop,
            "take_profit1": holding.take_profit1,
        }

    state = get_app_state()
    try:
        result = state.update_holding_controls(
            alert.ticker,
            HoldingControlUpdateRequest(
                stop_loss=new_stop,
                reason=f"system_cycle:{run_id}:post_take_profit1_stop_tighten",
                updated_by="system_cycle:auto_sell",
            ),
        )
    except Exception as exc:
        return {
            "status": "error",
            "reason": "holding_control_update_failed",
            "error": str(exc),
        }
    return {
        "status": "updated",
        "audit_id": result.audit.id,
        "old_stop_loss": result.audit.old_stop_loss,
        "new_stop_loss": result.audit.new_stop_loss,
        "message_cn": result.message_cn,
    }


def _auto_execution_report(
    *,
    enabled: bool,
    mode: str,
    actions: list[dict[str, Any]],
    portfolio_risk_gate: dict[str, Any] | None = None,
    daily_loss_gate: dict[str, Any] | None = None,
    live_execution_gate: dict[str, Any] | None = None,
    broker_account_gate: dict[str, Any] | None = None,
    account_equity_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "mode": mode,
        "portfolio_risk_gate": portfolio_risk_gate,
        "daily_loss_gate": daily_loss_gate,
        "live_execution_gate": live_execution_gate,
        "broker_account_gate": broker_account_gate,
        "account_equity_gate": account_equity_gate,
        "action_count": len(actions),
        "buy_order_count": sum(
            1 for item in actions if item.get("action") == "buy_recommendation" and item.get("status") == "executed"
        ),
        "sell_order_count": sum(
            1 for item in actions if item.get("action") == "sell_alert" and item.get("status") == "executed"
        ),
        "skipped_count": sum(1 for item in actions if item.get("status") == "skipped"),
        "error_count": sum(1 for item in actions if item.get("status") == "error"),
        "actions": actions,
    }
