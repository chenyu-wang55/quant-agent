from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from domain.entities.models import Direction, OrderExecutionMode

ACTIVE_STATUSES = {"new", "accepted", "held", "pending_new", "partially_filled"}


def _number(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("non_finite_protection_value")
    return result


def check_native_protection(state: Any, *, checked_at: datetime | None = None) -> tuple[bool, dict[str, Any]]:
    now = checked_at or datetime.now(timezone.utc)
    holdings = state.list_open_holdings()
    failures: list[dict[str, Any]] = []
    if not holdings:
        return True, {"required": True, "open_holding_count": 0, "protected_holding_count": 0, "failures": []}
    max_age = state.get_autopilot_policy().max_position_reconciliation_age_minutes
    reconciliation = state.get_position_reconciliation_gate(
        require_position_reconciliation=True, max_age_minutes=max_age, as_of=now,
    )
    reports = state.list_position_reconciliations(limit=1)
    broker_quantities = {item.ticker.upper(): item.broker_qty for item in reports[0].items} if reports else {}
    orders = [order for order in state.list_paper_orders(limit=10000)
              if order.execution_mode == OrderExecutionMode.LIVE and not order.dry_run
              and order.side == Direction.BUY and order.filled_qty > 0 and order.broker_order_id]
    seen_leg_ids: set[str] = set()
    for holding in holdings:
        ticker = holding.ticker.upper()
        reasons: list[str] = []
        stop_qty = target_qty = 0.0
        if not reconciliation["passed"] or ticker not in broker_quantities:
            reasons.append("broker_position_not_verified")
        elif not math.isclose(broker_quantities[ticker], holding.qty, abs_tol=1e-6):
            reasons.append("broker_position_quantity_mismatch")
        for order in orders:
            recommendation = state._get_recommendation_by_id(order.recommendation_id)
            matches = recommendation is not None and recommendation.ticker.upper() == ticker
            if not matches and order.recommendation_id != holding.source_recommendation_id:
                continue
            if not any(str(leg.get("status") or "").lower() in ACTIVE_STATUSES for leg in order.broker_protection):
                continue
            group: dict[str, float] = {"stop": 0.0, "target": 0.0}
            group_counts = {"stop": 0, "target": 0}
            for leg in order.broker_protection:
                if str(leg.get("status") or "").lower() not in ACTIVE_STATUSES:
                    continue
                kind = str(leg.get("order_type") or "").lower()
                role = "stop" if kind in {"stop", "stop_limit"} else "target" if kind == "limit" else None
                if role is None:
                    reasons.append("unknown_protection_type")
                    continue
                leg_id = str(leg.get("broker_order_id") or "")
                if not leg_id or leg_id in seen_leg_ids:
                    reasons.append("duplicate_or_missing_protection_id")
                seen_leg_ids.add(leg_id)
                if (str(leg.get("side") or "").lower() != "sell"
                    or str(leg.get("symbol") or "").upper() != ticker
                    or leg.get("parent_order_id") != order.broker_order_id):
                    reasons.append("protection_identity_mismatch")
                try:
                    observed = datetime.fromisoformat(str(leg.get("observed_at") or ""))
                    if observed.tzinfo is None or not 0 <= (now - observed).total_seconds() <= max_age * 60:
                        reasons.append("protection_snapshot_stale")
                    qty = _number(leg.get("qty"))
                    filled = _number(leg.get("filled_qty"))
                    if qty <= 0 or not 0 <= filled <= qty:
                        raise ValueError("invalid_protection_quantity")
                    price = _number(leg.get("stop_price" if role == "stop" else "limit_price"))
                    expected = holding.stop_loss if role == "stop" else holding.take_profit2
                    if not math.isclose(price, expected, abs_tol=0.005, rel_tol=0):
                        reasons.append("protection_price_mismatch")
                    group[role] += qty - filled
                    group_counts[role] += 1
                except (TypeError, ValueError):
                    reasons.append("protection_values_missing_or_invalid")
            # Each OCO group needs both paths; never add stop + target quantities.
            if group_counts != {"stop": 1, "target": 1} or not math.isclose(
                group["stop"], group["target"], abs_tol=1e-6
            ):
                reasons.append("active_native_bracket_missing")
            stop_qty += group["stop"]
            target_qty += group["target"]
        if not math.isclose(stop_qty, holding.qty, abs_tol=1e-6) or not math.isclose(
            target_qty, holding.qty, abs_tol=1e-6
        ):
            reasons.append("protection_quantity_mismatch")
            if stop_qty == 0 or target_qty == 0:
                reasons.append("active_native_bracket_missing")
        if reasons:
            failures.append({"ticker": ticker, "reason": reasons[0], "reasons": sorted(set(reasons)),
                             "holding_qty": holding.qty, "stop_qty": stop_qty, "target_qty": target_qty})
    return not failures, {
        "required": True, "open_holding_count": len(holdings),
        "protected_holding_count": len(holdings) - len(failures), "failures": failures,
        "reason": "broker_protection_not_ready" if failures else None,
    }
