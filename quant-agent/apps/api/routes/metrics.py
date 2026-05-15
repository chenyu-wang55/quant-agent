from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import AppState, get_app_state


router = APIRouter(tags=["metrics"])


@router.get("/metrics")
def get_metrics(state: AppState = Depends(get_app_state)) -> dict:
    latest_run_metrics = state.latest_run.run_metrics.model_dump() if state.latest_run else {}

    fills = [order for order in state.paper_orders if order.simulated_fill_price is not None]
    avg_fill = (
        sum(order.simulated_fill_price or 0.0 for order in fills) / len(fills)
        if fills
        else 0.0
    )

    return {
        "run_metrics": latest_run_metrics,
        "execution_metrics": {
            "paper_order_count": len(state.paper_orders),
            "avg_simulated_fill_price": round(avg_fill, 6),
            "open_position_count": sum(1 for pos in state.positions.values() if pos.qty > 0),
            "kill_switch_enabled": state.kill_switch.enabled,
        },
        "approval_metrics": {
            "approval_decision_count": len(state.approvals_by_recommendation_id),
        },
        "portfolio_metrics": {
            "open_holding_count": len(state.list_open_holdings()),
            "recent_sell_alerts": [alert.model_dump() for alert in state.recent_sell_alerts[:5]],
        },
        "event_metrics": {
            "pending_queue_size": state.event_queue.size(),
            "pending_preview": [event.model_dump() for event in state.event_queue.pending(limit=5)],
            "recent_consumed": [event.model_dump() for event in state.event_queue.consumed(limit=5)],
        },
        "operational_metrics": state.metrics_store.dump(),
    }
