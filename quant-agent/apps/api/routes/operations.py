from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import OperationControlCenter, SystemCycleRun
from infra.observability.alerts import OperationalAlertManager


class SystemCycleRunBody(BaseModel):
    top_n: int = Field(default=8, ge=1, le=50)
    min_confidence: float | None = Field(default=0.0, ge=0.0, le=1.0)
    consume_events: bool = False
    auto_sync_broker_statuses: bool = True
    max_broker_sync_items: int = Field(default=50, ge=0, le=500)
    auto_reconcile_broker_positions: bool = True
    position_reconciliation_qty_tolerance: float = Field(default=1e-6, ge=0.0)
    allow_auto_live_execution: bool | None = None
    use_autopilot_policy: bool = True
    as_of: datetime | None = None


router = APIRouter(tags=["operations"])


@router.get("/operations/alerts")
def list_operational_alerts() -> list[dict]:
    return OperationalAlertManager().list_active()


@router.get("/operations/system-runs", response_model=list[SystemCycleRun])
def list_system_cycle_runs(
    limit: int = Query(default=50, ge=1, le=500),
    status: str | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[SystemCycleRun]:
    return state.list_system_cycle_runs(limit=limit, status=status)


@router.get("/operations/control-center", response_model=OperationControlCenter)
def get_operation_control_center(
    recommendation_limit: int = Query(default=20, ge=1, le=100),
    refresh_alerts: bool = Query(default=True),
    state: AppState = Depends(get_app_state),
) -> OperationControlCenter:
    return state.build_operation_control_center(
        recommendation_limit=recommendation_limit,
        refresh_alerts=refresh_alerts,
    )


@router.post("/operations/system-cycle")
def run_system_cycle(body: SystemCycleRunBody) -> dict:
    from apps.worker.main import system_cycle

    return system_cycle(
        top_n=body.top_n,
        min_confidence=body.min_confidence,
        consume_events=body.consume_events,
        as_of=body.as_of,
        auto_sync_broker_statuses=body.auto_sync_broker_statuses,
        max_broker_sync_items=body.max_broker_sync_items,
        auto_reconcile_broker_positions=body.auto_reconcile_broker_positions,
        position_reconciliation_qty_tolerance=body.position_reconciliation_qty_tolerance,
        allow_auto_live_execution=body.allow_auto_live_execution,
        use_autopilot_policy=body.use_autopilot_policy,
    )
