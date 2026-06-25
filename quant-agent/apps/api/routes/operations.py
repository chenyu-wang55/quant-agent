from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import OperationControlCenter, SystemCycleRun


router = APIRouter(tags=["operations"])


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
