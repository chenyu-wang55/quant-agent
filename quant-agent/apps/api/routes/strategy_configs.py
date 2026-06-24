from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import StrategyConfigSnapshot


router = APIRouter(tags=["strategy-configs"])


@router.get("/strategy-configs", response_model=list[StrategyConfigSnapshot])
def list_strategy_configs(
    limit: int = Query(default=50, ge=1, le=500),
    state: AppState = Depends(get_app_state),
) -> list[StrategyConfigSnapshot]:
    return state.list_strategy_configs(limit=limit)


@router.get("/strategy-configs/{strategy_config_id}", response_model=StrategyConfigSnapshot)
def get_strategy_config(
    strategy_config_id: str,
    state: AppState = Depends(get_app_state),
) -> StrategyConfigSnapshot:
    item = state.get_strategy_config(strategy_config_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Strategy config not found")
    return item
