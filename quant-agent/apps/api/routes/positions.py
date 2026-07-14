from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import PositionState

router = APIRouter(tags=["positions"])


@router.get("/positions", response_model=list[PositionState])
def list_positions(state: AppState = Depends(get_app_state)) -> list[PositionState]:
    in_memory = [position for position in state.positions.values() if position.qty > 0]
    if in_memory:
        return in_memory
    return state.position_repo.list_open()
