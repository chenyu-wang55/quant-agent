from __future__ import annotations

from pydantic import BaseModel

from fastapi import APIRouter, Depends

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import KillSwitchState


class KillSwitchUpdateBody(BaseModel):
    enabled: bool
    reason: str | None = None
    updated_by: str = "operator"


router = APIRouter(tags=["execution-controls"])


@router.get("/execution/kill-switch", response_model=KillSwitchState)
def get_kill_switch(state: AppState = Depends(get_app_state)) -> KillSwitchState:
    return state.kill_switch


@router.post("/execution/kill-switch", response_model=KillSwitchState)
def set_kill_switch(
    body: KillSwitchUpdateBody,
    state: AppState = Depends(get_app_state),
) -> KillSwitchState:
    return state.set_kill_switch(enabled=body.enabled, reason=body.reason, updated_by=body.updated_by)
