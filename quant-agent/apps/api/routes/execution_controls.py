from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import AutoExecutionMode, AutopilotPolicy, KillSwitchState, MarketSessionStatus


class KillSwitchUpdateBody(BaseModel):
    enabled: bool
    reason: str | None = None
    updated_by: str = "operator"


class AutopilotPolicyUpdateBody(BaseModel):
    enabled: bool | None = None
    auto_approve_recommendations: bool | None = None
    auto_execute_approved: bool | None = None
    restrict_auto_execution_to_regular_hours: bool | None = None
    auto_execution_mode: AutoExecutionMode | None = None
    auto_approve_min_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    auto_approve_min_composite: float | None = Field(default=None, ge=0.0)
    max_auto_approvals: int | None = Field(default=None, ge=0)
    max_auto_buys: int | None = Field(default=None, ge=0)
    max_auto_sells: int | None = Field(default=None, ge=0)
    max_daily_auto_approvals: int | None = Field(default=None, ge=0)
    max_daily_auto_buys: int | None = Field(default=None, ge=0)
    max_daily_auto_sells: int | None = Field(default=None, ge=0)
    rebuy_cooldown_minutes: int | None = Field(default=None, ge=0)
    min_snapshot_bar_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    min_snapshot_fundamental_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    max_snapshot_bar_age_minutes: int | None = Field(default=None, ge=0)
    max_open_risk_pct: float | None = Field(default=None, ge=0.0, le=1.0)
    max_daily_realized_loss_pct: float | None = Field(default=None, ge=0.0, le=1.0)
    account_equity: float | None = Field(default=None, gt=0)
    risk_per_trade_pct: float | None = Field(default=None, gt=0, le=1.0)
    max_position_pct: float | None = Field(default=None, gt=0, le=1.0)
    max_gross_exposure_pct: float | None = Field(default=None, gt=0, le=5.0)
    max_sector_exposure_pct: float | None = Field(default=None, gt=0, le=5.0)
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


@router.get("/execution/autopilot-policy", response_model=AutopilotPolicy)
def get_autopilot_policy(state: AppState = Depends(get_app_state)) -> AutopilotPolicy:
    return state.get_autopilot_policy()


@router.post("/execution/autopilot-policy", response_model=AutopilotPolicy)
def set_autopilot_policy(
    body: AutopilotPolicyUpdateBody,
    state: AppState = Depends(get_app_state),
) -> AutopilotPolicy:
    updates = body.model_dump(exclude_unset=True)
    updates.setdefault("updated_by", body.updated_by)
    return state.update_autopilot_policy(updates)


@router.get("/execution/market-session", response_model=MarketSessionStatus)
def get_market_session(
    as_of: datetime | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> MarketSessionStatus:
    return state.get_market_session_status(as_of=as_of)
