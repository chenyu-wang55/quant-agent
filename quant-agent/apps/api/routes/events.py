from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from apps.api.dependencies import AppState, get_app_state
from infra.queue.events import SystemEvent


router = APIRouter(tags=["events"])


@router.get("/events/pending", response_model=list[SystemEvent])
def list_pending_events(
    limit: int = Query(default=100, ge=1, le=1000),
    state: AppState = Depends(get_app_state),
) -> list[SystemEvent]:
    return state.event_queue.pending(limit=limit)


@router.get("/events/consumed", response_model=list[SystemEvent])
def list_consumed_events(
    limit: int = Query(default=100, ge=1, le=1000),
    state: AppState = Depends(get_app_state),
) -> list[SystemEvent]:
    return state.event_queue.consumed(limit=limit)


@router.post("/events/consume", response_model=list[SystemEvent])
def consume_events(
    limit: int = Query(default=100, ge=1, le=1000),
    state: AppState = Depends(get_app_state),
) -> list[SystemEvent]:
    return state.consume_events(limit=limit)
