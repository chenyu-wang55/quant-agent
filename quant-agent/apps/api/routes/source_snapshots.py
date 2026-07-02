from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import (
    MarketBar,
    ResearchRunResult,
    SourceSnapshotDetail,
    SourceSnapshotExport,
    SourceSnapshotReplayCompareRequest,
    SourceSnapshotReplayComparison,
    SourceSnapshotReplayRequest,
    SourceSnapshotSummary,
)


router = APIRouter(tags=["source-snapshots"])


@router.get("/source-snapshots", response_model=list[SourceSnapshotSummary])
def list_source_snapshots(
    limit: int = Query(default=50, ge=1, le=500),
    state: AppState = Depends(get_app_state),
) -> list[SourceSnapshotSummary]:
    return state.list_source_snapshots(limit=limit)


@router.get("/source-snapshots/{source_snapshot_id}", response_model=SourceSnapshotDetail)
def get_source_snapshot(
    source_snapshot_id: str,
    event_limit: int = Query(default=20, ge=0, le=200),
    state: AppState = Depends(get_app_state),
) -> SourceSnapshotDetail:
    detail = state.get_source_snapshot_detail(source_snapshot_id, event_limit=event_limit)
    if detail is None:
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    return detail


@router.get("/source-snapshots/{source_snapshot_id}/export", response_model=SourceSnapshotExport)
def export_source_snapshot(
    source_snapshot_id: str,
    state: AppState = Depends(get_app_state),
) -> SourceSnapshotExport:
    snapshot_export = state.get_source_snapshot_export(source_snapshot_id)
    if snapshot_export is None:
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    return snapshot_export


@router.get("/source-snapshots/{source_snapshot_id}/bars/{ticker}", response_model=list[MarketBar])
def get_source_snapshot_bars(
    source_snapshot_id: str,
    ticker: str,
    limit: int = Query(default=60, ge=1, le=500),
    state: AppState = Depends(get_app_state),
) -> list[MarketBar]:
    if not state.source_snapshot_repo.snapshot_exists(source_snapshot_id):
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    return state.source_snapshot_repo.get_bars(source_snapshot_id, ticker, limit)


@router.post("/source-snapshots/{source_snapshot_id}/replay", response_model=ResearchRunResult)
def replay_source_snapshot(
    source_snapshot_id: str,
    replay_request: SourceSnapshotReplayRequest | None = None,
    state: AppState = Depends(get_app_state),
) -> ResearchRunResult:
    try:
        return state.replay_source_snapshot(
            source_snapshot_id,
            replay_request or SourceSnapshotReplayRequest(),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Source snapshot not found") from None


@router.post(
    "/source-snapshots/{source_snapshot_id}/replay/compare",
    response_model=SourceSnapshotReplayComparison,
)
def compare_source_snapshot_replay(
    source_snapshot_id: str,
    compare_request: SourceSnapshotReplayCompareRequest | None = None,
    state: AppState = Depends(get_app_state),
) -> SourceSnapshotReplayComparison:
    try:
        return state.compare_source_snapshot_replay(
            source_snapshot_id,
            compare_request or SourceSnapshotReplayCompareRequest(),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Source snapshot not found") from None
