from __future__ import annotations

from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import Recommendation


router = APIRouter(tags=["recommendations"])


class RecommendationDetail(BaseModel):
    recommendation: Recommendation
    signal_snapshot: dict
    feature_snapshot: dict
    approval: dict | None


@router.get("/recommendations", response_model=list[Recommendation])
def list_recommendations(
    ticker: str | None = Query(default=None),
    score_min: float | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[Recommendation]:
    if state.latest_run is None:
        items = state.recommendation_repo.list_latest(limit=200)
    else:
        items = list(state.latest_run.recommendations)

    if ticker:
        items = [item for item in items if item.ticker == ticker]
    if score_min is not None:
        items = [item for item in items if item.score_vector.get("composite", 0.0) >= score_min]
    return items


@router.get("/recommendations/latest", response_model=list[Recommendation])
def latest_recommendations(state: AppState = Depends(get_app_state)) -> list[Recommendation]:
    if state.latest_run is None:
        return state.recommendation_repo.list_latest(limit=100)
    return state.latest_run.recommendations


@router.get("/recommendations/{recommendation_id}", response_model=Recommendation)
def get_recommendation(
    recommendation_id: str,
    state: AppState = Depends(get_app_state),
) -> Recommendation:
    recommendation = state.recommendations_by_id.get(recommendation_id)
    if recommendation is None:
        recommendation = state.recommendation_repo.get(recommendation_id)
    if recommendation is None:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    return recommendation


@router.get("/recommendations/{recommendation_id}/evidence", response_model=RecommendationDetail)
def get_recommendation_evidence(
    recommendation_id: str,
    state: AppState = Depends(get_app_state),
) -> RecommendationDetail:
    recommendation = state.recommendations_by_id.get(recommendation_id)
    if recommendation is None:
        recommendation = state.recommendation_repo.get(recommendation_id)
    if recommendation is None:
        raise HTTPException(status_code=404, detail="Recommendation not found")

    signal = state.signals_by_ticker.get(recommendation.ticker)
    if signal is None:
        signal = state.signal_repo.get_latest_by_ticker(recommendation.ticker)

    feature = state.features_by_ticker.get(recommendation.ticker)
    if feature is None:
        feature = state.feature_repo.get(recommendation.feature_snapshot_id)

    approval = state.get_latest_approval(recommendation.id)

    return RecommendationDetail(
        recommendation=recommendation,
        signal_snapshot=signal.model_dump() if signal else {},
        feature_snapshot=feature.model_dump() if feature else {},
        approval=approval.model_dump() if approval else None,
    )
