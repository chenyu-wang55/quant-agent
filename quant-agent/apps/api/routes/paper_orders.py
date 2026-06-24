from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import PaperOrder, PaperOrderRequest


router = APIRouter(tags=["paper-orders"])


@router.post("/paper-orders", response_model=PaperOrder)
def submit_paper_order(
    request: PaperOrderRequest,
    state: AppState = Depends(get_app_state),
) -> PaperOrder:
    if state.kill_switch.enabled:
        raise HTTPException(status_code=423, detail="Execution is blocked by kill switch")

    recommendation = state.recommendations_by_id.get(request.recommendation_id)
    if recommendation is None:
        recommendation = state.recommendation_repo.get(request.recommendation_id)
    if recommendation is None:
        raise HTTPException(status_code=404, detail="Recommendation id not found")

    if request.side != recommendation.direction:
        raise HTTPException(
            status_code=409,
            detail="Paper order side must match the approved recommendation direction",
        )

    approval = state.get_latest_approval(request.recommendation_id)
    if approval is None or approval.decision.value != "approved":
        raise HTTPException(
            status_code=409,
            detail="Recommendation must be approved before paper-order routing",
        )

    order, updated_positions = state.paper_router.submit(
        recommendation=recommendation,
        request=request,
        positions=state.positions,
    )
    state.positions = updated_positions
    state.record_paper_order(order, recommendation=recommendation)
    return order
