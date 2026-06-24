from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import Direction, PaperOrder, PaperOrderRequest, PaperOrderStatus


router = APIRouter(tags=["paper-orders"])


def _parse_side(side: str | None) -> Direction | None:
    if side is None:
        return None
    normalized = side.upper()
    if normalized in {item.value for item in Direction}:
        return Direction(normalized)
    raise HTTPException(status_code=400, detail="side must be BUY or SHORT")


def _parse_status(status: str | None) -> PaperOrderStatus | None:
    if status is None:
        return None
    normalized = status.lower()
    if normalized in {item.value for item in PaperOrderStatus}:
        return PaperOrderStatus(normalized)
    raise HTTPException(status_code=400, detail="status must be submitted, filled, or canceled")


@router.get("/paper-orders", response_model=list[PaperOrder])
def list_paper_orders(
    limit: int = Query(default=100, ge=1, le=500),
    recommendation_id: str | None = Query(default=None),
    side: str | None = Query(default=None),
    status: str | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[PaperOrder]:
    return state.list_paper_orders(
        limit=limit,
        recommendation_id=recommendation_id,
        side=_parse_side(side),
        status=_parse_status(status),
    )


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
