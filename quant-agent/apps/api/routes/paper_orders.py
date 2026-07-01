from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import (
    Direction,
    PaperOrder,
    PaperOrderCancelRequest,
    PaperOrderFillRequest,
    PaperOrderRequest,
    PaperOrderRiskPlan,
    PaperOrderStatus,
    Recommendation,
)


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


def _get_recommendation_or_404(state: AppState, recommendation_id: str) -> Recommendation:
    recommendation = state.recommendations_by_id.get(recommendation_id)
    if recommendation is None:
        recommendation = state.recommendation_repo.get(recommendation_id)
    if recommendation is None:
        raise HTTPException(status_code=404, detail="Recommendation id not found")
    return recommendation


@router.post("/paper-orders/risk-plan", response_model=PaperOrderRiskPlan)
def get_paper_order_risk_plan(
    request: PaperOrderRequest,
    state: AppState = Depends(get_app_state),
) -> PaperOrderRiskPlan:
    recommendation = _get_recommendation_or_404(state, request.recommendation_id)
    if request.side != recommendation.direction:
        raise HTTPException(
            status_code=409,
            detail="Paper order side must match the recommendation direction",
        )
    return state.build_paper_order_risk_plan(recommendation=recommendation, request=request)


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

    recommendation = _get_recommendation_or_404(state, request.recommendation_id)

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

    pending_buy_gate = state.get_pending_buy_order_gate(
        ticker=recommendation.ticker,
        recommendation_id=request.recommendation_id,
    )
    if request.side == Direction.BUY and not pending_buy_gate["passed"]:
        raise HTTPException(status_code=409, detail=pending_buy_gate)

    risk_plan = state.build_paper_order_risk_plan(recommendation=recommendation, request=request)
    if request.enforce_risk_limits and not risk_plan.is_within_limits:
        raise HTTPException(status_code=409, detail=risk_plan.model_dump(mode="json"))

    try:
        order, updated_positions = state.execution_router.submit(
            recommendation=recommendation,
            request=request,
            positions=state.positions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    state.positions = updated_positions
    state.record_paper_order(order, recommendation=recommendation)
    return order


@router.post("/paper-orders/{order_id}/cancel", response_model=PaperOrder)
def cancel_paper_order(
    order_id: str,
    request: PaperOrderCancelRequest,
    state: AppState = Depends(get_app_state),
) -> PaperOrder:
    try:
        return state.cancel_paper_order(order_id=order_id, request=request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="paper order not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/paper-orders/{order_id}/fill", response_model=PaperOrder)
def fill_paper_order(
    order_id: str,
    request: PaperOrderFillRequest,
    state: AppState = Depends(get_app_state),
) -> PaperOrder:
    try:
        return state.fill_paper_order(order_id=order_id, request=request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="paper order not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
