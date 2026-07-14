from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import RecommendationApproval
from domain.policies.approval import ApprovalDecisionRequest


class ApprovalBody(BaseModel):
    decision: str
    approver: str
    notes: str | None = None


router = APIRouter(tags=["approvals"])


@router.post("/recommendations/{recommendation_id}/approval", response_model=RecommendationApproval)
def decide_recommendation(
    recommendation_id: str,
    body: ApprovalBody,
    state: AppState = Depends(get_app_state),
) -> RecommendationApproval:
    request = ApprovalDecisionRequest(
        recommendation_id=recommendation_id,
        decision=body.decision,
        approver=body.approver,
        notes=body.notes,
    )
    try:
        return state.decide_recommendation(request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/recommendations/{recommendation_id}/approval", response_model=RecommendationApproval)
def get_recommendation_approval(
    recommendation_id: str,
    state: AppState = Depends(get_app_state),
) -> RecommendationApproval:
    approval = state.get_latest_approval(recommendation_id)
    if approval is None:
        raise HTTPException(status_code=404, detail="No approval decision found")
    return approval
