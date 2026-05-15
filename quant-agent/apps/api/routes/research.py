from __future__ import annotations

from fastapi import APIRouter, Depends

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import ResearchRunRequest, ResearchRunResult


router = APIRouter(tags=["research"])


@router.post("/research/run", response_model=ResearchRunResult)
def run_research(
    request: ResearchRunRequest,
    state: AppState = Depends(get_app_state),
) -> ResearchRunResult:
    output = state.pipeline.run(request)
    state.ingest_run_output(request, output)
    return output.result
