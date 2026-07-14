from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from apps.api.dependencies import AppState, get_app_state
from infra.observability.health import HealthEvaluator

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "quant-agent-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health/live")
def liveness() -> dict[str, str]:
    return health()


@router.get("/health/ready")
def readiness(state: AppState = Depends(get_app_state)) -> JSONResponse:
    report = HealthEvaluator(state).evaluate()
    return JSONResponse(status_code=200 if report["ready"] else 503, content=report)
