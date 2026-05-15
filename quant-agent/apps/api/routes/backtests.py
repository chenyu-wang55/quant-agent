from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import BacktestRunRequest, BacktestRunResult, ResearchRunRequest, RunType
from infra.queue.events import EventType


router = APIRouter(tags=["backtests"])


@router.get("/backtests/runs", response_model=list[BacktestRunResult])
def list_backtest_runs(state: AppState = Depends(get_app_state)) -> list[BacktestRunResult]:
    return list(state.backtest_runs)


@router.post("/backtests/runs", response_model=BacktestRunResult)
def run_backtest(
    request: BacktestRunRequest,
    state: AppState = Depends(get_app_state),
) -> BacktestRunResult:
    template_request = state.last_research_request
    if template_request is None:
        template_request = ResearchRunRequest(
            run_type=RunType.RESEARCH_BATCH,
            objective="Backtest fallback template",
            as_of=datetime.now(timezone.utc) - timedelta(days=1),
        )

    result = state.backtest_engine.run(
        request=request,
        pipeline=state.pipeline,
        template_request=template_request,
    )
    state.backtest_runs.append(result)
    state.metrics_store.inc("backtest_runs")
    state.publish_event(
        EventType.MODEL_EVALUATION,
        {
            "run_id": result.run_id,
            "config_hash": result.config_hash,
            "metrics": result.metrics,
        },
    )
    return result
