from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import (
    BacktestRunRequest,
    BacktestRunResult,
    ResearchRunRequest,
    RunType,
    build_strategy_config_snapshot,
)
from infra.queue.events import EventType

router = APIRouter(tags=["backtests"])


@router.get("/backtests/runs", response_model=list[BacktestRunResult])
def list_backtest_runs(state: AppState = Depends(get_app_state)) -> list[BacktestRunResult]:
    return state.backtest_run_repo.list_recent()


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
    backtest_template = template_request.model_copy(
        update={
            "run_type": RunType.BACKTEST_EVALUATION,
            "publication": template_request.publication.model_copy(update={"top_n": request.top_n}),
        }
    )
    strategy_config = build_strategy_config_snapshot(backtest_template)
    if strategy_config.strategy_config_id != result.strategy_config_id:
        raise RuntimeError("backtest strategy configuration identity mismatch")
    state.strategy_config_repo.upsert(strategy_config)
    state.backtest_run_repo.upsert(result)
    state.metrics_store.inc("backtest_runs")
    state.publish_event(
        EventType.MODEL_EVALUATION,
        {
            "run_id": result.run_id,
            "config_hash": result.config_hash,
            "metrics": result.metrics,
            "strategy_config_id": result.strategy_config_id,
            "dataset_fingerprint": result.dataset_fingerprint,
            "acceptance": result.acceptance,
        },
    )
    return result
