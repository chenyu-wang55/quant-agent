from __future__ import annotations

from datetime import datetime, timezone

from domain.entities.models import BacktestRunRequest, ResearchRunRequest, RunType
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.pipeline import ResearchPipeline
from services.research.backtest_engine import BacktestEngine


def test_backtest_engine_produces_historical_metrics() -> None:
    provider = MockMarketDataProvider()
    pipeline = ResearchPipeline(provider=provider)
    template = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="unit-test-template",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
    )
    request = BacktestRunRequest(
        run_name="unit-backtest",
        start_date=datetime(2025, 4, 10, 9, 30, tzinfo=timezone.utc),
        end_date=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        benchmark="SPY",
        top_n=5,
        rebalance_frequency="monthly",
        transaction_cost_bps=10.0,
    )

    result = BacktestEngine().run(request=request, pipeline=pipeline, template_request=template)

    assert result.metrics["periods"] >= 1
    assert result.metrics["recommendation_count"] >= result.metrics["traded_count"]
    assert 0.0 <= result.metrics["fill_rate"] <= 1.0
    assert "annualized_return" in result.metrics
    assert "annualized_benchmark_return" in result.metrics
