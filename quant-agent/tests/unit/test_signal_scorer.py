from __future__ import annotations

from datetime import datetime, timezone

from domain.entities.models import SignalConfig
from services.features.engine import FeatureEngine
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.signal_scorer import SignalScorer


def test_signal_scorer_weighted_composite() -> None:
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)

    security = provider.get_universe("SP500", as_of)[0]
    bars = provider.get_bars(security.ticker, as_of)
    benchmark = provider.get_benchmark_bars("SPY", as_of)
    fundamentals = provider.get_fundamentals(security.ticker, as_of)
    events = provider.get_events([security.ticker], as_of)

    features = FeatureEngine().compute(security.ticker, bars, benchmark)
    config = SignalConfig()
    signal = SignalScorer().score(
        as_of=as_of,
        security=security,
        bars=bars,
        benchmark_bars=benchmark,
        feature=features,
        fundamentals=fundamentals,
        events=events,
        signal_config=config,
    )

    expected = (
        config.technical_weight * signal.technical_score
        + config.event_news_weight * signal.event_score
        + config.relative_strength_weight * signal.relative_strength_score
        + config.fundamental_weight * signal.fundamental_score
        + config.execution_quality_weight * signal.execution_quality_score
    )

    assert 0.0 <= signal.composite_score <= 1.0
    assert abs(signal.composite_score - expected) < 1e-6
