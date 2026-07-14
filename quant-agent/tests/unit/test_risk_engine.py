from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from domain.entities.models import (
    Direction,
    PricePlanConfig,
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
    SignalConfig,
    UniverseRules,
)
from domain.policies.rules import RejectionReason
from services.features.engine import FeatureEngine
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.pipeline import ResearchPipeline
from services.ranking.price_engine import PriceSizingEngine
from services.ranking.recommendation_builder import RecommendationBuilder
from services.ranking.signal_scorer import SignalScorer
from services.risk.engine import RiskEngine


def test_risk_engine_rejects_low_confidence_and_short_direction() -> None:
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    security = provider.get_universe("SP500", as_of)[0]

    bars = provider.get_bars(security.ticker, as_of)
    benchmark = provider.get_benchmark_bars("SPY", as_of)
    features = FeatureEngine().compute(security.ticker, bars, benchmark)
    signal = SignalScorer().score(
        as_of=as_of,
        security=security,
        bars=bars,
        benchmark_bars=benchmark,
        feature=features,
        fundamentals=provider.get_fundamentals(security.ticker, as_of),
        events=provider.get_events([security.ticker], as_of),
        signal_config=SignalConfig(),
    )

    trade_plan = PriceSizingEngine().build(
        security=security,
        bars=bars,
        feature=features,
        signal=signal,
        cfg=PricePlanConfig(),
        direction=Direction.BUY,
    )
    recommendation = RecommendationBuilder().build(
        security=security,
        signal=signal,
        trade_plan=trade_plan,
        source_snapshot_id="snapshot",
        feature_snapshot_id=features.id,
    )

    recommendation.confidence = 0.2
    recommendation.direction = Direction.SHORT

    decision = RiskEngine().evaluate(
        security=security,
        recommendation=recommendation,
        signal=signal,
        risk_policy=RiskPolicy(min_confidence=0.65),
    )

    assert decision.approved is False
    assert RejectionReason.BELOW_MIN_CONFIDENCE in decision.reason_codes
    assert RejectionReason.UNSUPPORTED_DIRECTION in decision.reason_codes


def test_risk_engine_rejects_entry_plan_too_far_from_spot() -> None:
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    security = provider.get_universe("SP500", as_of)[0]

    bars = provider.get_bars(security.ticker, as_of)
    benchmark = provider.get_benchmark_bars("SPY", as_of)
    features = FeatureEngine().compute(security.ticker, bars, benchmark)
    signal = SignalScorer().score(
        as_of=as_of,
        security=security,
        bars=bars,
        benchmark_bars=benchmark,
        feature=features,
        fundamentals=provider.get_fundamentals(security.ticker, as_of),
        events=provider.get_events([security.ticker], as_of),
        signal_config=SignalConfig(),
    )
    trade_plan = PriceSizingEngine().build(
        security=security,
        bars=bars,
        feature=features,
        signal=signal,
        cfg=PricePlanConfig(),
        direction=Direction.BUY,
    )
    recommendation = RecommendationBuilder().build(
        security=security,
        signal=signal,
        trade_plan=trade_plan,
        source_snapshot_id="snapshot",
        feature_snapshot_id=features.id,
    )

    recommendation.entry_zone_low = security.last_price * 1.6
    recommendation.entry_zone_high = security.last_price * 1.7

    decision = RiskEngine().evaluate(
        security=security,
        recommendation=recommendation,
        signal=signal,
        risk_policy=RiskPolicy(min_confidence=0.0, max_entry_gap_pct=0.15),
    )

    assert decision.approved is False
    assert RejectionReason.ENTRY_PLAN_TOO_FAR in decision.reason_codes


def test_risk_engine_rejects_invalid_buy_price_geometry() -> None:
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    security = provider.get_universe("SP500", as_of)[0]

    bars = provider.get_bars(security.ticker, as_of)
    benchmark = provider.get_benchmark_bars("SPY", as_of)
    features = FeatureEngine().compute(security.ticker, bars, benchmark)
    signal = SignalScorer().score(
        as_of=as_of,
        security=security,
        bars=bars,
        benchmark_bars=benchmark,
        feature=features,
        fundamentals=provider.get_fundamentals(security.ticker, as_of),
        events=provider.get_events([security.ticker], as_of),
        signal_config=SignalConfig(),
    )
    trade_plan = PriceSizingEngine().build(
        security=security,
        bars=bars,
        feature=features,
        signal=signal,
        cfg=PricePlanConfig(),
        direction=Direction.BUY,
    )
    recommendation = RecommendationBuilder().build(
        security=security,
        signal=signal,
        trade_plan=trade_plan,
        source_snapshot_id="snapshot",
        feature_snapshot_id=features.id,
    )

    recommendation.stop_loss = recommendation.entry_zone_high
    recommendation.tp1 = recommendation.entry_zone_low

    decision = RiskEngine().evaluate(
        security=security,
        recommendation=recommendation,
        signal=signal,
        risk_policy=RiskPolicy(min_confidence=0.0),
    )

    assert decision.approved is False
    assert RejectionReason.INVALID_PRICE_PLAN in decision.reason_codes
    assert "buy_stop_not_below_entry" in decision.failed_checks
    assert "buy_tp1_not_above_entry" in decision.failed_checks


def test_risk_engine_enforces_portfolio_and_liquidity_limits() -> None:
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    security = provider.get_universe("SP500", as_of)[0]

    bars = provider.get_bars(security.ticker, as_of)
    benchmark = provider.get_benchmark_bars("SPY", as_of)
    features = FeatureEngine().compute(security.ticker, bars, benchmark)
    signal = SignalScorer().score(
        as_of=as_of,
        security=security,
        bars=bars,
        benchmark_bars=benchmark,
        feature=features,
        fundamentals=provider.get_fundamentals(security.ticker, as_of),
        events=provider.get_events([security.ticker], as_of),
        signal_config=SignalConfig(),
    )
    trade_plan = PriceSizingEngine().build(
        security=security,
        bars=bars,
        feature=features,
        signal=signal,
        cfg=PricePlanConfig(),
        direction=Direction.BUY,
    )
    recommendation = RecommendationBuilder().build(
        security=security,
        signal=signal,
        trade_plan=trade_plan,
        source_snapshot_id="snapshot",
        feature_snapshot_id=features.id,
    )

    decision = RiskEngine().evaluate(
        security=security,
        recommendation=recommendation,
        signal=signal,
        risk_policy=RiskPolicy(
            min_confidence=0.0,
            max_gross_exposure=0.80,
            max_portfolio_beta=1.10,
            max_portfolio_volatility=0.20,
            max_liquidation_days=2.0,
        ),
        gross_weight=0.81,
        portfolio_beta=1.11,
        portfolio_volatility=0.21,
        max_liquidation_days=2.01,
    )

    assert decision.approved is False
    assert RejectionReason.GROSS_EXPOSURE in decision.reason_codes
    assert RejectionReason.PORTFOLIO_BETA in decision.reason_codes
    assert RejectionReason.PORTFOLIO_VOLATILITY in decision.reason_codes
    assert RejectionReason.LIQUIDITY_STRESS in decision.reason_codes
    assert {
        "max_gross_exposure",
        "max_portfolio_beta",
        "max_portfolio_volatility",
        "max_liquidation_days",
    }.issubset(decision.failed_checks)


def test_pipeline_applies_portfolio_exposure_limits_after_ranking() -> None:
    provider = MockMarketDataProvider()
    pipeline = ResearchPipeline(provider=provider)
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="portfolio exposure gating",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        universe="SP500",
        universe_rules=UniverseRules(
            min_price=1,
            min_avg_dollar_volume=1_000_000,
            max_spread_bps=100,
            min_market_cap_usd=100_000_000,
            max_candidates_after_filter=100,
        ),
        risk_policy=RiskPolicy(
            min_confidence=0.0,
            max_name_weight=0.20,
            max_sector_weight=0.12,
            max_correlated_cluster_weight=0.22,
            reject_on_material_evidence_conflict=False,
            event_trading_enabled=True,
        ),
        publication=PublicationConfig(top_n=8, output_channels=["api"]),
    )

    output = pipeline.run(request)
    exposure = output.result.universe_summary["portfolio_exposure"]

    assert output.result.recommendations
    assert all(weight <= 0.120001 for weight in exposure["sector_weights"].values())
    assert all(weight <= 0.220001 for weight in exposure["correlated_cluster_weights"].values())
    assert exposure["risk_metrics"]["gross_weight"] <= request.risk_policy.max_gross_exposure
    assert {
        "gross_weight",
        "beta",
        "annualized_volatility",
        "max_liquidation_days",
        "liquidity_stress_loss_pct",
    }.issubset(exposure["risk_metrics"])
    assert RejectionReason.SECTOR_CONCENTRATION in output.result.universe_summary["rejection_counts"]


def test_return_correlation_aligns_missing_bars_by_timestamp() -> None:
    provider = MockMarketDataProvider()
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)

    def bars(closes: list[float], offsets: list[int]):
        template = provider.get_bars("AAPL", start, lookback_days=1)[0]
        return [
            template.model_copy(
                update={
                    "timestamp": start + timedelta(days=offset),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adjusted_close": close,
                }
            )
            for close, offset in zip(closes, offsets)
        ]

    left = bars([100.0, 110.0, 99.0, 108.9, 98.01], [0, 1, 2, 3, 4])
    right = bars([100.0, 110.0, 121.0, 108.9], [0, 1, 3, 4])

    assert ResearchPipeline._return_correlation(left, right) == pytest.approx(1.0)
