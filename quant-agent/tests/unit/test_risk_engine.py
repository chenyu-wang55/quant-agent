from __future__ import annotations

from datetime import datetime, timezone

from domain.entities.models import Direction, PricePlanConfig, RiskPolicy, SignalConfig
from domain.policies.rules import RejectionReason
from services.features.engine import FeatureEngine
from services.ingestion.mock_provider import MockMarketDataProvider
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
