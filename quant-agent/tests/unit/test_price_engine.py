from __future__ import annotations

from datetime import datetime, timedelta, timezone

from domain.entities.models import (
    Direction,
    FeatureSnapshot,
    MarketBar,
    PricePlanConfig,
    SecurityMetadata,
    SignalConfig,
    SignalSnapshot,
)
from services.features.engine import FeatureEngine
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.price_engine import PriceSizingEngine
from services.ranking.signal_scorer import SignalScorer


def test_price_engine_generates_deterministic_buy_plan() -> None:
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

    plan = PriceSizingEngine().build(
        security=security,
        bars=bars,
        feature=features,
        signal=signal,
        cfg=PricePlanConfig(),
        direction=Direction.BUY,
    )

    assert plan.entry_zone_high > plan.entry_zone_low
    assert plan.stop_loss < plan.entry_zone_low
    assert plan.tp1 > plan.entry_zone_high
    assert plan.tp2 > plan.tp1
    assert plan.risk_reward >= 1.5


def test_price_engine_falls_back_when_entry_plan_is_far_from_current() -> None:
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    bars: list[MarketBar] = []
    for i in range(40):
        close = 100.0 + i * 0.05
        high = close + 1.0
        if i == 30:
            high = 500.0
        low = close - 1.0
        bars.append(
            MarketBar(
                ticker="TEST",
                timestamp=as_of - timedelta(days=40 - i),
                open=close,
                high=high,
                low=low,
                close=close,
                volume=1_000_000,
            )
        )

    feature = FeatureSnapshot(
        id="feature-test",
        ticker="TEST",
        timestamp=bars[-1].timestamp,
        atr=2.0,
        ma_20=100.0,
        ma_50=99.5,
        ma_200=98.0,
        volatility_20d=0.2,
        momentum_20d=0.1,
        relative_strength_63d=0.05,
        avg_dollar_volume_20d=100_000_000,
        breakout_level_20d=500.0,
        support_level_20d=98.0,
    )
    signal = SignalSnapshot(
        id="signal-test",
        ticker="TEST",
        timestamp=bars[-1].timestamp,
        trend_score=0.7,
        momentum_score=0.6,
        volatility_score=0.5,
        liquidity_score=0.8,
        relative_strength_score=0.6,
        event_score=0.5,
        fundamental_score=0.5,
        execution_quality_score=0.7,
        technical_score=0.6,
        composite_score=0.62,
        regime_label="risk_on",
        evidence_conflict=False,
    )
    security = SecurityMetadata(
        ticker="TEST",
        sector="Technology",
        market_cap_usd=100_000_000_000,
        avg_dollar_volume=80_000_000,
        last_price=bars[-1].close,
        spread_bps=12.0,
    )

    plan = PriceSizingEngine().build(
        security=security,
        bars=bars,
        feature=feature,
        signal=signal,
        cfg=PricePlanConfig(),
        direction=Direction.BUY,
    )

    current = bars[-1].close
    mid = (plan.entry_zone_low + plan.entry_zone_high) / 2.0
    assert abs(mid - current) / current <= 0.40
