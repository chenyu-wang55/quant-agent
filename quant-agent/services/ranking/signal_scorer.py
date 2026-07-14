from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from domain.entities.models import (
    FeatureSnapshot,
    FundamentalSnapshot,
    MarketBar,
    NewsEvent,
    SecurityMetadata,
    SignalConfig,
    SignalSnapshot,
)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _normalize(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    return _clamp((value - low) / (high - low))


def _sma(values: list[float], window: int) -> float:
    if not values:
        return 0.0
    if len(values) < window:
        return sum(values) / len(values)
    chunk = values[-window:]
    return sum(chunk) / len(chunk)


class SignalScorer:
    def score(
        self,
        as_of: datetime,
        security: SecurityMetadata,
        bars: list[MarketBar],
        benchmark_bars: list[MarketBar],
        feature: FeatureSnapshot,
        fundamentals: FundamentalSnapshot,
        events: list[NewsEvent],
        signal_config: SignalConfig,
    ) -> SignalSnapshot:
        closes = [bar.close for bar in bars]
        benchmark_closes = [bar.close for bar in benchmark_bars]

        last_close = closes[-1]
        trend_score = 0.3
        if feature.ma_200 > 0 and feature.ma_50 > 0:
            if last_close > feature.ma_50 > feature.ma_200:
                trend_score = 1.0
            elif last_close > feature.ma_50:
                trend_score = 0.7
            elif last_close > feature.ma_200:
                trend_score = 0.5

        momentum_score = _normalize(feature.momentum_20d, -0.20, 0.20)
        vol_target = 0.25
        vol_distance = abs(feature.volatility_20d - vol_target)
        volatility_score = _clamp(1.0 - (vol_distance / 0.25))

        technical_score = _clamp(0.5 * trend_score + 0.3 * momentum_score + 0.2 * volatility_score)
        relative_strength_score = _normalize(feature.relative_strength_63d, -0.20, 0.20)

        # Event/news stream (kept separate from price stream until composite stage)
        event_values: list[float] = []
        as_of_utc = as_of.astimezone(timezone.utc)
        for event in events:
            age_hours = max(0.0, (as_of_utc - event.published_at).total_seconds() / 3600)
            freshness = _clamp(1.0 - age_hours / (24 * 7))
            sentiment_component = _normalize(event.sentiment, -1.0, 1.0)
            event_values.append(_clamp(event.relevance * sentiment_component * freshness))
        event_score = sum(event_values) / len(event_values) if event_values else 0.5

        # Fundamentals
        growth_score = _normalize(fundamentals.revenue_growth_yoy, -0.10, 0.40)
        revision_score = _normalize(fundamentals.eps_revision_30d, -0.20, 0.20)
        quality_score = _normalize(fundamentals.roe, 0.0, 0.40)
        valuation_score = 1.0 - _normalize(fundamentals.pe_ttm, 10.0, 45.0)
        fundamental_score = _clamp(
            0.30 * growth_score + 0.30 * revision_score + 0.25 * quality_score + 0.15 * valuation_score
        )

        # Execution quality
        liquidity_score = _normalize(feature.avg_dollar_volume_20d, 10_000_000, 200_000_000)
        spread_score = 1.0 - _normalize(security.spread_bps, 2.0, 60.0)
        execution_quality_score = _clamp(0.7 * liquidity_score + 0.3 * spread_score)

        composite = _clamp(
            signal_config.technical_weight * technical_score
            + signal_config.event_news_weight * event_score
            + signal_config.relative_strength_weight * relative_strength_score
            + signal_config.fundamental_weight * fundamental_score
            + signal_config.execution_quality_weight * execution_quality_score
        )

        benchmark_ma_200 = _sma(benchmark_closes, 200)
        benchmark_last = benchmark_closes[-1]
        regime_label = "risk_on" if benchmark_last >= benchmark_ma_200 else "risk_off"

        evidence_conflict = (technical_score >= 0.70 and event_score <= 0.35) or (
            technical_score <= 0.35 and event_score >= 0.70
        )

        signal_id = hashlib.sha1(
            (
                f"{security.ticker}|{as_of_utc.isoformat()}|"
                f"{round(technical_score,4)}|{round(event_score,4)}|{round(composite,4)}"
            ).encode("utf-8")
        ).hexdigest()[:16]

        return SignalSnapshot(
            id=signal_id,
            ticker=security.ticker,
            timestamp=as_of_utc,
            trend_score=round(trend_score, 6),
            momentum_score=round(momentum_score, 6),
            volatility_score=round(volatility_score, 6),
            liquidity_score=round(liquidity_score, 6),
            relative_strength_score=round(relative_strength_score, 6),
            event_score=round(event_score, 6),
            fundamental_score=round(fundamental_score, 6),
            execution_quality_score=round(execution_quality_score, 6),
            technical_score=round(technical_score, 6),
            composite_score=round(composite, 6),
            regime_label=regime_label,
            evidence_conflict=evidence_conflict,
        )
