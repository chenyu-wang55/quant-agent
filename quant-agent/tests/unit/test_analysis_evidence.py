from datetime import datetime, timezone

import pytest

from domain.entities.models import Direction, PatternType, SecurityMetadata, SignalSnapshot, TradePlan
from services.ranking.recommendation_builder import RecommendationBuilder


@pytest.mark.parametrize(
    "score,regime,conflict",
    [(0.1, "risk_off", False), (-0.2, "neutral", False), (0.8, "risk_on", True), (0.8, "risk_off", False)],
)
def test_weak_or_conflicting_analysis_does_not_invent_bullish_facts(score, regime, conflict):
    signal = SignalSnapshot(
        id="signal",
        ticker="AAPL",
        timestamp=datetime(2026, 4, 10, tzinfo=timezone.utc),
        trend_score=score,
        momentum_score=score,
        volatility_score=score,
        liquidity_score=score,
        relative_strength_score=score,
        event_score=score,
        fundamental_score=score,
        execution_quality_score=score,
        technical_score=score,
        composite_score=score,
        regime_label=regime,
        evidence_conflict=conflict,
    )
    plan = TradePlan(
        ticker="AAPL",
        pattern=PatternType.PULLBACK,
        direction=Direction.BUY,
        entry_zone_low=99.12,
        entry_zone_high=101.23,
        stop_loss=95.34,
        tp1=110.45,
        tp2=120.56,
        holding_period="10 days",
        risk_reward=2,
    )
    security = SecurityMetadata(
        ticker="AAPL", sector="Technology", market_cap_usd=1e10, avg_dollar_volume=1e8, last_price=100, spread_bps=2
    )
    rec = RecommendationBuilder().build(security, signal, plan, "snapshot", "feature")
    assert rec.analysis and "不建议新开仓" in rec.analysis.action_guidance_cn
    assert "趋势向上" not in rec.analysis.report_cn
    assert "未附原始新闻证据" in rec.analysis.report_cn
    assert "不是预测胜率" in rec.analysis.summary
    assert rec.confidence_kind == "rule_signal_strength"
    assert rec.signal_strength == rec.confidence
    for name in ("entry_zone_low", "entry_zone_high", "stop_loss", "tp1", "tp2"):
        assert getattr(rec, name) == getattr(plan, name)
        assert f"{getattr(plan, name):.2f}" in rec.analysis.report_cn
