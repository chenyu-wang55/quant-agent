from __future__ import annotations

from datetime import datetime, timezone

from domain.entities.models import (
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
    SnapshotMode,
    UniverseRules,
)
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.pipeline import ResearchPipeline


def test_recommendation_ids_are_stable_for_same_point_in_time_snapshot() -> None:
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="determinism test",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        universe="SP500",
        universe_rules=UniverseRules(
            min_price=5,
            min_avg_dollar_volume=5_000_000,
            max_spread_bps=100,
            min_market_cap_usd=1_000_000_000,
            allowed_sectors=[],
            max_candidates_after_filter=100,
        ),
        risk_policy=RiskPolicy(
            min_confidence=0.0,
            earnings_blackout_minutes=15,
            max_name_weight=0.10,
            max_sector_weight=0.30,
            max_gross_exposure=1.0,
            max_correlated_cluster_weight=0.35,
            reject_on_material_evidence_conflict=False,
            event_trading_enabled=True,
        ),
        publication=PublicationConfig(top_n=3, output_channels=["api"]),
    )
    pipeline = ResearchPipeline(provider=MockMarketDataProvider())

    first = pipeline.run(request).result.recommendations
    second = pipeline.run(request).result.recommendations

    assert [rec.id for rec in first] == [rec.id for rec in second]
    assert [rec.generated_at for rec in first] == [rec.generated_at for rec in second]


def test_latest_mode_relaxes_min_confidence_without_flooring_to_sixty_percent() -> None:
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="latest threshold regression",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        snapshot_mode=SnapshotMode.LATEST,
        universe="SP500",
        universe_rules=UniverseRules(
            min_price=5,
            min_avg_dollar_volume=5_000_000,
            max_spread_bps=100,
            min_market_cap_usd=1_000_000_000,
            allowed_sectors=[],
            max_candidates_after_filter=100,
        ),
        risk_policy=RiskPolicy(
            min_confidence=0.0,
            earnings_blackout_minutes=15,
            max_name_weight=0.10,
            max_sector_weight=0.30,
            max_gross_exposure=1.0,
            max_correlated_cluster_weight=0.35,
            reject_on_material_evidence_conflict=False,
            event_trading_enabled=True,
        ),
        publication=PublicationConfig(top_n=3, output_channels=["api"]),
    )
    result = ResearchPipeline(provider=MockMarketDataProvider()).run(request).result

    assert result.signal_model["effective_min_confidence"] == 0.0
