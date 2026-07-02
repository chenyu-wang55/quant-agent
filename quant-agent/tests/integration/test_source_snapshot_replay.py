from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from domain.entities.models import (
    FundamentalSnapshot,
    MarketBar,
    NewsEvent,
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
    SecurityMetadata,
    UniverseRules,
)
from infra.db.init_db import init_db
from infra.db.repositories import SourceSnapshotRepository
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.pipeline import ResearchPipeline


class FailingProvider:
    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        raise AssertionError("live provider should not be called during snapshot replay")

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        raise AssertionError("live provider should not be called during snapshot replay")

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        raise AssertionError("live provider should not be called during snapshot replay")

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]:
        raise AssertionError("live provider should not be called during snapshot replay")

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        raise AssertionError("live provider should not be called during snapshot replay")

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]:
        raise AssertionError("live provider should not be called during snapshot replay")

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        raise AssertionError("live provider should not be called during snapshot replay")


def _request(source_snapshot_id: str) -> ResearchRunRequest:
    return ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="snapshot replay integration test",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        source_snapshot_id=source_snapshot_id,
        universe="SP500",
        universe_rules=UniverseRules(
            min_price=5,
            min_avg_dollar_volume=5_000_000,
            max_spread_bps=100,
            min_market_cap_usd=1_000_000_000,
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


def _projection(output) -> list[dict]:
    return [
        {
            "ticker": rec.ticker,
            "composite": round(rec.score_vector["composite"], 6),
            "entry_zone_low": round(rec.entry_zone_low, 4),
            "entry_zone_high": round(rec.entry_zone_high, 4),
            "stop_loss": round(rec.stop_loss, 4),
            "tp1": round(rec.tp1, 4),
            "tp2": round(rec.tp2, 4),
            "confidence": round(rec.confidence, 6),
        }
        for rec in output.result.recommendations
    ]


def test_source_snapshot_records_and_replays_without_live_provider() -> None:
    init_db()
    snapshot_id = f"test-snapshot-{uuid4().hex}"
    repository = SourceSnapshotRepository()

    first_output = ResearchPipeline(
        provider=MockMarketDataProvider(),
        snapshot_repository=repository,
    ).run(_request(snapshot_id))

    replay_output = ResearchPipeline(
        provider=FailingProvider(),
        snapshot_repository=repository,
    ).run(_request(snapshot_id))

    assert first_output.result.universe_summary["snapshot"]["operation"] == "recorded"
    assert replay_output.result.universe_summary["snapshot"]["operation"] == "replayed"
    assert _projection(replay_output) == _projection(first_output)
    assert repository.snapshot_exists(snapshot_id)
    metadata_quality = repository.get_metadata(snapshot_id)["data_quality"]
    summary = repository.get_summary(snapshot_id)
    assert summary is not None
    summary_quality = summary.data_quality
    assert metadata_quality["status"] == "complete"
    assert metadata_quality["bar_coverage"] == 1.0
    assert metadata_quality["fundamental_coverage"] == 1.0
    assert metadata_quality["latest_bar_at"]
    assert metadata_quality["latest_bar_age_minutes"] >= 0
    assert metadata_quality["captured_ticker_count"] > 0
    assert "SPY" in metadata_quality["extra_bar_tickers"]
    assert summary_quality["status"] == metadata_quality["status"]
    assert summary_quality["bar_coverage"] == metadata_quality["bar_coverage"]
    snapshot_export = repository.get_export(snapshot_id)
    assert snapshot_export is not None
    assert snapshot_export.source_snapshot_id == snapshot_id
    assert snapshot_export.metadata["data_quality"]["status"] == "complete"
    assert snapshot_export.bar_count == sum(len(bars) for bars in snapshot_export.bars_by_ticker.values())
    assert set(snapshot_export.fundamentals_by_ticker).issuperset(
        {rec.ticker for rec in first_output.result.recommendations}
    )
    assert snapshot_export.event_count == len(snapshot_export.events)
