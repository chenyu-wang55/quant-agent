from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import event, func, select

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
from infra.db.models import MarketBarRecord, SnapshotMarketBarRefRecord
from infra.db.repositories import SourceSnapshotRepository
from infra.db.session import SessionLocal, engine
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
    assert all(
        bar.source == "deterministic_mock"
        and bar.quality_status == "mock_verified"
        and bar.adjusted_close is not None
        for bars in snapshot_export.bars_by_ticker.values()
        for bar in bars
    )
    assert all(
        item.source == "deterministic_mock"
        and item.quality_status == "mock_verified"
        and item.available_at is not None
        for item in snapshot_export.fundamentals_by_ticker.values()
    )
    assert all(
        item.source == "deterministic_mock" and item.quality_status == "mock_verified"
        for item in snapshot_export.securities
    )


def test_identical_snapshot_bars_are_stored_once_and_referenced_twice() -> None:
    init_db()
    repository = SourceSnapshotRepository()
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    securities = provider.get_universe("SP500", as_of)[:2]
    bars_by_ticker = {
        security.ticker: provider.get_bars(security.ticker, as_of, lookback_days=5)
        for security in securities
    }
    fundamentals = {
        security.ticker: provider.get_fundamentals(security.ticker, as_of)
        for security in securities
    }
    expected_bar_count = sum(len(bars) for bars in bars_by_ticker.values())
    provider_name = f"dedupe-provider-{uuid4().hex}"
    with SessionLocal() as session:
        base_count_before = int(
            session.scalar(select(func.count()).select_from(MarketBarRecord)) or 0
        )
        ref_count_before = int(
            session.scalar(select(func.count()).select_from(SnapshotMarketBarRefRecord)) or 0
        )

    for suffix in ("a", "b"):
        repository.replace_snapshot(
            source_snapshot_id=f"dedupe-{suffix}-{uuid4().hex}",
            as_of=as_of,
            universe="SP500",
            provider_name=provider_name,
            securities=securities,
            bars_by_ticker=bars_by_ticker,
            fundamentals_by_ticker=fundamentals,
            events=[],
            earnings_minutes_by_ticker={},
        )

    with SessionLocal() as session:
        base_count = int(session.scalar(select(func.count()).select_from(MarketBarRecord)) or 0)
        ref_count = int(
            session.scalar(select(func.count()).select_from(SnapshotMarketBarRefRecord)) or 0
        )
    assert base_count - base_count_before == expected_bar_count
    assert ref_count - ref_count_before == expected_bar_count * 2


def test_revised_corporate_action_data_does_not_mutate_an_older_snapshot() -> None:
    init_db()
    repository = SourceSnapshotRepository()
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    security = provider.get_universe("SP500", as_of)[:1]
    ticker = security[0].ticker
    original = provider.get_bars(ticker, as_of, lookback_days=1)[0].model_copy(
        update={
            "adjusted_close": 99.0,
            "dividend": 0.0,
            "split_factor": 0.0,
            "source": "licensed-feed-v1",
            "quality_status": "verified",
        }
    )
    revised = original.model_copy(
        update={
            "adjusted_close": 49.0,
            "dividend": 1.0,
            "split_factor": 2.0,
            "source": "licensed-feed-v2",
        }
    )
    provider_name = f"immutable-provider-{uuid4().hex}"
    fundamentals = {ticker: provider.get_fundamentals(ticker, as_of)}
    snapshot_ids = {
        "v1": f"immutable-v1-{uuid4().hex}",
        "v2": f"immutable-v2-{uuid4().hex}",
    }

    for snapshot_id, bar in ((snapshot_ids["v1"], original), (snapshot_ids["v2"], revised)):
        repository.replace_snapshot(
            source_snapshot_id=snapshot_id,
            as_of=as_of,
            universe="SP500",
            provider_name=provider_name,
            securities=security,
            bars_by_ticker={ticker: [bar]},
            fundamentals_by_ticker=fundamentals,
            events=[],
            earnings_minutes_by_ticker={},
        )

    with SessionLocal() as session:
        stored = list(
            session.execute(
                select(MarketBarRecord)
                .where(MarketBarRecord.vendor_id == provider_name)
                .order_by(MarketBarRecord.adjusted_close.desc())
            ).scalars()
        )
    assert len(stored) == 2
    assert [item.adjusted_close for item in stored] == [99.0, 49.0]
    assert [item.dividend for item in stored] == [0.0, 1.0]
    assert [item.split_factor for item in stored] == [0.0, 2.0]
    assert [item.provenance_json["source"] for item in stored] == [
        "licensed-feed-v1",
        "licensed-feed-v2",
    ]
    old_snapshot_bar = repository.get_bars(snapshot_ids["v1"], ticker, limit=1)[0]
    revised_snapshot_bar = repository.get_bars(snapshot_ids["v2"], ticker, limit=1)[0]
    assert (old_snapshot_bar.adjusted_close, old_snapshot_bar.dividend) == (99.0, 0.0)
    assert (revised_snapshot_bar.adjusted_close, revised_snapshot_bar.dividend) == (49.0, 1.0)


def test_snapshot_listing_uses_a_fixed_number_of_queries() -> None:
    init_db()
    repository = SourceSnapshotRepository()
    provider = MockMarketDataProvider()
    as_of = datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc)
    security = provider.get_universe("SP500", as_of)[:1]
    bars = {security[0].ticker: provider.get_bars(security[0].ticker, as_of, lookback_days=2)}
    fundamentals = {
        security[0].ticker: provider.get_fundamentals(security[0].ticker, as_of)
    }
    for index in range(8):
        repository.replace_snapshot(
            source_snapshot_id=f"list-{index}-{uuid4().hex}",
            as_of=as_of,
            universe="SP500",
            provider_name=provider.__class__.__name__,
            securities=security,
            bars_by_ticker=bars,
            fundamentals_by_ticker=fundamentals,
            events=[],
            earnings_minutes_by_ticker={},
        )

    query_count = 0

    def count_query(*_args) -> None:
        nonlocal query_count
        query_count += 1

    event.listen(engine, "before_cursor_execute", count_query)
    try:
        summaries = repository.list_summaries(limit=500)
    finally:
        event.remove(engine, "before_cursor_execute", count_query)

    assert len(summaries) >= 8
    assert query_count == 5
