from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.ingestion.vendors.yfinance_provider import ProviderDataError, YFinanceProvider


def test_pipeline_records_new_snapshot_for_changed_inputs_at_same_path(tmp_path, monkeypatch):
    from domain.entities.models import ResearchRunRequest, RunType
    from infra.db.repositories import SourceSnapshotRepository
    from services.ingestion.mock_provider import MockMarketDataProvider
    from services.ranking.pipeline import ResearchPipeline

    provider = _provider()
    data_file = tmp_path / "universe.csv"
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(data_file))
    monkeypatch.delenv("POINT_IN_TIME_VALIDATION_REPORT", raising=False)
    data_file.write_text("ticker,source\nOLD,v1\n")
    monkeypatch.setattr(provider, "get_universe", lambda *_: [])
    monkeypatch.setattr(provider, "get_events", lambda *_: [])
    monkeypatch.setattr(provider, "get_benchmark_bars", MockMarketDataProvider().get_benchmark_bars)
    repository = SourceSnapshotRepository()
    pipeline = ResearchPipeline(provider=provider, snapshot_repository=repository)
    request = ResearchRunRequest(run_type=RunType.RESEARCH_BATCH, objective="fingerprint snapshot regression",
                                 as_of=datetime(2026, 4, 10, tzinfo=timezone.utc))
    first = pipeline.run(request).result
    fingerprint = repository.get_summary(first.source_snapshot_id).data_quality["dataset_fingerprint"]
    data_file.write_text("ticker,source\nNEW,v2\n")
    second = pipeline.run(request).result
    assert first.source_snapshot_id != second.source_snapshot_id
    assert repository.get_summary(second.source_snapshot_id).data_quality["dataset_fingerprint"] != fingerprint
    with pytest.raises(ValueError, match="fingerprint_mismatch"):
        pipeline.run(request.model_copy(update={"source_snapshot_id": first.source_snapshot_id}))


def _provider():
    # No vendor network: constructor only initializes local caches.
    return YFinanceProvider()


def test_wrapped_news_legacy_news_dedup_and_unknown_schema(monkeypatch) -> None:
    provider = _provider()
    now = datetime.now(timezone.utc)
    at = now - timedelta(hours=1)
    legacy = {"uuid": "same", "providerPublishTime": int(at.timestamp()), "title": "Earnings miss",
              "relatedTickers": ["AAPL", "MSFT"], "link": "https://example.test/event"}
    wrapped = {"id": "same", "content": {"pubDate": at.replace(microsecond=0).isoformat(),
               "title": "Earnings miss", "canonicalUrl": {"url": "https://example.test/event"},
               "finance": {"stockTickers": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}}}
    ticker = type("Ticker", (), {"news": [legacy, wrapped, {"unexpected": "shape"}]})()
    monkeypatch.setattr(provider, "_ticker", lambda _: ticker)
    events = provider.get_events(["AAPL", "MSFT"], now)
    assert len(events) == 1
    assert events[0].tickers == ["AAPL", "MSFT"]
    assert events[0].sentiment < 0
    assert events[0].source_url == "https://example.test/event"
    assert provider.get_quality_report()["status"] == "blocked"
    assert any("news_schema_unknown" in issue for issue in provider.get_quality_report()["issues"])


def test_csv_cache_changes_at_same_path_and_pins_each_run(tmp_path, monkeypatch) -> None:
    provider = _provider()
    path = tmp_path / "universe.csv"
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(path))
    path.write_text("ticker,source\nOLD,v1\n")
    assert provider._csv_rows("POINT_IN_TIME_UNIVERSE_CSV")[0]["ticker"] == "OLD"
    path.write_text("ticker,source\nNEW,v2\n")
    assert provider._csv_rows("POINT_IN_TIME_UNIVERSE_CSV")[0]["ticker"] == "NEW"
    with pytest.raises(ProviderDataError, match="changed during run"):
        with provider.input_session():
            assert provider._csv_rows("POINT_IN_TIME_UNIVERSE_CSV")[0]["ticker"] == "NEW"
            path.write_text("ticker,source\nTHIRD,v3\n")
            provider._csv_rows("POINT_IN_TIME_UNIVERSE_CSV")
    with provider.input_session():
        assert provider._csv_rows("POINT_IN_TIME_UNIVERSE_CSV")[0]["ticker"] == "THIRD"
