from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from domain.entities.models import (
    FundamentalSnapshot,
    MarketBar,
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
    UniverseRules,
)
from infra.db.init_db import init_db
from infra.db.repositories import SourceSnapshotRepository
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ingestion.vendors.yfinance_provider import (
    ProviderUnavailableError,
    YFinanceProvider,
)
from services.ranking.pipeline import ResearchPipeline

AS_OF = datetime(2026, 4, 10, 13, 30, tzinfo=timezone.utc)


def _request(snapshot_id: str) -> ResearchRunRequest:
    return ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="data provenance test",
        as_of=AS_OF,
        source_snapshot_id=snapshot_id,
        universe_rules=UniverseRules(
            min_price=0,
            min_avg_dollar_volume=0,
            max_spread_bps=10_000,
            min_market_cap_usd=0,
            max_candidates_after_filter=100,
        ),
        risk_policy=RiskPolicy(
            min_confidence=0,
            earnings_blackout_minutes=0,
            max_name_weight=1,
            max_sector_weight=10,
            max_gross_exposure=10,
            max_correlated_cluster_weight=10,
            reject_on_material_evidence_conflict=False,
            event_trading_enabled=True,
        ),
        publication=PublicationConfig(top_n=20),
    )


class FutureFundamentalsProvider(MockMarketDataProvider):
    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        item = super().get_fundamentals(ticker, as_of)
        future = as_of.astimezone(timezone.utc) + timedelta(minutes=1)
        return item.model_copy(update={"timestamp": future, "available_at": future})


class FailedEventsProvider(MockMarketDataProvider):
    def get_events(self, tickers: list[str], as_of: datetime, lookback_days: int = 7):
        raise ProviderUnavailableError("news vendor unavailable")


def test_future_fundamentals_are_rejected_and_block_live_execution() -> None:
    init_db()
    repository = SourceSnapshotRepository()
    snapshot_id = "future-fundamentals"
    output = ResearchPipeline(
        provider=FutureFundamentalsProvider(),
        snapshot_repository=repository,
    ).run(_request(snapshot_id))

    assert output.result.recommendations == []
    failures = output.result.universe_summary["provider_failures"]
    assert failures
    assert {failure["error_type"] for failure in failures} == {"TemporalDataViolation"}
    summary = repository.get_summary(snapshot_id)
    assert summary is not None
    assert summary.data_quality["status"] in {"blocked", "partial"}
    assert summary.data_quality["live_execution_allowed"] is False
    assert summary.data_quality["provider_failure_count"] > 0


def test_top_level_provider_failure_is_not_silently_converted_to_empty_data() -> None:
    with pytest.raises(ProviderUnavailableError, match="news vendor unavailable"):
        ResearchPipeline(provider=FailedEventsProvider()).run(_request("failed-events"))


def test_repository_rejects_future_market_bars() -> None:
    init_db()
    repository = SourceSnapshotRepository()
    future_bar = MarketBar(
        ticker="AAPL",
        timestamp=AS_OF + timedelta(seconds=1),
        open=100,
        high=101,
        low=99,
        close=100,
        volume=1_000,
        source="test",
        quality_status="verified",
    )
    with pytest.raises(ValueError, match="newer than as_of"):
        repository.replace_snapshot(
            source_snapshot_id="future-bar",
            as_of=AS_OF,
            universe="SP500",
            provider_name="test",
            securities=[],
            bars_by_ticker={"AAPL": [future_bar]},
            fundamentals_by_ticker={},
            events=[],
            earnings_minutes_by_ticker={},
            provider_quality={
                "status": "verified",
                "issues": [],
                "failures": [],
                "fallback_fields": [],
            },
        )


def test_yfinance_retry_raises_after_final_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = object.__new__(YFinanceProvider)
    monkeypatch.setattr("services.ingestion.vendors.yfinance_provider.time.sleep", lambda *_: None)
    attempts = 0

    def fail():
        nonlocal attempts
        attempts += 1
        raise OSError("network down")

    with pytest.raises(ProviderUnavailableError, match="history failed after 3 attempts"):
        provider._retry(fail, "history")
    assert attempts == 3


def test_historical_latest_price_uses_as_of_bars_not_live_quote() -> None:
    provider = object.__new__(YFinanceProvider)
    requested: list[datetime] = []

    def bars(ticker: str, as_of: datetime, lookback_days: int = 260):
        requested.append(as_of)
        return [
            MarketBar(
                ticker=ticker,
                timestamp=as_of,
                open=10,
                high=11,
                low=9,
                close=10.5,
                volume=100,
            )
        ]

    provider.get_bars = bars  # type: ignore[method-assign]
    provider._fast_info = lambda *_args, **_kwargs: pytest.fail("live quote accessed")  # type: ignore[method-assign]
    historical_as_of = datetime(2020, 1, 2, tzinfo=timezone.utc)
    assert provider.get_latest_price("AAPL", historical_as_of) == 10.5
    assert requested == [historical_as_of]
