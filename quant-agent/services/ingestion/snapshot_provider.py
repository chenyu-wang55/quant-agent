from __future__ import annotations

from datetime import datetime

from domain.entities.models import FundamentalSnapshot, MarketBar, NewsEvent, SecurityMetadata
from infra.db.repositories import SourceSnapshotRepository
from services.ingestion.interfaces import DataProvider


class SnapshotRecordingProvider:
    def __init__(
        self,
        delegate: DataProvider,
        source_snapshot_id: str,
        repository: SourceSnapshotRepository,
    ) -> None:
        self.delegate = delegate
        self.source_snapshot_id = source_snapshot_id
        self.repository = repository
        self.securities: list[SecurityMetadata] = []
        self.bars_by_ticker: dict[str, list[MarketBar]] = {}
        self.fundamentals_by_ticker: dict[str, FundamentalSnapshot] = {}
        self.events_by_source_id: dict[str, NewsEvent] = {}
        self.earnings_minutes_by_ticker: dict[str, int | None] = {}

    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        self.securities = self.delegate.get_universe(universe, as_of)
        return self.securities

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        return self.delegate.get_latest_price(ticker, as_of)

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        bars = self.delegate.get_bars(ticker, as_of, lookback_days)
        self._store_bars(ticker, bars)
        return bars

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]:
        bars = self.delegate.get_benchmark_bars(benchmark, as_of, lookback_days)
        self._store_bars(benchmark, bars)
        return bars

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        fundamentals = self.delegate.get_fundamentals(ticker, as_of)
        self.fundamentals_by_ticker[ticker.upper()] = fundamentals
        return fundamentals

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]:
        events = self.delegate.get_events(tickers, as_of, lookback_days)
        for event in events:
            self.events_by_source_id[event.source_id] = event
        return events

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        minutes = self.delegate.get_upcoming_earnings_minutes(ticker, as_of)
        self.earnings_minutes_by_ticker[ticker.upper()] = minutes
        return minutes

    def persist(self, as_of: datetime, universe: str) -> None:
        self.repository.replace_snapshot(
            source_snapshot_id=self.source_snapshot_id,
            as_of=as_of,
            universe=universe,
            provider_name=type(self.delegate).__name__,
            securities=self.securities,
            bars_by_ticker=self.bars_by_ticker,
            fundamentals_by_ticker=self.fundamentals_by_ticker,
            events=self.events_by_source_id.values(),
            earnings_minutes_by_ticker=self.earnings_minutes_by_ticker,
        )

    def _store_bars(self, ticker: str, bars: list[MarketBar]) -> None:
        ticker_upper = ticker.upper()
        existing = self.bars_by_ticker.get(ticker_upper, [])
        if len(bars) >= len(existing):
            self.bars_by_ticker[ticker_upper] = bars


class SnapshotReplayProvider:
    def __init__(self, source_snapshot_id: str, repository: SourceSnapshotRepository) -> None:
        self.source_snapshot_id = source_snapshot_id
        self.repository = repository
        self.metadata = repository.get_metadata(source_snapshot_id)

    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        _ = (universe, as_of)
        return self.repository.get_securities(self.source_snapshot_id)

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=1)
        if not bars:
            return None
        return float(bars[-1].close)

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        _ = as_of
        bars = self.repository.get_bars(self.source_snapshot_id, ticker, lookback_days)
        if not bars:
            raise ValueError(f"No snapshot bars for {ticker} in {self.source_snapshot_id}")
        return bars

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]:
        return self.get_bars(benchmark, as_of, lookback_days)

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        _ = as_of
        fundamentals = self.repository.get_fundamental(self.source_snapshot_id, ticker)
        if fundamentals is None:
            raise ValueError(f"No snapshot fundamentals for {ticker} in {self.source_snapshot_id}")
        return fundamentals

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]:
        _ = (as_of, lookback_days)
        return self.repository.get_events(self.source_snapshot_id, tickers)

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        _ = as_of
        earnings = dict(self.metadata.get("earnings_minutes_by_ticker") or {})
        value = earnings.get(ticker.upper())
        return int(value) if value is not None else None
