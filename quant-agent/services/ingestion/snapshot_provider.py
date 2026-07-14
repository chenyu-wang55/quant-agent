from __future__ import annotations

from datetime import datetime, timedelta, timezone

from domain.entities.models import FundamentalSnapshot, MarketBar, NewsEvent, SecurityMetadata
from infra.db.repositories import SourceSnapshotRepository
from services.ingestion.interfaces import DataProvider


class TemporalDataViolation(ValueError):
    """Raised when a provider returns information unavailable at the requested as-of time."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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
        self.quality_issues: list[str] = []
        self.failures: list[dict[str, str]] = []

    def _issue(self, message: str) -> None:
        if message not in self.quality_issues:
            self.quality_issues.append(message)

    @staticmethod
    def _require_not_future(label: str, value: datetime, as_of: datetime) -> None:
        if _utc(value) > _utc(as_of):
            raise TemporalDataViolation(
                f"{label} timestamp {_utc(value).isoformat()} exceeds as_of {_utc(as_of).isoformat()}"
            )

    def record_failure(self, operation: str, ticker: str | None, exc: Exception) -> None:
        self.failures.append(
            {
                "operation": operation,
                "ticker": ticker or "",
                "error_type": type(exc).__name__,
                "message": str(exc)[:500],
            }
        )

    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        self.securities = self.delegate.get_universe(universe, as_of)
        for security in self.securities:
            if security.as_of is None:
                self._issue(f"security_as_of_missing:{security.ticker.upper()}")
            else:
                self._require_not_future(f"security:{security.ticker}", security.as_of, as_of)
            if security.source == "unknown" or security.quality_status == "unverified":
                self._issue(f"security_provenance_unverified:{security.ticker.upper()}")
            for field in security.fallback_fields:
                self._issue(f"security_fallback:{security.ticker.upper()}:{field}")
        return self.securities

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        return self.delegate.get_latest_price(ticker, as_of)

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        bars = self.delegate.get_bars(ticker, as_of, lookback_days)
        for bar in bars:
            self._require_not_future(f"bar:{ticker}", bar.timestamp, as_of)
            if bar.source == "unknown" or bar.quality_status == "unverified":
                self._issue(f"bar_provenance_unverified:{ticker.upper()}")
        self._store_bars(ticker, bars)
        return bars

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]:
        bars = self.delegate.get_benchmark_bars(benchmark, as_of, lookback_days)
        for bar in bars:
            self._require_not_future(f"bar:{benchmark}", bar.timestamp, as_of)
            if bar.source == "unknown" or bar.quality_status == "unverified":
                self._issue(f"bar_provenance_unverified:{benchmark.upper()}")
        self._store_bars(benchmark, bars)
        return bars

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        fundamentals = self.delegate.get_fundamentals(ticker, as_of)
        self._require_not_future(f"fundamental:{ticker}", fundamentals.timestamp, as_of)
        if fundamentals.available_at is not None:
            self._require_not_future(
                f"fundamental_available_at:{ticker}", fundamentals.available_at, as_of
            )
        else:
            self._issue(f"fundamental_available_at_missing:{ticker.upper()}")
        if fundamentals.source == "unknown" or fundamentals.quality_status == "unverified":
            self._issue(f"fundamental_provenance_unverified:{ticker.upper()}")
        for field in fundamentals.fallback_fields:
            self._issue(f"fundamental_fallback:{ticker.upper()}:{field}")
        self.fundamentals_by_ticker[ticker.upper()] = fundamentals
        return fundamentals

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]:
        events = self.delegate.get_events(tickers, as_of, lookback_days)
        for event in events:
            self._require_not_future(f"event_published:{event.source_id}", event.published_at, as_of)
            self._require_not_future(f"event_ingested:{event.source_id}", event.ingested_at, as_of)
            if event.source == "unknown" or event.quality_status == "unverified":
                self._issue(f"event_provenance_unverified:{event.source_id}")
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
            provider_quality=self.get_quality_report(),
        )

    def get_quality_report(self) -> dict[str, object]:
        delegate_report_fn = getattr(self.delegate, "get_quality_report", None)
        delegate_report = (
            dict(delegate_report_fn() or {}) if callable(delegate_report_fn) else {}
        )
        issues = list(delegate_report.get("issues") or []) + list(self.quality_issues)
        failures = list(delegate_report.get("failures") or []) + list(self.failures)
        fallback_fields = list(delegate_report.get("fallback_fields") or [])
        status = str(delegate_report.get("status") or "unverified")
        if failures or issues or fallback_fields:
            status = "blocked"
        return {
            "status": status,
            "issues": issues,
            "failures": failures,
            "fallback_fields": fallback_fields,
        }

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
        bars = self.repository.get_bars(self.source_snapshot_id, ticker, lookback_days)
        bars = [bar for bar in bars if _utc(bar.timestamp) <= _utc(as_of)]
        if not bars:
            raise ValueError(f"No snapshot bars for {ticker} in {self.source_snapshot_id}")
        return bars

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]:
        return self.get_bars(benchmark, as_of, lookback_days)

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        fundamentals = self.repository.get_fundamental(self.source_snapshot_id, ticker)
        if fundamentals is None:
            raise ValueError(f"No snapshot fundamentals for {ticker} in {self.source_snapshot_id}")
        if _utc(fundamentals.timestamp) > _utc(as_of):
            raise TemporalDataViolation(f"Snapshot fundamentals for {ticker} exceed as_of")
        if fundamentals.available_at and _utc(fundamentals.available_at) > _utc(as_of):
            raise TemporalDataViolation(f"Snapshot fundamentals availability for {ticker} exceeds as_of")
        return fundamentals

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]:
        cutoff = _utc(as_of) - timedelta(days=lookback_days)
        return [
            event
            for event in self.repository.get_events(self.source_snapshot_id, tickers)
            if cutoff <= _utc(event.published_at) <= _utc(as_of)
            and _utc(event.ingested_at) <= _utc(as_of)
        ]

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        _ = as_of
        earnings = dict(self.metadata.get("earnings_minutes_by_ticker") or {})
        value = earnings.get(ticker.upper())
        return int(value) if value is not None else None
