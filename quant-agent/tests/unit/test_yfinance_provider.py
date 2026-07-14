from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.ingestion.vendors import yfinance_provider as provider_module
from services.ingestion.vendors.yfinance_provider import (
    ProviderDataError,
    ProviderUnavailableError,
    YFinanceProvider,
)


class _Index:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def to_pydatetime(self) -> datetime:
        return self.value


class _History:
    def __init__(self, rows: list[tuple[datetime, dict]]) -> None:
        self.rows = rows
        self.empty = not rows

    def iterrows(self):
        return [(_Index(timestamp), row) for timestamp, row in self.rows]


class _GrowthColumn(list):
    def dropna(self):
        return _GrowthColumn(value for value in self if value is not None)

    def head(self, count: int):
        return _GrowthColumn(self[:count])


class _Trend:
    empty = False
    columns = ["growth"]

    def __getitem__(self, key: str):
        assert key == "growth"
        return _GrowthColumn([0.2, 0.4, None])


class _FakeTicker:
    def __init__(self, now: datetime) -> None:
        self.info = {
            "trailingPE": 20.0,
            "returnOnEquity": 0.25,
            "revenueGrowth": 0.15,
            "currentPrice": 101.0,
        }
        self.fast_info = {"last_price": 102.0}
        self.earnings_trend = _Trend()
        self.calendar = {"Earnings Date": [now + timedelta(hours=3)]}
        self.news = [
            {
                "uuid": "valid-news",
                "providerPublishTime": int((now - timedelta(hours=1)).timestamp()),
                "title": "Strong growth beats record",
                "summary": "A useful summary",
                "relatedTickers": ["AAPL"],
                "type": "story",
                "canonicalUrl": {"url": "https://example.test/story"},
            },
            {"providerPublishTime": None},
            {"providerPublishTime": "bad-time"},
            {"providerPublishTime": int((now + timedelta(hours=1)).timestamp())},
            {"providerPublishTime": int((now - timedelta(days=30)).timestamp())},
        ]
        self.history_rows = [
            (
                now - timedelta(days=2),
                {
                    "Open": 98.0,
                    "High": 101.0,
                    "Low": 97.0,
                    "Close": 100.0,
                    "Volume": 1_000_000.0,
                    "Adj Close": 99.5,
                    "Dividends": 0.2,
                    "Stock Splits": 0.0,
                },
            ),
            (
                now - timedelta(days=1),
                {
                    "Open": 100.0,
                    "High": 104.0,
                    "Low": 99.0,
                    "Close": 103.0,
                    "Volume": 2_000_000.0,
                    "Adj Close": 102.5,
                    "Dividends": 0.0,
                    "Stock Splits": 2.0,
                },
            ),
            (
                now + timedelta(days=1),
                {
                    "Open": 103.0,
                    "High": 105.0,
                    "Low": 102.0,
                    "Close": 104.0,
                    "Volume": 3_000_000.0,
                },
            ),
        ]

    def history(self, **_kwargs):
        return _History(self.history_rows)


class _FakeYFinance:
    def __init__(self, now: datetime) -> None:
        self.now = now
        self.created: dict[str, _FakeTicker] = {}

    def Ticker(self, ticker: str) -> _FakeTicker:
        return self.created.setdefault(ticker, _FakeTicker(self.now))


def _provider(now: datetime) -> YFinanceProvider:
    provider = YFinanceProvider()
    provider.yf = _FakeYFinance(now)
    provider._ticker_cache.clear()
    provider._info_cache.clear()
    provider._fast_info_cache.clear()
    provider._history_cache.clear()
    provider._csv_cache.clear()
    provider._quality_issues.clear()
    provider._fallback_fields.clear()
    return provider


def _write_csv(path, header: str, *rows: str) -> None:
    path.write_text("\n".join([header, *rows, ""]), encoding="utf-8")


def test_helpers_retry_cache_quality_and_parsing(monkeypatch, tmp_path) -> None:
    now = datetime.now(timezone.utc)
    provider = _provider(now)
    monkeypatch.setattr(provider_module.time, "sleep", lambda _seconds: None)

    attempts = iter([RuntimeError("temporary"), RuntimeError("temporary"), 42])

    def eventually_succeeds():
        value = next(attempts)
        if isinstance(value, Exception):
            raise value
        return value

    assert provider._retry(eventually_succeeds, "eventual success") == 42
    assert provider._is_rate_limited(RuntimeError("too many requests")) is True
    assert provider._is_rate_limited(RuntimeError("ordinary")) is False
    with pytest.raises(ProviderUnavailableError):
        provider._retry(lambda: (_ for _ in ()).throw(RuntimeError("down")), "failure")

    provider._quality_issue("issue")
    provider._quality_issue("issue")
    provider._fallback("field")
    provider._fallback("field")
    assert provider.get_quality_report() == {
        "status": "blocked",
        "issues": ["issue"],
        "failures": [],
        "fallback_fields": ["field"],
        "point_in_time_validation": {},
    }
    assert YFinanceProvider._to_float(None, 3.0) == 3.0
    assert YFinanceProvider._to_float("bad", 4.0) == 4.0
    assert YFinanceProvider._headline_sentiment("strong growth but weak warning") == 0.0
    assert YFinanceProvider._stooq_symbol(provider, "AAPL") == "aapl.us"
    assert YFinanceProvider._stooq_symbol(provider, "BRK.B") == "brk.b"
    assert provider._fallback_meta("AAPL")[0] == "Technology"
    assert provider._fallback_meta("UNKNOWN") == ("Unknown", 50_000_000_000.0)
    assert provider._is_historical_mode(now - timedelta(days=1)) is True
    assert provider._is_historical_mode(now + timedelta(minutes=1)) is False

    assert provider._parse_utc("2026-01-01T00:00:00Z").tzinfo == timezone.utc
    with pytest.raises(ProviderDataError, match="explicit timezone"):
        provider._parse_utc("2026-01-01")
    assert provider._parse_utc("", end_of_time=True).year == datetime.max.year
    assert provider._ticker("aapl") is provider._ticker("AAPL")
    assert provider._info("AAPL") is provider._info("AAPL")
    assert provider._fast_info("AAPL") is provider._fast_info("AAPL")
    provider._info("AAPL", refresh=True)
    provider._fast_info("AAPL", refresh=True)

    monkeypatch.delenv("POINT_IN_TIME_EVENTS_CSV", raising=False)
    with pytest.raises(ProviderDataError, match="is required"):
        provider._csv_rows("POINT_IN_TIME_EVENTS_CSV")
    missing = tmp_path / "missing.csv"
    monkeypatch.setenv("POINT_IN_TIME_EVENTS_CSV", str(missing))
    with pytest.raises(ProviderDataError, match="not found"):
        provider._csv_rows("POINT_IN_TIME_EVENTS_CSV")
    existing = tmp_path / "events.csv"
    _write_csv(existing, "source_id", "one")
    monkeypatch.setenv("POINT_IN_TIME_EVENTS_CSV", str(existing))
    assert provider._csv_rows("POINT_IN_TIME_EVENTS_CSV") == [{"source_id": "one"}]
    assert provider._csv_rows("POINT_IN_TIME_EVENTS_CSV") == [{"source_id": "one"}]


def test_live_universe_bars_prices_fundamentals_events_and_earnings(monkeypatch, tmp_path) -> None:
    now = datetime.now(timezone.utc)
    provider = _provider(now)
    universe = tmp_path / "universe.csv"
    _write_csv(
        universe,
        "universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source",
        "SP500,AAPL,2020-01-01T00:00:00Z,,Technology,3000000000000,8,licensed-feed",
    )
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(universe))

    securities = provider.get_universe("SP500", now)
    assert len(securities) == 1
    assert securities[0].ticker == "AAPL"
    assert securities[0].last_price == 103.0
    assert securities[0].quality_status == "verified"

    bars = provider.get_bars("AAPL", now, lookback_days=30)
    assert bars is provider.get_bars("AAPL", now, lookback_days=30)
    assert len(bars) == 2
    assert bars[-1].adjusted_close == 102.5
    assert bars[-1].split_factor == 2.0
    assert provider.get_benchmark_bars("AAPL", now, 30) == bars
    assert provider.get_latest_price("AAPL", now) == 102.0

    fake_ticker = provider._ticker("AAPL")
    fake_ticker.fast_info = {"last_price": 0}
    assert provider.get_latest_price("AAPL", now) == 101.0
    fake_ticker.info = {
        "trailingPE": 20.0,
        "returnOnEquity": 0.25,
        "revenueGrowth": 0.15,
    }
    assert provider.get_latest_price("AAPL", now) == 103.0

    fundamentals = provider.get_fundamentals("AAPL", now)
    assert fundamentals.eps_revision_30d == pytest.approx(0.3)
    assert fundamentals.quality_status == "verified"
    events = provider.get_events(["AAPL"], now, lookback_days=7)
    assert [event.source_id for event in events] == ["valid-news"]
    assert events[0].sentiment > 0
    assert events[0].source_url == "https://example.test/story"
    assert provider.get_upcoming_earnings_minutes("AAPL", now) == 180

    fake_ticker.calendar = {"Earnings Date": [now - timedelta(minutes=1)]}
    assert provider.get_upcoming_earnings_minutes("AAPL", now) is None
    fake_ticker.calendar = {}
    assert provider.get_upcoming_earnings_minutes("AAPL", now) is None


def test_live_fundamental_errors_and_eps_fallback() -> None:
    now = datetime.now(timezone.utc)
    provider = _provider(now)
    ticker = provider._ticker("AAPL")
    ticker.info = {"trailingPE": 20.0}
    with pytest.raises(ProviderDataError, match="fundamentals missing"):
        provider.get_fundamentals("AAPL", now)

    ticker.info = {
        "trailingPE": 20.0,
        "returnOnEquity": 0.25,
        "revenueGrowth": 0.15,
    }
    provider._info_cache.clear()
    original_retry = provider._retry

    def retry_with_failed_trend(fn, operation: str):
        if "earnings_trend" in operation:
            raise RuntimeError("trend unavailable")
        return original_retry(fn, operation)

    provider._retry = retry_with_failed_trend
    result = provider.get_fundamentals("AAPL", now)
    assert result.eps_revision_30d == 0
    assert result.quality_status == "fallback"
    assert result.fallback_fields == ["eps_revision_30d"]


def test_historical_csv_fundamentals_events_earnings_and_price(monkeypatch, tmp_path) -> None:
    as_of = datetime(2025, 6, 15, 12, tzinfo=timezone.utc)
    provider = _provider(as_of)
    fundamentals = tmp_path / "fundamentals.csv"
    _write_csv(
        fundamentals,
        "ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source",
        "AAPL,2024-12-31T00:00:00Z,2025-02-01T00:00:00Z,18,0.20,0.10,0.01,filing",
        "AAPL,2025-03-31T00:00:00Z,2025-05-01T00:00:00Z,19,0.21,0.11,0.02,filing",
        "AAPL,2025-06-30T00:00:00Z,2025-08-01T00:00:00Z,99,0.99,0.99,0.99,future",
    )
    events = tmp_path / "events.csv"
    _write_csv(
        events,
        "source_id,published_at,ingested_at,headline,normalized_text,tickers,event_type,sentiment,relevance,horizon,source_url,source",
        "evt-valid,2025-06-14T12:00:00Z,2025-06-14T12:05:00Z,Beat growth,Beat growth,AAPL;MSFT,news,0.5,0.9,short,https://e/1,archive",
        "evt-other,2025-06-14T12:00:00Z,2025-06-14T12:05:00Z,Other,Other,TSLA,news,0,0.5,short,https://e/2,archive",
        "evt-future-ingest,2025-06-14T12:00:00Z,2025-06-16T12:00:00Z,Future,Future,AAPL,news,0,0.5,short,https://e/3,archive",
        "evt-old,2025-05-01T12:00:00Z,2025-05-01T12:05:00Z,Old,Old,AAPL,news,0,0.5,short,https://e/4,archive",
    )
    earnings = tmp_path / "earnings.csv"
    _write_csv(
        earnings,
        "ticker,known_at,earnings_at,source",
        "AAPL,2025-01-01T00:00:00Z,2025-06-16T12:00:00Z,calendar",
        "AAPL,2025-06-01T00:00:00Z,2025-06-17T12:00:00Z,calendar",
        "MSFT,2025-01-01T00:00:00Z,2025-06-20T12:00:00Z,calendar",
    )
    monkeypatch.setenv("POINT_IN_TIME_FUNDAMENTALS_CSV", str(fundamentals))
    monkeypatch.setenv("POINT_IN_TIME_EVENTS_CSV", str(events))
    monkeypatch.setenv("POINT_IN_TIME_EARNINGS_CSV", str(earnings))

    snapshot = provider.get_fundamentals("AAPL", as_of)
    assert snapshot.pe_ttm == 19
    assert snapshot.available_at == datetime(2025, 5, 1, tzinfo=timezone.utc)
    assert [event.source_id for event in provider.get_events(["AAPL"], as_of, 7)] == ["evt-valid"]
    assert provider.get_upcoming_earnings_minutes("AAPL", as_of) == 2 * 24 * 60
    assert provider.get_upcoming_earnings_minutes("UNKNOWN", as_of) is None

    historical_price = provider.get_latest_price("AAPL", as_of)
    assert historical_price == 103.0


def test_historical_missing_and_incomplete_records_fail_closed(monkeypatch, tmp_path) -> None:
    as_of = datetime(2025, 6, 15, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    fundamentals = tmp_path / "fundamentals.csv"
    _write_csv(
        fundamentals,
        "ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source",
        "AAPL,2025-03-31T00:00:00Z,2025-05-01T00:00:00Z,,0.2,0.1,0.0,filing",
    )
    monkeypatch.setenv("POINT_IN_TIME_FUNDAMENTALS_CSV", str(fundamentals))
    with pytest.raises(ProviderDataError, match="incomplete"):
        provider.get_fundamentals("AAPL", as_of)
    with pytest.raises(ProviderDataError, match="No point-in-time fundamentals"):
        provider.get_fundamentals("MSFT", as_of)

    _write_csv(
        fundamentals,
        "ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source",
        "AAPL,2025-03-31T00:00:00Z,2025-05-01T00:00:00Z,20,0.2,0.1,0.0,",
    )
    provider._csv_cache.clear()
    with pytest.raises(ProviderDataError, match="source is missing"):
        provider.get_fundamentals("AAPL", as_of)

    _write_csv(
        fundamentals,
        "ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source",
        "AAPL,2025-06-01T00:00:00Z,2025-05-01T00:00:00Z,20,0.2,0.1,0.0,filing",
    )
    provider._csv_cache.clear()
    with pytest.raises(ProviderDataError, match="period_end is after available_at"):
        provider.get_fundamentals("AAPL", as_of)


def test_historical_event_and_earnings_provenance_fail_closed(monkeypatch, tmp_path) -> None:
    as_of = datetime(2025, 6, 15, 12, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    events = tmp_path / "events.csv"
    _write_csv(
        events,
        "source_id,published_at,ingested_at,headline,normalized_text,tickers,event_type,sentiment,relevance,horizon,source_url,source",
        "evt,2025-06-14T12:00:00Z,2025-06-14T11:00:00Z,Headline,Headline,AAPL,news,0,1,short,https://e/1,archive",
    )
    monkeypatch.setenv("POINT_IN_TIME_EVENTS_CSV", str(events))
    with pytest.raises(ProviderDataError, match="ingested before publication"):
        provider.get_events(["AAPL"], as_of, 7)

    earnings = tmp_path / "earnings.csv"
    _write_csv(
        earnings,
        "ticker,known_at,earnings_at,source",
        "AAPL,2025-06-01T00:00:00Z,2025-06-20T00:00:00Z,",
    )
    monkeypatch.setenv("POINT_IN_TIME_EARNINGS_CSV", str(earnings))
    with pytest.raises(ProviderDataError, match="source is missing"):
        provider.get_upcoming_earnings_minutes("AAPL", as_of)


def test_static_universe_fallback_and_validation(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    provider = _provider(now)
    monkeypatch.delenv("POINT_IN_TIME_UNIVERSE_CSV", raising=False)
    monkeypatch.setenv("YFINANCE_ALLOW_STATIC_UNIVERSE", "1")
    provider._UNIVERSE_MAP = {"ONE": ["AAPL"]}
    security = provider.get_universe("ONE", now)[0]
    assert security.quality_status == "fallback"
    assert security.fallback_fields == ["universe_membership"]
    assert provider.get_quality_report()["status"] == "blocked"
    with pytest.raises(ProviderDataError, match="Unknown universe"):
        provider.get_universe("UNKNOWN", now)


def test_universe_rows_require_ticker_and_complete_metadata(monkeypatch, tmp_path) -> None:
    as_of = datetime(2025, 6, 15, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    universe = tmp_path / "universe.csv"
    header = "universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source"
    _write_csv(universe, header, "SP500,,2020-01-01T00:00:00Z,,Tech,1,1,feed")
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(universe))
    with pytest.raises(ProviderDataError, match="missing ticker"):
        provider.get_universe("SP500", as_of)

    universe.write_text(
        header + "\nSP500,AAPL,2020-01-01T00:00:00Z,,,0,0,feed\n",
        encoding="utf-8",
    )
    provider._csv_cache.clear()
    with pytest.raises(ProviderDataError, match="metadata incomplete"):
        provider.get_universe("SP500", as_of)
    with pytest.raises(ProviderDataError, match="No point-in-time NASDAQ100"):
        provider.get_universe("NASDAQ100", as_of)


def test_production_point_in_time_universe_requires_full_constituent_coverage(monkeypatch, tmp_path) -> None:
    as_of = datetime(2025, 6, 15, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    universe = tmp_path / "universe.csv"
    _write_csv(
        universe,
        "universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source",
        "SP500,AAPL,2020-01-01T00:00:00Z,,Technology,3000000000000,8,licensed-feed",
    )
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(universe))
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "0")

    with pytest.raises(ProviderDataError, match="coverage is incomplete"):
        provider.get_universe("SP500", as_of)


def test_point_in_time_configuration_validates_required_universes_and_sources(monkeypatch, tmp_path) -> None:
    as_of = datetime(2025, 6, 15, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    universe = tmp_path / "universe.csv"
    _write_csv(
        universe,
        "universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source",
        "SP500,AAPL,2020-01-01T00:00:00Z,,Technology,3000000000000,8,licensed-feed\n"
        "NASDAQ100,MSFT,2020-01-01T00:00:00Z,,Technology,2500000000000,7,nasdaq-index-data",
    )
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(universe))

    report = provider.validate_point_in_time_configuration(as_of)

    assert report["SP500"]["constituent_count"] == 1
    assert report["NASDAQ100"]["sources"] == ["nasdaq-index-data"]


def test_backtest_data_validation_is_mandatory_and_returns_dataset_fingerprint(monkeypatch, tmp_path) -> None:
    provider = _provider(datetime.now(timezone.utc))
    for env_name in (
        "POINT_IN_TIME_UNIVERSE_CSV",
        "POINT_IN_TIME_FUNDAMENTALS_CSV",
        "POINT_IN_TIME_EVENTS_CSV",
        "POINT_IN_TIME_EARNINGS_CSV",
    ):
        monkeypatch.delenv(env_name, raising=False)
    with pytest.raises(ProviderDataError, match="POINT_IN_TIME_UNIVERSE_CSV is required"):
        provider.validate_backtest_data(
            datetime(2026, 1, 5, tzinfo=timezone.utc),
            datetime(2026, 1, 7, tzinfo=timezone.utc),
        )

    universe = tmp_path / "universe.csv"
    fundamentals = tmp_path / "fundamentals.csv"
    events = tmp_path / "events.csv"
    earnings = tmp_path / "earnings.csv"
    _write_csv(
        universe,
        "universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source",
        "SP500,AAPL,2020-01-01T00:00:00Z,,Technology,3000000000000,8,licensed-feed",
    )
    _write_csv(
        fundamentals,
        "ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source",
        "AAPL,2025-09-30T00:00:00Z,2026-01-01T00:00:00Z,20,0.2,0.1,0.01,filing-feed",
    )
    _write_csv(
        events,
        "source_id,published_at,ingested_at,headline,normalized_text,tickers,event_type,sentiment,relevance,horizon,source_url,source",
    )
    _write_csv(earnings, "ticker,known_at,earnings_at,source")
    monkeypatch.setenv("POINT_IN_TIME_REQUIRED_UNIVERSES", "SP500")
    monkeypatch.setenv("POINT_IN_TIME_UNIVERSE_CSV", str(universe))
    monkeypatch.setenv("POINT_IN_TIME_FUNDAMENTALS_CSV", str(fundamentals))
    monkeypatch.setenv("POINT_IN_TIME_EVENTS_CSV", str(events))
    monkeypatch.setenv("POINT_IN_TIME_EARNINGS_CSV", str(earnings))

    report = provider.validate_backtest_data(
        datetime(2026, 1, 5, tzinfo=timezone.utc),
        datetime(2026, 1, 7, tzinfo=timezone.utc),
    )

    assert report["status"] == "verified"
    assert len(report["dataset_fingerprint"]) == 64
    assert provider.get_quality_report()["point_in_time_validation"]["backtest_range"] == report


class _Response:
    def __init__(self, body: str) -> None:
        self.body = body

    def read(self) -> bytes:
        return self.body.encode()


def test_stooq_parser_and_explicit_fallback(monkeypatch) -> None:
    as_of = datetime(2025, 6, 15, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    payload = "\n".join(
        [
            "Date,Open,High,Low,Close,Volume",
            "bad-date,1,2,0,1,100",
            "2025-06-13,10,12,9,11,1000",
            "2025-06-20,11,13,10,12,1200",
        ]
    )
    monkeypatch.setattr(provider_module, "urlopen", lambda *_args, **_kwargs: _Response(payload))
    bars = provider._get_bars_from_stooq("AAPL", as_of, 10)
    assert len(bars) == 1
    assert bars[0].close == 11

    ticker = provider._ticker("AAPL")
    ticker.history = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("vendor down"))
    monkeypatch.setattr(provider_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setenv("ALLOW_STOOQ_FALLBACK", "1")
    fallback = provider.get_bars("AAPL", as_of, 10)
    assert fallback[0].source == "stooq_fallback"
    assert fallback[0].quality_status == "fallback"

    empty_provider = _provider(datetime.now(timezone.utc))
    monkeypatch.setattr(provider_module, "urlopen", lambda *_args, **_kwargs: _Response(""))
    with pytest.raises(ValueError, match="No stooq bars"):
        empty_provider._get_bars_from_stooq("AAPL", as_of, 10)


def test_bar_vendor_failure_requires_explicit_fallback(monkeypatch) -> None:
    as_of = datetime(2025, 6, 15, tzinfo=timezone.utc)
    provider = _provider(datetime.now(timezone.utc))
    provider._ticker("AAPL").history = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("vendor down"))
    monkeypatch.setattr(provider_module.time, "sleep", lambda _seconds: None)
    monkeypatch.delenv("ALLOW_STOOQ_FALLBACK", raising=False)
    with pytest.raises(ProviderUnavailableError):
        provider.get_bars("AAPL", as_of, 10)


def test_earnings_datetime_extraction_variants() -> None:
    now = datetime.now(timezone.utc)
    assert YFinanceProvider._extract_earnings_datetime(None) is None
    assert YFinanceProvider._extract_earnings_datetime({}) is None
    assert YFinanceProvider._extract_earnings_datetime({"Earnings Date": []}) is None
    assert YFinanceProvider._extract_earnings_datetime({"Earnings Date": ["invalid"]}) is None
    assert YFinanceProvider._extract_earnings_datetime({"Earnings Date": [now]}) == now
    naive = datetime(2026, 1, 1)
    assert YFinanceProvider._extract_earnings_datetime({"Earnings Date": naive}).tzinfo == timezone.utc
