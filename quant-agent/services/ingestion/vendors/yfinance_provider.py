from __future__ import annotations

import csv
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from domain.entities.models import FundamentalSnapshot, MarketBar, NewsEvent, SecurityMetadata
from infra.config import env_flag, env_int, env_text
from services.ingestion.interfaces import DataProvider
from services.ingestion.point_in_time_validator import (
    PointInTimeDatasetPaths,
    PointInTimeValidationError,
    parse_zoned_timestamp,
    validate_point_in_time_dataset,
)


class ProviderUnavailableError(RuntimeError):
    pass


class ProviderDataError(ValueError):
    pass


class YFinanceProvider(DataProvider):
    """Lightweight vendor adapter.

    Scope:
    - Universe metadata uses static ticker lists plus live metadata from yfinance.
    - Bars/fundamentals/news/earnings calendar come from yfinance.
    - Sentiment/relevance labels are deterministic derived fields from headlines.
    """

    _UNIVERSE_MAP: dict[str, list[str]] = {
        "SP500": [
            "AAPL",
            "MSFT",
            "NVDA",
            "AMZN",
            "GOOGL",
            "META",
            "AVGO",
            "LLY",
            "JPM",
            "XOM",
            "UNH",
            "COST",
            "PG",
            "HD",
            "CRM",
            "NFLX",
            "AMD",
            "ADBE",
            "PFE",
            "BA",
        ],
        "NASDAQ100": [
            "AAPL",
            "MSFT",
            "NVDA",
            "AMZN",
            "GOOGL",
            "META",
            "AVGO",
            "AMD",
            "NFLX",
            "ADBE",
            "COST",
            "CRM",
            "PEP",
            "CSCO",
            "INTU",
        ],
    }

    _FALLBACK_METADATA: dict[str, tuple[str, float]] = {
        "AAPL": ("Technology", 2_900_000_000_000),
        "MSFT": ("Technology", 3_100_000_000_000),
        "NVDA": ("Technology", 2_500_000_000_000),
        "AMZN": ("Consumer Discretionary", 1_900_000_000_000),
        "GOOGL": ("Communication Services", 2_100_000_000_000),
        "META": ("Communication Services", 1_400_000_000_000),
        "AVGO": ("Technology", 900_000_000_000),
        "LLY": ("Health Care", 700_000_000_000),
        "JPM": ("Financials", 650_000_000_000),
        "XOM": ("Energy", 500_000_000_000),
        "UNH": ("Health Care", 600_000_000_000),
        "COST": ("Consumer Staples", 400_000_000_000),
        "PG": ("Consumer Staples", 360_000_000_000),
        "HD": ("Consumer Discretionary", 350_000_000_000),
        "CRM": ("Technology", 330_000_000_000),
        "AMD": ("Technology", 280_000_000_000),
        "ADBE": ("Technology", 260_000_000_000),
        "PFE": ("Health Care", 180_000_000_000),
        "BA": ("Industrials", 140_000_000_000),
        "NFLX": ("Communication Services", 300_000_000_000),
        "PEP": ("Consumer Staples", 240_000_000_000),
        "CSCO": ("Technology", 200_000_000_000),
        "INTU": ("Technology", 170_000_000_000),
        "SPY": ("ETF", 500_000_000_000),
    }

    def __init__(self) -> None:
        try:
            import yfinance as yf
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            raise RuntimeError(
                "yfinance is not installed. Install optional vendor deps or use DATA_PROVIDER=mock."
            ) from exc
        self.yf = yf
        self._ticker_cache: dict[str, Any] = {}
        self._info_cache: dict[str, dict[str, Any]] = {}
        self._fast_info_cache: dict[str, dict[str, Any]] = {}
        self._history_cache: dict[tuple[str, str, int], list[MarketBar]] = {}
        self._csv_cache: dict[str, list[dict[str, str]]] = {}
        self._quality_issues: list[str] = []
        self._fallback_fields: list[str] = []
        self._point_in_time_validation: dict[str, object] = {}

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        name = exc.__class__.__name__.lower()
        text = str(exc).lower()
        return "ratelimit" in name or "rate limit" in text or "too many requests" in text

    def _retry(self, fn, operation: str):
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                return fn()
            except Exception as exc:
                last_error = exc
                if self._is_rate_limited(exc) and attempt < 2:
                    time.sleep(1.0 + attempt)
                    continue
                if attempt < 2:
                    time.sleep(0.2)
                    continue
                break
        raise ProviderUnavailableError(f"{operation} failed after 3 attempts: {last_error}") from last_error

    def _quality_issue(self, issue: str) -> None:
        if issue not in self._quality_issues:
            self._quality_issues.append(issue)

    def _fallback(self, field: str) -> None:
        if field not in self._fallback_fields:
            self._fallback_fields.append(field)

    def get_quality_report(self) -> dict[str, object]:
        blocked = bool(self._quality_issues or self._fallback_fields)
        return {
            "status": "blocked" if blocked else "verified",
            "issues": list(self._quality_issues),
            "failures": [],
            "fallback_fields": list(self._fallback_fields),
            "point_in_time_validation": dict(self._point_in_time_validation),
        }

    def _csv_rows(self, env_name: str) -> list[dict[str, str]]:
        path_value = env_text(env_name)
        if not path_value:
            raise ProviderDataError(f"{env_name} is required for point-in-time data")
        path = Path(path_value).expanduser().resolve()
        cache_key = str(path)
        if cache_key not in self._csv_cache:
            if not path.is_file():
                raise ProviderDataError(f"point-in-time data file not found: {path}")
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                self._csv_cache[cache_key] = [dict(row) for row in csv.DictReader(handle)]
        return self._csv_cache[cache_key]

    @staticmethod
    def _parse_utc(value: str, *, end_of_time: bool = False) -> datetime:
        try:
            return parse_zoned_timestamp(
                value,
                field="point-in-time timestamp",
                allow_blank=end_of_time,
            )
        except PointInTimeValidationError as exc:
            raise ProviderDataError(str(exc)) from exc

    def _ticker(self, ticker: str):
        key = ticker.upper()
        if key not in self._ticker_cache:
            self._ticker_cache[key] = self.yf.Ticker(key)
        return self._ticker_cache[key]

    def _info(self, ticker: str, refresh: bool = False) -> dict[str, Any]:
        key = ticker.upper()
        if not refresh and key in self._info_cache:
            return self._info_cache[key]
        data = self._retry(lambda: dict(self._ticker(key).info or {}), f"yfinance info {key}")
        self._info_cache[key] = data
        return data

    def _fast_info(self, ticker: str, refresh: bool = False) -> dict[str, Any]:
        key = ticker.upper()
        if not refresh and key in self._fast_info_cache:
            return self._fast_info_cache[key]
        data = self._retry(lambda: dict(self._ticker(key).fast_info or {}), f"yfinance fast_info {key}")
        self._fast_info_cache[key] = data
        return data

    def _fallback_meta(self, ticker: str) -> tuple[str, float]:
        return self._FALLBACK_METADATA.get(ticker.upper(), ("Unknown", 50_000_000_000.0))

    def _stooq_symbol(self, ticker: str) -> str:
        symbol = ticker.lower().replace("^", "")
        if "." not in symbol:
            symbol = f"{symbol}.us"
        return symbol

    @staticmethod
    def _is_historical_mode(as_of: datetime) -> bool:
        now_utc = datetime.now(timezone.utc)
        return as_of.astimezone(timezone.utc) < (now_utc - timedelta(minutes=5))

    def _get_bars_from_stooq(self, ticker: str, as_of: datetime, lookback_days: int) -> list[MarketBar]:
        symbol = self._stooq_symbol(ticker)
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        payload = self._retry(
            lambda: urlopen(url, timeout=12).read().decode("utf-8"),
            f"stooq history {ticker}",
        )
        if not payload.strip():
            raise ValueError(f"No stooq bars for {ticker}")

        rows: list[MarketBar] = []
        end = as_of.astimezone(timezone.utc)
        reader = csv.DictReader(StringIO(payload))
        for row in reader:
            try:
                date_text = str(row.get("Date") or "")
                if not date_text:
                    continue
                ts = datetime.fromisoformat(date_text).replace(tzinfo=timezone.utc)
                if ts > end:
                    continue
                open_px = float(row["Open"])
                high_px = float(row["High"])
                low_px = float(row["Low"])
                close_px = float(row["Close"])
                volume = float(row.get("Volume") or 0.0)
                rows.append(
                    MarketBar(
                        ticker=ticker,
                        timestamp=ts,
                        open=open_px,
                        high=high_px,
                        low=low_px,
                        close=close_px,
                        volume=volume,
                    )
                )
            except Exception:
                continue

        if not rows:
            raise ValueError(f"No stooq bars for {ticker}")
        return rows[-lookback_days:]

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _headline_sentiment(text: str) -> float:
        lower = text.lower()
        positive = ["beat", "surge", "growth", "upgrade", "strong", "record", "outperform", "raise"]
        negative = ["miss", "drop", "downgrade", "weak", "cut", "lawsuit", "warning", "delay"]
        score = 0.0
        for word in positive:
            if word in lower:
                score += 0.2
        for word in negative:
            if word in lower:
                score -= 0.2
        return max(-1.0, min(1.0, score))

    @staticmethod
    def _extract_earnings_datetime(calendar_obj: Any) -> datetime | None:
        if calendar_obj is None:
            return None

        value: Any = None
        # DataFrame-like case
        if hasattr(calendar_obj, "index") and hasattr(calendar_obj, "loc"):
            try:
                if "Earnings Date" in list(calendar_obj.index):
                    row = calendar_obj.loc["Earnings Date"]
                    value = row.iloc[0] if hasattr(row, "iloc") else row
            except Exception:
                value = None

        # dict-like case
        if value is None and isinstance(calendar_obj, dict):
            value = calendar_obj.get("Earnings Date")

        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
        if value is None:
            return None

        if hasattr(value, "to_pydatetime"):
            dt = value.to_pydatetime()
        elif isinstance(value, datetime):
            dt = value
        else:
            try:
                dt = datetime.fromisoformat(str(value))
            except Exception:
                return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _point_in_time_memberships(self, universe: str, as_of: datetime) -> list[dict[str, str]]:
        rows = self._csv_rows("POINT_IN_TIME_UNIVERSE_CSV")
        as_of_utc = as_of.astimezone(timezone.utc)
        matches = [
            row
            for row in rows
            if str(row.get("universe") or "").upper() == universe.upper()
            and self._parse_utc(str(row.get("effective_from") or ""))
            <= as_of_utc
            < self._parse_utc(str(row.get("effective_to") or ""), end_of_time=True)
        ]
        if not matches:
            raise ProviderDataError(f"No point-in-time {universe} constituents available at {as_of_utc.isoformat()}")
        normalized_universe = universe.upper()
        tickers = [str(row.get("ticker") or "").upper().strip() for row in matches]
        duplicates = sorted({ticker for ticker in tickers if ticker and tickers.count(ticker) > 1})
        if duplicates:
            raise ProviderDataError(
                f"Overlapping point-in-time {normalized_universe} memberships: " + ", ".join(duplicates)
            )
        default_minimums = {"SP500": 450, "NASDAQ100": 90}
        default_minimum = 1 if env_flag("QUANT_AGENT_TEST_MODE") else default_minimums.get(normalized_universe, 1)
        minimum = env_int(
            f"POINT_IN_TIME_MIN_{normalized_universe}_CONSTITUENTS",
            default_minimum,
            minimum=1,
        )
        if len(matches) < minimum:
            raise ProviderDataError(
                f"Point-in-time {normalized_universe} constituent coverage is incomplete: "
                f"found {len(matches)}, require at least {minimum}"
            )
        return matches

    @staticmethod
    def _validate_membership_metadata(
        universe: str,
        memberships: list[dict[str, str]],
    ) -> dict[str, object]:
        sources: set[str] = set()
        for row in memberships:
            ticker = str(row.get("ticker") or "").upper().strip()
            if not ticker:
                raise ProviderDataError(f"point-in-time {universe} row is missing ticker")
            sector = str(row.get("sector") or "").strip()
            source = str(row.get("source") or "").strip()
            try:
                market_cap = float(row.get("market_cap_usd") or 0)
                spread_bps = float(row.get("spread_bps") or 0)
            except (TypeError, ValueError) as exc:
                raise ProviderDataError(f"point-in-time metadata invalid for {ticker}") from exc
            if not sector or market_cap <= 0 or spread_bps <= 0 or not source:
                raise ProviderDataError(
                    f"point-in-time metadata incomplete for {ticker}: "
                    "sector, market_cap_usd, spread_bps, and source are required"
                )
            sources.add(source)
        return {
            "constituent_count": len(memberships),
            "sources": sorted(sources),
            "status": "verified",
        }

    def validate_point_in_time_configuration(self, as_of: datetime) -> dict[str, object]:
        configured = env_text("POINT_IN_TIME_REQUIRED_UNIVERSES", "SP500,NASDAQ100")
        universes = [item.strip().upper() for item in configured.split(",") if item.strip()]
        if not universes:
            raise ProviderDataError("POINT_IN_TIME_REQUIRED_UNIVERSES cannot be empty")
        report: dict[str, object] = {}
        for universe in universes:
            memberships = self._point_in_time_memberships(universe, as_of)
            report[universe] = self._validate_membership_metadata(universe, memberships)
        self._point_in_time_validation = report
        return report

    def validate_backtest_data(self, start: datetime, end: datetime) -> dict[str, Any]:
        configured = env_text("POINT_IN_TIME_REQUIRED_UNIVERSES", "SP500,NASDAQ100")
        universes = [item.strip().upper() for item in configured.split(",") if item.strip()]
        if not universes:
            raise ProviderDataError("POINT_IN_TIME_REQUIRED_UNIVERSES cannot be empty")
        default_minimums = {"SP500": 450, "NASDAQ100": 90}
        required_universes = {
            universe: env_int(
                f"POINT_IN_TIME_MIN_{universe}_CONSTITUENTS",
                1 if env_flag("QUANT_AGENT_TEST_MODE") else default_minimums.get(universe, 1),
                minimum=1,
            )
            for universe in universes
        }
        path_values: dict[str, str] = {}
        for env_name in (
            "POINT_IN_TIME_UNIVERSE_CSV",
            "POINT_IN_TIME_FUNDAMENTALS_CSV",
            "POINT_IN_TIME_EVENTS_CSV",
            "POINT_IN_TIME_EARNINGS_CSV",
        ):
            value = env_text(env_name)
            if not value:
                raise ProviderDataError(f"{env_name} is required for point-in-time backtests")
            path_values[env_name] = value
        try:
            report = validate_point_in_time_dataset(
                paths=PointInTimeDatasetPaths(
                    universe=Path(path_values["POINT_IN_TIME_UNIVERSE_CSV"]),
                    fundamentals=Path(path_values["POINT_IN_TIME_FUNDAMENTALS_CSV"]),
                    events=Path(path_values["POINT_IN_TIME_EVENTS_CSV"]),
                    earnings=Path(path_values["POINT_IN_TIME_EARNINGS_CSV"]),
                ),
                start=start,
                end=end,
                required_universes=required_universes,
                max_fundamental_age_days=env_int(
                    "POINT_IN_TIME_MAX_FUNDAMENTAL_AGE_DAYS",
                    550,
                    minimum=1,
                ),
            )
        except PointInTimeValidationError as exc:
            raise ProviderDataError(f"point-in-time backtest dataset validation failed: {exc}") from exc
        self._point_in_time_validation["backtest_range"] = report
        return report

    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        try:
            memberships = self._point_in_time_memberships(universe, as_of)
        except ProviderDataError:
            allow_static = env_flag("YFINANCE_ALLOW_STATIC_UNIVERSE")
            if not allow_static:
                raise
            tickers = self._UNIVERSE_MAP.get(universe.upper())
            if not tickers:
                raise ProviderDataError(f"Unknown universe: {universe}")
            self._quality_issue("static_universe_membership_not_point_in_time")
            self._fallback("universe_membership")
            memberships = [
                {
                    "ticker": ticker,
                    "sector": self._fallback_meta(ticker)[0],
                    "market_cap_usd": str(self._fallback_meta(ticker)[1]),
                    "spread_bps": "25",
                    "source": "static_universe_fallback",
                }
                for ticker in tickers
            ]

        self._point_in_time_validation[universe.upper()] = self._validate_membership_metadata(
            universe.upper(), memberships
        )
        results: list[SecurityMetadata] = []
        for row in memberships:
            ticker = str(row.get("ticker") or "").upper()
            if not ticker:
                raise ProviderDataError("Point-in-time universe row is missing ticker")
            sector = str(row.get("sector") or "").strip()
            market_cap = self._to_float(row.get("market_cap_usd"), default=0.0)
            spread_bps = self._to_float(row.get("spread_bps"), default=0.0)
            if not sector or market_cap <= 0 or spread_bps <= 0:
                raise ProviderDataError(
                    f"Point-in-time universe metadata incomplete for {ticker}; "
                    "sector, market_cap_usd and spread_bps are required"
                )
            bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=30)
            if not bars:
                raise ProviderDataError(f"No point-in-time bars for universe member {ticker}")
            last_price = float(bars[-1].close)
            tail = bars[-20:]
            avg_dollar_volume = sum(max(0.0, float(bar.volume) * float(bar.close)) for bar in tail) / len(tail)
            source = str(row.get("source") or "").strip()
            quality_status = "fallback" if source == "static_universe_fallback" else "verified"
            results.append(
                SecurityMetadata(
                    ticker=ticker,
                    sector=sector,
                    market_cap_usd=market_cap,
                    avg_dollar_volume=avg_dollar_volume,
                    last_price=last_price,
                    spread_bps=spread_bps,
                    as_of=as_of.astimezone(timezone.utc),
                    source=source,
                    quality_status=quality_status,
                    fallback_fields=(["universe_membership"] if quality_status == "fallback" else []),
                )
            )
        return results

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        if self._is_historical_mode(as_of):
            bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=2)
            return float(bars[-1].close) if bars else None
        fast_info = self._fast_info(ticker, refresh=True)
        price = self._to_float(fast_info.get("last_price"), default=0.0)
        if price > 0:
            return price

        info = self._info(ticker, refresh=True)
        price = self._to_float(
            info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose"),
            default=0.0,
        )
        if price > 0:
            return price

        bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=2)
        if not bars:
            return None
        return float(bars[-1].close)

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        end = as_of.astimezone(timezone.utc)
        cache_key = (ticker.upper(), end.date().isoformat(), int(lookback_days))
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]

        start = end - timedelta(days=max(lookback_days * 2, 365))
        try:
            history = self._retry(
                lambda: self._ticker(ticker).history(
                    start=start.date(),
                    end=end.date(),
                    interval="1d",
                    auto_adjust=False,
                ),
                f"yfinance history {ticker}",
            )
        except ProviderUnavailableError:
            allow_stooq = env_flag("ALLOW_STOOQ_FALLBACK")
            if not allow_stooq:
                raise
            self._quality_issue(f"stooq_bar_fallback:{ticker.upper()}")
            self._fallback(f"bars:{ticker.upper()}")
            fallback_bars = self._get_bars_from_stooq(ticker=ticker, as_of=as_of, lookback_days=lookback_days)
            fallback_bars = [
                bar.model_copy(update={"source": "stooq_fallback", "quality_status": "fallback"})
                for bar in fallback_bars
            ]
            self._history_cache[cache_key] = fallback_bars
            return fallback_bars
        if history is None or history.empty:
            raise ProviderDataError(f"yfinance returned no bars for {ticker}")

        bars: list[MarketBar] = []
        for idx, row in history.iterrows():
            timestamp = idx.to_pydatetime()
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)
            if timestamp > end:
                continue
            bars.append(
                MarketBar(
                    ticker=ticker,
                    timestamp=timestamp,
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=float(row["Volume"]),
                    adjusted_close=(
                        float(row["Adj Close"]) if "Adj Close" in row and row["Adj Close"] is not None else None
                    ),
                    dividend=self._to_float(row.get("Dividends"), default=0.0),
                    split_factor=self._to_float(row.get("Stock Splits"), default=0.0),
                    source="yfinance_history",
                    quality_status="verified",
                )
            )
        bars = bars[-lookback_days:]
        if not bars:
            raise ProviderDataError(f"yfinance returned no as-of bars for {ticker}")
        self._history_cache[cache_key] = bars
        return bars

    def get_benchmark_bars(self, benchmark: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        return self.get_bars(benchmark, as_of, lookback_days)

    def _point_in_time_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        as_of_utc = as_of.astimezone(timezone.utc)
        rows = [
            row
            for row in self._csv_rows("POINT_IN_TIME_FUNDAMENTALS_CSV")
            if str(row.get("ticker") or "").upper() == ticker.upper()
            and self._parse_utc(str(row.get("available_at") or "")) <= as_of_utc
        ]
        if not rows:
            raise ProviderDataError(f"No point-in-time fundamentals for {ticker} at {as_of_utc.isoformat()}")
        row = max(rows, key=lambda item: self._parse_utc(str(item.get("available_at") or "")))
        period_end = self._parse_utc(str(row.get("period_end") or ""))
        available_at = self._parse_utc(str(row.get("available_at") or ""))
        if period_end > available_at:
            raise ProviderDataError(f"Point-in-time fundamentals period_end is after available_at for {ticker}")
        source = str(row.get("source") or "").strip()
        if not source:
            raise ProviderDataError(f"Point-in-time fundamentals source is missing for {ticker}")
        values = {
            field: self._to_float(row.get(field), default=float("nan"))
            for field in ("pe_ttm", "roe", "revenue_growth_yoy", "eps_revision_30d")
        }
        if any(value != value for value in values.values()):
            raise ProviderDataError(f"Point-in-time fundamentals are incomplete for {ticker}")
        return FundamentalSnapshot(
            ticker=ticker.upper(),
            timestamp=available_at,
            period_end=period_end,
            available_at=available_at,
            source=source,
            quality_status="verified",
            **values,
        )

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        historical_mode = self._is_historical_mode(as_of)
        if historical_mode:
            return self._point_in_time_fundamentals(ticker, as_of)
        info = self._info(ticker)
        pe_value = info.get("trailingPE")
        roe_value = info.get("returnOnEquity")
        revenue_growth_value = info.get("revenueGrowth")
        raw_values = {
            "pe_ttm": pe_value,
            "roe": roe_value,
            "revenue_growth_yoy": revenue_growth_value,
        }
        missing = [field for field, value in raw_values.items() if value is None]
        if missing:
            raise ProviderDataError(f"yfinance fundamentals missing for {ticker}: {', '.join(missing)}")
        assert pe_value is not None and roe_value is not None and revenue_growth_value is not None
        pe_ttm = float(pe_value)
        roe = float(roe_value)
        revenue_growth = float(revenue_growth_value)

        # yfinance does not consistently expose 30d EPS revision. Use analyst trend proxy when available.
        eps_revision = 0.0
        eps_revision_fallback = False
        try:
            trend = self._retry(
                lambda: self._ticker(ticker).earnings_trend,
                f"yfinance earnings_trend {ticker}",
            )
            if trend is not None and hasattr(trend, "empty") and not trend.empty:
                # proxy: current vs next estimate trend if available
                if "growth" in trend.columns:
                    growth_values = [self._to_float(v) for v in list(trend["growth"].dropna().head(2))]
                    if growth_values:
                        eps_revision = sum(growth_values) / len(growth_values)
        except Exception as exc:
            self._quality_issue(f"eps_revision_unavailable:{ticker.upper()}:{type(exc).__name__}")
            self._fallback(f"fundamentals:{ticker.upper()}:eps_revision_30d")
            eps_revision = 0.0
            eps_revision_fallback = True

        return FundamentalSnapshot(
            ticker=ticker,
            timestamp=as_of.astimezone(timezone.utc),
            pe_ttm=pe_ttm,
            roe=roe,
            revenue_growth_yoy=revenue_growth,
            eps_revision_30d=eps_revision,
            period_end=None,
            available_at=as_of.astimezone(timezone.utc),
            source="yfinance_current_info",
            quality_status=("fallback" if eps_revision_fallback else "verified"),
            fallback_fields=(["eps_revision_30d"] if eps_revision_fallback else []),
        )

    def _point_in_time_events(self, tickers: list[str], as_of: datetime, lookback_days: int) -> list[NewsEvent]:
        ticker_set = {ticker.upper() for ticker in tickers}
        as_of_utc = as_of.astimezone(timezone.utc)
        cutoff = as_of_utc - timedelta(days=lookback_days)
        events: list[NewsEvent] = []
        for row in self._csv_rows("POINT_IN_TIME_EVENTS_CSV"):
            row_tickers = {
                ticker.strip().upper()
                for ticker in str(row.get("tickers") or "").replace(",", ";").split(";")
                if ticker.strip()
            }
            if ticker_set and not ticker_set.intersection(row_tickers):
                continue
            published_at = self._parse_utc(str(row.get("published_at") or ""))
            ingested_at = self._parse_utc(str(row.get("ingested_at") or ""))
            source_id = str(row.get("source_id") or "").strip()
            source = str(row.get("source") or "").strip()
            if not source_id or not source:
                raise ProviderDataError("Point-in-time event source_id and source are required")
            if ingested_at < published_at:
                raise ProviderDataError(f"Point-in-time event {source_id} was ingested before publication")
            if not (cutoff <= published_at <= as_of_utc and ingested_at <= as_of_utc):
                continue
            events.append(
                NewsEvent(
                    source_id=source_id,
                    published_at=published_at,
                    ingested_at=ingested_at,
                    headline=str(row.get("headline") or ""),
                    normalized_text=str(row.get("normalized_text") or row.get("headline") or ""),
                    tickers=sorted(row_tickers),
                    event_type=str(row.get("event_type") or "news"),
                    sentiment=self._to_float(row.get("sentiment"), default=0.0),
                    relevance=self._to_float(row.get("relevance"), default=0.0),
                    horizon=str(row.get("horizon") or "short"),
                    source_url=str(row.get("source_url") or ""),
                    source=source,
                    quality_status="verified",
                )
            )
        return sorted(events, key=lambda event: event.published_at, reverse=True)

    def get_events(self, tickers: list[str], as_of: datetime, lookback_days: int = 7) -> list[NewsEvent]:
        if self._is_historical_mode(as_of):
            return self._point_in_time_events(tickers, as_of, lookback_days)
        as_of_utc = as_of.astimezone(timezone.utc)
        cutoff = as_of_utc - timedelta(days=lookback_days)
        events: list[NewsEvent] = []

        for ticker in tickers:
            items = self._retry(lambda: self._ticker(ticker).news or [], f"yfinance news {ticker}")
            for idx, item in enumerate(items):
                ts = item.get("providerPublishTime")
                if ts is None:
                    continue
                try:
                    published_at = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                except Exception:
                    continue
                if published_at > as_of_utc:
                    continue
                if published_at < cutoff:
                    continue

                title = str(item.get("title") or "")
                summary = str(item.get("summary") or title)
                related = item.get("relatedTickers") or [ticker]
                source_url = item.get("link")
                if source_url is None and isinstance(item.get("canonicalUrl"), dict):
                    source_url = item["canonicalUrl"].get("url")
                source_url = str(source_url or "")

                sentiment = self._headline_sentiment(title)
                relevance = 1.0 if ticker in related else 0.7
                source_id = str(item.get("uuid") or f"{ticker}-{int(ts)}-{idx}")

                events.append(
                    NewsEvent(
                        source_id=source_id,
                        published_at=published_at,
                        ingested_at=as_of_utc,
                        headline=title,
                        normalized_text=summary,
                        tickers=[str(t).upper() for t in related if t],
                        event_type=str(item.get("type") or "news"),
                        sentiment=sentiment,
                        relevance=relevance,
                        horizon="short",
                        source_url=source_url,
                        source="yfinance_news",
                        quality_status="verified",
                    )
                )

        events.sort(key=lambda e: e.published_at, reverse=True)
        return events

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        as_of_utc = as_of.astimezone(timezone.utc)
        if self._is_historical_mode(as_of):
            rows = [
                row
                for row in self._csv_rows("POINT_IN_TIME_EARNINGS_CSV")
                if str(row.get("ticker") or "").upper() == ticker.upper()
                and self._parse_utc(str(row.get("known_at") or "")) <= as_of_utc
            ]
            if not rows:
                return None
            row = max(
                rows,
                key=lambda item: self._parse_utc(str(item.get("known_at") or "")),
            )
            source = str(row.get("source") or "").strip()
            if not source:
                raise ProviderDataError(f"Point-in-time earnings source is missing for {ticker}")
            known_at = self._parse_utc(str(row.get("known_at") or ""))
            historical_earnings_dt = self._parse_utc(str(row.get("earnings_at") or ""))
            if known_at > historical_earnings_dt:
                raise ProviderDataError(f"Point-in-time earnings known_at is after earnings_at for {ticker}")
            delta_minutes = int((historical_earnings_dt - as_of_utc).total_seconds() / 60)
            return delta_minutes if delta_minutes >= 0 else None
        calendar_obj = self._retry(lambda: self._ticker(ticker).calendar, f"yfinance calendar {ticker}")

        earnings_dt = self._extract_earnings_datetime(calendar_obj)
        if earnings_dt is None:
            return None

        delta_minutes = int((earnings_dt - as_of_utc).total_seconds() / 60)
        if delta_minutes < 0:
            return None
        return delta_minutes
