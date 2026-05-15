from __future__ import annotations

import csv
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any
from urllib.request import urlopen

from domain.entities.models import FundamentalSnapshot, MarketBar, NewsEvent, SecurityMetadata
from services.ingestion.interfaces import DataProvider


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
            import yfinance as yf  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on optional dependency
            raise RuntimeError(
                "yfinance is not installed. Install optional vendor deps or use DATA_PROVIDER=mock."
            ) from exc
        self.yf = yf
        self._ticker_cache: dict[str, Any] = {}
        self._info_cache: dict[str, dict[str, Any]] = {}
        self._fast_info_cache: dict[str, dict[str, Any]] = {}
        self._history_cache: dict[tuple[str, str, int], list[MarketBar]] = {}

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        name = exc.__class__.__name__.lower()
        text = str(exc).lower()
        return "ratelimit" in name or "rate limit" in text or "too many requests" in text

    def _retry(self, fn, default):
        for attempt in range(3):
            try:
                return fn()
            except Exception as exc:
                if self._is_rate_limited(exc) and attempt < 2:
                    time.sleep(1.0 + attempt)
                    continue
                if attempt < 2:
                    time.sleep(0.2)
                    continue
                return default
        return default

    def _ticker(self, ticker: str):
        key = ticker.upper()
        if key not in self._ticker_cache:
            self._ticker_cache[key] = self.yf.Ticker(key)
        return self._ticker_cache[key]

    def _info(self, ticker: str, refresh: bool = False) -> dict[str, Any]:
        key = ticker.upper()
        if not refresh and key in self._info_cache:
            return self._info_cache[key]
        data = self._retry(lambda: dict(self._ticker(key).info or {}), {})
        self._info_cache[key] = data
        return data

    def _fast_info(self, ticker: str, refresh: bool = False) -> dict[str, Any]:
        key = ticker.upper()
        if not refresh and key in self._fast_info_cache:
            return self._fast_info_cache[key]
        data = self._retry(lambda: dict(self._ticker(key).fast_info or {}), {})
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
        return as_of.astimezone(timezone.utc) <= (now_utc - timedelta(days=2))

    def _get_bars_from_stooq(self, ticker: str, as_of: datetime, lookback_days: int) -> list[MarketBar]:
        symbol = self._stooq_symbol(ticker)
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        payload = self._retry(lambda: urlopen(url, timeout=12).read().decode("utf-8"), "")
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

    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        tickers = self._UNIVERSE_MAP.get(universe.upper(), self._UNIVERSE_MAP["SP500"])
        results: list[SecurityMetadata] = []
        historical_mode = self._is_historical_mode(as_of)
        for ticker in tickers:
            fallback_sector, fallback_market_cap = self._fallback_meta(ticker)
            if historical_mode:
                try:
                    bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=30)
                except Exception:
                    bars = []
                last_price = float(bars[-1].close) if bars else 0.0
                tail = bars[-20:] if len(bars) >= 20 else bars
                avg_vol = (
                    sum(max(0.0, float(bar.volume)) for bar in tail) / len(tail)
                    if tail
                    else 0.0
                )
                avg_dollar_volume = avg_vol * max(last_price, 0.0)
                results.append(
                    SecurityMetadata(
                        ticker=ticker,
                        sector=fallback_sector,
                        market_cap_usd=fallback_market_cap,
                        avg_dollar_volume=avg_dollar_volume,
                        last_price=last_price,
                        spread_bps=25.0,
                    )
                )
                continue

            fast_info = self._fast_info(ticker)
            info = self._info(ticker)

            market_cap = self._to_float(fast_info.get("market_cap") or info.get("marketCap"))
            last_price = self._to_float(
                fast_info.get("last_price")
                or info.get("currentPrice")
                or info.get("regularMarketPrice")
            )
            avg_vol = self._to_float(
                fast_info.get("ten_day_average_volume")
                or fast_info.get("three_month_average_volume")
                or info.get("averageDailyVolume10Day")
                or info.get("averageVolume")
            )

            if last_price <= 0:
                try:
                    bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=2)
                    if bars:
                        last_price = float(bars[-1].close)
                except Exception:
                    last_price = 0.0

            if avg_vol <= 0:
                try:
                    bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=30)
                    if bars:
                        tail = bars[-20:] if len(bars) >= 20 else bars
                        avg_vol = sum(max(0.0, float(bar.volume)) for bar in tail) / len(tail)
                except Exception:
                    avg_vol = 0.0

            if market_cap <= 0:
                market_cap = fallback_market_cap

            bid = self._to_float(fast_info.get("bid") or info.get("bid"))
            ask = self._to_float(fast_info.get("ask") or info.get("ask"))
            if bid > 0 and ask > 0 and ask >= bid:
                mid = (bid + ask) / 2.0
                spread_bps = ((ask - bid) / mid) * 10_000 if mid > 0 else 25.0
            else:
                spread_bps = 25.0

            avg_dollar_volume = avg_vol * max(last_price, 0.0)
            results.append(
                SecurityMetadata(
                    ticker=ticker,
                    sector=str(info.get("sector") or fallback_sector),
                    market_cap_usd=market_cap,
                    avg_dollar_volume=avg_dollar_volume,
                    last_price=last_price,
                    spread_bps=spread_bps,
                )
            )
        return results

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        _ = as_of
        fast_info = self._fast_info(ticker, refresh=True)
        price = self._to_float(fast_info.get("last_price"), default=0.0)
        if price > 0:
            return price

        info = self._info(ticker, refresh=True)
        price = self._to_float(
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose"),
            default=0.0,
        )
        if price > 0:
            return price

        bars = self.get_bars(ticker=ticker, as_of=datetime.now(timezone.utc), lookback_days=2)
        if not bars:
            return None
        return float(bars[-1].close)

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        end = as_of.astimezone(timezone.utc)
        cache_key = (ticker.upper(), end.date().isoformat(), int(lookback_days))
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]

        start = end - timedelta(days=max(lookback_days * 2, 365))
        history = self._retry(
            lambda: self._ticker(ticker).history(
                start=start.date(),
                end=end.date(),
                interval="1d",
                auto_adjust=False,
            ),
            None,
        )
        if history is None or history.empty:
            bars = self._get_bars_from_stooq(ticker=ticker, as_of=as_of, lookback_days=lookback_days)
            self._history_cache[cache_key] = bars
            return bars

        bars: list[MarketBar] = []
        for idx, row in history.tail(lookback_days).iterrows():
            timestamp = idx.to_pydatetime().replace(tzinfo=timezone.utc)
            bars.append(
                MarketBar(
                    ticker=ticker,
                    timestamp=timestamp,
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=float(row["Volume"]),
                )
            )
        self._history_cache[cache_key] = bars
        return bars

    def get_benchmark_bars(self, benchmark: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        return self.get_bars(benchmark, as_of, lookback_days)

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        historical_mode = self._is_historical_mode(as_of)
        info = self._info(ticker)
        pe_ttm = self._to_float(info.get("trailingPE") or info.get("forwardPE"), default=22.0)
        roe = self._to_float(info.get("returnOnEquity"), default=0.16)
        revenue_growth = self._to_float(info.get("revenueGrowth") or info.get("earningsGrowth"), default=0.08)

        # yfinance does not consistently expose 30d EPS revision. Use analyst trend proxy when available.
        eps_revision = 0.0
        try:
            trend = None if historical_mode else self._retry(lambda: self._ticker(ticker).earnings_trend, None)
            if trend is not None and hasattr(trend, "empty") and not trend.empty:
                # proxy: current vs next estimate trend if available
                if "growth" in trend.columns:
                    growth_values = [self._to_float(v) for v in list(trend["growth"].dropna().head(2))]
                    if growth_values:
                        eps_revision = sum(growth_values) / len(growth_values)
        except Exception:
            eps_revision = 0.0

        return FundamentalSnapshot(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc),
            pe_ttm=pe_ttm,
            roe=roe,
            revenue_growth_yoy=revenue_growth,
            eps_revision_30d=eps_revision,
        )

    def get_events(self, tickers: list[str], as_of: datetime, lookback_days: int = 7) -> list[NewsEvent]:
        if self._is_historical_mode(as_of):
            return []
        as_of_utc = as_of.astimezone(timezone.utc)
        cutoff = as_of_utc - timedelta(days=lookback_days)
        events: list[NewsEvent] = []

        for ticker in tickers:
            items = self._retry(lambda: self._ticker(ticker).news or [], [])
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
                    )
                )

        events.sort(key=lambda e: e.published_at, reverse=True)
        return events

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        as_of_utc = as_of.astimezone(timezone.utc)
        calendar_obj = None
        try:
            calendar_obj = self._retry(lambda: self._ticker(ticker).calendar, None)
        except Exception:
            calendar_obj = None

        earnings_dt = self._extract_earnings_datetime(calendar_obj)
        if earnings_dt is None:
            return None

        delta_minutes = int((earnings_dt - as_of_utc).total_seconds() / 60)
        if delta_minutes < 0:
            return None
        return delta_minutes
