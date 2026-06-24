from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone

from domain.entities.models import FundamentalSnapshot, MarketBar, NewsEvent, SecurityMetadata


def _seed(*parts: str) -> int:
    value = "|".join(parts).encode("utf-8")
    digest = hashlib.sha256(value).hexdigest()
    return int(digest[:16], 16)


def _rng(*parts: str) -> random.Random:
    return random.Random(_seed(*parts))


def _business_days(as_of: datetime, count: int) -> list[datetime]:
    as_of_utc = as_of.astimezone(timezone.utc)
    current = as_of_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    days: list[datetime] = []
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current -= timedelta(days=1)
    return list(reversed(days))


class MockMarketDataProvider:
    """Deterministic, point-in-time friendly mock provider for MVP and tests."""

    _BASE_UNIVERSE: list[tuple[str, str, float]] = [
        ("AAPL", "Technology", 2_900_000_000_000),
        ("MSFT", "Technology", 3_100_000_000_000),
        ("NVDA", "Technology", 2_500_000_000_000),
        ("AMZN", "Consumer Discretionary", 1_900_000_000_000),
        ("GOOGL", "Communication Services", 2_100_000_000_000),
        ("META", "Communication Services", 1_400_000_000_000),
        ("AVGO", "Technology", 900_000_000_000),
        ("LLY", "Health Care", 700_000_000_000),
        ("JPM", "Financials", 650_000_000_000),
        ("XOM", "Energy", 500_000_000_000),
        ("UNH", "Health Care", 600_000_000_000),
        ("COST", "Consumer Staples", 400_000_000_000),
        ("PG", "Consumer Staples", 360_000_000_000),
        ("HD", "Consumer Discretionary", 350_000_000_000),
        ("CRM", "Technology", 330_000_000_000),
        ("NFLX", "Communication Services", 300_000_000_000),
        ("AMD", "Technology", 280_000_000_000),
        ("ADBE", "Technology", 260_000_000_000),
        ("PFE", "Health Care", 180_000_000_000),
        ("BA", "Industrials", 140_000_000_000),
    ]

    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]:
        _ = universe  # Kept for provider interchangeability.
        day_key = as_of.date().isoformat()
        result: list[SecurityMetadata] = []

        for ticker, sector, market_cap in self._BASE_UNIVERSE:
            rng = _rng("universe", day_key, ticker)
            latest_price = self.get_latest_price(ticker=ticker, as_of=as_of)
            last_price = round(float(latest_price), 2) if latest_price is not None else round(20 + rng.random() * 600, 2)
            avg_dollar_volume = round(8_000_000 + rng.random() * 180_000_000, 2)
            spread_bps = round(4 + rng.random() * 60, 2)
            result.append(
                SecurityMetadata(
                    ticker=ticker,
                    sector=sector,
                    market_cap_usd=market_cap,
                    avg_dollar_volume=avg_dollar_volume,
                    last_price=last_price,
                    spread_bps=spread_bps,
                )
            )

        return result

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None:
        bars = self.get_bars(ticker=ticker, as_of=as_of, lookback_days=2)
        if not bars:
            return None
        return float(bars[-1].close)

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]:
        days = _business_days(as_of, lookback_days)
        day_key = as_of.date().isoformat()
        rng = _rng("bars", ticker, day_key)
        base_price = 30 + (_seed("base", ticker) % 500)
        drift = ((int(_seed("drift", ticker) % 200) - 100) / 10000.0) + 0.0002
        vol = 0.012 + ((_seed("vol", ticker) % 50) / 10_000.0)
        price = float(base_price)
        avg_daily_dollar = 25_000_000 + (_seed("adv", ticker) % 130_000_000)

        bars: list[MarketBar] = []
        for day in days:
            overnight = rng.gauss(0, vol / 3)
            open_px = max(1.0, price * (1 + overnight))
            intraday = drift + rng.gauss(0, vol)
            close_px = max(1.0, open_px * (1 + intraday))
            wick = abs(rng.gauss(0, vol / 2))
            high_px = max(open_px, close_px) * (1 + wick)
            low_px = min(open_px, close_px) * max(0.7, 1 - wick)
            volume = max(100_000.0, avg_daily_dollar / max(close_px, 1.0))

            bars.append(
                MarketBar(
                    ticker=ticker,
                    timestamp=day,
                    open=round(open_px, 4),
                    high=round(high_px, 4),
                    low=round(low_px, 4),
                    close=round(close_px, 4),
                    volume=round(volume, 2),
                )
            )
            price = close_px

        return bars

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]:
        return self.get_bars(benchmark, as_of, lookback_days)

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot:
        day_key = as_of.date().isoformat()
        rng = _rng("fundamentals", ticker, day_key)
        pe_ttm = round(10 + rng.random() * 45, 2)
        roe = round(0.05 + rng.random() * 0.45, 4)
        revenue_growth = round(-0.10 + rng.random() * 0.55, 4)
        eps_revision = round(-0.20 + rng.random() * 0.40, 4)
        return FundamentalSnapshot(
            ticker=ticker,
            timestamp=as_of.astimezone(timezone.utc),
            pe_ttm=pe_ttm,
            roe=roe,
            revenue_growth_yoy=revenue_growth,
            eps_revision_30d=eps_revision,
        )

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]:
        events: list[NewsEvent] = []
        day_key = as_of.date().isoformat()
        for ticker in tickers:
            rng = _rng("events", ticker, day_key)
            n_events = int(rng.random() * 3)
            for idx in range(n_events):
                published_at = as_of.astimezone(timezone.utc) - timedelta(
                    hours=6 + int(rng.random() * 24 * lookback_days)
                )
                ingested_at = published_at + timedelta(minutes=5)
                sentiment = round(-1 + rng.random() * 2, 4)
                relevance = round(0.2 + rng.random() * 0.8, 4)
                event_type = ["earnings", "guidance", "macro", "filing", "news"][
                    int(rng.random() * 5)
                ]
                events.append(
                    NewsEvent(
                        source_id=f"{ticker}-{day_key}-{idx}",
                        published_at=published_at,
                        ingested_at=ingested_at,
                        headline=f"{ticker} {event_type} update {idx + 1}",
                        normalized_text=f"Deterministic mock event for {ticker}",
                        tickers=[ticker],
                        event_type=event_type,
                        sentiment=sentiment,
                        relevance=relevance,
                        horizon="short",
                        source_url=f"https://example.com/{ticker}/{day_key}/{idx}",
                    )
                )
        return events

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None:
        day_key = as_of.date().isoformat()
        rng = _rng("earnings", ticker, day_key)
        days_to_event = int(rng.random() * 15) - 2
        if days_to_event < 0:
            return None
        return days_to_event * 24 * 60 + int(rng.random() * 240)
