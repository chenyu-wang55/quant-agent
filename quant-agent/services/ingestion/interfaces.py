from __future__ import annotations

from datetime import datetime
from typing import Protocol

from domain.entities.models import FundamentalSnapshot, MarketBar, NewsEvent, SecurityMetadata


class DataProvider(Protocol):
    def get_universe(self, universe: str, as_of: datetime) -> list[SecurityMetadata]: ...

    def get_latest_price(self, ticker: str, as_of: datetime) -> float | None: ...

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260) -> list[MarketBar]: ...

    def get_benchmark_bars(
        self, benchmark: str, as_of: datetime, lookback_days: int = 260
    ) -> list[MarketBar]: ...

    def get_fundamentals(self, ticker: str, as_of: datetime) -> FundamentalSnapshot: ...

    def get_events(
        self, tickers: list[str], as_of: datetime, lookback_days: int = 7
    ) -> list[NewsEvent]: ...

    def get_upcoming_earnings_minutes(self, ticker: str, as_of: datetime) -> int | None: ...
