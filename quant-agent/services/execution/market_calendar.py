from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
from exchange_calendars.errors import DateOutOfBounds

_EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class ExchangeSession:
    trading_date: date
    is_trading_day: bool
    close_time: str = "16:00"
    is_early_close: bool = False
    holiday_name: str | None = None
    calendar: str = "XNYS"


@lru_cache(maxsize=1)
def _xnys_calendar():
    # A fixed broad range keeps results deterministic within a process and covers
    # historical research as well as future scheduling. Dates outside this range
    # fail closed below instead of falling back to hand-written holiday rules.
    return xcals.get_calendar("XNYS", start="1990-01-01", end="2050-12-31")


def _regular_holiday_name(trading_date: date) -> str | None:
    calendar = _xnys_calendar()
    holidays = calendar.regular_holidays.holidays(
        start=trading_date,
        end=trading_date,
        return_name=True,
    )
    if holidays.empty:
        return None
    return str(holidays.iloc[0])


def xnys_session(trading_date: date) -> ExchangeSession:
    if trading_date.weekday() >= 5:
        return ExchangeSession(
            trading_date=trading_date,
            is_trading_day=False,
            holiday_name="Weekend",
        )

    calendar = _xnys_calendar()
    label = pd.Timestamp(trading_date)
    try:
        is_session = bool(calendar.is_session(label))
    except DateOutOfBounds:
        return ExchangeSession(
            trading_date=trading_date,
            is_trading_day=False,
            holiday_name="XNYS calendar unavailable",
        )
    if not is_session:
        return ExchangeSession(
            trading_date=trading_date,
            is_trading_day=False,
            holiday_name=_regular_holiday_name(trading_date) or "XNYS ad-hoc closure",
        )

    close = calendar.session_close(label).to_pydatetime().astimezone(_EASTERN)
    close_time = close.strftime("%H:%M")
    return ExchangeSession(
        trading_date=trading_date,
        is_trading_day=True,
        close_time=close_time,
        is_early_close=close_time != "16:00",
    )
