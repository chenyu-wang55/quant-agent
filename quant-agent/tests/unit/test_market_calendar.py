from __future__ import annotations

from datetime import date

from services.execution.market_calendar import xnys_session


def test_xnys_calendar_covers_ad_hoc_closures_and_early_closes() -> None:
    carter_mourning = xnys_session(date(2025, 1, 9))
    assert carter_mourning.is_trading_day is False
    assert carter_mourning.holiday_name == "XNYS ad-hoc closure"

    post_thanksgiving = xnys_session(date(2026, 11, 27))
    assert post_thanksgiving.is_trading_day is True
    assert post_thanksgiving.is_early_close is True
    assert post_thanksgiving.close_time == "13:00"

    regular = xnys_session(date(2026, 7, 2))
    assert regular.is_trading_day is True
    assert regular.is_early_close is False
    assert regular.close_time == "16:00"


def test_xnys_calendar_fails_closed_outside_supported_range() -> None:
    unsupported = xnys_session(date(2051, 1, 3))

    assert unsupported.is_trading_day is False
    assert unsupported.holiday_name == "XNYS calendar unavailable"
