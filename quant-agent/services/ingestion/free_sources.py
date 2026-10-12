"""Small, rate-limited downloads from free public sources. No broker or database access."""
from __future__ import annotations

import csv
import json
import math
import re
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from typing import Any
from zoneinfo import ZoneInfo

import httpx2 as httpx

from services.execution.market_calendar import xnys_session
from services.ingestion.sec_facts import FreeDataError, normalized_cik, require_utc

_EASTERN = ZoneInfo("America/New_York")


def normalized_ticker(value: str) -> str:
    value = value.strip().upper()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-^=]{0,19}", value):
        raise FreeDataError("invalid research ticker")
    return value


class SECClient:
    """Single-process sequential client; 403 is terminal, 429/5xx retries are bounded."""

    def __init__(self, user_agent: str, *, transport: httpx.BaseTransport | None = None,
                 sleep: Callable[[float], None] = time.sleep):
        if not user_agent.strip() or "\n" in user_agent or "\r" in user_agent:
            raise FreeDataError("SEC User-Agent must identify the research client")
        self._sleep = sleep
        self._client = httpx.Client(
            timeout=20, transport=transport, follow_redirects=False,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )

    def __enter__(self) -> SECClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self._client.close()

    def get_json(self, path: str) -> bytes:
        if not re.fullmatch(r"/(?:submissions|api/xbrl/companyfacts)/[A-Za-z0-9.\-]+\.json", path):
            raise FreeDataError("unsupported SEC API path")
        for attempt in range(3):
            self._sleep(0.25)  # Below the official 10 requests/s per-user ceiling.
            response = self._client.get(f"https://data.sec.gov{path}")
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt < 2:
                    raw_delay = response.headers.get("Retry-After", "")
                    # Do not retry earlier than a server-requested delay. Long
                    # delays abort this run and can be retried by the user later.
                    if raw_delay and (not raw_delay.isdigit() or int(raw_delay) > 30):
                        response.raise_for_status()
                    self._sleep(max(2 ** attempt, int(raw_delay or "0")))
                    continue
            response.raise_for_status()
            raw = response.content
            if len(raw) > 30 * 1024 * 1024:
                raise FreeDataError("SEC response exceeds the 30 MiB research limit")
            if not isinstance(json.loads(raw), dict):
                raise FreeDataError("SEC response must be a JSON object")
            return raw
        raise FreeDataError("SEC request did not complete")

    def issuer(self, cik: str) -> dict[str, bytes]:
        cik = normalized_cik(cik)
        name = f"CIK{cik}.json"
        raw = self.get_json(f"/submissions/{name}")
        payload = json.loads(raw)
        if normalized_cik(payload.get("cik", "")) != cik:
            raise FreeDataError("SEC submissions CIK mismatch")
        files = payload.get("filings", {}).get("files", [])
        if not isinstance(files, list) or len(files) > 100:
            raise FreeDataError("unsupported SEC historical file list")
        result = {f"{cik}_submissions.json": raw}
        for item in files:
            filename = str(item.get("name", ""))
            if not re.fullmatch(rf"CIK{cik}-submissions-\d{{3}}\.json", filename):
                raise FreeDataError("unsafe SEC historical filename")
            if filename in result:
                raise FreeDataError("duplicate SEC historical filename")
            result[filename] = self.get_json(f"/submissions/{filename}")
        result[f"{cik}_companyfacts.json"] = self.get_json(f"/api/xbrl/companyfacts/CIK{cik}.json")
        return result


def normalize_daily_bars(frame: Any, *, ticker: str, start: date, end: date,
                         decision_at: datetime) -> list[dict[str, Any]]:
    """A daily OHLC bar becomes available after session close, never at midnight."""
    ticker = normalized_ticker(ticker)
    cutoff = require_utc(decision_at)
    rows: list[dict[str, Any]] = []
    dates: set[date] = set()
    for index, row in frame.iterrows():
        timestamp = index.to_pydatetime()
        if timestamp.tzinfo is None:
            raise FreeDataError("Yahoo daily bar index must have a timezone")
        trading_date = timestamp.astimezone(_EASTERN).date()
        if not start <= trading_date <= end:
            continue
        session = xnys_session(trading_date)
        if not session.is_trading_day:
            raise FreeDataError("Yahoo returned a non-XNYS-session daily bar")
        close_hour, close_minute = map(int, session.close_time.split(":"))
        closed = datetime(trading_date.year, trading_date.month, trading_date.day,
                          close_hour, close_minute, tzinfo=_EASTERN).astimezone(timezone.utc)
        available = closed + timedelta(minutes=15)
        if available > cutoff:
            continue
        if trading_date in dates:
            raise FreeDataError("duplicate Yahoo daily bar")
        dates.add(trading_date)
        values = {name.lower(): float(row[name]) for name in ("Open", "High", "Low", "Close", "Volume")}
        if any(not math.isfinite(value) for value in values.values()):
            raise FreeDataError("Yahoo OHLCV contains non-finite values")
        if any(values[key] <= 0 for key in ("open", "high", "low", "close")) or values["volume"] < 0:
            raise FreeDataError("Yahoo OHLCV contains invalid prices or volume")
        if values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
            raise FreeDataError("Yahoo OHLC range is inconsistent")
        extras: dict[str, float | None] = {}
        for field, column in (("adjusted_close", "Adj Close"), ("dividend", "Dividends"),
                              ("split_factor", "Stock Splits")):
            value = float(row[column]) if column in row else None
            if value is not None and (not math.isfinite(value) or value < 0):
                raise FreeDataError(f"Yahoo {column} is invalid")
            if field == "adjusted_close" and value == 0:
                raise FreeDataError("Yahoo adjusted close must be positive when present")
            extras[field] = value
        rows.append({"ticker": ticker, "timestamp": closed.isoformat(), "available_at": available.isoformat(),
                     **values, **extras, "source": "yfinance_history", "quality_status": "research_only"})
    if not rows:
        raise FreeDataError("no Yahoo bars available in the requested decision interval")
    return sorted(rows, key=lambda row: row["timestamp"])


def download_yahoo_bars(ticker: str, start: date, end: date, decision_at: datetime) -> list[dict[str, Any]]:
    import yfinance as yf

    frame = yf.Ticker(normalized_ticker(ticker)).history(
        start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
        interval="1d", auto_adjust=False, actions=True, repair=False, timeout=15, raise_errors=True,
    )
    return normalize_daily_bars(frame, ticker=ticker, start=start, end=end, decision_at=decision_at)


def download_stooq_bars(ticker: str, start: date, end: date, decision_at: datetime) -> list[dict[str, Any]]:
    """Explicit alternate source; absent adjustment/action fields stay unknown."""
    import pandas as pd

    ticker = normalized_ticker(ticker)
    symbol = f"{ticker.lower()}.us"
    with httpx.Client(timeout=20, follow_redirects=False) as client:
        response = client.get("https://stooq.com/q/d/l/", params={
            "s": symbol, "i": "d", "d1": start.strftime("%Y%m%d"), "d2": end.strftime("%Y%m%d"),
        })
        response.raise_for_status()
        if len(response.content) > 5 * 1024 * 1024:
            raise FreeDataError("Stooq response exceeds the 5 MiB research limit")
        reader = csv.DictReader(StringIO(response.text))
        if set(reader.fieldnames or []) != {"Date", "Open", "High", "Low", "Close", "Volume"}:
            raise FreeDataError("Stooq returned no data or an unknown CSV contract")
        rows = list(reader)
    if not rows:
        raise FreeDataError("Stooq returned no daily bars")
    frame = pd.DataFrame(rows)
    frame.index = pd.DatetimeIndex([
        datetime.combine(date.fromisoformat(row["Date"]), datetime.min.time(), _EASTERN) for row in rows
    ])
    result = normalize_daily_bars(frame, ticker=ticker, start=start, end=end, decision_at=decision_at)
    for row in result:
        row["source"] = "stooq_history"
        row["corporate_action_adjustment_verified"] = False
    return result
