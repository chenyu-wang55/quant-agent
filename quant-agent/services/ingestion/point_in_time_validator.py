from __future__ import annotations

import csv
import hashlib
import json
import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from services.execution.market_calendar import xnys_session


class PointInTimeValidationError(ValueError):
    pass


@dataclass(frozen=True)
class PointInTimeDatasetPaths:
    universe: Path
    fundamentals: Path
    events: Path
    earnings: Path


@dataclass(frozen=True)
class _Membership:
    universe: str
    ticker: str
    effective_from: datetime
    effective_to: datetime
    source: str


@dataclass(frozen=True)
class _Fundamental:
    ticker: str
    available_at: datetime


_UNIVERSE_COLUMNS = {
    "universe",
    "ticker",
    "effective_from",
    "effective_to",
    "sector",
    "market_cap_usd",
    "spread_bps",
    "source",
}
_FUNDAMENTAL_COLUMNS = {
    "ticker",
    "period_end",
    "available_at",
    "pe_ttm",
    "roe",
    "revenue_growth_yoy",
    "eps_revision_30d",
    "source",
}
_EVENT_COLUMNS = {
    "source_id",
    "published_at",
    "ingested_at",
    "headline",
    "normalized_text",
    "tickers",
    "event_type",
    "sentiment",
    "relevance",
    "horizon",
    "source_url",
    "source",
}
_EARNINGS_COLUMNS = {"ticker", "known_at", "earnings_at", "source"}


def parse_zoned_timestamp(value: str, *, field: str, allow_blank: bool = False) -> datetime:
    text = (value or "").strip()
    if not text:
        if allow_blank:
            return datetime.max.replace(tzinfo=timezone.utc)
        raise PointInTimeValidationError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PointInTimeValidationError(f"{field} is not a valid ISO-8601 timestamp: {text}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PointInTimeValidationError(f"{field} must include an explicit timezone: {text}")
    return parsed.astimezone(timezone.utc)


def _required_text(row: dict[str, str], field: str, *, location: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise PointInTimeValidationError(f"{location}: {field} is required")
    return value


def _positive_number(row: dict[str, str], field: str, *, location: str) -> float:
    raw = _required_text(row, field, location=location)
    try:
        value = float(raw)
    except ValueError as exc:
        raise PointInTimeValidationError(f"{location}: {field} must be numeric") from exc
    if not math.isfinite(value) or value <= 0:
        raise PointInTimeValidationError(f"{location}: {field} must be finite and greater than zero")
    return value


def _finite_number(row: dict[str, str], field: str, *, location: str) -> float:
    raw = _required_text(row, field, location=location)
    try:
        value = float(raw)
    except ValueError as exc:
        raise PointInTimeValidationError(f"{location}: {field} must be numeric") from exc
    if not math.isfinite(value):
        raise PointInTimeValidationError(f"{location}: {field} must be finite")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_csv(path: Path, required_columns: set[str], *, label: str) -> list[dict[str, str]]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise PointInTimeValidationError(f"{label} file not found: {resolved}")
    with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = {str(column).strip() for column in (reader.fieldnames or []) if column}
        missing = sorted(required_columns - columns)
        if missing:
            raise PointInTimeValidationError(f"{label} CSV is missing required columns: {', '.join(missing)}")
        return [
            {str(key): str(value or "") for key, value in row.items() if key is not None}
            for row in reader
            if any(str(value or "").strip() for value in row.values())
        ]


def _trading_points(start: datetime, end: datetime) -> list[datetime]:
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    if start_utc >= end_utc:
        raise PointInTimeValidationError("validation start must be earlier than end")
    points: list[datetime] = []
    point = start_utc
    while point <= end_utc:
        session = xnys_session(point.date())
        if session.holiday_name == "XNYS calendar unavailable":
            raise PointInTimeValidationError(
                f"XNYS calendar is unavailable for validation date {point.date().isoformat()}"
            )
        if session.is_trading_day:
            points.append(point)
        point += timedelta(days=1)
    if not points:
        raise PointInTimeValidationError("validation range contains no XNYS trading sessions")
    return points


def _validate_memberships(
    rows: list[dict[str, str]],
    *,
    required_universes: dict[str, int],
    trading_points: list[datetime],
) -> tuple[list[_Membership], dict[str, dict[str, Any]], dict[str, datetime]]:
    memberships: list[_Membership] = []
    sources_by_universe: dict[str, set[str]] = {universe: set() for universe in required_universes}
    for index, row in enumerate(rows, start=2):
        location = f"universe row {index}"
        universe = _required_text(row, "universe", location=location).upper()
        if universe not in required_universes:
            continue
        ticker = _required_text(row, "ticker", location=location).upper()
        _required_text(row, "sector", location=location)
        _positive_number(row, "market_cap_usd", location=location)
        _positive_number(row, "spread_bps", location=location)
        source = _required_text(row, "source", location=location)
        effective_from = parse_zoned_timestamp(row.get("effective_from", ""), field=f"{location} effective_from")
        effective_to = parse_zoned_timestamp(
            row.get("effective_to", ""),
            field=f"{location} effective_to",
            allow_blank=True,
        )
        if effective_from >= effective_to:
            raise PointInTimeValidationError(f"{location}: effective_from must be earlier than effective_to")
        memberships.append(
            _Membership(
                universe=universe,
                ticker=ticker,
                effective_from=effective_from,
                effective_to=effective_to,
                source=source,
            )
        )
        sources_by_universe[universe].add(source)

    grouped: dict[tuple[str, str], list[_Membership]] = {}
    for membership in memberships:
        grouped.setdefault((membership.universe, membership.ticker), []).append(membership)
    for (universe, ticker), intervals in grouped.items():
        ordered = sorted(intervals, key=lambda item: item.effective_from)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current.effective_from < previous.effective_to:
                raise PointInTimeValidationError(f"overlapping {universe} membership intervals for {ticker}")

    report: dict[str, dict[str, Any]] = {}
    first_required_at: dict[str, datetime] = {}
    for universe, minimum in required_universes.items():
        universe_rows = [row for row in memberships if row.universe == universe]
        if not universe_rows:
            raise PointInTimeValidationError(f"no membership rows found for required universe {universe}")
        observed_counts: list[int] = []
        unique_tickers: set[str] = set()
        for point in trading_points:
            active = [row for row in universe_rows if row.effective_from <= point < row.effective_to]
            tickers = {row.ticker for row in active}
            if len(tickers) < minimum:
                raise PointInTimeValidationError(
                    f"{universe} coverage at {point.isoformat()} is {len(tickers)}; require {minimum}"
                )
            observed_counts.append(len(tickers))
            unique_tickers.update(tickers)
            for ticker in tickers:
                first_required_at.setdefault(ticker, point)
        report[universe] = {
            "required_minimum": minimum,
            "minimum_observed": min(observed_counts),
            "maximum_observed": max(observed_counts),
            "unique_ticker_count": len(unique_tickers),
            "sources": sorted(sources_by_universe[universe]),
        }
    return memberships, report, first_required_at


def _validate_fundamentals(
    rows: list[dict[str, str]],
    *,
    required_tickers: set[str],
    trading_points: list[datetime],
    memberships: list[_Membership],
    max_age_days: int,
) -> dict[str, Any]:
    fundamentals: list[_Fundamental] = []
    sources: set[str] = set()
    for index, row in enumerate(rows, start=2):
        location = f"fundamentals row {index}"
        ticker = _required_text(row, "ticker", location=location).upper()
        period_end = parse_zoned_timestamp(row.get("period_end", ""), field=f"{location} period_end")
        available_at = parse_zoned_timestamp(row.get("available_at", ""), field=f"{location} available_at")
        if period_end > available_at:
            raise PointInTimeValidationError(f"{location}: period_end cannot be after available_at")
        for field in ("pe_ttm", "roe", "revenue_growth_yoy", "eps_revision_30d"):
            _finite_number(row, field, location=location)
        source = _required_text(row, "source", location=location)
        sources.add(source)
        fundamentals.append(_Fundamental(ticker=ticker, available_at=available_at))

    availability: dict[str, list[datetime]] = {}
    for item in fundamentals:
        availability.setdefault(item.ticker, []).append(item.available_at)
    for values in availability.values():
        values.sort()

    missing_tickers = sorted(required_tickers - availability.keys())
    if missing_tickers:
        sample = ", ".join(missing_tickers[:10])
        raise PointInTimeValidationError(
            f"fundamentals are missing for {len(missing_tickers)} active tickers: {sample}"
        )

    max_observed_age = 0
    for point in trading_points:
        active_tickers = {row.ticker for row in memberships if row.effective_from <= point < row.effective_to}
        for ticker in active_tickers:
            timestamps = availability[ticker]
            position = bisect_right(timestamps, point) - 1
            if position < 0:
                raise PointInTimeValidationError(
                    f"no fundamental record was available for {ticker} at {point.isoformat()}"
                )
            age_days = int((point - timestamps[position]).total_seconds() // 86400)
            if age_days > max_age_days:
                raise PointInTimeValidationError(
                    f"fundamental record for {ticker} is {age_days} days old at "
                    f"{point.isoformat()}; maximum is {max_age_days}"
                )
            max_observed_age = max(max_observed_age, age_days)
    return {
        "row_count": len(rows),
        "ticker_count": len(availability),
        "required_ticker_count": len(required_tickers),
        "max_age_days_allowed": max_age_days,
        "max_age_days_observed": max_observed_age,
        "sources": sorted(sources),
    }


def _validate_events(rows: list[dict[str, str]]) -> dict[str, Any]:
    sources: set[str] = set()
    source_ids: set[str] = set()
    for index, row in enumerate(rows, start=2):
        location = f"events row {index}"
        source_id = _required_text(row, "source_id", location=location)
        if source_id in source_ids:
            raise PointInTimeValidationError(f"{location}: duplicate source_id {source_id}")
        source_ids.add(source_id)
        published_at = parse_zoned_timestamp(row.get("published_at", ""), field=f"{location} published_at")
        ingested_at = parse_zoned_timestamp(row.get("ingested_at", ""), field=f"{location} ingested_at")
        if ingested_at < published_at:
            raise PointInTimeValidationError(f"{location}: ingested_at cannot precede published_at")
        _required_text(row, "headline", location=location)
        _required_text(row, "tickers", location=location)
        _finite_number(row, "sentiment", location=location)
        relevance = _finite_number(row, "relevance", location=location)
        if not 0 <= relevance <= 1:
            raise PointInTimeValidationError(f"{location}: relevance must be between 0 and 1")
        source = _required_text(row, "source", location=location)
        sources.add(source)
    return {"row_count": len(rows), "unique_source_id_count": len(source_ids), "sources": sorted(sources)}


def _validate_earnings(rows: list[dict[str, str]]) -> dict[str, Any]:
    sources: set[str] = set()
    tickers: set[str] = set()
    for index, row in enumerate(rows, start=2):
        location = f"earnings row {index}"
        ticker = _required_text(row, "ticker", location=location).upper()
        known_at = parse_zoned_timestamp(row.get("known_at", ""), field=f"{location} known_at")
        earnings_at = parse_zoned_timestamp(row.get("earnings_at", ""), field=f"{location} earnings_at")
        if known_at > earnings_at:
            raise PointInTimeValidationError(f"{location}: known_at cannot be after earnings_at")
        source = _required_text(row, "source", location=location)
        sources.add(source)
        tickers.add(ticker)
    return {"row_count": len(rows), "ticker_count": len(tickers), "sources": sorted(sources)}


def validate_point_in_time_dataset(
    *,
    paths: PointInTimeDatasetPaths,
    start: datetime,
    end: datetime,
    required_universes: dict[str, int],
    max_fundamental_age_days: int = 550,
) -> dict[str, Any]:
    if not required_universes or any(minimum < 1 for minimum in required_universes.values()):
        raise PointInTimeValidationError("required universes must have positive minimum counts")
    if max_fundamental_age_days < 1:
        raise PointInTimeValidationError("max_fundamental_age_days must be positive")

    normalized_universes = {name.strip().upper(): minimum for name, minimum in required_universes.items()}
    trading_points = _trading_points(start, end)
    universe_rows = _load_csv(paths.universe, _UNIVERSE_COLUMNS, label="universe")
    fundamental_rows = _load_csv(paths.fundamentals, _FUNDAMENTAL_COLUMNS, label="fundamentals")
    event_rows = _load_csv(paths.events, _EVENT_COLUMNS, label="events")
    earnings_rows = _load_csv(paths.earnings, _EARNINGS_COLUMNS, label="earnings")

    memberships, universe_report, first_required_at = _validate_memberships(
        universe_rows,
        required_universes=normalized_universes,
        trading_points=trading_points,
    )
    fundamentals_report = _validate_fundamentals(
        fundamental_rows,
        required_tickers=set(first_required_at),
        trading_points=trading_points,
        memberships=memberships,
        max_age_days=max_fundamental_age_days,
    )
    events_report = _validate_events(event_rows)
    earnings_report = _validate_earnings(earnings_rows)
    resolved_paths = {
        "universe": paths.universe.expanduser().resolve(),
        "fundamentals": paths.fundamentals.expanduser().resolve(),
        "events": paths.events.expanduser().resolve(),
        "earnings": paths.earnings.expanduser().resolve(),
    }
    file_reports = {
        name: {
            "path": str(path),
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for name, path in resolved_paths.items()
    }
    fingerprint_payload = {
        "start": start.astimezone(timezone.utc).isoformat(),
        "end": end.astimezone(timezone.utc).isoformat(),
        "required_universes": normalized_universes,
        "max_fundamental_age_days": max_fundamental_age_days,
        "files": {name: details["sha256"] for name, details in file_reports.items()},
    }
    dataset_fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "status": "verified",
        "dataset_fingerprint": dataset_fingerprint,
        "start": start.astimezone(timezone.utc).isoformat(),
        "end": end.astimezone(timezone.utc).isoformat(),
        "xnys_trading_session_count": len(trading_points),
        "universes": universe_report,
        "fundamentals": fundamentals_report,
        "events": events_report,
        "earnings": earnings_report,
        "files": file_reports,
    }
