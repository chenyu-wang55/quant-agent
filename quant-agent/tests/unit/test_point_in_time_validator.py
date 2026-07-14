from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts.validate_point_in_time_data import main
from services.ingestion.point_in_time_validator import (
    PointInTimeDatasetPaths,
    PointInTimeValidationError,
    parse_zoned_timestamp,
    validate_point_in_time_dataset,
)

UNIVERSE_HEADER = "universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source"
FUNDAMENTALS_HEADER = "ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source"
EVENTS_HEADER = (
    "source_id,published_at,ingested_at,headline,normalized_text,tickers,event_type,"
    "sentiment,relevance,horizon,source_url,source"
)
EARNINGS_HEADER = "ticker,known_at,earnings_at,source"


def _write(path, header: str, *rows: str) -> None:
    path.write_text("\n".join([header, *rows, ""]), encoding="utf-8")


def _valid_paths(tmp_path) -> PointInTimeDatasetPaths:
    universe = tmp_path / "universe.csv"
    fundamentals = tmp_path / "fundamentals.csv"
    events = tmp_path / "events.csv"
    earnings = tmp_path / "earnings.csv"
    _write(
        universe,
        UNIVERSE_HEADER,
        "SP500,AAPL,2020-01-01T00:00:00Z,,Technology,3000000000000,8,licensed-index-feed",
    )
    _write(
        fundamentals,
        FUNDAMENTALS_HEADER,
        "AAPL,2025-09-30T00:00:00Z,2026-01-01T00:00:00Z,25,0.3,0.1,0.02,licensed-fundamentals",
    )
    _write(events, EVENTS_HEADER)
    _write(earnings, EARNINGS_HEADER)
    return PointInTimeDatasetPaths(
        universe=universe,
        fundamentals=fundamentals,
        events=events,
        earnings=earnings,
    )


def test_point_in_time_dataset_validator_returns_hashed_range_evidence(tmp_path) -> None:
    paths = _valid_paths(tmp_path)
    report = validate_point_in_time_dataset(
        paths=paths,
        start=datetime(2026, 1, 5, tzinfo=timezone.utc),
        end=datetime(2026, 1, 7, tzinfo=timezone.utc),
        required_universes={"SP500": 1},
        max_fundamental_age_days=30,
    )

    assert report["status"] == "verified"
    assert report["xnys_trading_session_count"] == 3
    assert report["universes"]["SP500"]["minimum_observed"] == 1
    assert report["fundamentals"]["max_age_days_observed"] == 6
    assert len(report["dataset_fingerprint"]) == 64
    assert len(report["files"]["universe"]["sha256"]) == 64


def test_point_in_time_timestamps_must_have_explicit_timezone() -> None:
    with pytest.raises(PointInTimeValidationError, match="explicit timezone"):
        parse_zoned_timestamp("2026-01-01T00:00:00", field="test timestamp")


def test_point_in_time_dataset_fingerprint_changes_with_file_content(tmp_path) -> None:
    paths = _valid_paths(tmp_path)
    kwargs = {
        "paths": paths,
        "start": datetime(2026, 1, 5, tzinfo=timezone.utc),
        "end": datetime(2026, 1, 7, tzinfo=timezone.utc),
        "required_universes": {"SP500": 1},
        "max_fundamental_age_days": 30,
    }
    first = validate_point_in_time_dataset(**kwargs)
    _write(
        paths.events,
        EVENTS_HEADER,
        "evt,2026-01-05T12:00:00Z,2026-01-05T12:01:00Z,Headline,Headline,AAPL,news,0,1,short,https://e/1,archive",
    )
    second = validate_point_in_time_dataset(**kwargs)

    assert first["dataset_fingerprint"] != second["dataset_fingerprint"]


def test_point_in_time_validator_rejects_overlapping_membership(tmp_path) -> None:
    paths = _valid_paths(tmp_path)
    _write(
        paths.universe,
        UNIVERSE_HEADER,
        "SP500,AAPL,2020-01-01T00:00:00Z,2026-06-01T00:00:00Z,Technology,1,1,feed",
        "SP500,AAPL,2026-01-01T00:00:00Z,,Technology,1,1,feed",
    )
    with pytest.raises(PointInTimeValidationError, match="overlapping"):
        validate_point_in_time_dataset(
            paths=paths,
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            end=datetime(2026, 1, 7, tzinfo=timezone.utc),
            required_universes={"SP500": 1},
        )


def test_point_in_time_validator_rejects_stale_fundamentals(tmp_path) -> None:
    paths = _valid_paths(tmp_path)
    with pytest.raises(PointInTimeValidationError, match="days old"):
        validate_point_in_time_dataset(
            paths=paths,
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            end=datetime(2026, 1, 7, tzinfo=timezone.utc),
            required_universes={"SP500": 1},
            max_fundamental_age_days=1,
        )


def test_point_in_time_validator_rejects_invalid_event_provenance(tmp_path) -> None:
    paths = _valid_paths(tmp_path)
    _write(
        paths.events,
        EVENTS_HEADER,
        "evt,2026-01-05T12:00:00Z,2026-01-05T11:00:00Z,Headline,Headline,AAPL,news,0,1,short,https://e/1,archive",
    )
    with pytest.raises(PointInTimeValidationError, match="ingested_at cannot precede"):
        validate_point_in_time_dataset(
            paths=paths,
            start=datetime(2026, 1, 5, tzinfo=timezone.utc),
            end=datetime(2026, 1, 7, tzinfo=timezone.utc),
            required_universes={"SP500": 1},
        )


def test_point_in_time_validation_cli_writes_report(tmp_path, capsys) -> None:
    paths = _valid_paths(tmp_path)
    output = tmp_path / "validation-report.json"
    exit_code = main(
        [
            "--start",
            "2026-01-05T00:00:00Z",
            "--end",
            "2026-01-07T00:00:00Z",
            "--universe",
            str(paths.universe),
            "--fundamentals",
            str(paths.fundamentals),
            "--events",
            str(paths.events),
            "--earnings",
            str(paths.earnings),
            "--required-universe",
            "SP500:1",
            "--max-fundamental-age-days",
            "30",
            "--output",
            str(output),
        ]
    )

    assert exit_code == 0
    assert '"status": "verified"' in capsys.readouterr().out
    assert '"status": "verified"' in output.read_text(encoding="utf-8")
