"""Immutable, explicitly incomplete free research snapshots, separate from production PIT inputs."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from services.ingestion.free_sources import normalized_ticker
from services.ingestion.sec_facts import (
    FreeDataError,
    normalize_facts,
    normalize_filings,
    normalized_cik,
    require_utc,
    select_facts_as_of,
)

_GAPS = [
    "historical_universe_membership_not_verified",
    "historical_ticker_mapping_not_verified",
    "delisted_price_and_terminal_cashflow_coverage_not_verified",
    "historical_source_vintage_not_verified",
    "eps_estimate_revision_30d_unavailable",
    "earnings_calendar_known_at_unavailable",
    "historical_news_event_coverage_unavailable",
    "historical_bid_ask_spread_unavailable",
    "production_fundamental_ratios_not_derived",
]


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def _fingerprint(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(_json_bytes({key: value for key, value in manifest.items()
                                     if key != "dataset_fingerprint"})).hexdigest()


def verify_free_dataset(path: Path) -> dict[str, Any]:
    path = path.resolve()
    manifest = json.loads((path / "manifest.json").read_bytes())
    if manifest.get("schema_version") != 1 or manifest.get("kind") != "free_research_snapshot":
        raise FreeDataError("unsupported free research manifest")
    if manifest.get("dataset_fingerprint") != _fingerprint(manifest):
        raise FreeDataError("free research manifest fingerprint mismatch")
    if manifest.get("production_ready") is not False or manifest.get("pit_validation_status") != "not_verified":
        raise FreeDataError("free research snapshot cannot claim production PIT verification")
    for filename, receipt in manifest["files"].items():
        relative = Path(filename)
        target = path / relative
        if relative.is_absolute() or ".." in relative.parts or target.is_symlink():
            raise FreeDataError("unsafe free research archive path")
        if not target.resolve().is_relative_to(path):
            raise FreeDataError("free research archive path escapes snapshot")
        raw = target.read_bytes()
        if hashlib.sha256(raw).hexdigest() != receipt["sha256"] or len(raw) != receipt["bytes"]:
            raise FreeDataError("free research archive content changed")
    return manifest


def collect_free_dataset(
    *, securities: dict[str, str], benchmark: str, start: date, end: date, decision_at: datetime, output_dir: Path,
    fetch_sec: Callable[[str], dict[str, bytes]],
    fetch_bars: Callable[[str, date, date, datetime], list[dict[str, Any]]],
) -> tuple[Path, dict[str, Any]]:
    cutoff = require_utc(decision_at)
    if start > end or end > cutoff.date():
        raise FreeDataError("research dates must satisfy start <= end <= decision_at date")
    if not securities or len(securities) > 20:
        raise FreeDataError("provide between 1 and 20 explicit research securities")
    normalized = {normalized_ticker(ticker): normalized_cik(cik) for ticker, cik in securities.items()}
    if len(normalized) != len(securities) or len(set(normalized.values())) != len(normalized):
        raise FreeDataError("research ticker and CIK mappings must be unique")
    benchmark = normalized_ticker(benchmark)
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[dict[str, Any]] = []
    summaries: dict[str, Any] = {}
    files: dict[str, dict[str, Any]] = {}

    with tempfile.TemporaryDirectory(prefix=".collect-", dir=output_dir) as temporary:
        staging = Path(temporary) / "snapshot"
        staging.mkdir(mode=0o700)

        def archive(filename: str, raw: bytes, source: str) -> None:
            relative = Path(filename)
            if relative.is_absolute() or ".." in relative.parts or filename in files:
                raise FreeDataError("unsafe or duplicate archive filename")
            destination = staging / relative
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with destination.open("xb") as handle:
                handle.write(raw)
            destination.chmod(0o600)
            files[filename] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "source": source}

        def json_lines(rows: list[dict[str, Any]]) -> bytes:
            return b"".join((json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n").encode()
                            for row in rows)

        for ticker, cik in sorted(normalized.items()):
            summary: dict[str, Any] = {}
            summaries[ticker] = summary
            try:
                raw_files = fetch_sec(cik)
                # Pin and parse the identical bytes. No later provider reads can
                # replace one source within the snapshot.
                for name, raw in sorted(raw_files.items()):
                    archive(f"sec/{cik}/{name}", raw, "sec_public_api")
                companyfacts = json.loads(raw_files[f"{cik}_companyfacts.json"])
                submissions = [json.loads(raw) for name, raw in raw_files.items()
                               if name != f"{cik}_companyfacts.json"]
                filings, filing_counts = normalize_filings(submissions, cik=cik, decision_at=cutoff)
                facts, fact_counts = normalize_facts(companyfacts, cik=cik, filings=filings, decision_at=cutoff)
                if not filings or not facts:
                    raise FreeDataError("no eligible SEC filings and facts for the decision time")
                selected = select_facts_as_of(facts, cutoff)
                archive(f"normalized/{ticker}_filings.jsonl", json_lines(list(filings.values())), "sec_public_api")
                archive(f"normalized/{ticker}_facts.jsonl", json_lines(facts), "sec_public_api")
                archive(f"normalized/{ticker}_facts_as_of.jsonl", json_lines(selected), "sec_public_api")
                summary.update({"sec_filings": len(filings), "sec_observations": len(facts),
                                "as_of_facts": len(selected), "filing_filter_counts": filing_counts,
                                "fact_filter_counts": fact_counts})
            except Exception as exc:
                errors.append({"component": "sec", "ticker": ticker, "error_type": type(exc).__name__,
                               "http_status": getattr(getattr(exc, "response", None), "status_code", None)})

        for ticker in sorted(set(normalized) | {benchmark}):
            summary = summaries.setdefault(ticker, {})
            try:
                bars = fetch_bars(ticker, start, end, cutoff)
                if not bars:
                    raise FreeDataError("no free bars returned")
                # Also validate injected/imported downloaders before archiving.
                for bar in bars:
                    if bar["ticker"] != ticker or bar["quality_status"] != "research_only":
                        raise FreeDataError("unexpected free bar identity or quality claim")
                    available = datetime.fromisoformat(bar["available_at"])
                    if require_utc(available) > cutoff:
                        raise FreeDataError("free bar is not available at decision_at")
                price_sources = {bar["source"] for bar in bars}
                if len(price_sources) != 1 or not price_sources <= {"yfinance_history", "stooq_history"}:
                    raise FreeDataError("free bar sources must be explicit and consistent")
                archive(f"normalized/{ticker}_bars.jsonl", json_lines(bars), next(iter(price_sources)))
                summary.update({"bars": len(bars), "first_bar": bars[0]["timestamp"], "last_bar": bars[-1]["timestamp"]})
            except Exception as exc:
                errors.append({"component": "bars", "ticker": ticker, "error_type": type(exc).__name__,
                               "http_status": getattr(getattr(exc, "response", None), "status_code", None)})

        manifest: dict[str, Any] = {
            "schema_version": 1, "kind": "free_research_snapshot", "collector_version": "free-sec-prices-v1",
            "status": "partial" if errors else "research_only", "collection_complete": not errors,
            "production_ready": False, "pit_validation_status": "not_verified",
            "collected_at": datetime.now(timezone.utc).isoformat(), "decision_at": cutoff.isoformat(),
            "start": start.isoformat(), "end": end.isoformat(), "securities": normalized, "benchmark": benchmark,
            "universe_kind": "explicit_research_selection", "survivorship_bias_free": False,
            "source_cost_usd": 0, "paid_api_calls": 0, "raw_sec_responses_may_include_future_records": True,
            "bar_availability": "XNYS_session_close_plus_15m", "missing_requirements": _GAPS.copy(),
            "summary": summaries, "errors": errors, "files": files,
        }
        manifest["dataset_fingerprint"] = _fingerprint(manifest)
        (staging / "manifest.json").write_bytes(_json_bytes(manifest))
        (staging / "manifest.json").chmod(0o600)
        # A content-named destination is never updated in place.
        destination = output_dir / manifest["dataset_fingerprint"]
        if destination.exists():
            if verify_free_dataset(destination) != manifest:
                raise FreeDataError("snapshot destination is occupied by different content")
        else:
            os.rename(staging, destination)
    return destination, verify_free_dataset(destination)
