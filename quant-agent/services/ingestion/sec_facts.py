"""Public SEC observations for research, with filing-time filtering.

This is a fact layer, not a replacement for the production PIT dataset. Current
Company Facts responses do not prove that the vendor vintage was archived then.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from services.ingestion.point_in_time_validator import parse_zoned_timestamp

_EASTERN = ZoneInfo("America/New_York")
_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")
CONCEPTS = {
    "us-gaap": {
        "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
        "NetIncomeLoss", "StockholdersEquity", "EarningsPerShareDiluted", "EarningsPerShareBasic",
        "CommonStockSharesOutstanding", "Assets", "Liabilities",
        "NetCashProvidedByUsedInOperatingActivities", "PaymentsToAcquirePropertyPlantAndEquipment",
    },
    "dei": {"EntityCommonStockSharesOutstanding"},
}


class FreeDataError(ValueError):
    pass


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FreeDataError("decision_at must have an explicit timezone")
    return value.astimezone(timezone.utc)


def normalized_cik(value: str | int) -> str:
    text = str(value).strip()
    if not re.fullmatch(r"\d{1,10}", text) or int(text) <= 0:
        raise FreeDataError("CIK must be a positive number with at most ten digits")
    return text.zfill(10)


def filing_availability(filed: str, accepted: str | None) -> tuple[datetime, str]:
    """Use a conservative end-of-filing-day bound, never period end or midnight UTC."""
    filing_day = date.fromisoformat(filed)
    bound = datetime.combine(filing_day + timedelta(days=1), datetime.min.time(), _EASTERN)
    method = "filing_date_end_of_day_bound"
    if accepted:
        timestamp = parse_zoned_timestamp(accepted, field="acceptanceDateTime")
        # Some dissemination dates differ at midnight. Large discrepancies are
        # rejected; ambiguous data must not be silently assigned an early time.
        if abs((timestamp.astimezone(_EASTERN).date() - filing_day).days) > 1:
            raise FreeDataError("acceptanceDateTime conflicts with filingDate")
        bound = max(bound, timestamp + timedelta(minutes=5))
        method = "filing_date_bound_and_acceptance_plus_5m"
    return bound.astimezone(timezone.utc), method


def normalize_filings(
    payloads: list[dict[str, Any]], *, cik: str, decision_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    cik = normalized_cik(cik)
    cutoff = require_utc(decision_at)
    result: dict[str, dict[str, Any]] = {}
    counts = {"future": 0, "date_only": 0}
    for payload in payloads:
        if "cik" in payload and normalized_cik(payload["cik"]) != cik:
            raise FreeDataError("SEC submissions CIK does not match requested issuer")
        rows = payload.get("filings", {}).get("recent", payload)
        accessions = rows.get("accessionNumber")
        if not isinstance(accessions, list):
            raise FreeDataError("SEC submissions accessionNumber array is missing")
        required = ("filingDate", "form", "primaryDocument")
        for key in required:
            if not isinstance(rows.get(key), list) or len(rows[key]) != len(accessions):
                raise FreeDataError(f"SEC submissions {key} array has inconsistent length")
        for key in ("acceptanceDateTime", "reportDate"):
            if key in rows and (not isinstance(rows[key], list) or len(rows[key]) != len(accessions)):
                raise FreeDataError(f"SEC submissions {key} array has inconsistent length")
        acceptance_times = rows.get("acceptanceDateTime", [None] * len(accessions))
        report_dates = rows.get("reportDate", [""] * len(accessions))
        for index, accession in enumerate(accessions):
            if not isinstance(accession, str) or not _ACCESSION.fullmatch(accession):
                raise FreeDataError("invalid SEC accession number")
            accepted = acceptance_times[index]
            available, method = filing_availability(rows["filingDate"][index], accepted)
            if available > cutoff:
                counts["future"] += 1
                continue
            document = str(rows["primaryDocument"][index])
            if "/" in document or "\\" in document or ".." in document:
                raise FreeDataError("SEC primaryDocument must be a filename")
            prefix = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}"
            record = {
                "cik": cik, "accession": accession, "form": rows["form"][index],
                "filed": rows["filingDate"][index], "available_at": available.isoformat(),
                "availability_method": method,
                "report_date": report_dates[index],
                "source_url": f"{prefix}/{quote(document)}" if document else f"{prefix}/{accession}-index.htm",
            }
            if accession in result and result[accession] != record:
                # Old files may omit acceptance times. Retain the more conservative
                # bound, but require the identity and contents to match.
                previous = result[accession]
                for key in ("form", "filed", "report_date", "source_url"):
                    if previous[key] != record[key]:
                        raise FreeDataError("conflicting SEC submission records")
                if record["available_at"] <= previous["available_at"]:
                    continue
            result[accession] = record
    counts["date_only"] = sum(
        item["availability_method"] == "filing_date_end_of_day_bound" for item in result.values()
    )
    return result, counts


def normalize_facts(
    payload: dict[str, Any], *, cik: str, filings: dict[str, dict[str, Any]], decision_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    cik = normalized_cik(cik)
    cutoff = require_utc(decision_at)
    if normalized_cik(payload.get("cik", "")) != cik:
        raise FreeDataError("SEC Company Facts CIK does not match requested issuer")
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        raise FreeDataError("SEC Company Facts facts object is missing")
    counts = {"future": 0, "missing_filing": 0, "unsupported_form": 0}
    observations: dict[tuple[str, ...], dict[str, Any]] = {}
    for namespace, concepts in CONCEPTS.items():
        for concept in sorted(concepts):
            for unit, entries in facts.get(namespace, {}).get(concept, {}).get("units", {}).items():
                for entry in entries:
                    filed = str(entry.get("filed", ""))
                    # Check dates before joining; old filings missing from the
                    # downloaded history remain missing rather than guessed.
                    fallback, _ = filing_availability(filed, None)
                    if fallback > cutoff:
                        counts["future"] += 1
                        continue
                    if entry.get("form") not in {"10-K", "10-Q", "10-K/A", "10-Q/A", "20-F", "20-F/A"}:
                        counts["unsupported_form"] += 1
                        continue
                    accession = str(entry.get("accn", ""))
                    filing = filings.get(accession)
                    if not filing:
                        counts["missing_filing"] += 1
                        continue
                    if parse_zoned_timestamp(filing["available_at"], field="available_at") > cutoff:
                        counts["future"] += 1
                        continue
                    if filing["filed"] != filed or filing["form"] != entry["form"]:
                        raise FreeDataError("SEC fact and submission disagree on filing identity")
                    end = date.fromisoformat(str(entry.get("end", "")))
                    start = date.fromisoformat(entry["start"]) if entry.get("start") else None
                    if end > date.fromisoformat(filed) or (start and start > end):
                        raise FreeDataError("SEC fact has an invalid reporting interval")
                    value = entry.get("val")
                    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
                        raise FreeDataError("SEC fact value must be finite and numeric")
                    record = {
                        "cik": cik, "namespace": namespace, "concept": concept, "unit": unit,
                        "start": start.isoformat() if start else None, "end": end.isoformat(), "value": value,
                        "accession": accession, "form": entry["form"], "filed": filed,
                        "available_at": filing["available_at"], "availability_method": filing["availability_method"],
                        "source_url": filing["source_url"], "source": "sec_companyfacts",
                        "quality_status": "research_only", "source_vintage_verified": False,
                    }
                    key = (namespace, concept, unit, record["start"] or "", record["end"], accession)
                    if key in observations and observations[key]["value"] != value:
                        raise FreeDataError("conflicting values for the same SEC fact and filing")
                    observations[key] = record
    return sorted(observations.values(), key=lambda row: (
        row["available_at"], row["namespace"], row["concept"], row["unit"], row["start"] or "", row["end"], row["accession"],
    )), counts


def select_facts_as_of(observations: list[dict[str, Any]], decision_at: datetime) -> list[dict[str, Any]]:
    """Latest *then available* disclosure per concept/period; retain units and periods."""
    cutoff = require_utc(decision_at)
    selected: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in observations:
        available = parse_zoned_timestamp(row["available_at"], field="available_at")
        if available > cutoff:
            continue
        key = tuple(str(row.get(name) or "") for name in ("cik", "namespace", "concept", "unit", "start", "end"))
        previous = selected.get(key)
        previous_at = parse_zoned_timestamp(previous["available_at"], field="available_at") if previous else None
        if previous and previous_at == available and previous["value"] != row["value"]:
            raise FreeDataError("ambiguous SEC revisions at the same availability time")
        if previous_at is None or available > previous_at:
            selected[key] = row
    return [selected[key] for key in sorted(selected)]
