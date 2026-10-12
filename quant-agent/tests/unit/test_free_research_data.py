from __future__ import annotations

import json
from datetime import date, datetime, timezone

import httpx2 as httpx
import pandas as pd
import pytest

from scripts.collect_free_data import main
from services.ingestion.free_dataset import collect_free_dataset, verify_free_dataset
from services.ingestion.free_sources import SECClient, download_stooq_bars, normalize_daily_bars
from services.ingestion.sec_facts import (
    FreeDataError,
    filing_availability,
    normalize_facts,
    normalize_filings,
    select_facts_as_of,
)

CIK = "0000320193"
OLD = "0000320193-24-000001"
NEW = "0000320193-25-000001"
CUTOFF = datetime(2024, 12, 31, 23, 59, tzinfo=timezone.utc)


def _submissions():
    return {"cik": 320193, "filings": {"recent": {
        "accessionNumber": [OLD, NEW], "filingDate": ["2024-02-01", "2025-02-01"],
        "acceptanceDateTime": ["2024-02-01T21:00:00Z", "2025-02-01T21:00:00Z"],
        "form": ["10-K", "10-K/A"], "primaryDocument": ["annual.htm", "annual-a.htm"],
        "reportDate": ["2023-12-31", "2023-12-31"],
    }, "files": []}}


def _facts():
    return {"cik": 320193, "facts": {"us-gaap": {"Revenues": {"units": {"USD": [
        {"accn": OLD, "filed": "2024-02-01", "form": "10-K", "start": "2023-01-01", "end": "2023-12-31", "val": 100},
        {"accn": NEW, "filed": "2025-02-01", "form": "10-K/A", "start": "2023-01-01", "end": "2023-12-31", "val": 120},
    ]}}}}}


def _raw_files(_cik):
    return {f"{CIK}_submissions.json": json.dumps(_submissions()).encode(),
            f"{CIK}_companyfacts.json": json.dumps(_facts()).encode()}


def _bar_frame(day="2024-12-30"):
    return pd.DataFrame({"Open": [100], "High": [110], "Low": [90], "Close": [105], "Volume": [1000],
                         "Adj Close": [104], "Dividends": [0], "Stock Splits": [0]},
                        index=pd.DatetimeIndex([day], tz="America/New_York"))


def _bars(ticker, start, end, cutoff):
    return normalize_daily_bars(_bar_frame(), ticker=ticker, start=start, end=end, decision_at=cutoff)


def _collect(tmp_path, **kwargs):
    return collect_free_dataset(securities={"AAPL": CIK}, benchmark="SPY", start=date(2024, 12, 1),
                                end=date(2024, 12, 31), decision_at=CUTOFF, output_dir=tmp_path,
                                fetch_sec=kwargs.get("fetch_sec", _raw_files), fetch_bars=kwargs.get("fetch_bars", _bars))


def test_sec_date_only_availability_is_end_of_filing_day_in_eastern():
    available, method = filing_availability("2024-02-01", None)
    assert available.isoformat() == "2024-02-02T05:00:00+00:00"
    assert method == "filing_date_end_of_day_bound"
    submissions = _submissions()
    del submissions["filings"]["recent"]["acceptanceDateTime"]
    before = datetime(2024, 2, 2, 4, 59, tzinfo=timezone.utc)
    filings, _ = normalize_filings([submissions], cik=CIK, decision_at=before)
    assert not filings


def test_sec_acceptance_date_conflicts_and_naive_timestamps_are_rejected():
    with pytest.raises(FreeDataError, match="conflicts"):
        filing_availability("2024-02-01", "2024-02-05T21:00:00Z")
    with pytest.raises(ValueError, match="timezone"):
        filing_availability("2024-02-01", "2024-02-01T21:00:00")


def test_future_restatement_never_enters_an_earlier_snapshot():
    filings, filing_counts = normalize_filings([_submissions()], cik=CIK, decision_at=CUTOFF)
    observations, fact_counts = normalize_facts(_facts(), cik=CIK, filings=filings, decision_at=CUTOFF)
    assert filing_counts["future"] == 1 and fact_counts["future"] == 1
    assert [row["value"] for row in observations] == [100]
    assert observations[0]["source_vintage_verified"] is False
    assert select_facts_as_of(observations, CUTOFF)[0]["accession"] == OLD


def test_as_of_selection_uses_only_then_available_revision():
    later = datetime(2025, 12, 31, tzinfo=timezone.utc)
    filings, _ = normalize_filings([_submissions()], cik=CIK, decision_at=later)
    observations, _ = normalize_facts(_facts(), cik=CIK, filings=filings, decision_at=later)
    assert select_facts_as_of(observations, CUTOFF)[0]["value"] == 100
    assert select_facts_as_of(observations, later)[0]["value"] == 120
    # Semantically equal timestamps with different offsets cannot conceal a conflict.
    conflict = {**observations[0], "available_at": "2024-02-02T00:00:00-05:00", "value": 99}
    with pytest.raises(FreeDataError, match="ambiguous"):
        select_facts_as_of([observations[0], conflict], CUTOFF)


@pytest.mark.parametrize("kind", ["cik", "columns", "document"])
def test_sec_filing_identity_and_contract_changes_fail_closed(kind):
    payload = _submissions()
    if kind == "cik":
        payload["cik"] = 789019
    elif kind == "columns":
        payload["filings"]["recent"]["filingDate"] = []
    else:
        payload["filings"]["recent"]["primaryDocument"][0] = "../other.htm"
    with pytest.raises(FreeDataError):
        normalize_filings([payload], cik=CIK, decision_at=CUTOFF)


def test_missing_filing_does_not_get_a_synthetic_fact_or_value():
    observations, counts = normalize_facts(_facts(), cik=CIK, filings={}, decision_at=CUTOFF)
    assert not observations and counts["missing_filing"] == 1


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True])
def test_sec_invalid_numeric_fact_is_rejected(value):
    facts = _facts()
    facts["facts"]["us-gaap"]["Revenues"]["units"]["USD"][0]["val"] = value
    filings, _ = normalize_filings([_submissions()], cik=CIK, decision_at=CUTOFF)
    with pytest.raises(FreeDataError, match="numeric"):
        normalize_facts(facts, cik=CIK, filings=filings, decision_at=CUTOFF)


def test_duplicate_conflicting_facts_are_not_averaged():
    facts = _facts()
    entries = facts["facts"]["us-gaap"]["Revenues"]["units"]["USD"]
    entries.append({**entries[0], "val": 99})
    filings, _ = normalize_filings([_submissions()], cik=CIK, decision_at=CUTOFF)
    with pytest.raises(FreeDataError, match="conflicting"):
        normalize_facts(facts, cik=CIK, filings=filings, decision_at=CUTOFF)


def test_annual_quarterly_and_unit_contexts_are_kept_separate():
    facts = _facts()
    entries = facts["facts"]["us-gaap"]["Revenues"]["units"]["USD"]
    entries.append({**entries[0], "start": "2023-10-01", "val": 25})
    facts["facts"]["us-gaap"]["Revenues"]["units"]["EUR"] = [{**entries[0], "val": 90}]
    filings, _ = normalize_filings([_submissions()], cik=CIK, decision_at=CUTOFF)
    observations, _ = normalize_facts(facts, cik=CIK, filings=filings, decision_at=CUTOFF)
    selected = select_facts_as_of(observations, CUTOFF)
    assert len(selected) == 3
    assert {(row["start"], row["unit"], row["value"]) for row in selected} == {
        ("2023-01-01", "USD", 100), ("2023-10-01", "USD", 25), ("2023-01-01", "EUR", 90),
    }


def test_early_close_daily_bar_is_unavailable_before_close_plus_delay():
    frame = _bar_frame("2025-11-28")  # Thanksgiving Friday: closes at 13:00 Eastern.
    kwargs = {"ticker": "AAPL", "start": date(2025, 11, 28), "end": date(2025, 11, 28)}
    with pytest.raises(FreeDataError, match="no Yahoo bars"):
        normalize_daily_bars(frame, decision_at=datetime(2025, 11, 28, 18, 14, tzinfo=timezone.utc), **kwargs)
    bars = normalize_daily_bars(frame, decision_at=datetime(2025, 11, 28, 18, 15, tzinfo=timezone.utc), **kwargs)
    assert bars[0]["timestamp"] == "2025-11-28T18:00:00+00:00"
    assert bars[0]["available_at"] == "2025-11-28T18:15:00+00:00"


@pytest.mark.parametrize("kind", ["nan", "range", "naive", "duplicate"])
def test_bad_daily_price_contract_cannot_be_archived_as_good_data(kind):
    frame = _bar_frame()
    if kind == "nan":
        frame["Close"] = float("nan")
    elif kind == "range":
        frame["High"] = 95
    elif kind == "naive":
        frame.index = frame.index.tz_localize(None)
    else:
        frame = pd.concat([frame, frame])
    with pytest.raises(FreeDataError):
        normalize_daily_bars(frame, ticker="AAPL", start=date(2024, 12, 1), end=date(2024, 12, 31), decision_at=CUTOFF)


def test_sec_403_is_terminal_and_no_unbounded_retries():
    calls = []
    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(403)
    with SECClient("research client", transport=httpx.MockTransport(handle), sleep=lambda _: None) as client:
        with pytest.raises(httpx.HTTPStatusError):
            client.issuer(CIK)
    assert len(calls) == 1


def test_sec_429_backoff_is_bounded_and_respects_server_delay():
    delays = []
    responses = iter([httpx.Response(429, headers={"Retry-After": "3"}), httpx.Response(200, json={"ok": True})])
    with SECClient("research client", transport=httpx.MockTransport(lambda _: next(responses)), sleep=delays.append) as client:
        assert json.loads(client.get_json(f"/submissions/CIK{CIK}.json")) == {"ok": True}
    assert delays == [0.25, 3, 0.25]
    responses = iter([httpx.Response(429, headers={"Retry-After": "3600"})])
    with SECClient("research client", transport=httpx.MockTransport(lambda _: next(responses)), sleep=lambda _: None) as client:
        with pytest.raises(httpx.HTTPStatusError):
            client.get_json(f"/submissions/CIK{CIK}.json")


def test_sec_history_requests_are_fixed_host_and_safe_filenames():
    payload = _submissions()
    payload["filings"]["files"] = [{"name": "../../outside.json"}]
    with SECClient("research client", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload)),
                   sleep=lambda _: None) as client:
        with pytest.raises(FreeDataError, match="unsafe"):
            client.issuer(CIK)
        with pytest.raises(FreeDataError, match="unsupported"):
            client.get_json("https://example.test/steal.json")


def test_sec_historical_files_are_collected_and_date_only_records_are_supported():
    payload = _submissions()
    name = f"CIK{CIK}-submissions-001.json"
    payload["filings"]["files"] = [{"name": name}]
    historical = {**payload["filings"]["recent"]}
    del historical["acceptanceDateTime"]
    responses = iter([payload, historical, _facts()])
    requests = []
    def handle(request):
        requests.append(request.url)
        return httpx.Response(200, json=next(responses))
    with SECClient("research client", transport=httpx.MockTransport(handle), sleep=lambda _: None) as client:
        raw = client.issuer(CIK)
    assert name in raw and len(raw) == 3 and len(requests) == 3
    filings, counts = normalize_filings([historical], cik=CIK, decision_at=CUTOFF)
    assert len(filings) == 1 and counts["date_only"] == 1


def test_non_json_sec_payload_is_not_treated_as_empty_financials():
    with SECClient("research client", transport=httpx.MockTransport(lambda _: httpx.Response(200, text="blocked")),
                   sleep=lambda _: None) as client:
        with pytest.raises(ValueError):
            client.get_json(f"/submissions/CIK{CIK}.json")


def test_stooq_preserves_unknown_adjustments_and_explicit_source(monkeypatch):
    raw = "Date,Open,High,Low,Close,Volume\n2024-12-30,100,110,90,105,1000\n"
    client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, text=raw)))
    monkeypatch.setattr("services.ingestion.free_sources.httpx.Client", lambda **_: client)
    bars = download_stooq_bars("AAPL", date(2024, 12, 1), date(2024, 12, 31), CUTOFF)
    assert bars[0]["source"] == "stooq_history"
    assert bars[0]["adjusted_close"] is None and bars[0]["dividend"] is None and bars[0]["split_factor"] is None
    assert bars[0]["corporate_action_adjustment_verified"] is False


def test_stooq_no_data_and_contract_changes_do_not_fill_zeros(monkeypatch):
    client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, text="No data")))
    monkeypatch.setattr("services.ingestion.free_sources.httpx.Client", lambda **_: client)
    with pytest.raises(FreeDataError, match="unknown CSV contract"):
        download_stooq_bars("AAPL", date(2024, 12, 1), date(2024, 12, 31), CUTOFF)


def test_snapshot_is_research_only_and_does_not_fill_production_gaps(tmp_path):
    path, report = _collect(tmp_path)
    assert report["status"] == "research_only" and report["collection_complete"]
    assert report["production_ready"] is False and report["pit_validation_status"] == "not_verified"
    assert report["survivorship_bias_free"] is False
    assert "eps_estimate_revision_30d_unavailable" in report["missing_requirements"]
    assert not (path / "fundamentals.csv").exists()
    assert report["summary"]["AAPL"]["sec_observations"] == 1
    assert verify_free_dataset(path) == report


def test_partial_sec_failure_preserves_real_bars_and_machine_readable_failure(tmp_path):
    def denied(_):
        raise httpx.HTTPStatusError("denied", request=httpx.Request("GET", "https://data.sec.gov/test"),
                                   response=httpx.Response(403))
    path, report = _collect(tmp_path, fetch_sec=denied)
    assert report["status"] == "partial" and not report["collection_complete"]
    assert report["errors"][0]["http_status"] == 403
    assert report["summary"]["AAPL"]["bars"] == 1 and "sec_observations" not in report["summary"]["AAPL"]
    assert (path / "normalized/AAPL_bars.jsonl").exists()
    assert report["paid_api_calls"] == 0


def test_price_failure_keeps_financial_evidence_and_never_creates_fake_bars(tmp_path):
    def unavailable(*_):
        raise FreeDataError("price source unavailable")
    path, report = _collect(tmp_path, fetch_bars=unavailable)
    assert report["status"] == "partial" and report["summary"]["AAPL"]["as_of_facts"] == 1
    assert len(report["errors"]) == 2 and not (path / "normalized/AAPL_bars.jsonl").exists()


def test_future_bar_or_mixed_sources_are_rejected_by_the_archive(tmp_path):
    def future(ticker, start, end, cutoff):
        bars = _bars(ticker, start, end, cutoff)
        bars[0]["available_at"] = "2025-01-02T21:15:00+00:00"
        return bars
    path, report = _collect(tmp_path, fetch_bars=future)
    assert report["status"] == "partial" and not (path / "normalized/AAPL_bars.jsonl").exists()


def test_snapshot_integrity_and_new_runs_never_overwrite_previous_input(tmp_path):
    path, first = _collect(tmp_path)
    original = (path / "manifest.json").read_bytes()
    next_path, _ = _collect(tmp_path)
    assert next_path != path and (path / "manifest.json").read_bytes() == original
    target = path / next(iter(first["files"]))
    target.write_bytes(b"changed")
    with pytest.raises(FreeDataError, match="content changed"):
        verify_free_dataset(path)


def test_corrupted_manifest_and_symlinked_inputs_fail_integrity_validation(tmp_path):
    path, report = _collect(tmp_path)
    target = path / next(iter(report["files"]))
    copy = tmp_path / "outside.json"
    copy.write_bytes(target.read_bytes())
    target.unlink()
    target.symlink_to(copy)
    with pytest.raises(FreeDataError, match="unsafe"):
        verify_free_dataset(path)
    manifest = {**report, "production_ready": True}
    (path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(FreeDataError, match="fingerprint mismatch"):
        verify_free_dataset(path)


def test_cli_offline_verification_never_constructs_network_client(tmp_path, monkeypatch, capsys):
    path, report = _collect(tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError("offline verification made a network call")
    monkeypatch.setattr("scripts.collect_free_data.SECClient", forbidden)
    assert main(["--verify", str(path)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["archive_integrity"] == "verified" and output["production_ready"] is False
    assert output["dataset_fingerprint"] == report["dataset_fingerprint"]
    assert main(["--verify", str(path), "--security", "AAPL:320193"]) == 2


def test_cli_collects_only_explicit_free_source_and_reports_incomplete_data(tmp_path, monkeypatch, capsys):
    class FakeSEC:
        def __init__(self, *_):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def issuer(self, cik):
            return _raw_files(cik)
    def unavailable(*_):
        raise FreeDataError("unavailable")
    monkeypatch.setattr("scripts.collect_free_data.SECClient", FakeSEC)
    monkeypatch.setattr("scripts.collect_free_data.download_stooq_bars", unavailable)
    assert main(["--security", "AAPL:320193", "--start", "2024-12-01", "--end", "2024-12-31",
                 "--decision-at", CUTOFF.isoformat(), "--price-source", "stooq", "--output-dir", str(tmp_path)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "partial"


@pytest.mark.parametrize("args", [[], ["--security", "AAPL:0"], ["--security", "../bad:320193"]])
def test_missing_or_invalid_cli_inputs_do_not_start_network_collection(args, monkeypatch):
    def forbidden(*_, **__):
        raise AssertionError("invalid input made a network call")
    monkeypatch.setattr("scripts.collect_free_data.SECClient", forbidden)
    assert main(args) == 2


def test_free_archive_cannot_be_passed_as_a_verified_production_pit_report(tmp_path):
    from services.ingestion.point_in_time_validator import PointInTimeDatasetPaths, validate_point_in_time_dataset
    path, _ = _collect(tmp_path)
    with pytest.raises(ValueError):
        validate_point_in_time_dataset(
            paths=PointInTimeDatasetPaths(universe=path / "normalized/AAPL_bars.jsonl",
                                         fundamentals=path / "normalized/AAPL_facts.jsonl",
                                         events=path / "manifest.json", earnings=path / "manifest.json"),
            start=datetime(2024, 12, 1, tzinfo=timezone.utc), end=CUTOFF, required_universes={"SP500": 450},
        )
