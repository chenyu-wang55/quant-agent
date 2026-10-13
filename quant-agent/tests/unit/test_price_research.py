from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone

import httpx2 as httpx
import pytest

from scripts.run_price_research import main
from services.ingestion.sec_facts import FreeDataError
from services.ingestion.twelve_prices import PriceQuota, TwelvePriceClient, read_price_key
from services.research.price_data import (
    PriceDataset,
    canonical,
    digest,
    load_price_dataset,
    normalize_twelve,
    save_price_dataset,
    sessions,
)
from services.research.price_only import PriceResearchConfig, rank_prices, run_price_backtest, run_price_study


def _payload(symbol, start=date(2015, 1, 1), end=date(2018, 12, 31), *, growth=0.0005, wobble=0.01):
    rows = []
    for index, day in enumerate(sessions(start, end)):
        close = 100 * math.exp(growth * index) * (1 + wobble * math.sin(index / 10))
        opening = close * 0.999
        rows.append({"datetime": day.isoformat(), "open": str(opening), "high": str(close * 1.01),
                     "low": str(close * 0.99), "close": str(close), "volume": "1000000"})
    return {"status": "ok", "meta": {"symbol": symbol, "interval": "1day", "currency": "USD",
                                      "exchange_timezone": "America/New_York"}, "values": list(reversed(rows))}


def _config():
    return PriceResearchConfig(
        selected_on=date(2026, 10, 13), securities={"AAPL": "Technology", "MSFT": "Technology", "JPM": "Financials"},
        data_start=date(2015, 1, 1), data_end=date(2018, 12, 31), backtest_start=date(2016, 1, 4),
        validation_start=date(2017, 1, 1), holdout_start=date(2018, 1, 1), backtest_end=date(2018, 12, 31),
        selection_at=datetime(2019, 1, 2, 15, tzinfo=timezone.utc),
    )


def _raw():
    return {symbol: canonical(_payload(symbol, growth=0.0002 if symbol == "SPY" else 0.0007))
            for symbol in ("AAPL", "MSFT", "JPM", "SPY")}


def _dataset(tmp_path):
    return save_price_dataset(_raw(), start=date(2015, 1, 1), end=date(2018, 12, 31),
                              output_dir=tmp_path, source_kind="synthetic_test")


def test_eod_publication_uses_next_trading_midnight_not_close_or_weekend():
    payload = _payload("AAPL", date(2025, 11, 28), date(2025, 11, 28))
    bars = normalize_twelve(payload, "AAPL", date(2025, 11, 28), date(2025, 11, 28))
    assert bars[0].available_at.isoformat() == "2025-12-01T05:00:00+00:00"
    assert bars[0].day == date(2025, 11, 28)


@pytest.mark.parametrize("kind", ["missing", "timezone", "symbol", "duplicate", "range", "nan", "status"])
def test_source_contracts_missing_sessions_and_bad_prices_fail_closed(kind):
    start, end = date(2025, 1, 2), date(2025, 1, 3)
    payload = _payload("AAPL", start, end)
    if kind == "missing":
        payload["values"].pop()
    elif kind == "timezone":
        payload["meta"]["exchange_timezone"] = "UTC"
    elif kind == "symbol":
        payload["meta"]["symbol"] = "MSFT"
    elif kind == "duplicate":
        payload["values"].append(payload["values"][0])
    elif kind == "range":
        payload["values"][0]["high"] = "1"
    elif kind == "nan":
        payload["values"][0]["volume"] = "NaN"
    else:
        payload["status"] = "error"
    with pytest.raises(FreeDataError):
        normalize_twelve(payload, "AAPL", start, end)


def test_archive_tamper_and_symlink_are_rejected(tmp_path):
    path, dataset = _dataset(tmp_path)
    assert load_price_dataset(path).fingerprint == dataset.fingerprint
    assert dataset.manifest["production_pit_verified"] is False
    target = path / "AAPL.json"
    original = target.read_bytes()
    target.write_bytes(b"changed")
    with pytest.raises(FreeDataError, match="bytes changed"):
        load_price_dataset(path)
    target.unlink()
    other = tmp_path / "other.json"
    other.write_bytes(original)
    target.symlink_to(other)
    with pytest.raises(FreeDataError, match="symbolic"):
        load_price_dataset(path)


def test_identical_input_archive_is_reused_without_overwriting(tmp_path):
    path, dataset = _dataset(tmp_path)
    original = (path / "manifest.json").read_bytes()
    again, replay = _dataset(tmp_path)
    assert path == again and dataset.fingerprint == replay.fingerprint
    assert (path / "manifest.json").read_bytes() == original


def test_archive_manifest_symlinks_are_rejected(tmp_path):
    path, _ = _dataset(tmp_path)
    manifest = path / "manifest.json"
    other = tmp_path / "manifest_copy.json"
    other.write_bytes(manifest.read_bytes())
    manifest.unlink()
    manifest.symlink_to(other)
    with pytest.raises(FreeDataError, match="symbolic"):
        load_price_dataset(path)


def test_future_prices_cannot_change_an_earlier_ranking(tmp_path):
    _, dataset = _dataset(tmp_path)
    cutoff = datetime(2017, 6, 1, 13, 15, tzinfo=timezone.utc)
    before = rank_prices(dataset, _config(), cutoff)
    raw = _raw()
    payload = json.loads(raw["AAPL"])
    for row in payload["values"]:
        if date.fromisoformat(row["datetime"]) >= date(2017, 6, 1):
            for field in ("open", "high", "low", "close"):
                row[field] = str(float(row[field]) * 100)
    raw["AAPL"] = canonical(payload)
    _, changed = save_price_dataset(raw, start=date(2015, 1, 1), end=date(2018, 12, 31),
                                    output_dir=tmp_path, source_kind="synthetic_test")
    assert rank_prices(changed, _config(), cutoff) == before


def test_insufficient_warmup_and_misaligned_price_dates_are_not_filled(tmp_path):
    _, dataset = _dataset(tmp_path)
    with pytest.raises(FreeDataError, match="200"):
        rank_prices(dataset, _config(), datetime(2015, 2, 1, tzinfo=timezone.utc))
    corrupted = PriceDataset(bars={**dataset.bars, "AAPL": dataset.bars["AAPL"][:-1]},
                             fingerprint=dataset.fingerprint, manifest=dataset.manifest)
    with pytest.raises(FreeDataError, match="decision date"):
        rank_prices(corrupted, _config(), _config().selection_at)


def test_sector_position_and_gross_caps_apply_and_no_candidate_is_executable(tmp_path):
    _, dataset = _dataset(tmp_path)
    config = _config().model_copy(update={"max_sector_weight": 0.10, "max_gross_weight": 0.20})
    result = rank_prices(dataset, config, config.selection_at)
    selected = [row for row in result["candidates"] if row["target_weight"] > 0]
    assert len(selected) == 2 and result["cash_weight"] == pytest.approx(0.8)
    assert all(row["target_weight"] <= 0.1 and row["execution_allowed"] is False for row in selected)
    assert sum(row["target_weight"] for row in selected if row["sector"] == "Technology") <= 0.1


def test_backtest_fills_after_decision_and_remains_long_only_without_leverage(tmp_path):
    _, dataset = _dataset(tmp_path)
    result = run_price_backtest(dataset, _config())
    assert result["trades"] and result["sell_legs"] > 0
    for trade in result["trades"]:
        assert trade["adjusted_units"] > 0 and trade["costs"] >= 0
        if trade["decision_at"] != "precommitted_final_close":
            assert datetime.fromisoformat(trade["decision_at"]).hour == 14 or datetime.fromisoformat(trade["decision_at"]).hour == 13
            assert datetime.fromisoformat(trade["decision_at"]).date() == date.fromisoformat(trade["date"])
    assert all(point["cash"] >= -1e-5 and 0 <= point["gross_exposure"] <= 1 for point in result["equity_curve"])
    assert result["equity_curve"][-1]["cash"] == pytest.approx(result["equity_curve"][-1]["equity"])
    assert set(result["segments"]) == {"development", "validation", "holdout"}
    ledger_cash = _config().initial_cash + sum(
        (trade["notional"] - trade["costs"]) if trade["side"] == "SELL" else -(trade["notional"] + trade["costs"])
        for trade in result["trades"]
    )
    assert ledger_cash == pytest.approx(result["equity_curve"][-1]["equity"])
    assert sum(trade["realized_pnl"] for trade in result["trades"] if trade["side"] == "SELL") == pytest.approx(
        result["equity_curve"][-1]["equity"] - _config().initial_cash)


def test_open_gap_affects_fills_but_not_preopen_ranking(tmp_path):
    _, dataset = _dataset(tmp_path)
    first_day = _config().backtest_start
    cutoff = datetime(2016, 1, 4, 14, 15, tzinfo=timezone.utc)
    ranking = rank_prices(dataset, _config(), cutoff)
    raw = _raw()
    for symbol in ("AAPL", "MSFT", "JPM"):
        payload = json.loads(raw[symbol])
        for row in payload["values"]:
            if row["datetime"] == first_day.isoformat():
                for field in ("open", "high", "low", "close"):
                    row[field] = str(float(row[field]) * 1.1)
        raw[symbol] = canonical(payload)
    _, gap_dataset = save_price_dataset(raw, start=date(2015, 1, 1), end=date(2018, 12, 31),
                                        output_dir=tmp_path, source_kind="synthetic_test")
    assert rank_prices(gap_dataset, _config(), cutoff) == ranking
    before = run_price_backtest(dataset, _config())["trades"][0]
    after = run_price_backtest(gap_dataset, _config())["trades"][0]
    assert after["adjusted_price"] == pytest.approx(before["adjusted_price"] * 1.1)
    assert after["adjusted_units"] < before["adjusted_units"]


def test_cost_stress_reduces_returns_and_replay_is_deterministic(tmp_path):
    _, dataset = _dataset(tmp_path)
    first = run_price_study(dataset, _config())
    assert first == run_price_study(dataset, _config())
    assert first["cost_stress"]["2.0"]["strategy"]["total_return"] < first["backtest"]["strategy"]["total_return"]
    assert first["production_acceptance"]["passed"] is False
    assert first["execution_allowed"] is False


def test_frozen_config_rejects_invalid_windows_and_unsafe_limits():
    payload = _config().model_dump(mode="json")
    payload["max_position_weight"] = 0.9
    with pytest.raises(ValueError):
        PriceResearchConfig.model_validate(payload)
    payload = _config().model_dump(mode="json")
    payload["holdout_start"] = "2015-01-01"
    with pytest.raises(ValueError):
        PriceResearchConfig.model_validate(payload)


def test_key_loading_never_executes_shell_and_uses_last_assignment(tmp_path, monkeypatch):
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    path = tmp_path / ".env.local"
    path.write_text('TWELVE_DATA_API_KEY=""\nexport TWELVE_DATA_API_KEY="abcdefgh12345678"\n')
    assert read_price_key(tmp_path) == "abcdefgh12345678"
    path.write_text('TWELVE_DATA_API_KEY="$(touch /tmp/should-not-run)"\n')
    with pytest.raises(FreeDataError):
        read_price_key(tmp_path)


def test_basic_quota_is_shared_and_request_bursts_are_bounded(tmp_path):
    first = PriceQuota(tmp_path / "quota.sqlite", "account")
    second = PriceQuota(tmp_path / "quota.sqlite", "account")
    for index in range(7):
        assert first.reserve(100 + index) == 0
    assert second.reserve(107) > 0
    assert second.reserve(162) == 0
    for index in range(192):
        assert first.reserve(200 + index * 65) == 0
    with pytest.raises(FreeDataError, match="daily"):
        second.reserve(20_000)


def test_twelve_authentication_never_appears_in_url_or_cached_data(tmp_path):
    key = "abcdefgh12345678"
    requests = []
    payload = _payload("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    def handle(request):
        requests.append(request)
        assert request.headers["Authorization"] == f"apikey {key}"
        assert key not in str(request.url) and request.url.host == "api.twelvedata.com"
        return httpx.Response(200, json=payload)
    with TwelvePriceClient(key, quota_path=tmp_path / "quota.sqlite", transport=httpx.MockTransport(handle)) as client:
        raw = client.daily("AAPL", date(2025, 1, 2), date(2025, 1, 3))
        assert client.daily("AAPL", date(2025, 1, 2), date(2025, 1, 3)) == raw
    assert len(requests) == 1
    assert all(key.encode() not in path.read_bytes() for path in (tmp_path / "raw_cache").glob("*.json"))


def test_free_quota_wait_is_bounded_and_advances_before_request(tmp_path):
    key = "abcdefgh12345678"
    now = [107.0]
    waits = []
    def sleep(seconds):
        assert 0 < seconds <= 60
        waits.append(seconds)
        now[0] += seconds
    payload = _payload("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    with TwelvePriceClient(key, quota_path=tmp_path / "quota.sqlite", clock=lambda: now[0], sleep=sleep,
                           transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as client:
        for index in range(7):
            client._quota.reserve(100 + index)
        client.daily("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    assert waits and now[0] >= 161


def test_private_key_echo_or_vendor_error_body_is_never_archived(tmp_path):
    key = "abcdefgh12345678"
    payload = _payload("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    payload["private_echo"] = key
    with TwelvePriceClient(key, quota_path=tmp_path / "quota.sqlite",
                           transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as client:
        with pytest.raises(FreeDataError, match="private"):
            client.daily("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    assert not (tmp_path / "raw_cache").exists()


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_vendor_rejection_stops_without_identity_switch_or_hidden_paid_calls(tmp_path, status):
    calls = []
    def handle(request):
        calls.append(request)
        return httpx.Response(status)
    with TwelvePriceClient("abcdefgh12345678", quota_path=tmp_path / "quota.sqlite",
                           transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(FreeDataError, match=f"HTTP {status}"):
            client.daily("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    assert len(calls) == 1


def test_failed_or_partial_source_response_is_not_cached_as_complete(tmp_path):
    payload = _payload("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    payload["values"].pop()
    with TwelvePriceClient("abcdefgh12345678", quota_path=tmp_path / "quota.sqlite",
                           transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as client:
        with pytest.raises(FreeDataError, match="missing"):
            client.daily("AAPL", date(2025, 1, 2), date(2025, 1, 3))
    assert not (tmp_path / "raw_cache").exists()


def test_cli_refuses_synthetic_data_without_relabeling_it_real(tmp_path, capsys):
    path, _ = _dataset(tmp_path / "datasets")
    config = tmp_path / "config.json"
    config.write_bytes(canonical(_config().model_dump(mode="json")))
    assert main(["--dataset", str(path), "--config", str(config), "--output-dir", str(tmp_path / "out")]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "blocked"
    assert not (tmp_path / "out/latest.json").exists()


def test_cli_and_read_endpoint_success_with_simulated_vendor_transport(tmp_path, monkeypatch, capsys):
    from apps.api.routes.price_research import latest_price_research
    config = tmp_path / "config.json"
    config.write_bytes(canonical(_config().model_dump(mode="json")))
    path, _ = save_price_dataset(_raw(), start=date(2015, 1, 1), end=date(2018, 12, 31), output_dir=tmp_path / "datasets")
    output_dir = tmp_path / "out"
    assert main(["--dataset", str(path), "--config", str(config), "--output-dir", str(output_dir)]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "completed_research" and summary["execution_allowed"] is False
    monkeypatch.setattr("apps.api.routes.price_research._LATEST", output_dir / "latest.json")
    assert latest_price_research()["run_id"] == summary["run_id"]
    # Replays must not silently rewrite a different report under the same ID.
    report_path = output_dir / "runs" / summary["run_id"] / "report.json"
    report_path.write_text("changed")
    assert main(["--dataset", str(path), "--config", str(config), "--output-dir", str(output_dir)]) == 2


def test_report_fingerprint_and_authentication_are_enforced(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from apps.api.main import app
    path = tmp_path / "latest.json"
    monkeypatch.setattr("apps.api.routes.price_research._LATEST", path)
    with TestClient(app) as client:
        assert client.get("/research/price-only/latest").status_code == 401
        response = client.get("/research/price-only/latest", headers={"x-access-password": "test-access-password"})
        assert response.status_code == 200 and response.json()["status"] == "awaiting_real_data"
    _, dataset = _dataset(tmp_path / "dataset")
    report = run_price_study(dataset, _config())
    report["source_kind"] = "real_twelvedata"
    report["status"] = "completed_research"
    report["run_id"] = "price_study_" + digest({key: value for key, value in report.items() if key != "run_id"})[:24]
    path.write_bytes(canonical(report))
    from apps.api.routes.price_research import latest_price_research
    assert latest_price_research()["execution_allowed"] is False
    report["selection"]["data_date"] = "2026-01-01"
    path.write_bytes(canonical(report))
    from fastapi import HTTPException
    with pytest.raises(HTTPException, match="503"):
        latest_price_research()


def test_read_api_never_exposes_unsafe_or_synthetic_reports(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from apps.api.routes.price_research import latest_price_research
    path = tmp_path / "latest.json"
    monkeypatch.setattr("apps.api.routes.price_research._LATEST", path)
    assert latest_price_research()["status"] == "awaiting_real_data"
    _, dataset = _dataset(tmp_path / "dataset")
    report = run_price_study(dataset, _config())
    path.write_bytes(canonical(report))
    with pytest.raises(HTTPException):
        latest_price_research()
    report["source_kind"] = "real_twelvedata"
    report["execution_allowed"] = True
    path.write_bytes(canonical(report))
    with pytest.raises(HTTPException):
        latest_price_research()


def test_failed_refresh_does_not_republish_old_candidates_as_current(tmp_path, monkeypatch):
    from apps.api.routes.price_research import latest_price_research
    path = tmp_path / "latest.json"
    path.write_text('{"status":"old_result"}')
    attempt = tmp_path / "latest_attempt.json"
    attempt.write_text('{"status":"blocked","reason":"source unavailable"}')
    monkeypatch.setattr("apps.api.routes.price_research._LATEST", path)
    result = latest_price_research()
    assert result["status"] == "blocked" and result["selection"] is None
    assert result["last_success_available"] is True and result["execution_allowed"] is False


@pytest.mark.parametrize("filename,payload", [
    ("latest_attempt.json", []),
    ("latest_attempt.json", {"status": "unexpected"}),
    ("latest.json", []),
    ("latest.json", {"production_acceptance": []}),
])
def test_malformed_published_state_returns_service_unavailable(tmp_path, monkeypatch, filename, payload):
    from fastapi import HTTPException

    from apps.api.routes.price_research import latest_price_research
    monkeypatch.setattr("apps.api.routes.price_research._LATEST", tmp_path / "latest.json")
    (tmp_path / filename).write_bytes(canonical(payload))
    with pytest.raises(HTTPException, match="503"):
        latest_price_research()
