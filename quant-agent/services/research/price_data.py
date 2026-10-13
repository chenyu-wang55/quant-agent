"""Dated, immutable daily-price research inputs. Never a production PIT approval."""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from services.execution.market_calendar import xnys_session
from services.ingestion.free_sources import normalized_ticker
from services.ingestion.sec_facts import FreeDataError

EASTERN = ZoneInfo("America/New_York")


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


@lru_cache(maxsize=256)
def _session_dates(start: date, end: date) -> tuple[date, ...]:
    if start > end:
        raise FreeDataError("price interval start must not exceed end")
    result = []
    day = start
    while day <= end:
        session = xnys_session(day)
        if session.holiday_name == "XNYS calendar unavailable":
            raise FreeDataError("price research date is outside the supported exchange calendar")
        if session.is_trading_day:
            result.append(day)
        day += timedelta(days=1)
    return tuple(result)


def sessions(start: date, end: date) -> list[date]:
    return list(_session_dates(start, end))


@lru_cache(maxsize=32768)
def next_session(day: date) -> date:
    for offset in range(1, 15):
        candidate = day + timedelta(days=offset)
        if xnys_session(candidate).is_trading_day:
            return candidate
    raise FreeDataError("cannot identify the next price publication session")


@dataclass(frozen=True)
class PriceBar:
    day: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    available_at: datetime

    def record(self) -> dict[str, Any]:
        return {**asdict(self), "day": self.day.isoformat(), "available_at": self.available_at.isoformat()}


def normalize_twelve(payload: dict[str, Any], symbol: str, start: date, end: date) -> tuple[PriceBar, ...]:
    if payload.get("status") != "ok" or not isinstance(payload.get("values"), list):
        raise FreeDataError("Twelve Data did not return an eligible daily time series")
    meta = payload.get("meta") or {}
    if (meta.get("symbol") != normalized_ticker(symbol) or meta.get("interval") != "1day"
            or meta.get("currency") != "USD" or meta.get("exchange_timezone") != "America/New_York"):
        raise FreeDataError("Twelve Data symbol, interval, currency or timezone does not match research scope")
    expected_dates = sessions(start, end)
    if not expected_dates:
        raise FreeDataError("daily-price interval contains no trading sessions")
    expected = set(expected_dates)
    publication_days = dict(zip(expected_dates[:-1], expected_dates[1:]))
    publication_days[expected_dates[-1]] = next_session(expected_dates[-1])
    bars: dict[date, PriceBar] = {}
    for item in payload["values"]:
        day = date.fromisoformat(item["datetime"])
        if not start <= day <= end:
            continue
        if day not in expected:
            raise FreeDataError("price source returned a non-trading date")
        values = {key: float(item[key]) for key in ("open", "high", "low", "close", "volume")}
        if any(not math.isfinite(value) for value in values.values()):
            raise FreeDataError("non-finite daily prices or volume")
        if min(values[key] for key in ("open", "high", "low", "close")) <= 0 or values["volume"] < 0:
            raise FreeDataError("invalid daily prices or volume")
        if values["high"] < max(values["open"], values["close"]) or values["low"] > min(values["open"], values["close"]):
            raise FreeDataError("inconsistent daily OHLC range")
        if day in bars:
            raise FreeDataError("duplicate daily price observation")
        # Publisher documents EOD availability at midnight on the next trading
        # day, not session close. Friday's data is not available on Saturday.
        available = datetime.combine(publication_days[day], datetime.min.time(), EASTERN).astimezone(timezone.utc)
        bars[day] = PriceBar(day=day, available_at=available, **values)
    missing = sorted(expected - set(bars))
    if missing:
        raise FreeDataError(f"{symbol} missing {len(missing)} research sessions, first {missing[0].isoformat()}")
    if not bars:
        raise FreeDataError("no complete daily prices for research")
    return tuple(bars[day] for day in sorted(bars))


@dataclass(frozen=True)
class PriceDataset:
    bars: dict[str, tuple[PriceBar, ...]]
    fingerprint: str
    manifest: dict[str, Any]


def save_price_dataset(raw_by_symbol: dict[str, bytes], *, start: date, end: date,
                       output_dir: Path, source_kind: str = "real_twelvedata") -> tuple[Path, PriceDataset]:
    if source_kind not in {"real_twelvedata", "synthetic_test"}:
        raise FreeDataError("unknown daily-price source kind")
    bars = {symbol: normalize_twelve(json.loads(raw), symbol, start, end) for symbol, raw in raw_by_symbol.items()}
    manifest: dict[str, Any] = {
        "schema_version": 1, "kind": "price_only_research_dataset", "source_kind": source_kind,
        "source_url": "https://api.twelvedata.com/time_series", "adjust": "all",
        "start": start.isoformat(), "end": end.isoformat(), "symbols": sorted(bars),
        "production_pit_verified": False, "survivorship_bias_free": False,
        "publication_rule": "next_XNYS_session_midnight_Eastern", "paid_endpoints_used": False,
        "files": {f"{symbol}.json": {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
                  for symbol, raw in sorted(raw_by_symbol.items())},
    }
    fingerprint = digest(manifest)
    manifest["dataset_fingerprint"] = fingerprint
    destination = output_dir.expanduser().resolve() / fingerprint
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return destination, load_price_dataset(destination)
    with tempfile.TemporaryDirectory(prefix=".collect-", dir=destination.parent) as temporary:
        staging = Path(temporary) / "snapshot"
        staging.mkdir(mode=0o700)
        for symbol, raw in raw_by_symbol.items():
            if symbol != normalized_ticker(symbol):
                raise FreeDataError("archive symbols must be canonical")
            target = staging / f"{symbol}.json"
            with target.open("xb") as handle:
                handle.write(raw)
            target.chmod(0o600)
        target = staging / "manifest.json"
        with target.open("xb") as handle:
            handle.write(canonical(manifest))
        target.chmod(0o600)
        try:
            os.rename(staging, destination)
        except OSError:
            if not destination.exists():
                raise
            return destination, load_price_dataset(destination)
    return destination, PriceDataset(bars=bars, fingerprint=fingerprint, manifest=manifest)


def load_price_dataset(path: Path) -> PriceDataset:
    path = path.expanduser().resolve()
    manifest_path = path / "manifest.json"
    if manifest_path.is_symlink():
        raise FreeDataError("daily-price manifest cannot be a symbolic link")
    manifest = json.loads(manifest_path.read_bytes())
    if not isinstance(manifest, dict) or manifest.get("source_kind") not in {"real_twelvedata", "synthetic_test"}:
        raise FreeDataError("unknown daily-price source kind")
    if manifest.get("schema_version") != 1 or manifest.get("kind") != "price_only_research_dataset":
        raise FreeDataError("not a daily-price research dataset")
    if manifest.get("production_pit_verified") is not False or manifest.get("adjust") != "all":
        raise FreeDataError("daily-price research requires explicit adjustment and unverified PIT status")
    fingerprint = str(manifest.get("dataset_fingerprint", ""))
    if fingerprint != digest({key: value for key, value in manifest.items() if key != "dataset_fingerprint"}):
        raise FreeDataError("daily-price manifest fingerprint mismatch")
    raw_by_symbol = {}
    for filename, info in manifest["files"].items():
        symbol = filename.removesuffix(".json")
        if filename != f"{normalized_ticker(symbol)}.json":
            raise FreeDataError("unsafe daily-price archive filename")
        target = path / filename
        if target.is_symlink():
            raise FreeDataError("daily-price archive cannot contain symbolic links")
        raw = target.read_bytes()
        if hashlib.sha256(raw).hexdigest() != info["sha256"] or len(raw) != info["bytes"]:
            raise FreeDataError("daily-price archive bytes changed")
        raw_by_symbol[symbol] = raw
    if set(raw_by_symbol) != set(manifest["symbols"]):
        raise FreeDataError("daily-price archive symbol coverage differs from manifest")
    bars = {symbol: normalize_twelve(json.loads(raw), symbol, date.fromisoformat(manifest["start"]),
                                    date.fromisoformat(manifest["end"])) for symbol, raw in raw_by_symbol.items()}
    return PriceDataset(bars=bars, fingerprint=fingerprint, manifest=manifest)
