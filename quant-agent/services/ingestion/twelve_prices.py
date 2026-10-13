"""Basic-plan daily-price requests with private header authentication and local quota limits."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any

import httpx2 as httpx

from services.ingestion.free_sources import normalized_ticker
from services.ingestion.sec_facts import FreeDataError
from services.research.price_data import normalize_twelve


class PriceQuota:
    def __init__(self, path: Path, account: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path, self.account = path, account
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS requests (account TEXT, at REAL)")
            db.execute("CREATE INDEX IF NOT EXISTS request_clock ON requests(account,at)")

    def reserve(self, now: float) -> float:
        with closing(sqlite3.connect(self.path, timeout=10)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            day_start = now - now % 86400
            used = db.execute("SELECT count(*) FROM requests WHERE account=? AND at>=?", (self.account, day_start)).fetchone()[0]
            if used >= 200:  # below Basic's 800/day; other applications may also use the account
                raise FreeDataError("local free research daily request quota exhausted")
            recent = db.execute("SELECT at FROM requests WHERE account=? AND at>? ORDER BY at",
                                (self.account, now - 61)).fetchall()
            if len(recent) >= 7:  # below Basic's 8 credits/minute
                return max(0.1, recent[0][0] + 61 - now)
            db.execute("INSERT INTO requests VALUES (?,?)", (self.account, now))
        return 0.0


def read_price_key(root: Path) -> str:
    import os

    value = os.getenv("TWELVE_DATA_API_KEY", "").strip()
    path = root / ".env.local"
    if not value and path.is_file():
        for line in path.read_text().splitlines():
            match = re.fullmatch(r"\s*(?:export\s+)?TWELVE_DATA_API_KEY\s*=\s*(.*?)\s*", line)
            if match:
                value = match.group(1).strip().strip("\"'")
        value = value.strip("\"'")
    if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", value) or value.lower() == "demo":
        raise FreeDataError("configure a personal free Basic TWELVE_DATA_API_KEY in .env.local; demo data is not accepted")
    return value


class TwelvePriceClient:
    def __init__(self, api_key: str, *, quota_path: Path, transport: httpx.BaseTransport | None = None,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.time):
        if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", api_key) or api_key.lower() == "demo":
            raise FreeDataError("a personal free Basic key is required")
        self._key = api_key
        self._quota = PriceQuota(quota_path, hashlib.sha256(api_key.encode()).hexdigest())
        self._sleep, self._clock = sleep, clock
        self._cache = quota_path.parent / "raw_cache"
        self._client = httpx.Client(timeout=25, follow_redirects=False, transport=transport,
                                    headers={"Authorization": f"apikey {api_key}", "Accept": "application/json"})

    def __enter__(self) -> TwelvePriceClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self._client.close()

    def daily(self, symbol: str, start: date, end: date) -> bytes:
        params = {
            "symbol": normalized_ticker(symbol), "interval": "1day", "start_date": start.isoformat(),
            "end_date": end.isoformat(), "outputsize": 5000, "adjust": "all", "timezone": "America/New_York",
        }
        request_hash = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
        cache_path = self._cache / f"{request_hash}.json"
        if cache_path.exists():
            if cache_path.is_symlink():
                raise FreeDataError("daily-price cache cannot be a symbolic link")
            envelope = json.loads(cache_path.read_bytes())
            raw = bytes.fromhex(envelope["response_hex"])
            if envelope["params"] != params or hashlib.sha256(raw).hexdigest() != envelope["sha256"]:
                raise FreeDataError("daily-price cached bytes or request identity changed")
            normalize_twelve(json.loads(raw), symbol, start, end)
            return raw
        for _ in range(3):
            wait = self._quota.reserve(self._clock())
            if wait == 0:
                break
            self._sleep(min(wait, 60))
        else:
            raise FreeDataError("free request quota is busy; retry this batch later")
        response = self._client.get("https://api.twelvedata.com/time_series", params=params)
        if response.status_code != 200:
            raise FreeDataError(f"daily-price source HTTP {response.status_code}; stopped without changing source or identity")
        if len(response.content) > 8 * 1024 * 1024:
            raise FreeDataError("daily-price source response exceeds research size limit")
        payload = json.loads(response.content)
        if payload.get("status") != "ok":
            # Do not archive an error body which could echo the private key.
            code = payload.get("code")
            safe_code = str(code) if isinstance(code, int) and 0 <= code <= 599 else "unknown"
            raise FreeDataError(f"daily-price source rejected request, code {safe_code}")
        if self._key.encode() in response.content:
            raise FreeDataError("source response unexpectedly contains private authentication material")
        normalize_twelve(payload, symbol, start, end)
        self._cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        raw = json.dumps({"params": params, "sha256": hashlib.sha256(response.content).hexdigest(),
                          "response_hex": response.content.hex()}, sort_keys=True).encode()
        with tempfile.NamedTemporaryFile(dir=self._cache, delete=False) as handle:
            handle.write(raw)
            temporary = Path(handle.name)
        try:
            temporary.replace(cache_path)
        finally:
            temporary.unlink(missing_ok=True)
        return response.content
