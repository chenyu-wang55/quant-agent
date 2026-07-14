from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def _utc_timestamp_text(value: datetime | str) -> str:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(timespec="microseconds")


def market_bar_content_hash(
    *,
    vendor_id: str,
    ticker: str,
    timestamp: datetime | str,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    adjusted_close: float | None,
    dividend: float,
    split_factor: float,
    quality_status: str,
    provenance: dict[str, Any] | None,
) -> str:
    """Hash every persisted bar field so snapshot references remain immutable."""

    payload = {
        "adjusted_close": float(adjusted_close) if adjusted_close is not None else None,
        "close": float(close),
        "dividend": float(dividend),
        "high": float(high),
        "low": float(low),
        "open": float(open_price),
        "provenance": dict(provenance or {}),
        "quality_status": str(quality_status),
        "split_factor": float(split_factor),
        "ticker": ticker.upper(),
        "timestamp": _utc_timestamp_text(timestamp),
        "vendor_id": str(vendor_id),
        "volume": float(volume),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
