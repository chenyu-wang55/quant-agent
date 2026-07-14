from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

from infra.db.models import OperationalMetricRecord
from infra.db.session import SessionLocal

_PROMETHEUS_NAME = re.compile(r"[^a-zA-Z0-9_:]")


def _metric_value(value: float) -> float:
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError("metric values must be finite")
    return numeric


def _prometheus_name(metric: str) -> str:
    normalized = _PROMETHEUS_NAME.sub("_", metric.strip())
    if not normalized:
        raise ValueError("metric name cannot be empty")
    if normalized[0].isdigit():
        normalized = f"_{normalized}"
    return f"quant_agent_{normalized}"


@dataclass
class MetricsStore:
    """Process-safe SQLite-backed counters and gauges with an in-memory fallback."""

    counters: dict[str, float] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)
    persist: bool = True
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    def inc(self, metric: str, value: float = 1.0) -> None:
        increment = _metric_value(value)
        with self._lock:
            self.counters[metric] = self.counters.get(metric, 0.0) + increment
            if not self.persist:
                return
            now = datetime.now(timezone.utc)
            statement = sqlite_insert(OperationalMetricRecord).values(
                metric=metric,
                kind="counter",
                value=increment,
                updated_at=now,
            )
            statement = statement.on_conflict_do_update(
                index_elements=[OperationalMetricRecord.metric],
                set_={
                    "kind": "counter",
                    "value": OperationalMetricRecord.value + increment,
                    "updated_at": now,
                },
            )
            try:
                with SessionLocal.begin() as session:
                    session.execute(statement)
            except OperationalError:
                # The store can be constructed before the initial migration. The
                # local value remains available and later writes become durable.
                return

    def set_gauge(self, metric: str, value: float) -> None:
        numeric = _metric_value(value)
        with self._lock:
            self.gauges[metric] = numeric
            if not self.persist:
                return
            now = datetime.now(timezone.utc)
            statement = sqlite_insert(OperationalMetricRecord).values(
                metric=metric,
                kind="gauge",
                value=numeric,
                updated_at=now,
            )
            statement = statement.on_conflict_do_update(
                index_elements=[OperationalMetricRecord.metric],
                set_={"kind": "gauge", "value": numeric, "updated_at": now},
            )
            try:
                with SessionLocal.begin() as session:
                    session.execute(statement)
            except OperationalError:
                return

    def dump(self) -> dict[str, dict[str, float]]:
        counters = dict(self.counters)
        gauges = dict(self.gauges)
        if self.persist:
            try:
                with SessionLocal() as session:
                    records = list(session.execute(select(OperationalMetricRecord)).scalars())
                for record in records:
                    target = counters if record.kind == "counter" else gauges
                    target[record.metric] = float(record.value)
            except OperationalError:
                pass
        return {"counters": counters, "gauges": gauges}

    def prometheus_text(self) -> str:
        metrics = self.dump()
        lines: list[str] = []
        for kind in ("counter", "gauge"):
            values = metrics[f"{kind}s"]
            for metric, value in sorted(values.items()):
                name = _prometheus_name(metric)
                lines.append(f"# TYPE {name} {kind}")
                lines.append(f"{name} {float(value):.12g}")
        return "\n".join(lines) + ("\n" if lines else "")

    def clear(self) -> None:
        with self._lock:
            self.counters.clear()
            self.gauges.clear()
            if not self.persist:
                return
            try:
                with SessionLocal.begin() as session:
                    session.execute(delete(OperationalMetricRecord))
            except OperationalError:
                return
