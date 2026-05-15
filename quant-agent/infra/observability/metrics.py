from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MetricsStore:
    counters: dict[str, float] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)

    def inc(self, metric: str, value: float = 1.0) -> None:
        self.counters[metric] = self.counters.get(metric, 0.0) + value

    def set_gauge(self, metric: str, value: float) -> None:
        self.gauges[metric] = value

    def dump(self) -> dict[str, dict[str, float]]:
        return {"counters": dict(self.counters), "gauges": dict(self.gauges)}
