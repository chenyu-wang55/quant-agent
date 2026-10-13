from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class LabelObservation:
    observation_id: str
    decision_at: datetime
    label_end: datetime
    signal_strength: float
    profitable_after_cost: bool

    def __post_init__(self) -> None:
        if (self.decision_at.tzinfo is None or self.label_end.tzinfo is None
                or self.label_end < self.decision_at
                or not math.isfinite(self.signal_strength) or not 0 <= self.signal_strength <= 1):
            raise ValueError("invalid timed calibration label")
        if not isinstance(self.profitable_after_cost, bool):
            raise ValueError("calibration outcome must be a boolean")


@dataclass(frozen=True)
class BinnedCalibrator:
    target: str
    training_end: str
    dataset_fingerprint: str
    strategy_config_id: str
    probabilities: tuple[float, ...]
    sample_counts: tuple[int, ...]
    baseline_probability: float
    sample_ids_hash: str

    @classmethod
    def fit(cls, observations: list[LabelObservation], *, training_end: datetime, target: str,
            dataset_fingerprint: str, strategy_config_id: str, bins: int = 10) -> BinnedCalibrator:
        """Fit on finalized training labels only; scores remain separate from probabilities."""
        if not observations or not target or not strategy_config_id or not 2 <= bins <= 50:
            raise ValueError("calibration requires labels, target, strategy and bounded bins")
        if len(dataset_fingerprint) != 64 or any(c not in "0123456789abcdef" for c in dataset_fingerprint):
            raise ValueError("calibration requires a dataset fingerprint")
        if training_end.tzinfo is None or any(item.label_end >= training_end for item in observations):
            raise ValueError("calibration labels cross training boundary")
        if len({item.observation_id for item in observations}) != len(observations):
            raise ValueError("duplicate calibration labels")
        counts, wins = [0] * bins, [0] * bins
        for item in observations:
            index = min(bins - 1, int(item.signal_strength * bins))
            counts[index] += 1
            wins[index] += int(item.profitable_after_cost)
        baseline = sum(item.profitable_after_cost for item in observations) / len(observations)
        # Shrink sparse bins toward training prevalence, never toward rule score.
        probabilities = tuple((wins[i] + 2 * baseline) / (counts[i] + 2) for i in range(bins))
        payload = [{**asdict(item), "decision_at": item.decision_at.isoformat(), "label_end": item.label_end.isoformat()}
                   for item in sorted(observations, key=lambda item: item.observation_id)]
        signature = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        return cls(target, training_end.isoformat(), dataset_fingerprint, strategy_config_id,
                   probabilities, tuple(counts), baseline, signature)

    def predict(self, signal_strength: float) -> float:
        if not math.isfinite(signal_strength) or not 0 <= signal_strength <= 1:
            raise ValueError("invalid rule strength")
        return self.probabilities[min(len(self.probabilities) - 1, int(signal_strength * len(self.probabilities)))]

    def evaluate(self, observations: list[LabelObservation]) -> dict:
        boundary = datetime.fromisoformat(self.training_end)
        if not observations or any(item.decision_at < boundary for item in observations):
            raise ValueError("evaluation must use subsequent unseen labels")
        predictions = [self.predict(item.signal_strength) for item in observations]
        outcomes = [int(item.profitable_after_cost) for item in observations]
        groups = []
        ece = baseline_ece = 0.0
        for index in range(10):
            members = [i for i, p in enumerate(predictions) if min(9, int(p * 10)) == index]
            if members:
                average = sum(predictions[i] for i in members) / len(members)
                frequency = sum(outcomes[i] for i in members) / len(members)
                ece += abs(average-frequency) * len(members) / len(observations)
                groups.append({"bin": index, "count": len(members), "probability": average, "frequency": frequency})
        baseline_ece = abs(self.baseline_probability - sum(outcomes)/len(outcomes))
        return {"sample_count": len(observations), "brier": sum((p-y)**2 for p, y in zip(predictions, outcomes))/len(outcomes),
                "ece": ece, "reliability": groups,
                "baseline_brier": sum((self.baseline_probability-y)**2 for y in outcomes)/len(outcomes),
                "baseline_ece": baseline_ece, "baseline_probability": self.baseline_probability}
