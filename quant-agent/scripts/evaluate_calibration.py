from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.research.calibration import BinnedCalibrator, LabelObservation  # noqa: E402
from services.research.walk_forward import WalkForwardPlan, add_months  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate calibration in frozen purged rolling windows; no trading")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--labels", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest_raw, labels_raw = args.manifest.read_bytes(), args.labels.read_bytes()
    manifest = json.loads(manifest_raw)
    if manifest["status"] != "frozen" or manifest["data_kind"] != "authorized_point_in_time":
        raise ValueError("research manifest must be frozen with authorized PIT data")
    if hashlib.sha256(labels_raw).hexdigest() != manifest["labels_sha256"]:
        raise ValueError("labels do not match frozen manifest")
    rows = json.loads(labels_raw)
    labels = [LabelObservation(**{**item, "decision_at": datetime.fromisoformat(item["decision_at"]),
                                 "label_end": datetime.fromisoformat(item["label_end"])}) for item in rows]
    if len({item.observation_id for item in labels}) != len(labels):
        raise ValueError("duplicate research labels")
    window_settings = manifest["windows"]
    plan = WalkForwardPlan(**{**window_settings, "start": datetime.fromisoformat(window_settings["start"]),
                              "end": datetime.fromisoformat(window_settings["end"])})
    results = []
    for window in plan.windows():
        parts = plan.partition(labels, window)
        if any(len(part) < manifest["minimum_labels_per_partition"] for part in parts.values()):
            raise ValueError("insufficient_labels_in_walk_forward_partition")
        model = BinnedCalibrator.fit(parts["train"], training_end=window.validation_start,
                                    target=manifest["target"], dataset_fingerprint=manifest["dataset_fingerprint"],
                                    strategy_config_id=manifest["strategy_config_id"], bins=manifest["bins"])
        results.append({"window": {key: value.isoformat() for key, value in asdict(window).items()},
                        "model": asdict(model), "validation": model.evaluate(parts["validation"]),
                        "oos": model.evaluate(parts["oos"])})
    report = {"status": "evaluated", "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
              "labels_sha256": hashlib.sha256(labels_raw).hexdigest(), "windows": results,
              "holdout_start": add_months(plan.end, -plan.holdout_months).isoformat(),
              "holdout_used": False, "adopted": False,
              "scope": "probability_diagnostics_only_requires_separate_return_and_holdout_acceptance"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2))
    print(json.dumps({"windows": len(results), "adopted": False, "output": str(args.output.resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
