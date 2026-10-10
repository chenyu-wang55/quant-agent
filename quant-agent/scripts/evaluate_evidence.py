from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.llm.evaluation import evaluate_extractions  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate frozen gold cases against archived responses; no API calls")
    parser.add_argument("--gold", required=True, type=Path)
    parser.add_argument("--predictions", required=True, type=Path, help="JSON map of case_id to response audit")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    raw = args.gold.read_bytes()
    predictions = args.predictions.read_bytes()
    report = evaluate_extractions(json.loads(raw), json.loads(predictions))
    report.update(gold_sha256=hashlib.sha256(raw).hexdigest(), predictions_sha256=hashlib.sha256(predictions).hexdigest())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({"quality_target_passed": report["quality_target_passed"], "output": str(args.output.resolve())}))
    return 0 if report["quality_target_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
