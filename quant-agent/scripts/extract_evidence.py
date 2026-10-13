from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.llm.client import BudgetLedger, EvidenceModelClient, ModelSettings  # noqa: E402
from services.llm.evidence import EvidenceDocument, render_extraction, validate_evidence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract quoted evidence; disabled unless explicitly configured")
    parser.add_argument("input", type=Path, help="JSON with decision_at and documents")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--archive-dir", type=Path, default=ROOT / "artifacts" / "llm")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.input.read_text())
    documents = [EvidenceDocument.model_validate(item) for item in payload["documents"]]
    decision_at = datetime.fromisoformat(payload["decision_at"])
    validate_evidence(documents, decision_at)
    settings = ModelSettings.from_env()
    if args.validate_only or not settings.enabled:
        print(json.dumps({"status": "validated" if args.validate_only else "disabled", "document_count": len(documents)}))
        return 0
    settings.require_configured()
    client = EvidenceModelClient(settings=settings, ledger=BudgetLedger(args.archive_dir / "budget.db"),
                                 archive_dir=args.archive_dir)
    try:
        result = client.extract(documents=documents, decision_at=decision_at, run_id=args.run_id)
    except Exception as exc:
        print(json.dumps({"status": "blocked", "error_type": type(exc).__name__, "facts": [],
                          "report_cn": "AI 抽取不可用，模型信号标记为缺失。继续使用证据驱动的规则报告；不添加未验证事实。"},
                         ensure_ascii=False))
        return 2
    print(json.dumps({"status": "extracted", "facts": result.model_dump() if result else None,
                      "report_cn": render_extraction(result, documents, decision_at) if result else ""}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
