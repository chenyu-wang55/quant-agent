from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException

from services.research.price_data import digest

router = APIRouter(tags=["price-research"])
_LATEST = Path(__file__).resolve().parents[3] / "artifacts/price_research/latest.json"


@router.get("/research/price-only/latest")
def latest_price_research() -> dict:
    attempt_path = _LATEST.with_name("latest_attempt.json")
    attempt = None
    if attempt_path.exists():
        try:
            if attempt_path.is_symlink():
                raise ValueError("unsafe attempt file")
            attempt = json.loads(attempt_path.read_bytes())
            if not isinstance(attempt, dict) or attempt.get("status") not in {"collecting", "blocked", "completed_research"}:
                raise ValueError("invalid attempt status")
            if attempt.get("status") in {"collecting", "blocked"}:
                return {"status": attempt["status"], "reason": attempt.get("reason"), "execution_allowed": False,
                        "selection": None, "backtest": None, "last_success_available": _LATEST.is_file()}
        except (ValueError, OSError, TypeError) as exc:
            raise HTTPException(status_code=503, detail="price research attempt status is invalid") from exc
    if not _LATEST.is_file():
        return {"status": "awaiting_real_data", "execution_allowed": False, "selection": None, "backtest": None}
    if _LATEST.is_symlink():
        raise HTTPException(status_code=503, detail="price research report integrity check failed")
    try:
        result = json.loads(_LATEST.read_bytes())
        if not isinstance(result, dict) or not isinstance(result.get("production_acceptance"), dict):
            raise ValueError("invalid research report structure")
        if (result.get("schema_version") != 1 or result.get("status") != "completed_research"
                or result.get("source_kind") != "real_twelvedata" or result.get("execution_allowed") is not False
                or result.get("production_acceptance", {}).get("passed") is not False):
            raise ValueError("ineligible research report")
        expected = "price_study_" + digest({key: value for key, value in result.items() if key != "run_id"})[:24]
        if result.get("run_id") != expected:
            raise ValueError("price research report fingerprint mismatch")
        if attempt and attempt.get("run_id") != result.get("run_id"):
            raise ValueError("latest report differs from latest completed attempt")
        return result
    except (ValueError, OSError, TypeError) as exc:
        raise HTTPException(status_code=503, detail="price research report is invalid") from exc
