from __future__ import annotations

import hashlib
import json

from domain.entities.models import AutopilotPolicy

# Bump when routing, approval, sizing or safety behavior changes.
EXECUTION_BEHAVIOR_VERSION = "2026-10-10-safety-v1"


def paper_evidence_signature(strategy_config_id: str | None, policy: AutopilotPolicy) -> str:
    behavior = policy.model_dump(mode="json", exclude={
        "policy_id", "enabled", "auto_execute_approved", "auto_execution_mode",
        "reason", "updated_at", "updated_by", "min_paper_shadow_trading_days",
    })
    payload = {"strategy_config_id": strategy_config_id, "policy": behavior,
               "execution_behavior_version": EXECUTION_BEHAVIOR_VERSION}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
