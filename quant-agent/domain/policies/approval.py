from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel


class ApprovalDecisionRequest(BaseModel):
    recommendation_id: str
    decision: str
    approver: str
    notes: str | None = None


@dataclass
class ApprovalPolicy:
    live_execution_requires_approval: bool = True
    require_reason_on_reject: bool = True

    def validate(self, request: ApprovalDecisionRequest) -> list[str]:
        issues: list[str] = []
        if request.decision not in {"approved", "rejected"}:
            issues.append("decision must be approved or rejected")
        if not request.approver.strip():
            issues.append("approver is required")
        if self.require_reason_on_reject and request.decision == "rejected":
            if not request.notes or not request.notes.strip():
                issues.append("notes are required for rejection")
        return issues
