from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from domain.entities.models import OrderExecutionMode
from infra.db.models import AutopilotPolicyRecord, ExecutionControlRecord, SystemEventRecord
from infra.db.session import SessionLocal
from infra.queue.events import EventType


def submission_mode(mode: OrderExecutionMode, *, dry_run: bool) -> str:
    return "live_dry_run" if mode == OrderExecutionMode.LIVE and dry_run else mode.value


def authorize_submission(*, order_id: str, execution_mode: str, automatic: bool = False) -> None:
    """Commit a durable permission before I/O, serialized with control writes.

    Lock writes committed before this transaction block submission. Permissions
    committed before a later lock are in flight, not retrospectively revoked.
    No database transaction is held across network I/O.
    """
    try:
        with SessionLocal() as session:
            session.execute(text("BEGIN IMMEDIATE"))
            control = session.scalars(select(ExecutionControlRecord).order_by(ExecutionControlRecord.id.desc())).first()
            if control is None or control.enabled:
                raise ValueError("Execution is blocked by kill switch (missing or enabled control)")
            # Intent provenance survives crashes before permission and broker I/O.
            # Recovery may reconcile an in-flight order, but a new send must still
            # obey the persisted policy that originally requested it.
            intents = session.scalars(select(SystemEventRecord).where(
                SystemEventRecord.event_type == EventType.EXECUTION_INTENT_RESERVED.value,
                SystemEventRecord.payload_json["order_id"].as_string() == order_id,
            ))
            intent_records = list(intents)
            automatic = automatic or any(event.payload_json.get("policy_required") is True for event in intent_records)
            revisions = {event.payload_json.get("policy_revision") for event in intent_records
                         if event.payload_json.get("policy_required") is True
                         and event.payload_json.get("policy_revision") is not None}
            policy = session.scalars(select(AutopilotPolicyRecord).order_by(AutopilotPolicyRecord.id.desc())).first()
            if automatic and (
                policy is None or not policy.enabled or not policy.auto_execute_approved
                or policy.auto_execution_mode != execution_mode
                or (bool(revisions) and revisions != {policy.id})
            ):
                raise ValueError("Automatic execution policy is disabled or changed")
            session.add(SystemEventRecord(
                id=uuid4().hex,
                event_type=EventType.EXECUTION_AUTHORIZED.value,
                payload_json={
                    "order_id": order_id, "execution_mode": execution_mode,
                    "automatic": automatic, "control_revision": control.id,
                    "policy_revision": policy.id if policy is not None else None,
                },
                created_at=datetime.now(timezone.utc), status="pending",
            ))
            session.commit()
    except SQLAlchemyError as exc:
        raise ValueError("execution_control_unavailable: submission blocked") from exc
