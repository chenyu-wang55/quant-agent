from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from infra.db.models import OperationalAlertRecord
from infra.db.session import SessionLocal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AlertSpec:
    key: str
    category: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


class OperationalAlertManager:
    def sync(self, alerts: Iterable[AlertSpec]) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        active = {alert.key: alert for alert in alerts}
        with SessionLocal.begin() as session:
            existing_active = list(
                session.execute(
                    select(OperationalAlertRecord).where(
                        OperationalAlertRecord.status == "active"
                    )
                ).scalars()
            )
            for record in existing_active:
                if record.alert_key not in active:
                    record.status = "resolved"
                    record.resolved_at = now
                    record.last_seen_at = now

            for alert in active.values():
                statement = sqlite_insert(OperationalAlertRecord).values(
                    alert_key=alert.key,
                    category=alert.category,
                    severity=alert.severity,
                    status="active",
                    message=alert.message,
                    details_json=alert.details,
                    first_seen_at=now,
                    last_seen_at=now,
                    resolved_at=None,
                )
                statement = statement.on_conflict_do_update(
                    index_elements=[OperationalAlertRecord.alert_key],
                    set_={
                        "category": alert.category,
                        "severity": alert.severity,
                        "status": "active",
                        "message": alert.message,
                        "details_json": alert.details,
                        "last_seen_at": now,
                        "resolved_at": None,
                    },
                )
                session.execute(statement)

        for alert in active.values():
            logger.warning(
                alert.message,
                extra={
                    "event": "operational_alert",
                    "alert_key": alert.key,
                    "alert_category": alert.category,
                    "alert_severity": alert.severity,
                },
            )
        return self.list_active()

    def list_active(self) -> list[dict[str, Any]]:
        with SessionLocal() as session:
            records = list(
                session.execute(
                    select(OperationalAlertRecord)
                    .where(OperationalAlertRecord.status == "active")
                    .order_by(
                        OperationalAlertRecord.severity.desc(),
                        OperationalAlertRecord.last_seen_at.desc(),
                    )
                ).scalars()
            )
        return [self._to_dict(record) for record in records]

    def clear(self) -> None:
        with SessionLocal.begin() as session:
            session.execute(delete(OperationalAlertRecord))

    @staticmethod
    def _to_dict(record: OperationalAlertRecord) -> dict[str, Any]:
        return {
            "key": record.alert_key,
            "category": record.category,
            "severity": record.severity,
            "status": record.status,
            "message": record.message,
            "details": dict(record.details_json or {}),
            "first_seen_at": record.first_seen_at.isoformat(),
            "last_seen_at": record.last_seen_at.isoformat(),
            "resolved_at": record.resolved_at.isoformat() if record.resolved_at else None,
        }
