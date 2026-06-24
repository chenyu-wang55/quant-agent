from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EventType(str, Enum):
    FEATURE_RECOMPUTATION = "feature_recomputation"
    RECOMMENDATION_READY = "recommendation_ready"
    PAPER_FILL = "paper_fill"
    MODEL_EVALUATION = "model_evaluation"
    SELL_ALERT = "sell_alert"
    PORTFOLIO_SELL = "portfolio_sell"


class EventStatus(str, Enum):
    PENDING = "pending"
    CONSUMED = "consumed"


class SystemEvent(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex[:16])
    event_type: EventType
    payload: dict
    created_at: datetime = Field(default_factory=utc_now)
    status: EventStatus = EventStatus.PENDING
