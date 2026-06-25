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
    ORDER_ROUTED = "order_routed"
    PAPER_FILL = "paper_fill"
    MODEL_EVALUATION = "model_evaluation"
    SELL_ALERT = "sell_alert"
    SELL_ROUTED = "sell_routed"
    PORTFOLIO_SELL = "portfolio_sell"
    HOLDING_CONTROLS_UPDATED = "holding_controls_updated"


class EventStatus(str, Enum):
    PENDING = "pending"
    CONSUMED = "consumed"


class SystemEvent(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex[:16])
    event_type: EventType
    payload: dict
    created_at: datetime = Field(default_factory=utc_now)
    status: EventStatus = EventStatus.PENDING
