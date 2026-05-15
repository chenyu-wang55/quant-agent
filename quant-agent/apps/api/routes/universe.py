from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from apps.api.dependencies import AppState, get_app_state


router = APIRouter(tags=["universe"])


@router.get("/universe")
def get_universe(
    universe: str = Query(default="SP500"),
    as_of: datetime | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> dict:
    timestamp = as_of or datetime.utcnow()
    records = state.provider.get_universe(universe=universe, as_of=timestamp)
    return {
        "universe": universe,
        "as_of": timestamp.isoformat(),
        "count": len(records),
        "items": [item.model_dump() for item in records],
    }
