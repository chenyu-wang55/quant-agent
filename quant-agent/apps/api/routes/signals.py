from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import SignalSnapshot

router = APIRouter(tags=["signals"])


@router.get("/signals/{ticker}", response_model=SignalSnapshot)
def get_signal(
    ticker: str,
    state: AppState = Depends(get_app_state),
) -> SignalSnapshot:
    signal = state.signals_by_ticker.get(ticker)
    if signal is None:
        signal = state.signal_repo.get_latest_by_ticker(ticker)
    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found for ticker")
    return signal
