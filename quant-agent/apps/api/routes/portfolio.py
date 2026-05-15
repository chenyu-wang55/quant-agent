from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import HoldingWatch, ManualBuyRequest, SellAlert


router = APIRouter(tags=["portfolio"])


@router.post("/portfolio/buys", response_model=HoldingWatch)
def record_manual_buy(
    request: ManualBuyRequest,
    state: AppState = Depends(get_app_state),
) -> HoldingWatch:
    if request.qty <= 0:
        raise HTTPException(status_code=400, detail="qty must be greater than 0")
    if request.buy_price <= 0:
        raise HTTPException(status_code=400, detail="buy_price must be greater than 0")
    return state.record_manual_buy(request)


@router.get("/portfolio/holdings", response_model=list[HoldingWatch])
def list_holdings(state: AppState = Depends(get_app_state)) -> list[HoldingWatch]:
    return state.list_open_holdings()


@router.post("/portfolio/holdings/{ticker}/close", response_model=HoldingWatch)
def close_holding(
    ticker: str,
    state: AppState = Depends(get_app_state),
) -> HoldingWatch:
    closed = state.close_holding(ticker)
    if closed is None:
        raise HTTPException(status_code=404, detail="holding not found")
    return closed


@router.get("/portfolio/alerts", response_model=list[SellAlert])
def get_sell_alerts(
    as_of: datetime | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[SellAlert]:
    return state.monitor_sell_alerts(as_of=as_of)
