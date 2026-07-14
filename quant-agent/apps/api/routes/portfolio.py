from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import (
    AlertExecutionResult,
    AlertSellRequest,
    BrokerOrderSyncRequest,
    BrokerOrderSyncResult,
    HoldingControlAudit,
    HoldingControlUpdateRequest,
    HoldingControlUpdateResult,
    HoldingStatus,
    HoldingWatch,
    ManualBuyRequest,
    ManualSellRequest,
    PortfolioPerformance,
    PortfolioSummary,
    PositionReconciliationReport,
    PositionReconciliationRequest,
    RecommendationAttributionReport,
    SellAlert,
    SellAlertAudit,
    SellAlertLevel,
    SellExecutionAudit,
    SellExecutionResult,
    TradeLedgerEntry,
    TradeSide,
)
from services.execution.broker_adapter import BrokerAdapterError

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


def _parse_holding_status(status: str) -> HoldingStatus | None:
    normalized = status.lower()
    if normalized == "all":
        return None
    if normalized in {HoldingStatus.OPEN.value, HoldingStatus.CLOSED.value}:
        return HoldingStatus(normalized)
    raise HTTPException(status_code=400, detail="status must be open, closed, or all")


def _parse_trade_side(side: str | None) -> TradeSide | None:
    if side is None:
        return None
    normalized = side.lower()
    if normalized in {TradeSide.BUY.value, TradeSide.SELL.value}:
        return TradeSide(normalized)
    raise HTTPException(status_code=400, detail="side must be buy or sell")


@router.get("/portfolio/holdings", response_model=list[HoldingWatch])
def list_holdings(
    status: str = Query(default="open"),
    limit: int = Query(default=100, ge=1, le=500),
    state: AppState = Depends(get_app_state),
) -> list[HoldingWatch]:
    return state.list_holdings(status=_parse_holding_status(status), limit=limit)


@router.get("/portfolio/summary", response_model=PortfolioSummary)
def get_portfolio_summary(state: AppState = Depends(get_app_state)) -> PortfolioSummary:
    return state.get_portfolio_summary()


@router.get("/portfolio/performance", response_model=PortfolioPerformance)
def get_portfolio_performance(
    limit: int = Query(default=10_000, ge=1, le=50_000),
    state: AppState = Depends(get_app_state),
) -> PortfolioPerformance:
    return state.get_portfolio_performance(limit=limit)


@router.get("/portfolio/recommendation-attribution", response_model=RecommendationAttributionReport)
def get_recommendation_attribution(
    limit: int = Query(default=10_000, ge=1, le=50_000),
    state: AppState = Depends(get_app_state),
) -> RecommendationAttributionReport:
    return state.get_recommendation_attribution(limit=limit)


@router.get("/portfolio/trades", response_model=list[TradeLedgerEntry])
def list_trade_ledger(
    limit: int = Query(default=100, ge=1, le=500),
    ticker: str | None = Query(default=None),
    side: str | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[TradeLedgerEntry]:
    return state.list_trade_ledger(limit=limit, ticker=ticker, side=_parse_trade_side(side))


@router.get("/portfolio/holding-control-audits", response_model=list[HoldingControlAudit])
def list_holding_control_audits(
    limit: int = Query(default=100, ge=1, le=500),
    ticker: str | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[HoldingControlAudit]:
    return state.list_holding_control_audits(limit=limit, ticker=ticker)


@router.get("/portfolio/sell-executions", response_model=list[SellExecutionAudit])
def list_sell_execution_audits(
    limit: int = Query(default=100, ge=1, le=500),
    ticker: str | None = Query(default=None),
    dry_run: bool | None = Query(default=None),
    applied_to_ledger: bool | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[SellExecutionAudit]:
    return state.list_sell_execution_audits(
        limit=limit,
        ticker=ticker,
        dry_run=dry_run,
        applied_to_ledger=applied_to_ledger,
    )


@router.post("/portfolio/sell-executions/broker-sync", response_model=BrokerOrderSyncResult)
def sync_broker_sell_statuses(
    request: BrokerOrderSyncRequest,
    state: AppState = Depends(get_app_state),
) -> BrokerOrderSyncResult:
    return state.sync_broker_sell_statuses(request)


@router.post("/portfolio/reconciliation", response_model=PositionReconciliationReport)
def reconcile_broker_positions(
    request: PositionReconciliationRequest,
    state: AppState = Depends(get_app_state),
) -> PositionReconciliationReport:
    try:
        return state.reconcile_broker_positions(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio/reconciliations", response_model=list[PositionReconciliationReport])
def list_position_reconciliations(
    limit: int = Query(default=100, ge=1, le=500),
    broker: str | None = Query(default=None),
    status: str | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[PositionReconciliationReport]:
    return state.list_position_reconciliations(limit=limit, broker=broker, status=status)


@router.get("/portfolio/alert-history", response_model=list[SellAlertAudit])
def list_sell_alert_audits(
    limit: int = Query(default=100, ge=1, le=500),
    ticker: str | None = Query(default=None),
    reason_code: str | None = Query(default=None),
    level: SellAlertLevel | None = Query(default=None),
    monitor_run_id: str | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[SellAlertAudit]:
    return state.list_sell_alert_audits(
        limit=limit,
        ticker=ticker,
        reason_code=reason_code,
        level=level,
        monitor_run_id=monitor_run_id,
    )


@router.post("/portfolio/holdings/{ticker}/close", response_model=HoldingWatch)
def close_holding(
    ticker: str,
    state: AppState = Depends(get_app_state),
) -> HoldingWatch:
    closed = state.close_holding(ticker)
    if closed is None:
        raise HTTPException(status_code=404, detail="holding not found")
    return closed


@router.patch("/portfolio/holdings/{ticker}/controls", response_model=HoldingControlUpdateResult)
def update_holding_controls(
    ticker: str,
    request: HoldingControlUpdateRequest,
    state: AppState = Depends(get_app_state),
) -> HoldingControlUpdateResult:
    try:
        return state.update_holding_controls(ticker=ticker, request=request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="open holding not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/portfolio/holdings/{ticker}/sell", response_model=SellExecutionResult)
def sell_holding(
    ticker: str,
    request: ManualSellRequest,
    state: AppState = Depends(get_app_state),
) -> SellExecutionResult:
    try:
        return state.sell_holding(ticker, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="open holding not found") from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    except BrokerAdapterError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio/alerts", response_model=list[SellAlert])
def get_sell_alerts(
    as_of: datetime | None = Query(default=None),
    state: AppState = Depends(get_app_state),
) -> list[SellAlert]:
    return state.monitor_sell_alerts(as_of=as_of)


@router.post("/portfolio/alerts/{ticker}/execute", response_model=AlertExecutionResult)
def execute_sell_alert(
    ticker: str,
    request: AlertSellRequest,
    state: AppState = Depends(get_app_state),
) -> AlertExecutionResult:
    try:
        return state.execute_sell_alert(ticker=ticker, request=request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="open holding not found") from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    except BrokerAdapterError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
