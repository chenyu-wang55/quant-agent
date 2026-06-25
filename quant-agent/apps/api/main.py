from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from apps.api.routes.backtests import router as backtests_router
from apps.api.routes.approvals import router as approvals_router
from apps.dashboard.main import router as dashboard_router
from apps.api.routes.execution_controls import router as execution_controls_router
from apps.api.routes.events import router as events_router
from apps.api.routes.health import router as health_router
from apps.api.routes.metrics import router as metrics_router
from apps.api.routes.operations import router as operations_router
from apps.api.routes.paper_orders import router as paper_orders_router
from apps.api.routes.portfolio import router as portfolio_router
from apps.api.routes.positions import router as positions_router
from apps.api.routes.recommendations import router as recommendations_router
from apps.api.routes.research import router as research_router
from apps.api.routes.signals import router as signals_router
from apps.api.routes.source_snapshots import router as source_snapshots_router
from apps.api.routes.strategy_configs import router as strategy_configs_router
from apps.api.routes.universe import router as universe_router
from infra.observability.logging import configure_logging


configure_logging()

ACCESS_PASSWORD = os.getenv("QUANT_AGENT_ACCESS_PASSWORD")
AUTH_DISABLED = os.getenv("QUANT_AGENT_DISABLE_AUTH", "0") == "1"
PUBLIC_PATHS = {"/health"}

app = FastAPI(
    title="Quant Research and Trading Recommendation API",
    version="0.1.0",
    description="PRD-aligned quant decision-support API",
)


@app.middleware("http")
async def access_password_middleware(request: Request, call_next):
    path = request.url.path
    is_docs = path.startswith("/docs") or path.startswith("/openapi") or path.startswith("/redoc")
    if path not in PUBLIC_PATHS and not is_docs and not AUTH_DISABLED:
        if not ACCESS_PASSWORD:
            return JSONResponse(
                status_code=503,
                content={"detail": "QUANT_AGENT_ACCESS_PASSWORD is not configured"},
            )
        provided = request.headers.get("x-access-password") or request.query_params.get("pwd")
        if provided != ACCESS_PASSWORD:
            return JSONResponse(
                status_code=401,
                content={"detail": "Unauthorized: provide valid access password"},
            )
    return await call_next(request)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)

app.include_router(health_router)
app.include_router(universe_router)
app.include_router(research_router)
app.include_router(recommendations_router)
app.include_router(approvals_router)
app.include_router(dashboard_router)
app.include_router(signals_router)
app.include_router(source_snapshots_router)
app.include_router(strategy_configs_router)
app.include_router(portfolio_router)
app.include_router(paper_orders_router)
app.include_router(positions_router)
app.include_router(backtests_router)
app.include_router(execution_controls_router)
app.include_router(events_router)
app.include_router(metrics_router)
app.include_router(operations_router)


def run() -> None:
    uvicorn.run("apps.api.main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    run()
