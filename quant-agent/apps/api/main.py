from __future__ import annotations

import logging
import re
import time
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from apps.api.auth import (
    auth_is_configured,
    authenticate_request,
    csrf_is_valid,
    required_role,
    role_allows,
)
from apps.api.auth import (
    router as auth_router,
)
from apps.api.routes.approvals import router as approvals_router
from apps.api.routes.backtests import router as backtests_router
from apps.api.routes.events import router as events_router
from apps.api.routes.execution_controls import router as execution_controls_router
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
from apps.dashboard.main import router as dashboard_router
from infra.config import CoreSettings
from infra.observability.logging import configure_logging, log_context

configure_logging()

logger = logging.getLogger(__name__)
_CORRELATION_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

AUTH_DISABLED = CoreSettings.from_env().auth_disabled
PUBLIC_PATHS = {
    "/health",
    "/health/live",
    "/health/ready",
    "/login",
    "/auth/login",
    "/favicon.ico",
}

app = FastAPI(
    title="Quant Research and Trading Recommendation API",
    version="0.1.0",
    description="PRD-aligned quant decision-support API",
)


@app.middleware("http")
async def access_password_middleware(request: Request, call_next):
    candidate = (request.headers.get("x-correlation-id") or "").strip()
    correlation_id = candidate if _CORRELATION_ID.fullmatch(candidate) else uuid4().hex
    started = time.perf_counter()
    with log_context(correlation_id=correlation_id):
        path = request.url.path

        def finalize(response: Response) -> Response:
            duration_ms = round((time.perf_counter() - started) * 1000.0, 3)
            response.headers["x-correlation-id"] = correlation_id
            logger.info(
                "request completed",
                extra={
                    "event": "http_request",
                    "method": request.method,
                    "path": path,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                },
            )
            return response

        if "pwd" in request.query_params:
            return finalize(
                JSONResponse(
                    status_code=400,
                    content={"detail": "URL password authentication is disabled; use /login or request headers"},
                )
            )
        if path not in PUBLIC_PATHS and not AUTH_DISABLED:
            if not auth_is_configured():
                return finalize(
                    JSONResponse(
                        status_code=503,
                        content={"detail": "QUANT_AGENT_ACCESS_PASSWORD is not configured"},
                    )
                )
            context = authenticate_request(request)
            if context is None:
                if path == "/dashboard" or path.startswith("/dashboard/"):
                    return finalize(
                        RedirectResponse(url=f"/login?next={path}", status_code=303)
                    )
                return finalize(
                    JSONResponse(
                        status_code=401,
                        content={"detail": "Unauthorized"},
                    )
                )
            needed = required_role(request.method, path)
            if not role_allows(context.role, needed):
                return finalize(
                    JSONResponse(
                        status_code=403,
                        content={"detail": f"Forbidden: {needed} role required"},
                    )
                )
            if not csrf_is_valid(request, context):
                return finalize(
                    JSONResponse(status_code=403, content={"detail": "CSRF validation failed"})
                )
            request.state.auth = context
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "request failed",
                extra={"event": "http_request", "method": request.method, "path": path},
            )
            raise
        return finalize(response)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)

app.include_router(health_router)
app.include_router(auth_router)
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
