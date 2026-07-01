from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from domain.entities.models import (
    OrderExecutionMode,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderStatus,
    PositionState,
    Recommendation,
)
from services.execution.paper_router import PaperExecutionRouter


class ExecutionRouter:
    def __init__(self, paper_router: PaperExecutionRouter | None = None) -> None:
        self.paper_router = paper_router or PaperExecutionRouter()

    def submit(
        self,
        recommendation: Recommendation,
        request: PaperOrderRequest,
        positions: dict[str, PositionState],
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        execution_mode = OrderExecutionMode(request.execution_mode)
        if execution_mode == OrderExecutionMode.PAPER:
            return self.paper_router.submit(
                recommendation=recommendation,
                request=request,
                positions=positions,
            )
        if execution_mode == OrderExecutionMode.LIVE:
            return self._submit_live(
                recommendation=recommendation,
                request=request,
                positions=positions,
            )
        raise ValueError(f"Unsupported execution mode: {request.execution_mode}")

    @staticmethod
    def _submit_live(
        recommendation: Recommendation,
        request: PaperOrderRequest,
        positions: dict[str, PositionState],
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        if not request.dry_run and not request.confirm_live:
            raise ValueError("Live execution requires dry_run=true or confirm_live=true")
        if not request.dry_run:
            raise NotImplementedError("Live broker execution adapter is not configured")

        submitted_at = datetime.now(timezone.utc)
        entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
        reference_price = request.limit_price if request.limit_price is not None else entry_mid
        order = PaperOrder(
            id=uuid4().hex[:16],
            recommendation_id=request.recommendation_id,
            source_snapshot_id=recommendation.source_snapshot_id,
            strategy_config_id=recommendation.strategy_config_id,
            side=request.side,
            qty=request.qty,
            limit_price=request.limit_price,
            execution_mode=OrderExecutionMode.LIVE,
            dry_run=True,
            broker_order_id=f"live_dryrun_{uuid4().hex[:12]}",
            adapter_message=(
                "live_dry_run_only: order was validated but not sent to a broker"
            ),
            submitted_at=submitted_at,
            status=PaperOrderStatus.SUBMITTED,
            simulated_fill_price=round(reference_price, 4),
            filled_at=None,
            cancel_reason=None,
        )
        return order, dict(positions)
