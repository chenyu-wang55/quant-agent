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
from services.execution.broker_adapter import (
    BrokerAdapterError,
    BrokerExecutionAdapter,
    BrokerOrderPlacement,
    build_broker_adapter_from_env,
)
from services.execution.paper_router import PaperExecutionRouter


class ExecutionRouter:
    def __init__(
        self,
        paper_router: PaperExecutionRouter | None = None,
        broker_adapter: BrokerExecutionAdapter | None = None,
    ) -> None:
        self.paper_router = paper_router or PaperExecutionRouter()
        self.broker_adapter = broker_adapter if broker_adapter is not None else build_broker_adapter_from_env()

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

    def _submit_live(
        self,
        recommendation: Recommendation,
        request: PaperOrderRequest,
        positions: dict[str, PositionState],
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        if not request.dry_run and not request.confirm_live:
            raise ValueError("Live execution requires dry_run=true or confirm_live=true")
        if not request.dry_run:
            if self.broker_adapter is None:
                raise NotImplementedError("Live broker execution adapter is not configured")
            return self._submit_live_broker_order(
                recommendation=recommendation,
                request=request,
                positions=positions,
            )

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

    def _submit_live_broker_order(
        self,
        recommendation: Recommendation,
        request: PaperOrderRequest,
        positions: dict[str, PositionState],
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        if self.broker_adapter is None:
            raise NotImplementedError("Live broker execution adapter is not configured")

        local_order_id = uuid4().hex[:16]
        client_order_id = f"quant_{local_order_id}"
        try:
            broker_update = self.broker_adapter.submit_order(
                BrokerOrderPlacement(
                    client_order_id=client_order_id,
                    symbol=recommendation.ticker,
                    qty=request.qty,
                    side=request.side.value,
                    limit_price=request.limit_price,
                )
            )
        except BrokerAdapterError:
            raise
        except Exception as exc:
            raise BrokerAdapterError(f"Live broker order submit failed: {exc}") from exc

        raw_status = broker_update.raw_status.lower()
        if raw_status == "filled":
            if broker_update.filled_avg_price is None:
                raise BrokerAdapterError("Live broker returned filled without filled_avg_price")
            status = PaperOrderStatus.FILLED
        elif raw_status in {"canceled", "expired", "rejected"}:
            status = PaperOrderStatus.CANCELED
        else:
            status = PaperOrderStatus.SUBMITTED

        adapter_message = (
            f"{self.broker_adapter.name}: status={broker_update.raw_status}; "
            f"client_order_id={broker_update.client_order_id or client_order_id}"
        )
        if broker_update.message:
            adapter_message = f"{adapter_message}; message={broker_update.message}"

        submitted_at = broker_update.submitted_at or datetime.now(timezone.utc)
        order = PaperOrder(
            id=local_order_id,
            recommendation_id=request.recommendation_id,
            source_snapshot_id=recommendation.source_snapshot_id,
            strategy_config_id=recommendation.strategy_config_id,
            side=request.side,
            qty=request.qty,
            limit_price=request.limit_price,
            execution_mode=OrderExecutionMode.LIVE,
            dry_run=False,
            broker_order_id=broker_update.broker_order_id,
            adapter_message=adapter_message,
            submitted_at=submitted_at,
            status=status,
            simulated_fill_price=(
                round(float(broker_update.filled_avg_price), 6)
                if broker_update.filled_avg_price is not None
                else None
            ),
            filled_at=broker_update.filled_at if status == PaperOrderStatus.FILLED else None,
            cancel_reason=broker_update.message if status == PaperOrderStatus.CANCELED else None,
        )
        return order, dict(positions)
