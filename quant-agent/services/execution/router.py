from __future__ import annotations

import hashlib
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
        *,
        local_order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        local_order_id = local_order_id or uuid4().hex[:16]
        client_order_id = client_order_id or self._client_order_id(
            local_order_id=local_order_id, idempotency_key=request.idempotency_key
        )
        execution_mode = OrderExecutionMode(request.execution_mode)
        if execution_mode == OrderExecutionMode.PAPER:
            return self.paper_router.submit(
                recommendation=recommendation,
                request=request,
                positions=positions,
                local_order_id=local_order_id,
                client_order_id=client_order_id,
            )
        if execution_mode == OrderExecutionMode.LIVE:
            return self._submit_live(
                recommendation=recommendation,
                request=request,
                positions=positions,
                local_order_id=local_order_id,
                client_order_id=client_order_id,
            )
        raise ValueError(f"Unsupported execution mode: {request.execution_mode}")

    def prepare_intent(
        self,
        *,
        recommendation: Recommendation,
        request: PaperOrderRequest,
    ) -> PaperOrder:
        local_order_id = uuid4().hex[:16]
        client_order_id = self._client_order_id(
            local_order_id=local_order_id,
            idempotency_key=request.idempotency_key,
        )
        return PaperOrder(
            id=local_order_id,
            recommendation_id=request.recommendation_id,
            client_order_id=client_order_id,
            idempotency_key=request.idempotency_key,
            source_snapshot_id=recommendation.source_snapshot_id,
            strategy_config_id=recommendation.strategy_config_id,
            side=request.side,
            qty=request.qty,
            limit_price=request.limit_price,
            execution_mode=request.execution_mode,
            dry_run=request.dry_run,
            broker_order_id=None,
            adapter_message="submission_intent_reserved",
            submitted_at=datetime.now(timezone.utc),
            status=PaperOrderStatus.PENDING_SUBMIT,
        )

    @staticmethod
    def _client_order_id(*, local_order_id: str, idempotency_key: str | None) -> str:
        if not idempotency_key:
            return f"quant_{local_order_id}"
        digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:24]
        return f"quant_{digest}"

    def _submit_live(
        self,
        recommendation: Recommendation,
        request: PaperOrderRequest,
        positions: dict[str, PositionState],
        local_order_id: str,
        client_order_id: str,
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
                local_order_id=local_order_id,
                client_order_id=client_order_id,
            )

        submitted_at = datetime.now(timezone.utc)
        entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
        reference_price = request.limit_price if request.limit_price is not None else entry_mid
        order = PaperOrder(
            id=local_order_id,
            recommendation_id=request.recommendation_id,
            client_order_id=client_order_id,
            idempotency_key=request.idempotency_key,
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
        local_order_id: str,
        client_order_id: str,
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        if self.broker_adapter is None:
            raise NotImplementedError("Live broker execution adapter is not configured")

        try:
            broker_update = self.broker_adapter.submit_order(
                BrokerOrderPlacement(
                    client_order_id=client_order_id,
                    symbol=recommendation.ticker,
                    qty=request.qty,
                    side=request.side.value,
                    limit_price=request.limit_price,
                    order_class=("bracket" if request.side.value.lower() == "buy" else None),
                    take_profit_limit_price=(
                        recommendation.tp2 if request.side.value.lower() == "buy" else None
                    ),
                    stop_loss_price=(
                        recommendation.stop_loss if request.side.value.lower() == "buy" else None
                    ),
                )
            )
        except BrokerAdapterError:
            raise
        except Exception as exc:
            raise BrokerAdapterError(f"Live broker order submit failed: {exc}") from exc

        return self.order_from_broker_update(
            recommendation=recommendation,
            request=request,
            local_order_id=local_order_id,
            client_order_id=client_order_id,
            broker_update=broker_update,
        ), dict(positions)

    def order_from_broker_update(
        self,
        *,
        recommendation: Recommendation,
        request: PaperOrderRequest,
        local_order_id: str,
        client_order_id: str,
        broker_update,
    ) -> PaperOrder:
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
            client_order_id=client_order_id,
            idempotency_key=request.idempotency_key,
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
        return order
