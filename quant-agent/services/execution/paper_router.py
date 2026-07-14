from __future__ import annotations

from datetime import datetime, timezone

from domain.entities.models import (
    OrderExecutionMode,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderStatus,
    PositionState,
    Recommendation,
)


class PaperExecutionRouter:
    def submit(
        self,
        recommendation: Recommendation,
        request: PaperOrderRequest,
        positions: dict[str, PositionState],
        local_order_id: str,
        client_order_id: str,
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        positions = {ticker: position.model_copy(deep=True) for ticker, position in positions.items()}
        submitted_at = datetime.now(timezone.utc)
        entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2
        fill_price = request.limit_price if request.limit_price is not None else entry_mid

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
            execution_mode=OrderExecutionMode.PAPER,
            dry_run=False,
            broker_order_id=f"paper_{local_order_id}",
            adapter_message="paper_fill_simulated",
            submitted_at=submitted_at,
            status=PaperOrderStatus.FILLED,
            simulated_fill_price=round(fill_price, 4),
            filled_at=submitted_at,
            cancel_reason=None,
        )

        ticker = recommendation.ticker
        current = positions.get(ticker)
        if current is None:
            positions[ticker] = PositionState(
                ticker=ticker,
                open_time=submitted_at,
                avg_price=round(fill_price, 4),
                qty=request.qty,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                stop_state="active",
                target_state="active",
                last_mark=round(fill_price, 4),
            )
        else:
            new_qty = current.qty + request.qty
            if new_qty <= 0:
                current.realized_pnl += (fill_price - current.avg_price) * current.qty
                current.qty = 0
                current.last_mark = round(fill_price, 4)
                current.stop_state = "closed"
                current.target_state = "closed"
            else:
                blended_avg = (current.avg_price * current.qty + fill_price * request.qty) / new_qty
                current.avg_price = round(blended_avg, 4)
                current.qty = new_qty
                current.last_mark = round(fill_price, 4)
            positions[ticker] = current

        return order, positions
