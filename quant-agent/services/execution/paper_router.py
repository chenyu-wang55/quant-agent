from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from domain.entities.models import (
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
    ) -> tuple[PaperOrder, dict[str, PositionState]]:
        submitted_at = datetime.now(timezone.utc)
        entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2
        fill_price = request.limit_price if request.limit_price is not None else entry_mid

        order = PaperOrder(
            id=uuid4().hex[:16],
            recommendation_id=request.recommendation_id,
            side=request.side,
            qty=request.qty,
            limit_price=request.limit_price,
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
