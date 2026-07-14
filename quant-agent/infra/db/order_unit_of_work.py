from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import text

from domain.entities.models import (
    Direction,
    HoldingStatus,
    HoldingWatch,
    PaperOrder,
    PaperOrderStatus,
    PositionState,
    Recommendation,
)
from infra.db.models import (
    HoldingWatchRecord,
    PortfolioRiskReservationRecord,
    PositionStateRecord,
    SystemEventRecord,
    TradeLedgerRecord,
)
from infra.db.repositories import PaperOrderRepository
from infra.db.session import SessionLocal
from infra.queue.events import SystemEvent


@dataclass(frozen=True)
class OrderCommitResult:
    holding: HoldingWatch | None
    ledger_applied: bool


class OrderUnitOfWork:
    """Commit all local effects of a routed buy order as one transaction."""

    def __init__(self, before_commit: Callable[[], None] | None = None) -> None:
        self.before_commit = before_commit

    def commit(
        self,
        *,
        order: PaperOrder,
        positions: dict[str, PositionState],
        recommendation: Recommendation | None,
        events: list[SystemEvent],
    ) -> OrderCommitResult:
        holding: HoldingWatch | None = None
        ledger_applied = False
        with SessionLocal() as session:
            # SQLite's default deferred transaction permits two writers to make
            # decisions from stale state. Acquire the write lock before reading
            # the holding ledger so weighted quantity/price updates serialize.
            if session.bind is not None and session.bind.dialect.name == "sqlite":
                session.execute(text("BEGIN IMMEDIATE"))

            session.merge(PaperOrderRepository._to_record(order))
            reservation = session.get(PortfolioRiskReservationRecord, order.id)
            if reservation is not None:
                if order.status == PaperOrderStatus.FILLED:
                    reservation.status = "committed"
                    reservation.updated_at = datetime.now(timezone.utc)
                elif order.status in {
                    PaperOrderStatus.CANCELED,
                    PaperOrderStatus.SUBMIT_FAILED,
                }:
                    reservation.status = "released"
                    reservation.updated_at = datetime.now(timezone.utc)
            for position in positions.values():
                session.merge(
                    PositionStateRecord(
                        ticker=position.ticker,
                        open_time=position.open_time,
                        avg_price=position.avg_price,
                        qty=position.qty,
                        realized_pnl=position.realized_pnl,
                        unrealized_pnl=position.unrealized_pnl,
                        stop_state=position.stop_state,
                        target_state=position.target_state,
                        last_mark=position.last_mark,
                    )
                )

            if (
                recommendation is not None
                and order.status == PaperOrderStatus.FILLED
                and order.side == Direction.BUY
                and order.simulated_fill_price is not None
            ):
                holding, ledger_applied = self._apply_buy_fill(
                    session=session,
                    order=order,
                    recommendation=recommendation,
                )

            for event in events:
                session.merge(
                    SystemEventRecord(
                        id=event.id,
                        event_type=event.event_type.value,
                        payload_json=event.payload,
                        created_at=event.created_at,
                        status=event.status.value,
                    )
                )
            if self.before_commit is not None:
                self.before_commit()
            session.commit()

        return OrderCommitResult(holding=holding, ledger_applied=ledger_applied)

    @staticmethod
    def _apply_buy_fill(*, session, order: PaperOrder, recommendation: Recommendation) -> tuple[HoldingWatch, bool]:
        trade_id = f"order_fill_{order.id}"
        existing_trade = session.get(TradeLedgerRecord, trade_id)
        existing = session.get(HoldingWatchRecord, recommendation.ticker.upper())
        if existing_trade is not None:
            if existing is None:
                raise RuntimeError(f"Order {order.id} has a trade ledger row but no holding row")
            return OrderUnitOfWork._holding_to_domain(existing), False

        ticker = recommendation.ticker.upper()
        fill_price = round(float(order.simulated_fill_price or 0.0), 6)
        fill_time = order.filled_at or order.submitted_at
        now = datetime.now(timezone.utc)
        if existing is not None and existing.status == HoldingStatus.OPEN.value:
            new_qty = round(existing.qty + order.qty, 6)
            avg_buy_price = round(
                (existing.avg_buy_price * existing.qty + fill_price * order.qty) / max(new_qty, 1e-9),
                6,
            )
            bought_at = existing.bought_at
            realized_pnl = float(existing.realized_pnl or 0.0)
        else:
            new_qty = round(order.qty, 6)
            avg_buy_price = fill_price
            bought_at = fill_time
            realized_pnl = 0.0

        holding_record = HoldingWatchRecord(
            ticker=ticker,
            qty=new_qty,
            avg_buy_price=avg_buy_price,
            bought_at=bought_at,
            source_recommendation_id=recommendation.id,
            stop_loss=recommendation.stop_loss,
            take_profit1=recommendation.tp1,
            take_profit2=recommendation.tp2,
            note=f"paper_order_fill:{order.id}",
            status=HoldingStatus.OPEN.value,
            updated_at=now,
            realized_pnl=realized_pnl,
            closed_at=None,
            last_sell_price=existing.last_sell_price if existing is not None else None,
            last_sell_reason=existing.last_sell_reason if existing is not None else None,
        )
        session.merge(holding_record)
        session.add(
            TradeLedgerRecord(
                trade_id=trade_id,
                ticker=ticker,
                side="buy",
                qty=round(order.qty, 6),
                price=fill_price,
                executed_at=fill_time,
                source_recommendation_id=recommendation.id,
                source_snapshot_id=recommendation.source_snapshot_id,
                strategy_config_id=recommendation.strategy_config_id,
                reason=f"paper_order_fill:{order.id}",
                realized_pnl_delta=0.0,
                holding_status_after=HoldingStatus.OPEN.value,
                created_at=now,
            )
        )
        return OrderUnitOfWork._holding_to_domain(holding_record), True

    @staticmethod
    def _holding_to_domain(record: HoldingWatchRecord) -> HoldingWatch:
        return HoldingWatch(
            ticker=record.ticker,
            qty=record.qty,
            avg_buy_price=record.avg_buy_price,
            bought_at=record.bought_at,
            source_recommendation_id=record.source_recommendation_id,
            stop_loss=record.stop_loss,
            take_profit1=record.take_profit1,
            take_profit2=record.take_profit2,
            note=record.note,
            status=HoldingStatus(record.status),
            updated_at=record.updated_at,
            realized_pnl=float(record.realized_pnl or 0.0),
            closed_at=record.closed_at,
            last_sell_price=record.last_sell_price,
            last_sell_reason=record.last_sell_reason,
        )
