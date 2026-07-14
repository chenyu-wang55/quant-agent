from __future__ import annotations

from typing import Callable

from sqlalchemy import text

from domain.entities.models import HoldingWatch, SellExecutionAudit, TradeLedgerEntry
from infra.db.models import HoldingWatchRecord, SystemEventRecord, TradeLedgerRecord
from infra.db.repositories import SellExecutionAuditRepository
from infra.db.session import SessionLocal
from infra.queue.events import SystemEvent


class ConcurrentPortfolioUpdateError(RuntimeError):
    pass


class SellUnitOfWork:
    """Commit a sell audit, holding update, trade row, and event atomically."""

    def __init__(self, before_commit: Callable[[], None] | None = None) -> None:
        self.before_commit = before_commit

    def commit(
        self,
        *,
        audit: SellExecutionAudit,
        holding: HoldingWatch | None,
        trade: TradeLedgerEntry | None,
        event: SystemEvent,
    ) -> bool:
        ledger_applied = False
        with SessionLocal() as session:
            if session.bind is not None and session.bind.dialect.name == "sqlite":
                session.execute(text("BEGIN IMMEDIATE"))
            session.merge(SellExecutionAuditRepository._to_record(audit))

            if trade is not None:
                existing_trade = session.get(TradeLedgerRecord, trade.trade_id)
                if existing_trade is None:
                    if holding is None:
                        raise RuntimeError("Sell trade requires a holding update")
                    current_holding = session.get(HoldingWatchRecord, holding.ticker)
                    expected_qty_before = round(holding.qty + trade.qty, 6)
                    if current_holding is None or abs(current_holding.qty - expected_qty_before) > 1e-6:
                        actual_qty = current_holding.qty if current_holding is not None else None
                        raise ConcurrentPortfolioUpdateError(
                            f"Holding {holding.ticker} changed concurrently: "
                            f"expected qty {expected_qty_before}, found {actual_qty}"
                        )
                    session.merge(
                        HoldingWatchRecord(
                            ticker=holding.ticker,
                            qty=holding.qty,
                            avg_buy_price=holding.avg_buy_price,
                            bought_at=holding.bought_at,
                            source_recommendation_id=holding.source_recommendation_id,
                            stop_loss=holding.stop_loss,
                            take_profit1=holding.take_profit1,
                            take_profit2=holding.take_profit2,
                            note=holding.note,
                            status=holding.status.value,
                            updated_at=holding.updated_at,
                            realized_pnl=holding.realized_pnl,
                            closed_at=holding.closed_at,
                            last_sell_price=holding.last_sell_price,
                            last_sell_reason=holding.last_sell_reason,
                        )
                    )
                    session.add(
                        TradeLedgerRecord(
                            trade_id=trade.trade_id,
                            ticker=trade.ticker,
                            side=trade.side.value,
                            qty=trade.qty,
                            price=trade.price,
                            executed_at=trade.executed_at,
                            source_recommendation_id=trade.source_recommendation_id,
                            source_snapshot_id=trade.source_snapshot_id,
                            strategy_config_id=trade.strategy_config_id,
                            reason=trade.reason,
                            realized_pnl_delta=trade.realized_pnl_delta,
                            holding_status_after=(
                                trade.holding_status_after.value if trade.holding_status_after else None
                            ),
                            created_at=trade.created_at,
                        )
                    )
                    ledger_applied = True

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
        return ledger_applied
