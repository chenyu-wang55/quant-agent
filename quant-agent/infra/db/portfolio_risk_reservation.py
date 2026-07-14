from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, func, select, text

from infra.db.models import HoldingWatchRecord, PortfolioRiskReservationRecord
from infra.db.session import SessionLocal


class ConcurrentPortfolioRiskLimitError(ValueError):
    pass


class PortfolioRiskReservationRepository:
    def reserve(
        self,
        *,
        order_id: str,
        ticker: str,
        requested_notional: float,
        account_equity: float,
        max_gross_exposure_pct: float,
    ) -> dict[str, float]:
        now = datetime.now(timezone.utc)
        with SessionLocal() as session:
            if session.bind is not None and session.bind.dialect.name == "sqlite":
                session.execute(text("BEGIN IMMEDIATE"))
            existing = session.get(PortfolioRiskReservationRecord, order_id)
            if existing is not None:
                if (
                    existing.ticker != ticker.upper()
                    or abs(existing.requested_notional - requested_notional) > 1e-6
                ):
                    raise ConcurrentPortfolioRiskLimitError(
                        "portfolio risk reservation conflicts with existing order intent"
                    )
                return {
                    "current_gross": 0.0,
                    "active_reserved": float(existing.requested_notional),
                    "requested_notional": float(existing.requested_notional),
                    "gross_limit": float(existing.account_equity * existing.max_gross_exposure_pct),
                }

            current_gross = float(
                session.scalar(
                    select(
                        func.coalesce(
                            func.sum(HoldingWatchRecord.qty * HoldingWatchRecord.avg_buy_price),
                            0.0,
                        )
                    ).where(HoldingWatchRecord.status == "open")
                )
                or 0.0
            )
            active_reserved = float(
                session.scalar(
                    select(
                        func.coalesce(
                            func.sum(PortfolioRiskReservationRecord.requested_notional),
                            0.0,
                        )
                    ).where(PortfolioRiskReservationRecord.status == "active")
                )
                or 0.0
            )
            gross_limit = account_equity * max_gross_exposure_pct
            proposed = current_gross + active_reserved + requested_notional
            if proposed > gross_limit + 1e-6:
                raise ConcurrentPortfolioRiskLimitError(
                    "concurrent portfolio gross exposure limit exceeded: "
                    f"proposed={proposed:.2f}, limit={gross_limit:.2f}"
                )
            session.add(
                PortfolioRiskReservationRecord(
                    order_id=order_id,
                    ticker=ticker.upper(),
                    requested_notional=requested_notional,
                    account_equity=account_equity,
                    max_gross_exposure_pct=max_gross_exposure_pct,
                    status="active",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()
            return {
                "current_gross": current_gross,
                "active_reserved": active_reserved,
                "requested_notional": requested_notional,
                "gross_limit": gross_limit,
            }

    def release(self, order_id: str, *, status: str = "released") -> None:
        with SessionLocal() as session:
            record = session.get(PortfolioRiskReservationRecord, order_id)
            if record is None:
                return
            record.status = status
            record.updated_at = datetime.now(timezone.utc)
            session.commit()

    def clear_all(self) -> None:
        with SessionLocal() as session:
            session.execute(delete(PortfolioRiskReservationRecord))
            session.commit()
