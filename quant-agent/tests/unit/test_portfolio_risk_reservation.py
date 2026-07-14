from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from sqlalchemy import func, select

from infra.db.init_db import init_db
from infra.db.models import HoldingWatchRecord, PortfolioRiskReservationRecord
from infra.db.portfolio_risk_reservation import (
    ConcurrentPortfolioRiskLimitError,
    PortfolioRiskReservationRepository,
)
from infra.db.session import SessionLocal


def test_concurrent_orders_cannot_bypass_gross_exposure_limit() -> None:
    init_db()
    repository = PortfolioRiskReservationRepository()
    prefix = uuid4().hex
    with SessionLocal() as session:
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
        prior_reserved = float(
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
    account_equity = current_gross + prior_reserved + 100.0

    def reserve(index: int):
        return repository.reserve(
            order_id=f"{prefix}-{index}",
            ticker=f"T{index}",
            requested_notional=60.0,
            account_equity=account_equity,
            max_gross_exposure_pct=1.0,
        )

    outcomes: list[object] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(reserve, index) for index in range(2)]
        for future in futures:
            try:
                outcomes.append(future.result())
            except Exception as exc:  # asserted below
                outcomes.append(exc)

    assert sum(isinstance(item, dict) for item in outcomes) == 1
    errors = [item for item in outcomes if isinstance(item, Exception)]
    assert len(errors) == 1
    assert isinstance(errors[0], ConcurrentPortfolioRiskLimitError)
