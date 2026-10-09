from __future__ import annotations

from datetime import datetime, timedelta, timezone

from domain.entities.models import BacktestRunResult
from infra.db.init_db import init_db
from infra.db.repositories import BacktestRunRepository


def _result(
    *,
    run_id: str,
    created_at: datetime,
    accepted: bool,
    fingerprint: str = "a" * 64,
) -> BacktestRunResult:
    return BacktestRunResult(
        run_id=run_id,
        created_at=created_at,
        config_hash="c" * 24,
        strategy_config_id="strat_verified",
        dataset_fingerprint=fingerprint,
        metrics={"max_drawdown": 0.1},
        notes="retained acceptance evidence",
        segments={"out_of_sample": {"trade_count": 100, "profit_factor": 1.3}},
        acceptance={"passed": accepted, "checks": {}, "reasons": [] if accepted else ["failed"]},
        assumptions={"point_in_time_validation": {"status": "verified"}},
    )


def test_backtest_repository_retains_and_filters_accepted_evidence() -> None:
    init_db()
    repository = BacktestRunRepository()
    repository.clear_all()
    now = datetime.now(timezone.utc)
    rejected = _result(run_id="bt-rejected", created_at=now, accepted=False)
    accepted = _result(run_id="bt-accepted", created_at=now + timedelta(seconds=1), accepted=True)

    repository.upsert(rejected)
    repository.upsert(accepted)

    assert [item.run_id for item in repository.list_recent()] == ["bt-accepted", "bt-rejected"]
    match = repository.latest_accepted(
        strategy_config_id="strat_verified",
        dataset_fingerprint="a" * 64,
    )
    assert match is not None
    assert match.run_id == "bt-accepted"
    assert (
        repository.latest_accepted(
            strategy_config_id="strat_verified",
            dataset_fingerprint="b" * 64,
        )
        is None
    )
