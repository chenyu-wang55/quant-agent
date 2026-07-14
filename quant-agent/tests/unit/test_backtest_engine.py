from __future__ import annotations

from datetime import datetime, timezone

import pytest

from domain.entities.models import (
    BacktestRunRequest,
    MarketBar,
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
)
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.pipeline import ResearchPipeline
from services.research.backtest_engine import BacktestEngine, OpenPosition


def test_backtest_engine_produces_historical_metrics() -> None:
    provider = MockMarketDataProvider()
    validation_calls: list[tuple[datetime, datetime]] = []

    def validate_backtest_data(start: datetime, end: datetime) -> dict[str, str]:
        validation_calls.append((start, end))
        return {
            "status": "verified",
            "dataset_fingerprint": "unit-test-dataset-fingerprint",
        }

    provider.validate_backtest_data = validate_backtest_data  # type: ignore[attr-defined]
    pipeline = ResearchPipeline(provider=provider)
    template = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="unit-test-template",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        risk_policy=RiskPolicy(
            min_confidence=0,
            earnings_blackout_minutes=0,
            max_name_weight=1,
            max_sector_weight=10,
            max_gross_exposure=10,
            max_correlated_cluster_weight=10,
            reject_on_material_evidence_conflict=False,
            event_trading_enabled=True,
        ),
        publication=PublicationConfig(top_n=5),
    )
    request = BacktestRunRequest(
        run_name="unit-backtest",
        start_date=datetime(2025, 4, 10, 9, 30, tzinfo=timezone.utc),
        end_date=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        benchmark="SPY",
        top_n=5,
        rebalance_frequency="monthly",
        transaction_cost_bps=10.0,
    )

    result = BacktestEngine().run(request=request, pipeline=pipeline, template_request=template)

    assert result.metrics["periods"] >= 1
    assert result.metrics["recommendation_count"] >= result.metrics["traded_count"]
    assert 0.0 <= result.metrics["fill_rate"] <= 1.0
    assert "annualized_return" in result.metrics
    assert "annualized_benchmark_return" in result.metrics
    assert set(result.segments) == {"train", "validation", "out_of_sample"}
    assert all(segment["periods"] > 0 for segment in result.segments.values())
    assert "turnover" in result.metrics
    assert "total_fees" in result.metrics
    assert "total_slippage" in result.metrics
    assert result.assumptions["calendar"] == "XNYS"
    assert result.assumptions["same_bar_stop_target_priority"] == "stop_first_conservative"
    assert result.assumptions["point_in_time_validation"]["status"] == "verified"
    assert validation_calls == [(request.start_date, request.end_date)]
    assert sum(trade["side"] == "buy" for trade in result.trades) == result.metrics["traded_count"]
    buy_times = [trade["entry_at"] for trade in result.trades if trade["side"] == "buy"]
    assert buy_times == sorted(buy_times)

    repeated_provider = MockMarketDataProvider()
    repeated_provider.validate_backtest_data = (  # type: ignore[attr-defined]
        lambda _start, _end: {
            "status": "verified",
            "dataset_fingerprint": "unit-test-dataset-fingerprint",
        }
    )
    repeated = BacktestEngine().run(
        request=request,
        pipeline=ResearchPipeline(provider=repeated_provider),
        template_request=template,
    )
    assert repeated.run_id == result.run_id
    assert repeated.config_hash == result.config_hash
    assert repeated.metrics == result.metrics
    assert repeated.trades == result.trades

    changed_provider = MockMarketDataProvider()
    changed_provider.validate_backtest_data = (  # type: ignore[attr-defined]
        lambda _start, _end: {
            "status": "verified",
            "dataset_fingerprint": "changed-unit-test-dataset-fingerprint",
        }
    )
    changed = BacktestEngine().run(
        request=request,
        pipeline=ResearchPipeline(provider=changed_provider),
        template_request=template,
    )
    assert changed.config_hash != result.config_hash
    assert changed.run_id != result.run_id


class _CorporateActionProvider:
    def __init__(self, bar: MarketBar) -> None:
        self.bar = bar

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260):
        return [self.bar]


class _Pipeline:
    def __init__(self, provider) -> None:
        self.provider = provider


def test_backtest_applies_splits_dividends_gap_stops_and_bilateral_costs() -> None:
    entry_at = datetime(2026, 4, 9, 20, 0, tzinfo=timezone.utc)
    bar = MarketBar(
        ticker="AAPL",
        timestamp=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        open=44,
        high=50,
        low=40,
        close=46,
        volume=100_000,
        dividend=1.0,
        split_factor=2.0,
        source="test",
        quality_status="verified",
    )
    positions = {
        "AAPL": OpenPosition(
            ticker="AAPL",
            qty=10,
            entry_price=100,
            entry_at=entry_at,
            stop_loss=90,
            tp1=120,
            tp2=140,
            confidence=0.8,
            recommendation_id="rec-1",
            source_snapshot_id="snapshot-1",
            last_processed_at=entry_at,
        )
    }
    request = BacktestRunRequest(
        start_date=entry_at,
        end_date=bar.timestamp,
        transaction_cost_bps=10,
        slippage_bps=5,
    )

    cash, trades, stats = BacktestEngine()._advance_positions(
        positions=positions,
        cash=0,
        pipeline=_Pipeline(_CorporateActionProvider(bar)),  # type: ignore[arg-type]
        through=bar.timestamp,
        request=request,
        force_liquidation=False,
    )

    assert positions == {}
    assert trades[0]["reason"] == "stop_gap"
    assert trades[0]["qty"] == 20
    assert trades[0]["exit_price"] < bar.open
    assert trades[0]["dividends"] == 20
    assert stats["corporate_actions"] == 2
    assert stats["gap_fills"] == 1
    assert stats["fees"] > 0
    assert stats["slippage"] > 0
    assert cash > 0


class _MultiBarProvider:
    def __init__(self, bars: list[MarketBar]) -> None:
        self.bars = bars

    def get_bars(self, ticker: str, as_of: datetime, lookback_days: int = 260):
        return self.bars


def test_partial_exits_allocate_dividends_once_across_remaining_shares() -> None:
    entry_at = datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc)
    bars = [
        MarketBar(
            ticker="AAPL",
            timestamp=datetime(2026, 4, day, 20, 0, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=80,
            close=90,
            volume=5,
            dividend=1.0 if day == 9 else 0.0,
            source="test",
            quality_status="verified",
        )
        for day in (9, 10)
    ]
    positions = {
        "AAPL": OpenPosition(
            ticker="AAPL",
            qty=10,
            entry_price=100,
            entry_at=entry_at,
            stop_loss=90,
            tp1=120,
            tp2=140,
            confidence=0.8,
            recommendation_id="rec-partial",
            source_snapshot_id="snapshot-partial",
            last_processed_at=entry_at,
        )
    }
    pipeline = _Pipeline(_MultiBarProvider(bars))  # type: ignore[arg-type]
    request = BacktestRunRequest(
        start_date=entry_at,
        end_date=bars[-1].timestamp,
        transaction_cost_bps=0,
        slippage_bps=0,
        max_volume_participation=1.0,
    )

    cash, first_trades, first_stats = BacktestEngine()._advance_positions(
        positions=positions,
        cash=0,
        pipeline=pipeline,
        through=bars[0].timestamp,
        request=request,
        force_liquidation=False,
    )
    assert first_stats["partial_fills"] == 1
    assert first_trades[0]["qty"] == 5
    assert first_trades[0]["dividends"] == pytest.approx(5)
    assert positions["AAPL"].dividends == pytest.approx(5)

    cash, final_trades, _ = BacktestEngine()._advance_positions(
        positions=positions,
        cash=cash,
        pipeline=pipeline,
        through=bars[1].timestamp,
        request=request,
        force_liquidation=False,
    )
    assert positions == {}
    assert final_trades[0]["dividends"] == pytest.approx(5)
    assert sum(trade["dividends"] for trade in first_trades + final_trades) == pytest.approx(10)
    assert cash == pytest.approx(910)


def test_confidence_calibration_reports_brier_score_and_ece() -> None:
    calibration = BacktestEngine._confidence_calibration(
        [
            {"confidence": 0.9, "realized_return": 0.10},
            {"confidence": 0.8, "realized_return": -0.10},
        ],
        bins=2,
    )

    assert calibration["sample_count"] == 2
    assert calibration["brier_score"] == pytest.approx(0.325)
    assert calibration["ece"] == pytest.approx(0.35)
