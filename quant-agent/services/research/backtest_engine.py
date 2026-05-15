from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone

from domain.entities.models import (
    BacktestRunRequest,
    BacktestRunResult,
    Direction,
    MarketBar,
    Recommendation,
    ResearchRunRequest,
    RunType,
)
from services.ranking.pipeline import ResearchPipeline


def _frequency_to_step_days(rebalance_frequency: str) -> int:
    value = rebalance_frequency.strip().lower()
    mapping = {
        "daily": 1,
        "weekly": 5,
        "biweekly": 10,
        "monthly": 21,
    }
    return mapping.get(value, 21)


def _business_days(start: datetime, end: datetime, step_days: int) -> list[datetime]:
    points: list[datetime] = []
    current = start.astimezone(timezone.utc)
    business_day_idx = 0
    while current <= end:
        if current.weekday() < 5:
            if business_day_idx % max(step_days, 1) == 0:
                points.append(current)
            business_day_idx += 1
        current += timedelta(days=1)
    return points


def _annualized_return(period_returns: list[float], periods_per_year: float) -> float:
    if not period_returns:
        return 0.0
    equity = 1.0
    for value in period_returns:
        equity *= 1.0 + value
    total_periods = len(period_returns)
    if total_periods == 0 or periods_per_year <= 0:
        return 0.0
    years = total_periods / periods_per_year
    if years <= 0:
        return 0.0
    if equity <= 0:
        return -1.0
    return equity ** (1.0 / years) - 1.0


class BacktestEngine:
    def run(
        self,
        request: BacktestRunRequest,
        pipeline: ResearchPipeline,
        template_request: ResearchRunRequest,
    ) -> BacktestRunResult:
        step_days = _frequency_to_step_days(request.rebalance_frequency)
        as_of_points = _business_days(request.start_date, request.end_date, step_days)
        if len(as_of_points) == 1:
            next_point = min(request.end_date, as_of_points[0] + timedelta(days=step_days))
            if next_point > as_of_points[0]:
                as_of_points.append(next_point)

        period_returns: list[float] = []
        benchmark_returns: list[float] = []
        recommendation_count = 0
        traded_count = 0

        for idx, point in enumerate(as_of_points):
            if idx + 1 < len(as_of_points):
                period_end = as_of_points[idx + 1]
            else:
                period_end = min(request.end_date, point + timedelta(days=step_days))

            if period_end <= point:
                continue

            run_request = template_request.model_copy(
                update={
                    "run_type": RunType.BACKTEST_EVALUATION,
                    "as_of": point,
                    "publication": template_request.publication.model_copy(update={"top_n": request.top_n}),
                }
            )
            run_output = pipeline.run(run_request)
            recs = run_output.result.recommendations
            recommendation_count += len(recs)

            trades: list[float] = []
            for rec in recs:
                realized = self._simulate_trade_return(
                    recommendation=rec,
                    pipeline=pipeline,
                    period_start=point,
                    period_end=period_end,
                    tx_cost_bps=request.transaction_cost_bps,
                )
                if realized is not None:
                    traded_count += 1
                    trades.append(realized)

            period_returns.append(sum(trades) / len(trades) if trades else 0.0)
            benchmark_returns.append(
                self._simulate_benchmark_return(
                    pipeline=pipeline,
                    benchmark=request.benchmark,
                    period_start=point,
                    period_end=period_end,
                )
            )

        avg_return = sum(period_returns) / len(period_returns) if period_returns else 0.0
        avg_benchmark_return = sum(benchmark_returns) / len(benchmark_returns) if benchmark_returns else 0.0
        hit_rate = (
            sum(1 for value in period_returns if value > 0) / len(period_returns)
            if period_returns
            else 0.0
        )
        volatility = self._std(period_returns)
        periods_per_year = 252.0 / max(1.0, float(step_days))
        sharpe = (avg_return / volatility) * math.sqrt(periods_per_year) if volatility > 0 else 0.0
        annualized_return = _annualized_return(period_returns, periods_per_year)
        annualized_benchmark = _annualized_return(benchmark_returns, periods_per_year)
        alpha_annualized = annualized_return - annualized_benchmark

        excess_returns = [p - b for p, b in zip(period_returns, benchmark_returns)]
        excess_volatility = self._std(excess_returns)
        information_ratio = (
            (sum(excess_returns) / len(excess_returns)) / excess_volatility * math.sqrt(periods_per_year)
            if excess_returns and excess_volatility > 0
            else 0.0
        )

        max_drawdown = self._max_drawdown(period_returns)
        benchmark_max_drawdown = self._max_drawdown(benchmark_returns)
        fill_rate = traded_count / recommendation_count if recommendation_count > 0 else 0.0

        config_hash = hashlib.sha1(request.model_dump_json().encode("utf-8")).hexdigest()[:16]
        return BacktestRunResult(
            run_id=hashlib.sha1(f"{config_hash}|{datetime.now(timezone.utc).isoformat()}".encode("utf-8")).hexdigest()[:16],
            created_at=datetime.now(timezone.utc),
            config_hash=config_hash,
            metrics={
                "periods": float(len(period_returns)),
                "recommendation_count": float(recommendation_count),
                "traded_count": float(traded_count),
                "fill_rate": round(fill_rate, 6),
                "avg_period_return": round(avg_return, 6),
                "avg_period_benchmark_return": round(avg_benchmark_return, 6),
                "hit_rate": round(hit_rate, 6),
                "volatility": round(volatility, 6),
                "sharpe": round(sharpe, 6),
                "annualized_return": round(annualized_return, 6),
                "annualized_benchmark_return": round(annualized_benchmark, 6),
                "alpha_annualized": round(alpha_annualized, 6),
                "information_ratio": round(information_ratio, 6),
                "max_drawdown": round(max_drawdown, 6),
                "benchmark_max_drawdown": round(benchmark_max_drawdown, 6),
            },
            notes="Historical walk-forward backtest using provider market bars and rule-based entry/exit simulation.",
        )

    @staticmethod
    def _window_bars(
        pipeline: ResearchPipeline,
        ticker: str,
        period_start: datetime,
        period_end: datetime,
    ) -> list[MarketBar]:
        bars = pipeline.provider.get_bars(ticker=ticker, as_of=period_end, lookback_days=420)
        start_utc = period_start.astimezone(timezone.utc)
        end_utc = period_end.astimezone(timezone.utc)
        return [bar for bar in bars if start_utc < bar.timestamp <= end_utc]

    def _simulate_trade_return(
        self,
        recommendation: Recommendation,
        pipeline: ResearchPipeline,
        period_start: datetime,
        period_end: datetime,
        tx_cost_bps: float,
    ) -> float | None:
        try:
            bars = self._window_bars(
                pipeline=pipeline,
                ticker=recommendation.ticker,
                period_start=period_start,
                period_end=period_end,
            )
        except Exception:
            return None

        if not bars:
            return None

        entry = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
        tx_cost = tx_cost_bps / 10_000.0

        entry_index: int | None = None
        for idx, bar in enumerate(bars):
            if bar.low <= recommendation.entry_zone_high and bar.high >= recommendation.entry_zone_low:
                entry_index = idx
                break

        if entry_index is None:
            return None

        direction = recommendation.direction
        exit_price = bars[-1].close
        for bar in bars[entry_index:]:
            if direction == Direction.BUY:
                stop_hit = bar.low <= recommendation.stop_loss
                target2_hit = bar.high >= recommendation.tp2
                target1_hit = bar.high >= recommendation.tp1
                if stop_hit:
                    exit_price = recommendation.stop_loss
                    break
                if target2_hit:
                    exit_price = recommendation.tp2
                    break
                if target1_hit:
                    exit_price = recommendation.tp1
                    break
            else:
                stop_hit = bar.high >= recommendation.stop_loss
                target2_hit = bar.low <= recommendation.tp2
                target1_hit = bar.low <= recommendation.tp1
                if stop_hit:
                    exit_price = recommendation.stop_loss
                    break
                if target2_hit:
                    exit_price = recommendation.tp2
                    break
                if target1_hit:
                    exit_price = recommendation.tp1
                    break

        if direction == Direction.BUY:
            gross_return = (exit_price - entry) / max(entry, 0.01)
        else:
            gross_return = (entry - exit_price) / max(entry, 0.01)
        return gross_return - tx_cost

    def _simulate_benchmark_return(
        self,
        pipeline: ResearchPipeline,
        benchmark: str,
        period_start: datetime,
        period_end: datetime,
    ) -> float:
        try:
            bars = self._window_bars(
                pipeline=pipeline,
                ticker=benchmark,
                period_start=period_start,
                period_end=period_end,
            )
        except Exception:
            return 0.0

        if len(bars) < 2:
            return 0.0

        start_price = bars[0].close
        end_price = bars[-1].close
        return (end_price - start_price) / max(start_price, 0.01)

    @staticmethod
    def _std(values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return math.sqrt(var)

    @staticmethod
    def _max_drawdown(returns: list[float]) -> float:
        equity = 1.0
        peak = 1.0
        max_dd = 0.0
        for value in returns:
            equity *= 1.0 + value
            peak = max(peak, equity)
            if peak > 0:
                dd = (peak - equity) / peak
                max_dd = max(max_dd, dd)
        return max_dd
