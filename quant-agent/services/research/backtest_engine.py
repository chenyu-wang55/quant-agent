from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from domain.entities.models import (
    BacktestRunRequest,
    BacktestRunResult,
    Direction,
    MarketBar,
    Recommendation,
    ResearchRunRequest,
    RunType,
    build_strategy_config_snapshot,
)
from services.execution.market_calendar import xnys_session
from services.ranking.pipeline import ResearchPipeline


def _frequency_to_step_days(rebalance_frequency: str) -> int:
    return {
        "daily": 1,
        "weekly": 5,
        "biweekly": 10,
        "monthly": 21,
    }.get(rebalance_frequency.strip().lower(), 21)


def _trading_days(start: datetime, end: datetime) -> list[datetime]:
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    points: list[datetime] = []
    current = start_utc
    while current <= end_utc:
        if xnys_session(current.date()).is_trading_day:
            points.append(current)
        current += timedelta(days=1)
    return points


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _max_drawdown(returns: list[float]) -> float:
    equity = 1.0
    peak = 1.0
    maximum = 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _annualized_return(returns: list[float], periods_per_year: float) -> float:
    if not returns or periods_per_year <= 0:
        return 0.0
    equity = math.prod(1.0 + value for value in returns)
    if equity <= 0:
        return -1.0
    return equity ** (periods_per_year / len(returns)) - 1.0


@dataclass
class OpenPosition:
    ticker: str
    qty: float
    entry_price: float
    entry_at: datetime
    stop_loss: float
    tp1: float
    tp2: float
    confidence: float
    recommendation_id: str
    source_snapshot_id: str
    last_processed_at: datetime
    sector: str = "Unknown"
    entry_fee: float = 0.0
    bars_held: int = 0
    dividends: float = 0.0


@dataclass(frozen=True)
class BacktestAcceptanceThresholds:
    min_out_of_sample_trades: int = 100
    max_out_of_sample_drawdown: float = 0.15
    min_out_of_sample_profit_factor: float = 1.20
    min_out_of_sample_sharpe: float = 0.80
    max_position_exposure: float = 0.10
    max_sector_exposure: float = 0.30
    min_out_of_sample_payoff_ratio: float = 1.50
    min_out_of_sample_expectancy_after_cost: float = 0.0


class BacktestEngine:
    """Event-driven, long-only portfolio backtest with conservative fill assumptions."""

    def run(
        self,
        request: BacktestRunRequest,
        pipeline: ResearchPipeline,
        template_request: ResearchRunRequest,
    ) -> BacktestRunResult:
        point_in_time_validation: dict[str, Any] | None = None
        validate_backtest_data = getattr(pipeline.provider, "validate_backtest_data", None)
        if callable(validate_backtest_data):
            point_in_time_validation = validate_backtest_data(
                request.start_date,
                request.end_date,
            )
        if not point_in_time_validation or point_in_time_validation.get("status") != "verified":
            raise ValueError("point-in-time validation must return status=verified")
        dataset_fingerprint = str(point_in_time_validation.get("dataset_fingerprint") or "")
        if len(dataset_fingerprint) != 64:
            raise ValueError("point-in-time validation report is missing a SHA-256 dataset_fingerprint")
        backtest_template_request = template_request.model_copy(
            update={
                "run_type": RunType.BACKTEST_EVALUATION,
                "publication": template_request.publication.model_copy(update={"top_n": request.top_n}),
            }
        )
        strategy_config = build_strategy_config_snapshot(backtest_template_request)
        trading_days = _trading_days(request.start_date, request.end_date)
        step = _frequency_to_step_days(request.rebalance_frequency)
        rebalance_points = trading_days[::step]
        if trading_days and (not rebalance_points or rebalance_points[-1] != trading_days[-1]):
            rebalance_points.append(trading_days[-1])

        config_payload = request.model_dump_json() + backtest_template_request.model_dump_json()
        config_payload += dataset_fingerprint
        config_hash = hashlib.sha256(config_payload.encode("utf-8")).hexdigest()[:24]
        run_id = f"bt_{config_hash[:16]}"
        cash = float(request.initial_cash)
        positions: dict[str, OpenPosition] = {}
        trades: list[dict[str, Any]] = []
        equity_curve: list[dict[str, Any]] = [
            {
                "timestamp": request.start_date.astimezone(timezone.utc).isoformat(),
                "equity": cash,
                "cash": cash,
                "gross_exposure": 0.0,
            }
        ]
        recommendation_count = 0
        opened_count = 0
        partial_fill_count = 0
        gap_fill_count = 0
        corporate_action_count = 0
        total_fees = 0.0
        total_slippage = 0.0
        total_trade_notional = 0.0
        max_gross_exposure = 0.0
        max_position_exposure = 0.0
        max_sector_exposure = 0.0

        for index, point in enumerate(rebalance_points):
            (
                cash,
                advance_trades,
                advance_stats,
            ) = self._advance_positions(
                positions=positions,
                cash=cash,
                pipeline=pipeline,
                through=point,
                request=request,
                force_liquidation=False,
            )
            trades.extend(advance_trades)
            total_fees += advance_stats["fees"]
            total_slippage += advance_stats["slippage"]
            total_trade_notional += advance_stats["notional"]
            partial_fill_count += int(advance_stats["partial_fills"])
            gap_fill_count += int(advance_stats["gap_fills"])
            corporate_action_count += int(advance_stats["corporate_actions"])

            equity, gross = self._portfolio_value(cash, positions, pipeline, point)
            point_equity, point_gross = equity, gross
            max_gross_exposure = max(
                max_gross_exposure,
                gross / equity if equity > 0 else 0.0,
            )

            if index == len(rebalance_points) - 1:
                equity_curve.append(
                    {
                        "timestamp": point.isoformat(),
                        "equity": equity,
                        "cash": cash,
                        "gross_exposure": gross,
                    }
                )
                continue

            run_request = backtest_template_request.model_copy(
                update={
                    "as_of": point,
                    "source_snapshot_id": f"backtest:{config_hash}:{point.date().isoformat()}",
                }
            )
            output = pipeline.run(run_request)
            recommendations = output.result.recommendations
            recommendation_count += len(recommendations)
            sector_by_ticker = {
                item.ticker.upper(): item.sector or "Unknown"
                for item in pipeline.provider.get_universe(template_request.universe, point)
            }

            entry_candidates: list[tuple] = []
            for rank, recommendation in enumerate(recommendations):
                if recommendation.direction != Direction.BUY:
                    continue
                ticker = recommendation.ticker.upper()
                if ticker in positions:
                    continue
                entry = self._find_entry(
                    recommendation=recommendation,
                    pipeline=pipeline,
                    point=point,
                    trading_days=request.entry_valid_trading_days,
                    through=rebalance_points[index + 1],
                )
                if entry is None:
                    continue
                bar, raw_fill, gap_fill = entry
                entry_candidates.append(
                    (
                        bar.timestamp.astimezone(timezone.utc),
                        rank,
                        recommendation,
                        bar,
                        raw_fill,
                        gap_fill,
                    )
                )

            for _, _, recommendation, bar, raw_fill, gap_fill in sorted(entry_candidates):
                ticker = recommendation.ticker.upper()
                if ticker in positions:
                    continue
                sector = sector_by_ticker.get(ticker, "Unknown")
                fill_price = raw_fill * (1.0 + request.slippage_bps / 10_000.0)
                equity, gross = self._portfolio_value(cash, positions, pipeline, point)
                sector_gross = self._sector_gross(positions, pipeline, point).get(sector, 0.0)
                max_notional = min(
                    equity * request.max_position_pct,
                    max(0.0, equity * request.max_gross_exposure_pct - gross),
                    max(0.0, equity * request.max_sector_exposure_pct - sector_gross),
                    cash / (1.0 + request.transaction_cost_bps / 10_000.0),
                )
                if max_notional <= 0 or fill_price <= 0:
                    continue
                target_qty = max_notional / fill_price
                volume_cap = max(0.0, bar.volume * request.max_volume_participation)
                fill_qty = min(target_qty, volume_cap)
                if fill_qty <= 0:
                    continue
                if fill_qty + 1e-12 < target_qty:
                    partial_fill_count += 1
                if gap_fill:
                    gap_fill_count += 1
                notional = fill_qty * fill_price
                fee = notional * request.transaction_cost_bps / 10_000.0
                slippage = fill_qty * max(0.0, fill_price - raw_fill)
                cash -= notional + fee
                total_fees += fee
                total_slippage += slippage
                total_trade_notional += notional
                positions[ticker] = OpenPosition(
                    ticker=ticker,
                    qty=fill_qty,
                    entry_price=fill_price,
                    entry_at=bar.timestamp.astimezone(timezone.utc),
                    stop_loss=recommendation.stop_loss,
                    tp1=recommendation.tp1,
                    tp2=recommendation.tp2,
                    confidence=recommendation.confidence,
                    recommendation_id=recommendation.id,
                    source_snapshot_id=recommendation.source_snapshot_id,
                    last_processed_at=bar.timestamp.astimezone(timezone.utc),
                    sector=sector,
                    entry_fee=fee,
                )
                trades.append(
                    {
                        "side": "buy",
                        "ticker": ticker,
                        "qty": fill_qty,
                        "entry_at": bar.timestamp.astimezone(timezone.utc).isoformat(),
                        "entry_price": fill_price,
                        "raw_price": raw_fill,
                        "fee": fee,
                        "slippage": slippage,
                        "gap_fill": gap_fill,
                        "confidence": recommendation.confidence,
                        "recommendation_id": recommendation.id,
                        "source_snapshot_id": recommendation.source_snapshot_id,
                    }
                )
                opened_count += 1
                entry_gross = sum(position.qty * position.entry_price for position in positions.values())
                entry_equity = cash + entry_gross
                max_gross_exposure = max(
                    max_gross_exposure,
                    entry_gross / entry_equity if entry_equity > 0 else 0.0,
                )
                max_position_exposure = max(
                    max_position_exposure,
                    notional / entry_equity if entry_equity > 0 else 0.0,
                )
                max_sector_exposure = max(
                    max_sector_exposure,
                    (sector_gross + notional) / entry_equity if entry_equity > 0 else 0.0,
                )

            equity_curve.append(
                {
                    "timestamp": point.isoformat(),
                    "equity": point_equity,
                    "cash": point_equity - point_gross,
                    "gross_exposure": point_gross,
                }
            )

        if trading_days:
            cash, final_trades, final_stats = self._advance_positions(
                positions=positions,
                cash=cash,
                pipeline=pipeline,
                through=trading_days[-1],
                request=request,
                force_liquidation=True,
            )
            trades.extend(final_trades)
            total_fees += final_stats["fees"]
            total_slippage += final_stats["slippage"]
            total_trade_notional += final_stats["notional"]
            partial_fill_count += int(final_stats["partial_fills"])
            gap_fill_count += int(final_stats["gap_fills"])
            corporate_action_count += int(final_stats["corporate_actions"])
            final_equity, gross = self._portfolio_value(cash, positions, pipeline, trading_days[-1])
            final_point = {
                "timestamp": trading_days[-1].isoformat(),
                "equity": final_equity,
                "cash": cash,
                "gross_exposure": gross,
            }
            if equity_curve and equity_curve[-1]["timestamp"] == final_point["timestamp"]:
                equity_curve[-1] = final_point
            else:
                equity_curve.append(final_point)

        returns = self._equity_returns(equity_curve)
        benchmark_returns = self._benchmark_returns(
            pipeline, request.benchmark, [datetime.fromisoformat(str(point["timestamp"])) for point in equity_curve]
        )
        metrics = self._metrics(
            returns=returns,
            benchmark_returns=benchmark_returns,
            periods_per_year=252.0 / max(1, step),
        )
        final_equity = float(equity_curve[-1]["equity"] if equity_curve else request.initial_cash)
        avg_equity = (
            sum(float(point["equity"]) for point in equity_curve) / len(equity_curve)
            if equity_curve
            else request.initial_cash
        )
        closed_trades = [trade for trade in trades if trade.get("side") == "sell"]
        metrics.update(
            {
                "periods": float(len(returns)),
                "recommendation_count": float(recommendation_count),
                "traded_count": float(opened_count),
                "closed_trade_count": float(len(closed_trades)),
                "fill_rate": round(opened_count / recommendation_count if recommendation_count else 0.0, 6),
                "final_equity": round(final_equity, 6),
                "total_return": round(final_equity / request.initial_cash - 1.0, 6),
                "turnover": round(total_trade_notional / max(avg_equity, 0.01), 6),
                "total_fees": round(total_fees, 6),
                "total_slippage": round(total_slippage, 6),
                "partial_fill_count": float(partial_fill_count),
                "gap_fill_count": float(gap_fill_count),
                "corporate_action_count": float(corporate_action_count),
                "max_gross_exposure": round(max_gross_exposure, 6),
                "max_position_exposure": round(max_position_exposure, 6),
                "max_sector_exposure": round(max_sector_exposure, 6),
                "ending_open_position_count": float(len(positions)),
            }
        )

        segments = self._segment_metrics(
            returns,
            benchmark_returns,
            closed_trades,
            equity_curve,
            request.train_fraction,
            request.validation_fraction,
            252.0 / max(1, step),
        )
        calibration = self._confidence_calibration(closed_trades, request.confidence_bins)
        market_regimes = self._market_regime_metrics(
            returns,
            benchmark_returns,
            252.0 / max(1, step),
        )
        acceptance = self._acceptance(
            segments=segments,
            metrics=metrics,
            thresholds=BacktestAcceptanceThresholds(),
        )
        return BacktestRunResult(
            run_id=run_id,
            created_at=datetime.now(timezone.utc),
            config_hash=config_hash,
            strategy_config_id=strategy_config.strategy_config_id,
            dataset_fingerprint=dataset_fingerprint,
            metrics=metrics,
            notes=(
                "Point-in-time event-driven portfolio backtest using XNYS sessions, cash and "
                "overlapping positions, volume-limited fills, gap-aware exits, bilateral fees, "
                "slippage, dividends and split adjustments."
            ),
            segments=segments,
            calibration=calibration,
            market_regimes=market_regimes,
            acceptance=acceptance,
            equity_curve=equity_curve,
            trades=trades,
            assumptions={
                "calendar": "XNYS",
                "same_bar_stop_target_priority": "stop_first_conservative",
                "transaction_cost_bps_each_side": request.transaction_cost_bps,
                "slippage_bps_each_side": request.slippage_bps,
                "max_volume_participation": request.max_volume_participation,
                "point_in_time_data_required": True,
                "survivorship_bias_control": "provider must supply dated constituent membership",
                "point_in_time_validation": point_in_time_validation,
            },
        )

    @staticmethod
    def _bars_between(
        pipeline: ResearchPipeline,
        ticker: str,
        start: datetime,
        end: datetime,
    ) -> list[MarketBar]:
        days = max(30, (end.date() - start.date()).days * 2 + 30)
        bars = pipeline.provider.get_bars(ticker=ticker, as_of=end, lookback_days=days)
        start_utc = start.astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)
        return sorted(
            [bar for bar in bars if start_utc < bar.timestamp.astimezone(timezone.utc) <= end_utc],
            key=lambda bar: bar.timestamp,
        )

    def _find_entry(
        self,
        *,
        recommendation: Recommendation,
        pipeline: ResearchPipeline,
        point: datetime,
        trading_days: int,
        through: datetime,
    ) -> tuple[MarketBar, float, bool] | None:
        window_end = min(through, point + timedelta(days=max(7, trading_days * 3)))
        bars = self._bars_between(pipeline, recommendation.ticker, point, window_end)
        seen = 0
        midpoint = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
        for bar in bars:
            if not xnys_session(bar.timestamp.date()).is_trading_day:
                continue
            seen += 1
            if seen > trading_days:
                break
            if bar.open > recommendation.entry_zone_high:
                return bar, bar.open, True
            if bar.low <= recommendation.entry_zone_high and bar.high >= recommendation.entry_zone_low:
                if recommendation.entry_zone_low <= bar.open <= recommendation.entry_zone_high:
                    return bar, bar.open, False
                return bar, midpoint, False
        return None

    def _advance_positions(
        self,
        *,
        positions: dict[str, OpenPosition],
        cash: float,
        pipeline: ResearchPipeline,
        through: datetime,
        request: BacktestRunRequest,
        force_liquidation: bool,
    ) -> tuple[float, list[dict[str, Any]], dict[str, float]]:
        completed: list[dict[str, Any]] = []
        stats = {
            "fees": 0.0,
            "slippage": 0.0,
            "notional": 0.0,
            "partial_fills": 0.0,
            "gap_fills": 0.0,
            "corporate_actions": 0.0,
        }
        for ticker, position in list(positions.items()):
            bars = self._bars_between(pipeline, ticker, position.last_processed_at, through)
            if force_liquidation and not bars:
                recent = pipeline.provider.get_bars(ticker, through, lookback_days=5)
                eligible = [
                    bar for bar in recent if bar.timestamp.astimezone(timezone.utc) <= through.astimezone(timezone.utc)
                ]
                if eligible:
                    bars = [eligible[-1]]
            exit_bar: MarketBar | None = None
            raw_exit: float | None = None
            reason: str | None = None
            for bar in bars:
                position.last_processed_at = bar.timestamp.astimezone(timezone.utc)
                if bar.split_factor > 0 and abs(bar.split_factor - 1.0) > 1e-12:
                    position.qty *= bar.split_factor
                    position.entry_price /= bar.split_factor
                    position.stop_loss /= bar.split_factor
                    position.tp1 /= bar.split_factor
                    position.tp2 /= bar.split_factor
                    stats["corporate_actions"] += 1
                if bar.dividend:
                    dividend_cash = position.qty * bar.dividend
                    position.dividends += dividend_cash
                    cash += dividend_cash
                    stats["corporate_actions"] += 1
                position.bars_held += 1
                if bar.open <= position.stop_loss:
                    exit_bar, raw_exit, reason = bar, bar.open, "stop_gap"
                elif bar.open >= position.tp2:
                    exit_bar, raw_exit, reason = bar, bar.open, "target2_gap"
                elif bar.low <= position.stop_loss:
                    exit_bar, raw_exit, reason = bar, position.stop_loss, "stop"
                elif bar.high >= position.tp2:
                    exit_bar, raw_exit, reason = bar, position.tp2, "target2"
                elif bar.high >= position.tp1:
                    exit_bar, raw_exit, reason = bar, position.tp1, "target1"
                elif position.bars_held >= request.max_holding_trading_days:
                    exit_bar, raw_exit, reason = bar, bar.close, "max_holding"
                if exit_bar is not None:
                    break
            if force_liquidation and exit_bar is None and bars:
                exit_bar, raw_exit, reason = bars[-1], bars[-1].close, "end_of_test"
            if exit_bar is None or raw_exit is None or reason is None:
                continue

            exit_price = raw_exit * (1.0 - request.slippage_bps / 10_000.0)
            volume_cap = max(0.0, exit_bar.volume * request.max_volume_participation)
            exit_qty = min(position.qty, volume_cap)
            if force_liquidation:
                exit_qty = position.qty
            if exit_qty <= 0:
                continue
            if exit_qty + 1e-12 < position.qty:
                stats["partial_fills"] += 1
            notional = exit_qty * exit_price
            fee = notional * request.transaction_cost_bps / 10_000.0
            slippage = exit_qty * max(0.0, raw_exit - exit_price)
            cash += notional - fee
            stats["fees"] += fee
            stats["slippage"] += slippage
            stats["notional"] += notional
            allocated_dividends = position.dividends * (exit_qty / max(position.qty, 1e-12))
            allocated_entry_fee = position.entry_fee * (exit_qty / max(position.qty, 1e-12))
            realized = (
                (exit_price - position.entry_price) * exit_qty
                - fee
                - allocated_entry_fee
                + allocated_dividends
            )
            realized_return = realized / max(position.entry_price * exit_qty, 0.01)
            if reason.endswith("_gap"):
                stats["gap_fills"] += 1
            completed.append(
                {
                    "side": "sell",
                    "ticker": ticker,
                    "qty": exit_qty,
                    "entry_at": position.entry_at.isoformat(),
                    "exit_at": exit_bar.timestamp.astimezone(timezone.utc).isoformat(),
                    "entry_price": position.entry_price,
                    "exit_price": exit_price,
                    "reason": reason,
                    "entry_fee": allocated_entry_fee,
                    "fee": fee,
                    "slippage": slippage,
                    "dividends": allocated_dividends,
                    "realized_pnl": realized,
                    "realized_return": realized_return,
                    "confidence": position.confidence,
                    "recommendation_id": position.recommendation_id,
                    "source_snapshot_id": position.source_snapshot_id,
                }
            )
            position.dividends -= allocated_dividends
            position.entry_fee -= allocated_entry_fee
            position.qty -= exit_qty
            if position.qty <= 1e-12:
                positions.pop(ticker, None)
        return cash, completed, stats

    @staticmethod
    def _portfolio_value(
        cash: float,
        positions: dict[str, OpenPosition],
        pipeline: ResearchPipeline,
        as_of: datetime,
    ) -> tuple[float, float]:
        gross = 0.0
        for position in positions.values():
            bars = pipeline.provider.get_bars(position.ticker, as_of, lookback_days=5)
            eligible = [bar for bar in bars if bar.timestamp.astimezone(timezone.utc) <= as_of]
            mark = eligible[-1].close if eligible else position.entry_price
            gross += position.qty * mark
        return cash + gross, gross

    @staticmethod
    def _sector_gross(
        positions: dict[str, OpenPosition],
        pipeline: ResearchPipeline,
        as_of: datetime,
    ) -> dict[str, float]:
        values: dict[str, float] = {}
        for position in positions.values():
            bars = pipeline.provider.get_bars(position.ticker, as_of, lookback_days=5)
            eligible = [bar for bar in bars if bar.timestamp.astimezone(timezone.utc) <= as_of]
            mark = eligible[-1].close if eligible else position.entry_price
            values[position.sector] = values.get(position.sector, 0.0) + position.qty * mark
        return values

    @staticmethod
    def _equity_returns(equity_curve: list[dict[str, Any]]) -> list[float]:
        values = [float(point["equity"]) for point in equity_curve]
        return [values[index] / max(values[index - 1], 0.01) - 1.0 for index in range(1, len(values))]

    def _benchmark_returns(
        self,
        pipeline: ResearchPipeline,
        benchmark: str,
        points: list[datetime],
    ) -> list[float]:
        prices: list[float] = []
        for point in points:
            bars = pipeline.provider.get_benchmark_bars(benchmark, point, lookback_days=5)
            eligible = [bar for bar in bars if bar.timestamp.astimezone(timezone.utc) <= point]
            if not eligible:
                prices.append(prices[-1] if prices else 1.0)
                continue
            bar = eligible[-1]
            prices.append(float(bar.adjusted_close or bar.close))
        return [prices[index] / max(prices[index - 1], 0.01) - 1.0 for index in range(1, len(prices))]

    @staticmethod
    def _metrics(
        *,
        returns: list[float],
        benchmark_returns: list[float],
        periods_per_year: float,
    ) -> dict[str, float]:
        average = sum(returns) / len(returns) if returns else 0.0
        volatility = _std(returns)
        benchmark_average = sum(benchmark_returns) / len(benchmark_returns) if benchmark_returns else 0.0
        annualized = _annualized_return(returns, periods_per_year)
        benchmark_annualized = _annualized_return(benchmark_returns, periods_per_year)
        excess = [value - benchmark for value, benchmark in zip(returns, benchmark_returns)]
        excess_volatility = _std(excess)
        return {
            "avg_period_return": round(average, 6),
            "avg_period_benchmark_return": round(benchmark_average, 6),
            "hit_rate": round(sum(value > 0 for value in returns) / len(returns) if returns else 0.0, 6),
            "volatility": round(volatility, 6),
            "sharpe": round(
                average / volatility * math.sqrt(periods_per_year) if volatility else 0.0,
                6,
            ),
            "annualized_return": round(annualized, 6),
            "annualized_benchmark_return": round(benchmark_annualized, 6),
            "alpha_annualized": round(annualized - benchmark_annualized, 6),
            "information_ratio": round(
                (sum(excess) / len(excess)) / excess_volatility * math.sqrt(periods_per_year)
                if excess and excess_volatility
                else 0.0,
                6,
            ),
            "max_drawdown": round(_max_drawdown(returns), 6),
            "benchmark_max_drawdown": round(_max_drawdown(benchmark_returns), 6),
        }

    def _segment_metrics(
        self,
        returns: list[float],
        benchmark_returns: list[float],
        trades: list[dict[str, Any]],
        equity_curve: list[dict[str, Any]],
        train_fraction: float,
        validation_fraction: float,
        periods_per_year: float,
    ) -> dict[str, dict[str, Any]]:
        train_end = int(len(returns) * train_fraction)
        validation_end = train_end + int(len(returns) * validation_fraction)
        ranges = {
            "train": (0, train_end),
            "validation": (train_end, validation_end),
            "out_of_sample": (validation_end, len(returns)),
        }
        result: dict[str, dict[str, Any]] = {}
        for name, (start, end) in ranges.items():
            values = returns[start:end]
            benchmarks = benchmark_returns[start:end]
            metrics: dict[str, Any] = self._metrics(
                returns=values,
                benchmark_returns=benchmarks,
                periods_per_year=periods_per_year,
            )
            metrics["periods"] = len(values)
            start_at = self._curve_timestamp(equity_curve, start)
            end_at = self._curve_timestamp(equity_curve, end)
            segment_trades = [
                trade
                for trade in trades
                if self._timestamp_in_segment(trade.get("exit_at"), start_at, end_at, start == 0)
            ]
            metrics.update(self._trade_metrics(segment_trades))
            metrics["start_at"] = start_at.isoformat() if start_at else None
            metrics["end_at"] = end_at.isoformat() if end_at else None
            result[name] = metrics
        return result

    @staticmethod
    def _curve_timestamp(equity_curve: list[dict[str, Any]], index: int) -> datetime | None:
        if not equity_curve:
            return None
        bounded = min(max(index, 0), len(equity_curve) - 1)
        value = datetime.fromisoformat(str(equity_curve[bounded]["timestamp"]))
        return value.astimezone(timezone.utc)

    @staticmethod
    def _timestamp_in_segment(
        value: Any,
        start_at: datetime | None,
        end_at: datetime | None,
        include_start: bool,
    ) -> bool:
        if not value or start_at is None or end_at is None:
            return False
        timestamp = datetime.fromisoformat(str(value)).astimezone(timezone.utc)
        lower = timestamp >= start_at if include_start else timestamp > start_at
        return lower and timestamp <= end_at

    @staticmethod
    def _trade_metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
        pnls = [float(trade.get("realized_pnl") or 0.0) for trade in trades]
        returns = [float(trade.get("realized_return") or 0.0) for trade in trades]
        wins = [value for value in pnls if value > 0]
        losses = [value for value in pnls if value < 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        average_win = sum(wins) / len(wins) if wins else 0.0
        average_loss = abs(sum(losses) / len(losses)) if losses else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss else None
        payoff_ratio = average_win / average_loss if average_loss else None
        return {
            "trade_count": len(trades),
            "win_rate": round(len(wins) / len(pnls), 6) if pnls else 0.0,
            "gross_profit": round(gross_profit, 6),
            "gross_loss": round(gross_loss, 6),
            "profit_factor": round(profit_factor, 6) if profit_factor is not None else None,
            "profit_factor_infinite": bool(gross_profit > 0 and gross_loss == 0),
            "payoff_ratio": round(payoff_ratio, 6) if payoff_ratio is not None else None,
            "payoff_ratio_infinite": bool(average_win > 0 and average_loss == 0),
            "expectancy_after_cost": round(sum(returns) / len(returns), 6) if returns else 0.0,
        }

    def _market_regime_metrics(
        self,
        returns: list[float],
        benchmark_returns: list[float],
        periods_per_year: float,
    ) -> dict[str, dict[str, Any]]:
        grouped: dict[str, tuple[list[float], list[float]]] = {
            "bull": ([], []),
            "sideways": ([], []),
            "bear": ([], []),
        }
        for value, benchmark in zip(returns, benchmark_returns):
            regime = "bull" if benchmark > 0.01 else "bear" if benchmark < -0.01 else "sideways"
            grouped[regime][0].append(value)
            grouped[regime][1].append(benchmark)
        return {
            name: {
                "periods": len(values),
                **self._metrics(
                    returns=values,
                    benchmark_returns=benchmarks,
                    periods_per_year=periods_per_year,
                ),
            }
            for name, (values, benchmarks) in grouped.items()
        }

    @staticmethod
    def _acceptance(
        *,
        segments: dict[str, dict[str, Any]],
        metrics: dict[str, float],
        thresholds: BacktestAcceptanceThresholds,
    ) -> dict[str, Any]:
        out_of_sample = segments.get("out_of_sample", {})
        checks: dict[str, dict[str, Any]] = {
            "out_of_sample_trade_count": {
                "passed": int(out_of_sample.get("trade_count") or 0)
                >= thresholds.min_out_of_sample_trades,
                "actual": int(out_of_sample.get("trade_count") or 0),
                "minimum": thresholds.min_out_of_sample_trades,
            },
            "out_of_sample_max_drawdown": {
                "passed": float(out_of_sample.get("max_drawdown") or 0.0)
                <= thresholds.max_out_of_sample_drawdown,
                "actual": float(out_of_sample.get("max_drawdown") or 0.0),
                "maximum": thresholds.max_out_of_sample_drawdown,
            },
            "out_of_sample_profit_factor": BacktestEngine._ratio_check(
                out_of_sample,
                "profit_factor",
                "profit_factor_infinite",
                thresholds.min_out_of_sample_profit_factor,
            ),
            "out_of_sample_sharpe": {
                "passed": float(out_of_sample.get("sharpe") or 0.0)
                >= thresholds.min_out_of_sample_sharpe,
                "actual": float(out_of_sample.get("sharpe") or 0.0),
                "minimum": thresholds.min_out_of_sample_sharpe,
            },
            "max_position_exposure": {
                "passed": float(metrics.get("max_position_exposure") or 0.0)
                <= thresholds.max_position_exposure,
                "actual": float(metrics.get("max_position_exposure") or 0.0),
                "maximum": thresholds.max_position_exposure,
            },
            "max_sector_exposure": {
                "passed": float(metrics.get("max_sector_exposure") or 0.0)
                <= thresholds.max_sector_exposure,
                "actual": float(metrics.get("max_sector_exposure") or 0.0),
                "maximum": thresholds.max_sector_exposure,
            },
            "out_of_sample_payoff_ratio": BacktestEngine._ratio_check(
                out_of_sample,
                "payoff_ratio",
                "payoff_ratio_infinite",
                thresholds.min_out_of_sample_payoff_ratio,
            ),
            "out_of_sample_expectancy_after_cost": {
                "passed": float(out_of_sample.get("expectancy_after_cost") or 0.0)
                > thresholds.min_out_of_sample_expectancy_after_cost,
                "actual": float(out_of_sample.get("expectancy_after_cost") or 0.0),
                "minimum_exclusive": thresholds.min_out_of_sample_expectancy_after_cost,
            },
        }
        reasons = [name for name, check in checks.items() if not bool(check["passed"])]
        return {
            "passed": not reasons,
            "evaluated_segment": "out_of_sample",
            "checks": checks,
            "reasons": reasons,
        }

    @staticmethod
    def _ratio_check(
        values: dict[str, Any],
        ratio_name: str,
        infinite_name: str,
        minimum: float,
    ) -> dict[str, Any]:
        infinite = bool(values.get(infinite_name))
        raw = values.get(ratio_name)
        actual = float(raw) if raw is not None else None
        return {
            "passed": infinite or (actual is not None and actual >= minimum),
            "actual": "infinite" if infinite else actual,
            "minimum": minimum,
        }

    @staticmethod
    def _confidence_calibration(trades: list[dict[str, Any]], bins: int) -> dict[str, Any]:
        if not trades:
            return {"sample_count": 0, "brier_score": None, "ece": None, "bins": []}
        observations = [
            (max(0.0, min(1.0, float(trade["confidence"]))), float(trade["realized_return"]) > 0) for trade in trades
        ]
        brier = sum((confidence - float(won)) ** 2 for confidence, won in observations) / len(observations)
        rows: list[dict[str, Any]] = []
        ece = 0.0
        for index in range(bins):
            low = index / bins
            high = (index + 1) / bins
            members = [
                (confidence, won)
                for confidence, won in observations
                if low <= confidence < high or (index == bins - 1 and confidence == 1.0)
            ]
            if not members:
                continue
            avg_confidence = sum(item[0] for item in members) / len(members)
            win_rate = sum(float(item[1]) for item in members) / len(members)
            ece += abs(avg_confidence - win_rate) * len(members) / len(observations)
            rows.append(
                {
                    "lower": low,
                    "upper": high,
                    "count": len(members),
                    "avg_confidence": round(avg_confidence, 6),
                    "win_rate": round(win_rate, 6),
                }
            )
        return {
            "sample_count": len(observations),
            "brier_score": round(brier, 6),
            "ece": round(ece, 6),
            "bins": rows,
        }
