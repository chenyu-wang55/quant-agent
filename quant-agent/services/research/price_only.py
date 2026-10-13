"""Frozen, price-only stock research. Separate from the production multi-factor strategy."""
from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from statistics import mean, stdev
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from services.ingestion.free_sources import normalized_ticker
from services.ingestion.sec_facts import FreeDataError
from services.research.price_data import EASTERN, PriceDataset, digest, sessions


class PriceResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    strategy: str = "price_momentum_v1"
    selected_on: date
    securities: dict[str, str]
    benchmark: str = "SPY"
    data_start: date
    data_end: date
    backtest_start: date
    backtest_end: date
    validation_start: date
    holdout_start: date
    selection_at: datetime
    top_n: int = Field(default=8, ge=1, le=20)
    rebalance_sessions: int = Field(default=5, ge=1, le=21)
    initial_cash: float = Field(default=100_000, gt=0)
    max_position_weight: float = Field(default=0.10, gt=0, le=0.10)
    max_sector_weight: float = Field(default=0.30, gt=0, le=0.30)
    max_gross_weight: float = Field(default=0.80, gt=0, le=1)
    transaction_cost_bps: float = Field(default=10, ge=0)
    slippage_bps: float = Field(default=5, ge=0)

    @model_validator(mode="after")
    def check_scope(self) -> PriceResearchConfig:
        if self.strategy != "price_momentum_v1" or not 1 <= len(self.securities) <= 30:
            raise ValueError("unsupported price strategy or research pool size")
        if any(ticker != normalized_ticker(ticker) or not sector.strip() for ticker, sector in self.securities.items()):
            raise ValueError("research pool needs canonical tickers and explicit sectors")
        normalized_ticker(self.benchmark)
        if self.benchmark in self.securities:
            raise ValueError("benchmark must be separate from the stock selection pool")
        if not self.data_start < self.backtest_start < self.validation_start < self.holdout_start <= self.backtest_end <= self.data_end:
            raise ValueError("invalid frozen research windows")
        if self.selection_at.tzinfo is None or self.selection_at.utcoffset() is None:
            raise ValueError("selection_at requires an explicit timezone")
        if self.data_end >= self.selection_at.astimezone(EASTERN).date():
            raise ValueError("selection_at must follow the final daily price date")
        return self


def rank_prices(dataset: PriceDataset, config: PriceResearchConfig, decision_at: datetime) -> dict[str, Any]:
    if decision_at.tzinfo is None:
        raise FreeDataError("ranking cutoff must have a timezone")
    cutoff = decision_at.astimezone(timezone.utc)
    history = {symbol: [bar for bar in dataset.bars.get(symbol, ()) if bar.available_at <= cutoff]
               for symbol in set(config.securities) | {config.benchmark}}
    if any(len(bars) < 200 for bars in history.values()):
        raise FreeDataError("every stock and benchmark needs 200 already-available sessions")
    latest_days = {bars[-1].day for bars in history.values()}
    if len(latest_days) != 1:
        raise FreeDataError("stock and benchmark observations do not share a decision date")
    benchmark = history[config.benchmark]
    market_on = benchmark[-1].close >= mean(bar.close for bar in benchmark[-200:])
    benchmark_return = benchmark[-1].close / benchmark[-64].close - 1
    candidates: list[dict[str, Any]] = []
    for symbol, sector in sorted(config.securities.items()):
        bars = history[symbol]
        closes = [bar.close for bar in bars]
        ma50, ma200 = mean(closes[-50:]), mean(closes[-200:])
        momentum20 = closes[-1] / closes[-21] - 1
        relative63 = closes[-1] / closes[-64] - 1 - benchmark_return
        daily = [closes[index] / closes[index - 1] - 1 for index in range(len(closes) - 20, len(closes))]
        volatility = stdev(daily) * math.sqrt(252)
        true_ranges = [max(bars[index].high - bars[index].low, abs(bars[index].high - bars[index - 1].close),
                           abs(bars[index].low - bars[index - 1].close)) for index in range(len(bars) - 14, len(bars))]
        atr = mean(true_ranges)
        eligible = market_on and closes[-1] > ma50 > ma200 and relative63 > 0
        reasons = []
        if not market_on:
            reasons.append("benchmark_below_ma200")
        if not closes[-1] > ma50 > ma200:
            reasons.append("trend_filter_failed")
        if relative63 <= 0:
            reasons.append("relative_strength_not_positive")
        if volatility > 0.60:
            reasons.append("volatility_above_60pct")
            eligible = False
        candidates.append({
            "ticker": symbol, "sector": sector, "data_date": bars[-1].day.isoformat(),
            "reference_close": closes[-1], "ma50": ma50, "ma200": ma200,
            "momentum_20d": momentum20, "relative_strength_63d": relative63,
            "volatility_20d_annualized": volatility, "atr14": atr,
            "rule_score": 0.6 * relative63 + 0.4 * momentum20,
            "eligible": eligible, "reasons": reasons, "target_weight": 0.0,
            "reference_stop": max(0, closes[-1] - 1.5 * atr), "reference_target": closes[-1] + 3 * atr,
            "execution_allowed": False, "calibrated_probability": None,
        })
    candidates.sort(key=lambda row: (-row["rule_score"], row["ticker"]))
    sector_weights: dict[str, float] = {}
    gross, selected = 0.0, 0
    for rank, row in enumerate(candidates, start=1):
        row["rank"] = rank
        if not row["eligible"] or selected >= config.top_n:
            continue
        weight = min(config.max_position_weight, config.max_gross_weight - gross,
                     config.max_sector_weight - sector_weights.get(row["sector"], 0))
        if weight < config.max_position_weight - 1e-9:
            row["reasons"].append("portfolio_sector_or_gross_limit")
            continue
        row["target_weight"] = weight
        gross += weight
        sector_weights[row["sector"]] = sector_weights.get(row["sector"], 0) + weight
        selected += 1
    return {"decision_at": cutoff.isoformat(), "data_date": next(iter(latest_days)).isoformat(),
            "market_regime": "risk_on" if market_on else "risk_off", "candidates": candidates,
            "selected_count": selected, "cash_weight": max(0.0, 1 - gross), "execution_allowed": False}


def _metrics(points: list[dict[str, Any]], field: str, initial: float) -> dict[str, float]:
    if not points:
        raise FreeDataError("empty price backtest interval")
    returns = []
    previous = initial
    peak, drawdown = initial, 0.0
    for point in points:
        equity = point[field]
        returns.append(equity / previous - 1)
        previous = equity
        peak = max(peak, equity)
        drawdown = max(drawdown, (peak - equity) / peak)
    sigma = stdev(returns) if len(returns) > 1 else 0.0
    return {"total_return": previous / initial - 1, "annualized_return": (previous / initial) ** (252 / len(points)) - 1,
            "annualized_volatility": sigma * math.sqrt(252),
            "sharpe_zero_risk_free": mean(returns) / sigma * math.sqrt(252) if sigma else 0.0,
            "max_drawdown": drawdown, "final_equity": previous}


def run_price_backtest(dataset: PriceDataset, config: PriceResearchConfig, *, cost_multiplier: float = 1) -> dict[str, Any]:
    if not math.isfinite(cost_multiplier) or cost_multiplier <= 0:
        raise FreeDataError("cost multiplier must be positive and finite")
    required = set(config.securities) | {config.benchmark}
    if set(dataset.bars) != required:
        raise FreeDataError("daily-price dataset differs from the frozen research pool")
    dates = sessions(config.backtest_start, config.backtest_end)
    by_date = {ticker: {bar.day: bar for bar in bars} for ticker, bars in dataset.bars.items()}
    for ticker, rows in by_date.items():
        if set(dates) - set(rows):
            raise FreeDataError(f"{ticker} does not cover the complete backtest interval")
    cash = config.initial_cash
    quantities: dict[str, float] = {}
    cost_basis: dict[str, float] = {}
    trades: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    total_costs = 0.0
    per_side = (config.transaction_cost_bps + config.slippage_bps) * cost_multiplier / 10_000
    benchmark_units = config.initial_cash / (by_date[config.benchmark][dates[0]].open * (1 + per_side))

    def transact(symbol: str, amount: float, price: float, day: date, side: str, decision: str) -> None:
        nonlocal cash, total_costs
        if amount <= 1e-8:
            return
        notional = amount * price
        costs = notional * per_side
        pnl = None
        if side == "BUY":
            cash -= notional + costs
            quantities[symbol] = quantities.get(symbol, 0) + amount
            cost_basis[symbol] = cost_basis.get(symbol, 0) + notional + costs
        else:
            old_qty = quantities[symbol]
            allocated = cost_basis[symbol] * amount / old_qty
            pnl = notional - costs - allocated
            cash += notional - costs
            quantities[symbol] -= amount
            cost_basis[symbol] -= allocated
            if quantities[symbol] < 1e-8:
                quantities.pop(symbol)
                cost_basis.pop(symbol)
        total_costs += costs
        trades.append({"date": day.isoformat(), "ticker": symbol, "side": side,
                       "adjusted_units": amount, "adjusted_price": price, "notional": notional,
                       "costs": costs, "realized_pnl": pnl, "decision_at": decision})

    for index, day in enumerate(dates):
        cutoff = datetime.combine(day, time(9, 15), EASTERN)
        if index % config.rebalance_sessions == 0:
            ranking = rank_prices(dataset, config, cutoff)
            weights = {row["ticker"]: row["target_weight"] for row in ranking["candidates"] if row["target_weight"] > 0}
            opening_equity = cash + sum(qty * by_date[ticker][day].open for ticker, qty in quantities.items())
            desired = {ticker: opening_equity * weight / by_date[ticker][day].open for ticker, weight in weights.items()}
            for ticker in sorted(list(quantities)):
                sale = quantities[ticker] - desired.get(ticker, 0)
                transact(ticker, max(sale, 0), by_date[ticker][day].open, day, "SELL", ranking["decision_at"])
            for ticker in weights:
                price = by_date[ticker][day].open
                purchase = max(0, desired[ticker] - quantities.get(ticker, 0))
                purchase = min(purchase, max(0, cash) / (price * (1 + per_side)))
                transact(ticker, purchase, price, day, "BUY", ranking["decision_at"])
        if index == len(dates) - 1:
            for ticker in sorted(list(quantities)):
                transact(ticker, quantities[ticker], by_date[ticker][day].close, day, "SELL", "precommitted_final_close")
        equity = cash + sum(qty * by_date[ticker][day].close for ticker, qty in quantities.items())
        if cash < -1e-5 or equity <= 0:
            raise FreeDataError("price research portfolio cash invariant failed")
        benchmark_equity = benchmark_units * by_date[config.benchmark][day].close
        if index == len(dates) - 1:
            benchmark_equity *= 1 - per_side
        curve.append({"date": day.isoformat(), "equity": equity, "cash": cash, "benchmark_equity": benchmark_equity,
                      "gross_exposure": 1 - cash / equity})
    split_points = [("development", config.backtest_start, config.validation_start),
                    ("validation", config.validation_start, config.holdout_start),
                    ("holdout", config.holdout_start, config.backtest_end)]
    segments = {}
    for name, start, end in split_points:
        points = [point for point in curve if start <= date.fromisoformat(point["date"]) and
                  (date.fromisoformat(point["date"]) < end or (name == "holdout" and date.fromisoformat(point["date"]) == end))]
        first_index = curve.index(points[0]) if points else 0
        initial = curve[first_index - 1]["equity"] if first_index else config.initial_cash
        benchmark_initial = curve[first_index - 1]["benchmark_equity"] if first_index else config.initial_cash
        if points:
            segments[name] = {"strategy": _metrics(points, "equity", initial),
                              "benchmark": _metrics(points, "benchmark_equity", benchmark_initial),
                              "sessions": len(points)}
    strategy_metrics = _metrics(curve, "equity", config.initial_cash)
    benchmark_metrics = _metrics(curve, "benchmark_equity", config.initial_cash)
    closed = [trade for trade in trades if trade["side"] == "SELL"]
    gains = sum(max(trade["realized_pnl"], 0) for trade in closed)
    losses = sum(max(-trade["realized_pnl"], 0) for trade in closed)
    return {"strategy": strategy_metrics, "benchmark": benchmark_metrics, "segments": segments,
            "excess_annualized_return": strategy_metrics["annualized_return"] - benchmark_metrics["annualized_return"],
            "total_costs": total_costs, "sell_legs": len(closed),
            "profit_factor_sell_legs": gains / losses if losses else None,
            "cost_multiplier": cost_multiplier, "equity_curve": curve, "trades": trades}


def run_price_study(dataset: PriceDataset, config: PriceResearchConfig) -> dict[str, Any]:
    base = run_price_backtest(dataset, config)
    stress = {}
    for multiplier in (1.5, 2.0):
        result = run_price_backtest(dataset, config, cost_multiplier=multiplier)
        stress[str(multiplier)] = {key: result[key] for key in ("strategy", "benchmark", "segments", "total_costs")}
    latest = rank_prices(dataset, config, config.selection_at)
    if latest["data_date"] != config.data_end.isoformat():
        raise FreeDataError("latest research ranking does not use the required complete daily price date")
    payload = {
        "schema_version": 1,
        "status": "completed_research" if dataset.manifest["source_kind"] == "real_twelvedata" else "completed_synthetic_test",
        "research_kind": "explicit_pool_price_only",
        "strategy_config_id": digest(config.model_dump(mode="json")), "dataset_fingerprint": dataset.fingerprint,
        "source_kind": dataset.manifest["source_kind"], "config": config.model_dump(mode="json"),
        "selection": latest, "backtest": base, "cost_stress": stress,
        "execution_allowed": False, "production_acceptance": {"passed": False, "reason": "price_only_research_not_production_PIT"},
        "limitations": ["pool_selected_retrospectively_survivorship_and_selection_bias",
                        "sector_labels_are_frozen_research_assumptions_not_historical_PIT",
                        "current_vendor_adjustment_vintage_not_historical_source_archive",
                        "adjusted_price_simulation_units_not_actual_broker_share_prices",
                        "no_fundamental_news_earnings_calendar_or_bid_ask_features",
                        "fixed_rule_baseline_no_parameter_search_or_probability_calibration",
                        "zero_risk_free_return_and_no_cash_interest",
                        "daily_open_fills_with_fixed_cost_model_no_intraday_liquidity_simulation",
                        "sell_leg_statistics_are_not_independent_closed_trade_statistics"],
    }
    payload["run_id"] = "price_study_" + digest(payload)[:24]
    return payload
