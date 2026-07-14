from __future__ import annotations

import logging
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from domain.entities.models import (
    Direction,
    FeatureSnapshot,
    Recommendation,
    RejectedRecommendation,
    ResearchRunRequest,
    ResearchRunResult,
    RunMetrics,
    SecurityMetadata,
    SignalSnapshot,
    StrategyConfigSnapshot,
    build_strategy_config_snapshot,
)
from domain.policies.rules import RejectionReason
from infra.db.repositories import SourceSnapshotRepository
from services.features.engine import FeatureEngine
from services.ingestion.interfaces import DataProvider
from services.ingestion.snapshot_provider import SnapshotRecordingProvider, SnapshotReplayProvider
from services.ranking.price_engine import PriceSizingEngine
from services.ranking.recommendation_builder import RecommendationBuilder
from services.ranking.signal_scorer import SignalScorer
from services.risk.engine import RiskEngine

logger = logging.getLogger(__name__)


@dataclass
class PipelineOutput:
    result: ResearchRunResult
    signals_by_ticker: dict[str, SignalSnapshot]
    features_by_ticker: dict[str, FeatureSnapshot]
    strategy_config: StrategyConfigSnapshot


@dataclass
class RecommendationCandidate:
    security: SecurityMetadata
    recommendation: Recommendation
    signal: SignalSnapshot
    feature: FeatureSnapshot
    position_size_pct: float
    upcoming_earnings_minutes: int | None
    bars: list


class ResearchPipeline:
    def __init__(
        self,
        provider: DataProvider,
        feature_engine: FeatureEngine | None = None,
        signal_scorer: SignalScorer | None = None,
        price_engine: PriceSizingEngine | None = None,
        recommendation_builder: RecommendationBuilder | None = None,
        risk_engine: RiskEngine | None = None,
        snapshot_repository: SourceSnapshotRepository | None = None,
    ) -> None:
        self.provider = provider
        self.feature_engine = feature_engine or FeatureEngine()
        self.signal_scorer = signal_scorer or SignalScorer()
        self.price_engine = price_engine or PriceSizingEngine()
        self.recommendation_builder = recommendation_builder or RecommendationBuilder()
        self.risk_engine = risk_engine or RiskEngine()
        self.snapshot_repository = snapshot_repository

    def run(self, request: ResearchRunRequest) -> PipelineOutput:
        as_of = request.as_of.astimezone(timezone.utc)
        source_snapshot_id = request.source_snapshot_id or f"{as_of.isoformat()}#batch001"
        strategy_config = build_strategy_config_snapshot(request)
        provider = self.provider
        recording_provider: SnapshotRecordingProvider | None = None
        snapshot_operation = "disabled"
        if self.snapshot_repository is not None:
            if self.snapshot_repository.snapshot_exists(source_snapshot_id):
                provider = SnapshotReplayProvider(
                    source_snapshot_id=source_snapshot_id,
                    repository=self.snapshot_repository,
                )
                snapshot_operation = "replayed"
            else:
                recording_provider = SnapshotRecordingProvider(
                    delegate=self.provider,
                    source_snapshot_id=source_snapshot_id,
                    repository=self.snapshot_repository,
                )
                provider = recording_provider
                snapshot_operation = "recorded"

        effective_min_confidence = request.risk_policy.min_confidence
        effective_risk_policy = request.risk_policy.model_copy(
            update={"min_confidence": effective_min_confidence}
        )

        universe = provider.get_universe(request.universe, as_of)
        initial_count = len(universe)

        filtered: list = []
        rejected: list[RejectedRecommendation] = []
        rejection_counter: Counter[str] = Counter()

        for security in universe:
            codes = self._apply_universe_filters(security, request)
            if codes:
                for code in codes:
                    rejection_counter[code] += 1
                rejected.append(
                    RejectedRecommendation(
                        ticker=security.ticker,
                        rejection_reason_codes=codes,
                        failed_checks=["universe_filters"],
                    )
                )
                continue
            filtered.append(security)

        if len(filtered) > request.universe_rules.max_candidates_after_filter:
            filtered.sort(key=lambda s: s.avg_dollar_volume, reverse=True)
            overflow = filtered[request.universe_rules.max_candidates_after_filter :]
            filtered = filtered[: request.universe_rules.max_candidates_after_filter]
            for security in overflow:
                code = "over_candidate_limit"
                rejection_counter[code] += 1
                rejected.append(
                    RejectedRecommendation(
                        ticker=security.ticker,
                        rejection_reason_codes=[code],
                        failed_checks=["candidate_cap"],
                    )
                )

        events = provider.get_events([item.ticker for item in filtered], as_of)
        events_by_ticker: dict[str, list] = defaultdict(list)
        for event in events:
            for ticker in event.tickers:
                events_by_ticker[ticker].append(event)

        benchmark_bars = provider.get_benchmark_bars("SPY", as_of)
        if not benchmark_bars:
            raise ValueError("Benchmark provider returned no SPY bars")

        candidates: list[RecommendationCandidate] = []
        provider_failures: list[dict[str, str]] = []
        for security in filtered:
            try:
                bars = provider.get_bars(security.ticker, as_of)
                fundamentals = provider.get_fundamentals(security.ticker, as_of)
                benchmark_for_calc = benchmark_bars if benchmark_bars else bars

                feature = self.feature_engine.compute(
                    security.ticker,
                    bars,
                    benchmark_for_calc,
                    atr_window=request.price_plan_config.atr_window,
                )
                signal = self.signal_scorer.score(
                    as_of=as_of,
                    security=security,
                    bars=bars,
                    benchmark_bars=benchmark_for_calc,
                    feature=feature,
                    fundamentals=fundamentals,
                    events=events_by_ticker.get(security.ticker, []),
                    signal_config=request.signal_config,
                )

                trade_plan = self.price_engine.build(
                    security=security,
                    bars=bars,
                    feature=feature,
                    signal=signal,
                    cfg=request.price_plan_config,
                    direction=Direction.BUY,
                )

                recommendation = self.recommendation_builder.build(
                    security=security,
                    signal=signal,
                    trade_plan=trade_plan,
                    source_snapshot_id=source_snapshot_id,
                    feature_snapshot_id=feature.id,
                    strategy_config_id=strategy_config.strategy_config_id,
                )

                upcoming_earnings = provider.get_upcoming_earnings_minutes(security.ticker, as_of)
                candidates.append(
                    RecommendationCandidate(
                        security=security,
                        recommendation=recommendation,
                        signal=signal,
                        feature=feature,
                        position_size_pct=trade_plan.position_size_pct,
                        upcoming_earnings_minutes=upcoming_earnings,
                        bars=bars,
                    )
                )
            except Exception as exc:
                failure = {
                    "ticker": security.ticker.upper(),
                    "operation": "candidate_inputs",
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:500],
                }
                provider_failures.append(failure)
                logger.warning(
                    "Provider input failure for %s: %s",
                    security.ticker,
                    exc,
                    exc_info=True,
                )
                if recording_provider is not None:
                    recording_provider.record_failure(
                        "candidate_inputs", security.ticker, exc
                    )
                code = RejectionReason.MISSING_DATA
                rejection_counter[code] += 1
                rejected.append(
                    RejectedRecommendation(
                        ticker=security.ticker,
                        rejection_reason_codes=[code],
                        failed_checks=[
                            "provider_input_failure",
                            type(exc).__name__,
                        ],
                    )
                )

        candidates.sort(key=lambda item: item.recommendation.score_vector["composite"], reverse=True)
        approved: list[Recommendation] = []
        signals_by_ticker: dict[str, SignalSnapshot] = {}
        features_by_ticker: dict[str, FeatureSnapshot] = {}
        name_weights: dict[str, float] = defaultdict(float)
        sector_weights: dict[str, float] = defaultdict(float)
        cluster_weights: dict[str, float] = defaultdict(float)
        accepted_candidates: list[RecommendationCandidate] = []
        position_weights: dict[str, float] = {}
        portfolio_risk_metrics = {
            "gross_weight": 0.0,
            "beta": 0.0,
            "annualized_volatility": 0.0,
            "max_liquidation_days": 0.0,
            "liquidity_stress_loss_pct": 0.0,
        }

        for candidate in candidates:
            if len(approved) >= request.publication.top_n:
                break

            ticker = candidate.security.ticker.upper()
            sector = candidate.security.sector or "Unknown"
            position_size = max(0.0, candidate.position_size_pct)
            correlated = [
                item
                for item in accepted_candidates
                if self._return_correlation(candidate.bars, item.bars)
                >= effective_risk_policy.max_pairwise_correlation
            ]
            correlated_cluster_weight = position_size + sum(
                position_weights.get(item.security.ticker.upper(), 0.0)
                for item in correlated
            )
            cluster = "+".join(
                sorted(
                    {
                        ticker,
                        *(item.security.ticker.upper() for item in correlated),
                    }
                )
            )
            proposed_candidates = [*accepted_candidates, candidate]
            proposed_weights = {
                **position_weights,
                ticker: position_size,
            }
            proposed_risk_metrics = self._portfolio_risk_metrics(
                candidates=proposed_candidates,
                weights=proposed_weights,
                benchmark_bars=benchmark_bars,
                reference_equity=effective_risk_policy.portfolio_reference_equity,
                liquidity_participation=effective_risk_policy.liquidity_stress_participation,
            )

            decision = self.risk_engine.evaluate(
                security=candidate.security,
                recommendation=candidate.recommendation,
                signal=candidate.signal,
                risk_policy=effective_risk_policy,
                upcoming_earnings_minutes=candidate.upcoming_earnings_minutes,
                name_weight=name_weights[ticker] + position_size,
                sector_weight=sector_weights[sector] + position_size,
                correlated_cluster_weight=correlated_cluster_weight,
                gross_weight=proposed_risk_metrics["gross_weight"],
                portfolio_beta=proposed_risk_metrics["beta"],
                portfolio_volatility=proposed_risk_metrics["annualized_volatility"],
                max_liquidation_days=proposed_risk_metrics["max_liquidation_days"],
            )

            if decision.approved:
                approved.append(candidate.recommendation)
                signals_by_ticker[candidate.security.ticker] = candidate.signal
                features_by_ticker[candidate.security.ticker] = candidate.feature
                name_weights[ticker] += position_size
                sector_weights[sector] += position_size
                cluster_weights[cluster] = correlated_cluster_weight
                position_weights[ticker] = position_size
                accepted_candidates.append(candidate)
                portfolio_risk_metrics = proposed_risk_metrics
            else:
                for code in decision.reason_codes:
                    rejection_counter[code] += 1
                rejected.append(
                    RejectedRecommendation(
                        ticker=candidate.security.ticker,
                        rejection_reason_codes=decision.reason_codes,
                        failed_checks=decision.failed_checks,
                    )
                )

        top_recommendations = approved

        if recording_provider is not None:
            recording_provider.persist(as_of=as_of, universe=request.universe)

        total_candidates = max(1, initial_count)
        run_metrics = RunMetrics(
            recommendation_count=len(top_recommendations),
            rejection_rate=round(len(rejected) / total_candidates, 6),
            missing_data_rate=round(
                rejection_counter.get(RejectionReason.MISSING_DATA, 0) / total_candidates,
                6,
            ),
            explanation_latency_ms=3.0,
        )

        result = ResearchRunResult(
            run_type=request.run_type,
            generated_at=datetime.now(timezone.utc),
            source_snapshot_id=source_snapshot_id,
            strategy_config_id=strategy_config.strategy_config_id,
            universe_summary={
                "universe": request.universe,
                "initial_count": initial_count,
                "eligible_count": len(filtered),
                "rejection_counts": dict(rejection_counter),
                "candidate_count": len(candidates),
                "provider_failures": provider_failures,
                "snapshot": {
                    "source_snapshot_id": source_snapshot_id,
                    "operation": snapshot_operation,
                },
                "strategy": {
                    "strategy_config_id": strategy_config.strategy_config_id,
                    "config_hash": strategy_config.config_hash,
                },
                "portfolio_exposure": {
                    "name_weights": self._round_weight_map(name_weights),
                    "sector_weights": self._round_weight_map(sector_weights),
                    "correlated_cluster_weights": self._round_weight_map(cluster_weights),
                    "risk_metrics": {
                        key: round(value, 6)
                        for key, value in portfolio_risk_metrics.items()
                    },
                    "constraints": {
                        "max_name_weight": request.risk_policy.max_name_weight,
                        "max_sector_weight": request.risk_policy.max_sector_weight,
                        "max_correlated_cluster_weight": request.risk_policy.max_correlated_cluster_weight,
                        "max_gross_exposure": request.risk_policy.max_gross_exposure,
                        "max_portfolio_beta": request.risk_policy.max_portfolio_beta,
                        "max_portfolio_volatility": request.risk_policy.max_portfolio_volatility,
                        "max_liquidation_days": request.risk_policy.max_liquidation_days,
                    },
                },
            },
            signal_model={
                "model_type": "weighted_linear",
                "weights": {
                    "technical": request.signal_config.technical_weight,
                    "event_news": request.signal_config.event_news_weight,
                    "relative_strength": request.signal_config.relative_strength_weight,
                    "fundamental": request.signal_config.fundamental_weight,
                    "execution_quality": request.signal_config.execution_quality_weight,
                },
                "effective_min_confidence": round(effective_min_confidence, 6),
            },
            recommendations=top_recommendations,
            rejected_recommendations=rejected,
            run_metrics=run_metrics,
            publication_payload={
                "api_endpoints": [
                    "/universe",
                    "/research/run",
                    "/recommendations",
                    "/recommendations/{id}",
                    "/paper-orders",
                    "/metrics",
                ],
                "channels": request.publication.output_channels,
            },
        )

        return PipelineOutput(
            result=result,
            signals_by_ticker=signals_by_ticker,
            features_by_ticker=features_by_ticker,
            strategy_config=strategy_config,
        )

    @staticmethod
    def _apply_universe_filters(security, request: ResearchRunRequest) -> list[str]:
        codes: list[str] = []
        rules = request.universe_rules
        if security.last_price < rules.min_price:
            codes.append(RejectionReason.BELOW_MIN_PRICE)
        if security.avg_dollar_volume < rules.min_avg_dollar_volume:
            codes.append(RejectionReason.BELOW_MIN_LIQUIDITY)
        if security.spread_bps > rules.max_spread_bps:
            codes.append(RejectionReason.ABOVE_MAX_SPREAD)
        if security.market_cap_usd < rules.min_market_cap_usd:
            codes.append("below_min_market_cap")
        if rules.allowed_sectors and security.sector not in rules.allowed_sectors:
            codes.append("sector_not_allowed")
        return codes

    @staticmethod
    def _return_correlation(left_bars: list, right_bars: list) -> float:
        left_by_time = ResearchPipeline._returns_by_timestamp(left_bars)
        right_by_time = ResearchPipeline._returns_by_timestamp(right_bars)
        common = sorted(set(left_by_time).intersection(right_by_time))[-63:]
        size = len(common)
        if size < 3:
            return 0.0
        left = [left_by_time[timestamp] for timestamp in common]
        right = [right_by_time[timestamp] for timestamp in common]
        left_mean = sum(left) / size
        right_mean = sum(right) / size
        covariance = sum(
            (left[index] - left_mean) * (right[index] - right_mean)
            for index in range(size)
        )
        left_var = sum((value - left_mean) ** 2 for value in left)
        right_var = sum((value - right_mean) ** 2 for value in right)
        denominator = math.sqrt(left_var * right_var)
        return covariance / denominator if denominator > 0 else 0.0

    @staticmethod
    def _returns(bars: list) -> list[float]:
        return list(ResearchPipeline._returns_by_timestamp(bars).values())

    @staticmethod
    def _returns_by_timestamp(bars: list) -> dict[datetime, float]:
        ordered = sorted(
            bars,
            key=lambda bar: (
                bar.timestamp.replace(tzinfo=timezone.utc)
                if bar.timestamp.tzinfo is None
                else bar.timestamp.astimezone(timezone.utc)
            ),
        )
        returns: dict[datetime, float] = {}
        for previous, current in zip(ordered, ordered[1:]):
            previous_close = float(previous.adjusted_close or previous.close)
            if previous_close <= 0:
                continue
            timestamp = (
                current.timestamp.replace(tzinfo=timezone.utc)
                if current.timestamp.tzinfo is None
                else current.timestamp.astimezone(timezone.utc)
            )
            returns[timestamp] = float(current.adjusted_close or current.close) / previous_close - 1.0
        return returns

    @staticmethod
    def _portfolio_risk_metrics(
        *,
        candidates: list[RecommendationCandidate],
        weights: dict[str, float],
        benchmark_bars: list,
        reference_equity: float,
        liquidity_participation: float,
    ) -> dict[str, float]:
        if not candidates:
            return {
                "gross_weight": 0.0,
                "beta": 0.0,
                "annualized_volatility": 0.0,
                "max_liquidation_days": 0.0,
                "liquidity_stress_loss_pct": 0.0,
            }
        series = [ResearchPipeline._returns_by_timestamp(item.bars) for item in candidates]
        benchmark_returns = ResearchPipeline._returns_by_timestamp(benchmark_bars)
        common_timestamps = set(benchmark_returns)
        for values in series:
            common_timestamps.intersection_update(values)
        aligned_timestamps = sorted(common_timestamps)[-63:]
        portfolio_returns: list[float] = []
        benchmark: list[float] = []
        if aligned_timestamps:
            for timestamp in aligned_timestamps:
                portfolio_returns.append(
                    sum(
                        weights.get(item.security.ticker.upper(), 0.0) * values[timestamp]
                        for item, values in zip(candidates, series)
                    )
                )
                benchmark.append(benchmark_returns[timestamp])
        volatility = 0.0
        beta = 0.0
        if len(portfolio_returns) >= 2:
            portfolio_mean = sum(portfolio_returns) / len(portfolio_returns)
            variance = sum(
                (value - portfolio_mean) ** 2 for value in portfolio_returns
            ) / (len(portfolio_returns) - 1)
            volatility = math.sqrt(max(0.0, variance)) * math.sqrt(252)
            benchmark_mean = sum(benchmark) / len(benchmark)
            benchmark_variance = sum(
                (value - benchmark_mean) ** 2 for value in benchmark
            ) / (len(benchmark) - 1)
            covariance = sum(
                (portfolio_returns[index] - portfolio_mean)
                * (benchmark[index] - benchmark_mean)
                for index in range(len(portfolio_returns))
            ) / (len(portfolio_returns) - 1)
            beta = covariance / benchmark_variance if benchmark_variance > 0 else 0.0
        liquidation_days: list[float] = []
        stress_loss = 0.0
        for candidate in candidates:
            weight = weights.get(candidate.security.ticker.upper(), 0.0)
            daily_capacity = (
                candidate.security.avg_dollar_volume * liquidity_participation
            )
            days = (
                weight * reference_equity / daily_capacity
                if daily_capacity > 0
                else float("inf")
            )
            liquidation_days.append(days)
            stress_loss += weight * (
                candidate.security.spread_bps / 10_000.0
                + min(0.20, candidate.feature.volatility_20d * math.sqrt(max(days, 1.0) / 252))
            )
        return {
            "gross_weight": sum(max(0.0, value) for value in weights.values()),
            "beta": beta,
            "annualized_volatility": volatility,
            "max_liquidation_days": max(liquidation_days, default=0.0),
            "liquidity_stress_loss_pct": stress_loss,
        }

    @staticmethod
    def _round_weight_map(weights: dict[str, float]) -> dict[str, float]:
        return {key: round(value, 6) for key, value in sorted(weights.items()) if value > 0}
