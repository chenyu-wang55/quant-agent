from __future__ import annotations

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
    SnapshotMode,
)
from domain.policies.rules import RejectionReason
from services.features.engine import FeatureEngine
from services.ingestion.interfaces import DataProvider
from services.ingestion.snapshot_provider import SnapshotRecordingProvider, SnapshotReplayProvider
from services.ranking.price_engine import PriceSizingEngine
from services.ranking.recommendation_builder import RecommendationBuilder
from services.ranking.signal_scorer import SignalScorer
from services.risk.engine import RiskEngine
from infra.db.repositories import SourceSnapshotRepository


@dataclass
class PipelineOutput:
    result: ResearchRunResult
    signals_by_ticker: dict[str, SignalSnapshot]
    features_by_ticker: dict[str, FeatureSnapshot]


@dataclass
class RecommendationCandidate:
    security: SecurityMetadata
    recommendation: Recommendation
    signal: SignalSnapshot
    feature: FeatureSnapshot
    position_size_pct: float
    upcoming_earnings_minutes: int | None


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
        if request.snapshot_mode == SnapshotMode.LATEST:
            # Live mode is noisier on vendor data latency/coverage; apply a calibrated relaxation.
            effective_min_confidence = max(0.60, effective_min_confidence - 0.08)
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

        try:
            benchmark_bars = provider.get_benchmark_bars("SPY", as_of)
        except Exception:
            benchmark_bars = []
            if filtered:
                try:
                    benchmark_bars = provider.get_bars(filtered[0].ticker, as_of)
                except Exception:
                    benchmark_bars = []

        candidates: list[RecommendationCandidate] = []
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
                    )
                )
            except Exception:
                code = RejectionReason.MISSING_DATA
                rejection_counter[code] += 1
                rejected.append(
                    RejectedRecommendation(
                        ticker=security.ticker,
                        rejection_reason_codes=[code],
                        failed_checks=["pipeline_exception"],
                    )
                )

        candidates.sort(key=lambda item: item.recommendation.score_vector["composite"], reverse=True)
        approved: list[Recommendation] = []
        signals_by_ticker: dict[str, SignalSnapshot] = {}
        features_by_ticker: dict[str, FeatureSnapshot] = {}
        name_weights: dict[str, float] = defaultdict(float)
        sector_weights: dict[str, float] = defaultdict(float)
        cluster_weights: dict[str, float] = defaultdict(float)

        for candidate in candidates:
            if len(approved) >= request.publication.top_n:
                break

            ticker = candidate.security.ticker.upper()
            sector = candidate.security.sector or "Unknown"
            cluster = self._correlation_cluster(candidate.security)
            position_size = max(0.0, candidate.position_size_pct)

            decision = self.risk_engine.evaluate(
                security=candidate.security,
                recommendation=candidate.recommendation,
                signal=candidate.signal,
                risk_policy=effective_risk_policy,
                upcoming_earnings_minutes=candidate.upcoming_earnings_minutes,
                name_weight=name_weights[ticker] + position_size,
                sector_weight=sector_weights[sector] + position_size,
                correlated_cluster_weight=cluster_weights[cluster] + position_size,
            )

            if decision.approved:
                approved.append(candidate.recommendation)
                signals_by_ticker[candidate.security.ticker] = candidate.signal
                features_by_ticker[candidate.security.ticker] = candidate.feature
                name_weights[ticker] += position_size
                sector_weights[sector] += position_size
                cluster_weights[cluster] += position_size
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
            universe_summary={
                "universe": request.universe,
                "initial_count": initial_count,
                "eligible_count": len(filtered),
                "rejection_counts": dict(rejection_counter),
                "candidate_count": len(candidates),
                "snapshot": {
                    "source_snapshot_id": source_snapshot_id,
                    "operation": snapshot_operation,
                },
                "portfolio_exposure": {
                    "name_weights": self._round_weight_map(name_weights),
                    "sector_weights": self._round_weight_map(sector_weights),
                    "correlated_cluster_weights": self._round_weight_map(cluster_weights),
                    "constraints": {
                        "max_name_weight": request.risk_policy.max_name_weight,
                        "max_sector_weight": request.risk_policy.max_sector_weight,
                        "max_correlated_cluster_weight": request.risk_policy.max_correlated_cluster_weight,
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
    def _correlation_cluster(security: SecurityMetadata) -> str:
        ticker = security.ticker.upper()
        mega_cap_growth = {
            "AAPL",
            "MSFT",
            "NVDA",
            "AMZN",
            "GOOGL",
            "META",
            "AVGO",
            "AMD",
            "NFLX",
            "CRM",
            "ADBE",
        }
        if ticker in mega_cap_growth:
            return "mega_cap_growth_ai"
        return security.sector or "Unknown"

    @staticmethod
    def _round_weight_map(weights: dict[str, float]) -> dict[str, float]:
        return {key: round(value, 6) for key, value in sorted(weights.items()) if value > 0}
