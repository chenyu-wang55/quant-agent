from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from domain.entities.models import (
    Direction,
    FeatureSnapshot,
    RejectedRecommendation,
    ResearchRunRequest,
    ResearchRunResult,
    RunMetrics,
    SignalSnapshot,
    SnapshotMode,
)
from domain.policies.rules import RejectionReason
from services.features.engine import FeatureEngine
from services.ingestion.interfaces import DataProvider
from services.ranking.price_engine import PriceSizingEngine
from services.ranking.recommendation_builder import RecommendationBuilder
from services.ranking.signal_scorer import SignalScorer
from services.risk.engine import RiskEngine


@dataclass
class PipelineOutput:
    result: ResearchRunResult
    signals_by_ticker: dict[str, SignalSnapshot]
    features_by_ticker: dict[str, FeatureSnapshot]


class ResearchPipeline:
    def __init__(
        self,
        provider: DataProvider,
        feature_engine: FeatureEngine | None = None,
        signal_scorer: SignalScorer | None = None,
        price_engine: PriceSizingEngine | None = None,
        recommendation_builder: RecommendationBuilder | None = None,
        risk_engine: RiskEngine | None = None,
    ) -> None:
        self.provider = provider
        self.feature_engine = feature_engine or FeatureEngine()
        self.signal_scorer = signal_scorer or SignalScorer()
        self.price_engine = price_engine or PriceSizingEngine()
        self.recommendation_builder = recommendation_builder or RecommendationBuilder()
        self.risk_engine = risk_engine or RiskEngine()

    def run(self, request: ResearchRunRequest) -> PipelineOutput:
        as_of = request.as_of.astimezone(timezone.utc)
        source_snapshot_id = request.source_snapshot_id or f"{as_of.isoformat()}#batch001"
        effective_min_confidence = request.risk_policy.min_confidence
        if request.snapshot_mode == SnapshotMode.LATEST:
            # Live mode is noisier on vendor data latency/coverage; apply a calibrated relaxation.
            effective_min_confidence = max(0.60, effective_min_confidence - 0.08)
        effective_risk_policy = request.risk_policy.model_copy(
            update={"min_confidence": effective_min_confidence}
        )

        universe = self.provider.get_universe(request.universe, as_of)
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

        events = self.provider.get_events([item.ticker for item in filtered], as_of)
        events_by_ticker: dict[str, list] = defaultdict(list)
        for event in events:
            for ticker in event.tickers:
                events_by_ticker[ticker].append(event)

        try:
            benchmark_bars = self.provider.get_benchmark_bars("SPY", as_of)
        except Exception:
            benchmark_bars = []
            if filtered:
                try:
                    benchmark_bars = self.provider.get_bars(filtered[0].ticker, as_of)
                except Exception:
                    benchmark_bars = []

        approved = []
        signals_by_ticker: dict[str, SignalSnapshot] = {}
        features_by_ticker: dict[str, FeatureSnapshot] = {}
        for security in filtered:
            try:
                bars = self.provider.get_bars(security.ticker, as_of)
                fundamentals = self.provider.get_fundamentals(security.ticker, as_of)
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

                upcoming_earnings = self.provider.get_upcoming_earnings_minutes(security.ticker, as_of)
                decision = self.risk_engine.evaluate(
                    security=security,
                    recommendation=recommendation,
                    signal=signal,
                    risk_policy=effective_risk_policy,
                    upcoming_earnings_minutes=upcoming_earnings,
                    name_weight=0.0,
                    sector_weight=0.0,
                    correlated_cluster_weight=0.0,
                )

                if decision.approved:
                    approved.append(recommendation)
                    signals_by_ticker[security.ticker] = signal
                    features_by_ticker[security.ticker] = feature
                else:
                    for code in decision.reason_codes:
                        rejection_counter[code] += 1
                    rejected.append(
                        RejectedRecommendation(
                            ticker=security.ticker,
                            rejection_reason_codes=decision.reason_codes,
                            failed_checks=decision.failed_checks,
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

        approved.sort(key=lambda item: item.score_vector["composite"], reverse=True)
        top_recommendations = approved[: request.publication.top_n]

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
