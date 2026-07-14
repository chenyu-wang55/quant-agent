from __future__ import annotations

from domain.entities.models import Direction, Recommendation, RiskPolicy, SecurityMetadata, SignalSnapshot
from domain.policies.rules import PolicyDecision, RejectionReason


class RiskEngine:
    def evaluate(
        self,
        security: SecurityMetadata,
        recommendation: Recommendation,
        signal: SignalSnapshot,
        risk_policy: RiskPolicy,
        upcoming_earnings_minutes: int | None = None,
        name_weight: float = 0.0,
        sector_weight: float = 0.0,
        correlated_cluster_weight: float = 0.0,
        gross_weight: float = 0.0,
        portfolio_beta: float = 0.0,
        portfolio_volatility: float = 0.0,
        max_liquidation_days: float = 0.0,
    ) -> PolicyDecision:
        reason_codes: list[str] = []
        failed_checks: list[str] = []

        if security.last_price < 1:
            # Defensive impossible-value check from provider anomalies.
            reason_codes.append(RejectionReason.MISSING_DATA)
            failed_checks.append("invalid_last_price")

        if recommendation.confidence < risk_policy.min_confidence:
            reason_codes.append(RejectionReason.BELOW_MIN_CONFIDENCE)
            failed_checks.append("confidence_threshold")

        if security.spread_bps <= 0:
            reason_codes.append(RejectionReason.MISSING_DATA)
            failed_checks.append("invalid_spread")

        if recommendation.direction != Direction.BUY:
            reason_codes.append(RejectionReason.UNSUPPORTED_DIRECTION)
            failed_checks.append("mvp_buy_only")

        if recommendation.entry_zone_low >= recommendation.entry_zone_high:
            reason_codes.append(RejectionReason.INVALID_PRICE_PLAN)
            failed_checks.append("entry_zone_order")

        if recommendation.direction == Direction.BUY:
            if recommendation.stop_loss >= recommendation.entry_zone_low:
                reason_codes.append(RejectionReason.INVALID_PRICE_PLAN)
                failed_checks.append("buy_stop_not_below_entry")
            if recommendation.tp1 <= recommendation.entry_zone_high:
                reason_codes.append(RejectionReason.INVALID_PRICE_PLAN)
                failed_checks.append("buy_tp1_not_above_entry")
            if recommendation.tp2 <= recommendation.tp1:
                reason_codes.append(RejectionReason.INVALID_PRICE_PLAN)
                failed_checks.append("buy_tp2_not_above_tp1")

        if security.last_price > 0:
            entry_mid = (recommendation.entry_zone_low + recommendation.entry_zone_high) / 2.0
            gap_pct = abs(entry_mid - security.last_price) / security.last_price
            if gap_pct > risk_policy.max_entry_gap_pct:
                reason_codes.append(RejectionReason.ENTRY_PLAN_TOO_FAR)
                failed_checks.append("entry_plan_distance")

        if risk_policy.reject_on_material_evidence_conflict and signal.evidence_conflict:
            reason_codes.append(RejectionReason.EVIDENCE_CONFLICT)
            failed_checks.append("evidence_conflict")

        if upcoming_earnings_minutes is not None:
            if not risk_policy.event_trading_enabled and upcoming_earnings_minutes <= risk_policy.earnings_blackout_minutes:
                reason_codes.append(RejectionReason.EARNINGS_BLACKOUT)
                failed_checks.append("earnings_blackout")

        if name_weight > risk_policy.max_name_weight:
            reason_codes.append(RejectionReason.NAME_CONCENTRATION)
            failed_checks.append("max_name_weight")

        if sector_weight > risk_policy.max_sector_weight:
            reason_codes.append(RejectionReason.SECTOR_CONCENTRATION)
            failed_checks.append("max_sector_weight")

        if correlated_cluster_weight > risk_policy.max_correlated_cluster_weight:
            reason_codes.append(RejectionReason.CORRELATED_CLUSTER)
            failed_checks.append("max_correlated_cluster_weight")

        if gross_weight > risk_policy.max_gross_exposure:
            reason_codes.append(RejectionReason.GROSS_EXPOSURE)
            failed_checks.append("max_gross_exposure")

        if abs(portfolio_beta) > risk_policy.max_portfolio_beta:
            reason_codes.append(RejectionReason.PORTFOLIO_BETA)
            failed_checks.append("max_portfolio_beta")

        if portfolio_volatility > risk_policy.max_portfolio_volatility:
            reason_codes.append(RejectionReason.PORTFOLIO_VOLATILITY)
            failed_checks.append("max_portfolio_volatility")

        if max_liquidation_days > risk_policy.max_liquidation_days:
            reason_codes.append(RejectionReason.LIQUIDITY_STRESS)
            failed_checks.append("max_liquidation_days")

        return PolicyDecision(
            approved=len(reason_codes) == 0,
            reason_codes=sorted(set(reason_codes)),
            failed_checks=sorted(set(failed_checks)),
        )
