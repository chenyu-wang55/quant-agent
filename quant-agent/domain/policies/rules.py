from __future__ import annotations

from pydantic import BaseModel, Field


class RejectionReason:
    BELOW_MIN_PRICE = "below_min_price"
    BELOW_MIN_LIQUIDITY = "below_min_liquidity"
    ABOVE_MAX_SPREAD = "above_max_spread"
    BELOW_MIN_CONFIDENCE = "below_min_confidence"
    EARNINGS_BLACKOUT = "earnings_blackout"
    NAME_CONCENTRATION = "name_concentration"
    SECTOR_CONCENTRATION = "sector_concentration"
    CORRELATED_CLUSTER = "correlated_cluster"
    GROSS_EXPOSURE = "gross_exposure"
    PORTFOLIO_BETA = "portfolio_beta"
    PORTFOLIO_VOLATILITY = "portfolio_volatility"
    LIQUIDITY_STRESS = "liquidity_stress"
    ENTRY_PLAN_TOO_FAR = "entry_plan_too_far_from_spot"
    EVIDENCE_CONFLICT = "evidence_conflict"
    UNSUPPORTED_DIRECTION = "unsupported_direction"
    INVALID_PRICE_PLAN = "invalid_price_plan"
    MISSING_DATA = "missing_data"


class PolicyDecision(BaseModel):
    approved: bool
    reason_codes: list[str] = Field(default_factory=list)
    failed_checks: list[str] = Field(default_factory=list)
