from __future__ import annotations

from domain.entities.models import Recommendation


class ExplanationService:
    """Narrow LLM responsibility replacement for MVP.

    In MVP this is deterministic template generation so that all recommendation
    decisions remain auditable and reproducible.
    """

    def build(self, recommendation: Recommendation) -> str:
        return (
            f"{recommendation.ticker} is ranked with confidence {recommendation.confidence:.2f}. "
            f"Entry zone {recommendation.entry_zone_low:.2f}-{recommendation.entry_zone_high:.2f}, "
            f"stop {recommendation.stop_loss:.2f}, targets {recommendation.tp1:.2f}/{recommendation.tp2:.2f}. "
            f"Primary thesis: {'; '.join(recommendation.thesis[:3])}. "
            f"Invalidation: {'; '.join(recommendation.invalid_if[:2])}."
        )
