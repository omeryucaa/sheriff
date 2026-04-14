from __future__ import annotations

from app.config.scoring import MIN_SIGNALS_FOR_MEDIUM_CONFIDENCE, MIN_STRONG_SIGNALS_FOR_HIGH_CONFIDENCE
from app.models.canonical import CanonicalReviewDecision, CanonicalSignal


class ScoringService:
    ambiguity_markers = {"reporting", "criticism", "mourning", "ambiguity", "satire"}

    def determine_confidence(self, signals: list[CanonicalSignal], ambiguity_flags: list[str]) -> str:
        unique_families = {signal.family for signal in signals}
        strong_families = {signal.family for signal in signals if signal.strength == "strong"}

        confidence = "low"
        if len(unique_families) >= MIN_SIGNALS_FOR_MEDIUM_CONFIDENCE:
            confidence = "medium"
        if len(strong_families) >= MIN_STRONG_SIGNALS_FOR_HIGH_CONFIDENCE:
            confidence = "high"
        if self._has_ambiguity(ambiguity_flags) and confidence != "low":
            confidence = "medium" if confidence == "high" else "low"
        return confidence

    def apply_review_decision(
        self,
        *,
        signals: list[CanonicalSignal],
        ambiguity_flags: list[str],
        organization_link_score: int,
        importance_score: int,
        human_review_required: bool,
        reason: str,
    ) -> CanonicalReviewDecision:
        confidence = self.determine_confidence(signals, ambiguity_flags)
        corrected_importance = importance_score
        if self._has_ambiguity(ambiguity_flags):
            corrected_importance = max(1, importance_score - 2)
        priority = "low"
        if corrected_importance >= 8 or organization_link_score >= 8:
            priority = "high"
        elif corrected_importance >= 5 or organization_link_score >= 5:
            priority = "medium"
        if corrected_importance >= 9:
            priority = "critical"
        return CanonicalReviewDecision(
            importance_score=corrected_importance,
            priority_level=priority,
            human_review_required=human_review_required,
            confidence=confidence,
            reason=reason,
        )

    def _has_ambiguity(self, ambiguity_flags: list[str]) -> bool:
        return any(flag in self.ambiguity_markers for flag in ambiguity_flags)
