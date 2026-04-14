from __future__ import annotations

from app.config.review_rules import HUMAN_REVIEW_THRESHOLDS
from app.models.canonical import CanonicalAccountAggregate, CanonicalPostAnalysis


class ReviewService:
    def apply_thresholds(
        self,
        analysis: CanonicalPostAnalysis,
        aggregate: CanonicalAccountAggregate | None = None,
    ) -> CanonicalPostAnalysis:
        human_review_required = analysis.review.human_review_required
        if analysis.organization_link_score >= HUMAN_REVIEW_THRESHOLDS["organization_link_score_gte"]:
            human_review_required = True
        if analysis.review.importance_score >= HUMAN_REVIEW_THRESHOLDS["importance_score_gte"]:
            human_review_required = True
        if analysis.threat_level in {"yuksek", "kritik"}:
            human_review_required = True
        signal_families = {signal.family for signal in analysis.signals}
        if HUMAN_REVIEW_THRESHOLDS["coordination"] and "coordination" in signal_families:
            human_review_required = True
        if HUMAN_REVIEW_THRESHOLDS["information_leak"] and "information_leak" in signal_families:
            human_review_required = True
        if HUMAN_REVIEW_THRESHOLDS["violence_praise"] and "violence_praise" in signal_families:
            human_review_required = True

        priority_level = analysis.review.priority_level
        if aggregate:
            if aggregate.escalation_detected:
                human_review_required = True
                priority_level = "critical" if analysis.review.importance_score >= 8 else "high"
            elif aggregate.human_review_recommended and priority_level == "medium":
                priority_level = "high"

        analysis.review.human_review_required = human_review_required
        analysis.review.priority_level = priority_level
        return analysis
