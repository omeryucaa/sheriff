from __future__ import annotations

from collections import Counter

from app.models.canonical import CanonicalAccountAggregate, CanonicalCommentAnalysis, CanonicalPostAnalysis


class AggregationService:
    def build_account_aggregate(
        self,
        account_id: int,
        posts: list[CanonicalPostAnalysis],
        comments: list[CanonicalCommentAnalysis] | None = None,
    ) -> CanonicalAccountAggregate:
        comments = comments or []
        category_counter: Counter[str] = Counter()
        entity_counter: Counter[str] = Counter()
        signal_counter: Counter[str] = Counter()
        role_counter: Counter[str] = Counter()
        importance_scores: list[int] = []
        org_scores: list[int] = []
        escalation_detected = False
        last_importance = None
        last_org_score = None

        for post in posts:
            category_counter.update(post.categories)
            entity_counter.update(post.detected_entities)
            signal_counter.update(signal.family for signal in post.signals)
            role_counter.update([post.role] if post.role else [])
            importance_scores.append(post.review.importance_score)
            org_scores.append(post.organization_link_score)
            if last_importance is not None and post.review.importance_score > last_importance:
                escalation_detected = True
            if last_org_score is not None and post.organization_link_score > last_org_score:
                escalation_detected = True
            if "coordination" in {signal.family for signal in post.signals} and signal_counter.get("symbolic_affinity", 0) > 0:
                escalation_detected = True
            last_importance = post.review.importance_score
            last_org_score = post.organization_link_score

        dominant_themes = [name for name, count in category_counter.items() if count > 1]
        repeated_entities = [name for name, count in entity_counter.items() if count > 1]
        repeated_indicators = [name for name, count in signal_counter.items() if count > 1]
        role_trend = role_counter.most_common(1)[0][0] if role_counter else "unclear"
        avg_org_score = round(sum(org_scores) / len(org_scores), 2) if org_scores else 0.0
        max_importance = max(importance_scores) if importance_scores else 0
        human_review_recommended = escalation_detected or avg_org_score >= 7 or max_importance >= 8

        return CanonicalAccountAggregate(
            account_id=account_id,
            dominant_themes=dominant_themes,
            repeated_entities=repeated_entities,
            repeated_risk_indicators=repeated_indicators,
            role_trend=role_trend,
            average_organization_link_score=avg_org_score,
            max_importance_score=max_importance,
            escalation_detected=escalation_detected,
            human_review_recommended=human_review_recommended,
        )
