from __future__ import annotations

from app.models.canonical import CanonicalAccountAggregate, CanonicalPostAnalysis
from app.services.aggregation_service import AggregationService


def execute_aggregation_stage(
    *,
    aggregation_service: AggregationService,
    account_id: int,
    post_payloads: list[dict[str, object]],
) -> CanonicalAccountAggregate:
    posts = [CanonicalPostAnalysis.model_validate(item) for item in post_payloads]
    return aggregation_service.build_account_aggregate(account_id, posts)
