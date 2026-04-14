from __future__ import annotations

from app.adapters.legacy_projection import legacy_post_from_canonical
from app.models.canonical import CanonicalPostAnalysis
from app.pipeline.helpers import build_media_observation_context, parse_post_analysis_canonical, validate_post_analysis_canonical
from app.prompts import _build_post_history_context, build_parent_post_analysis_prompt, build_post_analysis_prompt
from app.prompts.builders import POST_ANALYSIS_JSON_SCHEMA
from app.services.normalization_service import NormalizationService
from app.services.review_service import ReviewService
from app.services.scoring_service import ScoringService
from app.services.stage_executor import StageExecutor


def _finalize_post_analysis(
    *,
    canonical: CanonicalPostAnalysis,
    normalization_service: NormalizationService,
    scoring_service: ScoringService,
    review_service: ReviewService,
    focus_entity: str | None,
) -> object:
    canonical.focus_entity = normalization_service.normalize_focus_entity(focus_entity)
    explicit_entities = list(canonical.detected_entities)
    normalized_entities = normalization_service.normalize_entities(
        [canonical.summary, canonical.analyst_note]
    )
    if explicit_entities or normalized_entities:
        merged_entities: list[str] = []
        for entity in explicit_entities + normalized_entities:
            if entity and entity != "belirsiz" and entity not in merged_entities:
                merged_entities.append(entity)
        canonical.detected_entities = merged_entities
        canonical.organization_link_score = max(canonical.organization_link_score, min(10, canonical.review.importance_score))
    canonical.review = scoring_service.apply_review_decision(
        signals=canonical.signals,
        ambiguity_flags=canonical.ambiguity_flags,
        organization_link_score=canonical.organization_link_score,
        importance_score=canonical.review.importance_score,
        human_review_required=False,
        reason=canonical.analyst_note or canonical.summary,
    )
    canonical = review_service.apply_thresholds(canonical)
    return legacy_post_from_canonical(canonical)


def execute_post_stage(
    *,
    stage_executor: StageExecutor,
    username: str,
    instagram_username: str,
    bio: str | None,
    caption: str | None,
    media_type: str,
    media_url: str,
    media_items: list[dict[str, str]] | None,
    media_observations: list[dict[str, object]],
    post_history_summaries: list[dict[str, object]],
    account_profile_summary: str,
    template_content: str | None,
    model: str | None,
    max_tokens: int,
    normalization_service: NormalizationService,
    scoring_service: ScoringService,
    review_service: ReviewService,
    attach_media: bool,
    related_account_id: int | None = None,
    related_post_id: int | None = None,
    focus_entity: str | None = None,
    trace_logger: object | None = None,
    trace_prefix: str | None = None,
) -> tuple[CanonicalPostAnalysis, object, str, dict[str, object]]:
    prompt = build_post_analysis_prompt(
        username=username,
        instagram_username=instagram_username,
        bio=bio,
        caption=caption,
        post_history_context=_build_post_history_context(post_history_summaries),
        account_profile_summary=account_profile_summary,
        media_context=build_media_observation_context(media_observations),
        known_organizations=normalization_service.render_known_organizations(focus_entity),
        focus_entity=normalization_service.normalize_focus_entity(focus_entity),
        template_content=template_content,
    )
    if attach_media:
        payload = stage_executor.vllm_service.build_payload(
            description=prompt,
            media_type=media_type,
            media_url=media_url,
            max_tokens=max_tokens,
            model=model,
            media_items=media_items,
        )
    else:
        payload = {
            "model": model or stage_executor.vllm_service.default_model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "stream": False,
        }
    result = stage_executor.execute(
        stage_name="post_analysis",
        prompt_key="post_analysis",
        prompt=prompt,
        payload=payload,
        validator=validate_post_analysis_canonical,
        target_schema=POST_ANALYSIS_JSON_SCHEMA,
        related_account_id=related_account_id,
        related_post_id=related_post_id,
        trace_logger=trace_logger,
        trace_prefix=trace_prefix,
    )
    canonical = result.value if result.value is not None else parse_post_analysis_canonical(result.answer)
    corrected_legacy = _finalize_post_analysis(
        canonical=canonical,
        normalization_service=normalization_service,
        scoring_service=scoring_service,
        review_service=review_service,
        focus_entity=focus_entity,
    )
    return canonical, corrected_legacy, prompt, payload


def execute_parent_post_stage(
    *,
    stage_executor: StageExecutor,
    username: str,
    instagram_username: str,
    bio: str | None,
    caption: str | None,
    media_count: int,
    single_media_analyses: list[dict[str, object]],
    template_content: str | None,
    model: str | None,
    max_tokens: int,
    normalization_service: NormalizationService,
    scoring_service: ScoringService,
    review_service: ReviewService,
    related_account_id: int | None = None,
    related_post_id: int | None = None,
    focus_entity: str | None = None,
    trace_logger: object | None = None,
    trace_prefix: str | None = None,
) -> tuple[CanonicalPostAnalysis, object, str, dict[str, object]]:
    prompt = build_parent_post_analysis_prompt(
        username=username,
        instagram_username=instagram_username,
        bio=bio,
        caption=caption,
        media_count=media_count,
        single_media_analyses=single_media_analyses,
        known_organizations=normalization_service.render_known_organizations(focus_entity),
        focus_entity=normalization_service.normalize_focus_entity(focus_entity),
        template_content=template_content,
    )
    payload = {
        "model": model or stage_executor.vllm_service.default_model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "stream": False,
    }
    result = stage_executor.execute(
        stage_name="post_analysis_parent_merge",
        prompt_key="post_analysis_parent_merge",
        prompt=prompt,
        payload=payload,
        validator=validate_post_analysis_canonical,
        target_schema=POST_ANALYSIS_JSON_SCHEMA,
        related_account_id=related_account_id,
        related_post_id=related_post_id,
        trace_logger=trace_logger,
        trace_prefix=trace_prefix,
    )
    canonical = result.value if result.value is not None else parse_post_analysis_canonical(result.answer)
    corrected_legacy = _finalize_post_analysis(
        canonical=canonical,
        normalization_service=normalization_service,
        scoring_service=scoring_service,
        review_service=review_service,
        focus_entity=focus_entity,
    )
    return canonical, corrected_legacy, prompt, payload
