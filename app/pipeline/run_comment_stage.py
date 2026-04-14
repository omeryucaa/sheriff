from __future__ import annotations

import re

from app.adapters.legacy_projection import legacy_comment_from_canonical
from app.models.canonical import CanonicalCommentAnalysis
from app.pipeline.helpers import parse_comment_analysis_canonical, validate_comment_analysis_canonical
from app.prompts import _build_commenter_history_context, build_comment_analysis_prompt
from app.prompts.builders import COMMENT_ANALYSIS_JSON_SCHEMA
from app.services.scoring_service import ScoringService
from app.services.stage_executor import StageExecutor


def _count_supportive_history_items(history: list[dict[str, object]]) -> int:
    total = 0
    for item in history:
        verdict = str(item.get("verdict") or "").strip().lower()
        if verdict in {"destekci_aktif", "destekci_pasif"}:
            total += 1
    return total


def _contains_strong_praise_signal(text: str | None) -> bool:
    content = str(text or "").strip().casefold()
    if not content:
        return False
    praise_tokens = (
        "helal",
        "bravo",
        "kralsin",
        "kralsınız",
        "adamsin",
        "adamsınız",
        "efsane",
        "reis",
        "buyuk adamsin",
        "seni seviyoruz",
        "yanindayiz",
        "arkandayiz",
        "destegimiz",
        "gurur duyuyoruz",
        "adam gibi adam",
    )
    return any(token in content for token in praise_tokens)


def _mentions_investigated_account(comment_text: str | None, investigated_aliases: list[str] | None) -> bool:
    content = str(comment_text or "").casefold()
    if not content:
        return False
    collapsed_content = re.sub(r"[^\w@]+", " ", content)
    normalized_content = f" {collapsed_content} "
    for alias in investigated_aliases or []:
        normalized_alias = str(alias or "").strip().casefold().lstrip("@")
        if len(normalized_alias) < 3:
            continue
        alias_with_at = f" @{normalized_alias} "
        alias_plain = f" {normalized_alias} "
        if alias_with_at in normalized_content or alias_plain in normalized_content:
            return True
    return False


def _apply_comment_score_calibration(
    *,
    canonical: CanonicalCommentAnalysis,
    comment_text: str,
    commenter_history: list[dict[str, object]],
    investigated_aliases: list[str] | None,
    same_batch_commenter_total: int | None,
    post_organization_link_score: int | None,
) -> CanonicalCommentAnalysis:
    comment_type = str(canonical.comment_type or "unclear").strip().lower()
    current_score = max(0, min(10, int(canonical.organization_link_score or 0)))
    sentiment = str(canonical.sentiment or "neutral").strip().lower()
    supportive_history_count = _count_supportive_history_items(commenter_history)
    repeated_support = str(canonical.repeated_support_language or "unclear").strip().lower() == "yes"
    same_batch_total = max(1, int(same_batch_commenter_total or 1))
    post_score = max(0, min(10, int(post_organization_link_score or 0)))
    strong_praise_signal = _contains_strong_praise_signal(comment_text) or _contains_strong_praise_signal(canonical.content_summary_tr)
    mentions_investigated_account = _mentions_investigated_account(comment_text, investigated_aliases)
    supportive_target_praise = (
        comment_type in {"support", "slogan"}
        and sentiment == "positive"
        and (strong_praise_signal or mentions_investigated_account)
    )

    floor_score = 0
    floor_importance = 1

    if comment_type in {"support", "slogan"}:
        floor_score = 3
        floor_importance = 2
        if post_score >= 6:
            floor_score = max(floor_score, 4)
            floor_importance = max(floor_importance, 3)
        if supportive_history_count >= 2 or repeated_support:
            floor_score = max(floor_score, 5)
            floor_importance = max(floor_importance, 4)
        if comment_type == "slogan" or supportive_history_count >= 4:
            floor_score = max(floor_score, 6)
            floor_importance = max(floor_importance, 5)
        if supportive_target_praise:
            floor_score = max(floor_score, 6)
            floor_importance = max(floor_importance, 5)
        if supportive_target_praise and strong_praise_signal and mentions_investigated_account:
            floor_score = max(floor_score, 7)
            floor_importance = max(floor_importance, 6)
        if same_batch_total >= 3 and sentiment == "positive":
            floor_score = max(floor_score, 7)
            floor_importance = max(floor_importance, 6)
    elif comment_type in {"coordination", "threat", "information_sharing"}:
        floor_score = 8
        floor_importance = 7

    calibrated_score = max(current_score, floor_score)
    if supportive_target_praise and calibrated_score < 10:
        bonus = 1
        if strong_praise_signal and mentions_investigated_account and supportive_history_count >= 2:
            bonus = 2
        calibrated_score = min(10, calibrated_score + bonus)
    canonical.organization_link_score = max(0, min(10, calibrated_score))
    canonical.review.importance_score = max(1, min(10, max(int(canonical.review.importance_score or 1), floor_importance)))

    if comment_type in {"support", "slogan"} and (
        supportive_history_count >= 2
        or repeated_support
        or supportive_target_praise
        or canonical.organization_link_score >= 6
    ):
        canonical.active_supporter_flag = True
    if canonical.organization_link_score >= 7:
        canonical.flagged = True

    if isinstance(canonical.legacy_payload, dict):
        if "organization_link_assessment" in canonical.legacy_payload and isinstance(canonical.legacy_payload.get("organization_link_assessment"), dict):
            canonical.legacy_payload["organization_link_assessment"]["organization_link_score"] = canonical.organization_link_score
        if "orgut_baglanti_skoru" in canonical.legacy_payload:
            canonical.legacy_payload["orgut_baglanti_skoru"] = canonical.organization_link_score
        if "review" in canonical.legacy_payload and isinstance(canonical.legacy_payload.get("review"), dict):
            canonical.legacy_payload["review"]["importance_score"] = canonical.review.importance_score

    return canonical


def execute_comment_stage(
    *,
    stage_executor: StageExecutor,
    post_analysis: str,
    username: str,
    bio: str | None,
    caption: str | None,
    account_profile_summary: str | None,
    commenter_username: str | None,
    comment_text: str,
    commenter_history: list[dict[str, object]],
    template_content: str | None,
    model: str | None,
    max_tokens: int,
    scoring_service: ScoringService,
    related_account_id: int | None = None,
    related_post_id: int | None = None,
    related_comment_id: int | None = None,
    focus_entity: str | None = None,
    post_summary: str | None = None,
    post_categories: list[str] | None = None,
    post_detected_entities: list[str] | None = None,
    post_role: str | None = None,
    post_organization_link_score: int | None = None,
    post_threat_level: str | None = None,
    investigated_aliases: list[str] | None = None,
    same_batch_commenter_total: int | None = None,
    trace_logger: object | None = None,
    trace_prefix: str | None = None,
) -> tuple[object, object, str, dict[str, object]]:
    prompt = build_comment_analysis_prompt(
        post_analysis=post_analysis,
        username=username,
        bio=bio,
        caption=caption,
        account_profile_summary=account_profile_summary,
        commenter_username=commenter_username,
        comment_text=comment_text,
        commenter_history_context=_build_commenter_history_context(commenter_history),
        post_summary=post_summary,
        post_categories=post_categories,
        post_detected_entities=post_detected_entities,
        post_role=post_role,
        post_organization_link_score=post_organization_link_score,
        post_threat_level=post_threat_level,
        focus_entity=focus_entity,
        template_content=template_content,
    )
    payload = {
        "model": model or stage_executor.vllm_service.default_model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "stream": False,
    }
    result = stage_executor.execute(
        stage_name="comment_analysis",
        prompt_key="comment_analysis",
        prompt=prompt,
        payload=payload,
        validator=validate_comment_analysis_canonical,
        target_schema=COMMENT_ANALYSIS_JSON_SCHEMA,
        related_account_id=related_account_id,
        related_post_id=related_post_id,
        related_comment_id=related_comment_id,
        trace_logger=trace_logger,
        trace_prefix=trace_prefix,
    )
    canonical = result.value if result.value is not None else parse_comment_analysis_canonical(result.answer)
    canonical.commenter_username = commenter_username
    canonical.text = comment_text
    canonical.focus_entity = focus_entity
    canonical = _apply_comment_score_calibration(
        canonical=canonical,
        comment_text=comment_text,
        commenter_history=commenter_history,
        investigated_aliases=investigated_aliases,
        same_batch_commenter_total=same_batch_commenter_total,
        post_organization_link_score=post_organization_link_score,
    )
    logger = getattr(trace_logger, "log", None)
    if callable(logger):
        logger(f"{trace_prefix or 'COMMENT_ANALYSIS'}_FINAL_CANONICAL", canonical.model_dump(mode="json"))
    legacy = legacy_comment_from_canonical(canonical)
    if callable(logger):
        logger(f"{trace_prefix or 'COMMENT_ANALYSIS'}_FINAL_LEGACY", legacy.model_dump(mode="json"))
    return canonical, legacy, prompt, payload
