from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import APIRouter, Depends, HTTPException

from app.adapters.legacy_projection import legacy_comment_from_canonical
from app.api.dependencies import get_db_service, get_minio_service, get_vllm_service
from app.pipeline.helpers import (
    build_graph_analysis_summary,
    build_media_observation_context,
    build_same_batch_commenter_history,
    empty_comment_summary,
    evaluate_media_deep_requirement,
    get_prompt_content,
    resolve_media_url,
    save_graph_capture_from_data_url,
    serialize_post_analysis,
    update_account_profile_summary,
)
from app.pipeline.run_aggregation_stage import execute_aggregation_stage
from app.pipeline.run_comment_stage import execute_comment_stage
from app.pipeline.run_media_deep_stage import execute_media_deep_stage
from app.pipeline.run_media_stage import execute_media_stage
from app.pipeline.run_post_stage import execute_post_stage
from app.prompts import _build_account_profile_stats_context
from app.prompts import build_graph_analysis_prompt
from app.schemas import (
    AnalyzeGraphRequest,
    AnalyzeGraphResponse,
    AnalyzeMediaRequest,
    AnalyzeMediaResponse,
    AnalyzePostAndCommentsRequest,
    AnalyzePostAndCommentsResponse,
    CommentAnalysis,
    CommentInput,
    SaveGraphCaptureRequest,
    SaveGraphCaptureResponse,
)
from app.services.aggregation_service import AggregationService
from app.services.normalization_service import NormalizationService
from app.services.review_service import ReviewService
from app.services.scoring_service import ScoringService
from app.services.stage_executor import StageExecutor
from app.settings import Settings, get_settings
from app.vllm_service import VLLMUpstreamError


router = APIRouter()


@router.post("/analyze-media", response_model=AnalyzeMediaResponse)
def analyze_media(
    request: AnalyzeMediaRequest,
    minio_service=Depends(get_minio_service),
    vllm_service=Depends(get_vllm_service),
) -> AnalyzeMediaResponse:
    try:
        media_url = minio_service.presigned_get_object(
            bucket=request.bucket,
            object_key=request.object_key,
            expires_seconds=request.expires_seconds,
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to create presigned URL: {exc}") from exc

    payload = vllm_service.build_payload(
        description=request.description,
        media_type=request.media_type,
        media_url=media_url,
        max_tokens=request.max_tokens,
        model=request.model,
    )
    try:
        raw = vllm_service.create_chat_completion(payload)
        model, answer, usage, finish_reason = vllm_service.extract_answer(raw)
    except VLLMUpstreamError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "vLLM upstream request failed",
                "upstream_status": exc.status_code,
                "upstream_error": exc.message,
            },
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"Invalid vLLM response: {exc}") from exc

    return AnalyzeMediaResponse(
        model=model or (request.model or vllm_service.default_model),
        answer=answer,
        usage=usage if isinstance(usage, dict) else None,
        finish_reason=finish_reason if isinstance(finish_reason, str) else None,
        media_url=media_url,
    )


@router.post("/analyze-post-and-comments", response_model=AnalyzePostAndCommentsResponse)
def analyze_post_and_comments(
    request: AnalyzePostAndCommentsRequest,
    minio_service=Depends(get_minio_service),
    vllm_service=Depends(get_vllm_service),
    db_service=Depends(get_db_service),
) -> AnalyzePostAndCommentsResponse:
    person_id, account_id = db_service.get_or_create_person_account(
        person_name=request.username,
        instagram_username=request.instagram_username or request.username,
        profile_photo_url=request.profile_photo_url,
        bio=request.bio,
    )
    account_profile_summary = db_service.get_account_profile_summary(account_id)
    post_history_summaries = db_service.get_post_history_summaries(account_id)
    normalization_service = NormalizationService(db_service)
    normalized_focus_entity = normalization_service.normalize_focus_entity(request.focus_entity)
    scoring_service = ScoringService()
    review_service = ReviewService()
    aggregation_service = AggregationService()
    stage_executor = StageExecutor(vllm_service=vllm_service, db_service=db_service)

    post_prompt_template = get_prompt_content(db_service, "post_analysis")
    media_prompt_template = get_prompt_content(db_service, "media_analysis")
    media_deep_prompt_template = get_prompt_content(db_service, "media_deep_analysis")
    comment_prompt_template = get_prompt_content(db_service, "comment_analysis")
    account_profile_template = get_prompt_content(db_service, "account_profile_update")
    media_url = resolve_media_url(
        media_url=request.media_url,
        bucket=request.bucket,
        object_key=request.object_key,
        expires_seconds=request.expires_seconds,
        minio_service=minio_service,
    )
    media_items = [{"media_type": request.media_type, "media_url": media_url}]
    media_observation_payloads: list[dict[str, object]] = []

    try:
        if request.enable_deep_media_analysis:
            canonical_media, _, _, _ = execute_media_stage(
                stage_executor=stage_executor,
                media_index=1,
                media_item=media_items[0],
                media_count=1,
                username=request.username,
                instagram_username=request.instagram_username or request.username,
                bio=request.bio,
                caption=request.caption,
                template_content=media_prompt_template,
                model=request.model,
                max_tokens=request.post_max_tokens,
            )
            media_observation = dict(canonical_media.legacy_payload)
            deep_required, deep_reason = evaluate_media_deep_requirement(media_observation)
            deep_payload: dict[str, object] = {}
            deep_status = "not_required"
            if deep_required:
                deep_status = "failed"
                deep_payload, _, _ = execute_media_deep_stage(
                    stage_executor=stage_executor,
                    media_index=1,
                    media_item=media_items[0],
                    media_count=1,
                    username=request.username,
                    instagram_username=request.instagram_username or request.username,
                    bio=request.bio,
                    caption=request.caption,
                    media_observation_context=build_media_observation_context([media_observation]),
                    template_content=media_deep_prompt_template,
                    model=request.model,
                    max_tokens=request.post_max_tokens,
                )
                deep_status = "completed"
            location_confidence = str(
                (
                    (deep_payload.get("location_assessment") or {})
                    if isinstance(deep_payload.get("location_assessment"), dict)
                    else {}
                ).get("location_confidence")
                or "unclear"
            )
            contains_vehicle = bool(
                (deep_payload.get("vehicle_plate_assessment") or {}).get("vehicle_present") == "yes"
                if isinstance(deep_payload.get("vehicle_plate_assessment"), dict)
                else any(item != "unclear" for item in canonical_media.vehicles)
            )
            contains_plate = bool(
                (deep_payload.get("vehicle_plate_assessment") or {}).get("plate_visible") == "yes"
                if isinstance(deep_payload.get("vehicle_plate_assessment"), dict)
                else bool(canonical_media.license_or_signage)
            )
            media_observation_payloads.append(
                {
                    **media_observation,
                    "deep_required": deep_required,
                    "deep_status": deep_status,
                    "deep_reason": deep_reason,
                    "location_confidence": location_confidence,
                    "contains_vehicle": contains_vehicle,
                    "contains_plate": contains_plate,
                    "deep_payload": deep_payload,
                }
            )
        canonical_post, legacy_post, _, _ = execute_post_stage(
            stage_executor=stage_executor,
            username=request.username,
            instagram_username=request.instagram_username or request.username,
            bio=request.bio,
            caption=request.caption,
            media_type=request.media_type,
            media_url=media_url,
            media_items=media_items,
            media_observations=media_observation_payloads,
            post_history_summaries=post_history_summaries,
            account_profile_summary=account_profile_summary,
            focus_entity=normalized_focus_entity,
            template_content=post_prompt_template,
            model=request.model,
            max_tokens=request.post_max_tokens,
            normalization_service=normalization_service,
            scoring_service=scoring_service,
            review_service=review_service,
            attach_media=True,
            related_account_id=account_id,
        )
    except VLLMUpstreamError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "vLLM upstream request failed during post analysis",
                "upstream_status": exc.status_code,
                "upstream_error": exc.message,
            },
        ) from exc

    post_analysis = serialize_post_analysis(legacy_post)
    comment_analyses: list[CommentAnalysis] = []
    summary = empty_comment_summary()
    canonical_comment_payloads: list[dict[str, object] | None] = [None] * len(request.comments)
    ordered_comment_analyses: list[CommentAnalysis | None] = [None] * len(request.comments)

    with ThreadPoolExecutor(max_workers=max(1, min(len(request.comments) or 1, 4))) as pool:
        futures: dict[object, int] = {}
        for index, comment in enumerate(request.comments):
            historical_comments: list[dict[str, object]] = []
            commenter_history_getter = getattr(db_service, "get_commenter_history", None)
            if callable(commenter_history_getter):
                historical_comments.extend(commenter_history_getter(comment.commenter_username))
            historical_comments.extend(
                build_same_batch_commenter_history(
                    [
                        {"commenter_username": c.commenter_username, "text": c.text}
                        for c in request.comments
                    ],
                    current_index=index,
                    commenter_username=comment.commenter_username,
                )
            )
            future = pool.submit(
                execute_comment_stage,
                stage_executor=stage_executor,
                post_analysis=post_analysis,
                username=request.username,
                bio=request.bio,
                caption=request.caption,
                account_profile_summary=account_profile_summary,
                commenter_username=comment.commenter_username,
                comment_text=comment.text,
                commenter_history=historical_comments,
                post_summary=canonical_post.summary,
                post_categories=canonical_post.categories,
                post_detected_entities=canonical_post.detected_entities,
                post_role=canonical_post.role,
                post_organization_link_score=canonical_post.organization_link_score,
                post_threat_level=canonical_post.threat_level,
                focus_entity=normalized_focus_entity,
                template_content=comment_prompt_template,
                model=request.model,
                max_tokens=request.comment_max_tokens,
                scoring_service=scoring_service,
                related_account_id=account_id,
            )
            futures[future] = index
        for future in as_completed(futures):
            index = futures[future]
            comment = request.comments[index]
            try:
                canonical_comment, legacy_comment, _, _ = future.result()
                canonical_comment_payloads[index] = canonical_comment.model_dump(mode="json")
                ordered_comment_analyses[index] = legacy_comment
            except Exception as exc:  # pragma: no cover
                ordered_comment_analyses[index] = CommentAnalysis(
                    commenter_username=comment.commenter_username,
                    text=comment.text,
                    verdict="belirsiz",
                    sentiment="neutral",
                    orgut_baglanti_skoru=0,
                    bayrak=False,
                    reason=f"Yorum analiz edilemedi: {exc}",
                )

    for item in ordered_comment_analyses:
        assert item is not None
        summary[item.verdict] += 1
        comment_analyses.append(item)

    persist_kwargs = {
        "person_name": request.username,
        "instagram_username": request.instagram_username or request.username,
        "profile_photo_url": request.profile_photo_url,
        "bio": request.bio,
        "media_type": request.media_type,
        "media_url": media_url,
        "media_items": media_items,
        "caption": request.caption,
        "post_analysis": legacy_post.ozet or "Gonderi ozetlenemedi.",
        "structured_analysis": legacy_post.model_dump(mode="json"),
        "model": request.model or vllm_service.default_model,
        "comment_analyses": [
            {
                "commenter_username": c.commenter_username,
                "text": c.text,
                "verdict": c.verdict,
                "sentiment": c.sentiment,
                "orgut_baglanti_skoru": c.orgut_baglanti_skoru,
                "bayrak": c.bayrak,
                "reason": c.reason,
            }
            for c in comment_analyses
        ],
    }
    if media_observation_payloads:
        persist_kwargs["media_observations"] = media_observation_payloads
    try:
        persisted = db_service.persist_post_and_comments(**persist_kwargs)
    except TypeError:
        persist_kwargs.pop("media_observations", None)
        persisted = db_service.persist_post_and_comments(**persist_kwargs)
    saver = getattr(db_service, "save_canonical_post_analysis", None)
    if callable(saver):
        saver(persisted.post_id, canonical_post.model_dump(mode="json"), canonical_post.model_dump(mode="json"))
    comment_saver = getattr(db_service, "save_canonical_comment_analysis", None)
    if callable(comment_saver):
        for comment_id, payload in zip(persisted.comment_ids, canonical_comment_payloads):
            if payload is not None:
                comment_saver(comment_id, payload, payload)

    updated_profile_summary = update_account_profile_summary(
        username=request.username,
        instagram_username=request.instagram_username or request.username,
        current_summary=account_profile_summary,
        parsed_post_analysis=legacy_post,
        post_history_summaries=db_service.get_post_history_summaries(account_id),
        vllm_service=vllm_service,
        model=request.model,
        history_stats_context=_build_account_profile_stats_context(db_service.get_post_history_summaries(account_id)),
        template_content=account_profile_template,
        trace_prefix="ANALYZE_POST_ACCOUNT_PROFILE",
    )
    db_service.update_account_profile_summary(account_id, updated_profile_summary)

    aggregate_saver = getattr(db_service, "save_account_aggregate", None)
    canonical_list_getter = getattr(db_service, "list_canonical_post_analyses_for_account", None)
    if callable(aggregate_saver) and callable(canonical_list_getter):
        post_payloads = canonical_list_getter(account_id)
        if canonical_post.model_dump(mode="json") not in post_payloads:
            post_payloads.append(canonical_post.model_dump(mode="json"))
        aggregate = execute_aggregation_stage(
            aggregation_service=aggregation_service,
            account_id=account_id,
            post_payloads=post_payloads,
        )
        aggregate_saver(account_id, aggregate.model_dump(mode="json"))

    return AnalyzePostAndCommentsResponse(
        model=request.model or vllm_service.default_model,
        person_id=person_id if person_id else persisted.person_id,
        instagram_account_id=account_id if account_id else persisted.instagram_account_id,
        post_id=persisted.post_id,
        comment_ids=persisted.comment_ids,
        media_url=media_url,
        post_analysis=post_analysis,
        comment_analyses=comment_analyses,
        summary=summary,
    )


@router.post("/accounts/{account_id}/graph-analysis", response_model=AnalyzeGraphResponse)
def analyze_account_graph(
    account_id: int,
    request: AnalyzeGraphRequest,
    vllm_service=Depends(get_vllm_service),
    db_service=Depends(get_db_service),
) -> AnalyzeGraphResponse:
    detail = db_service.get_account_detail(account_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Account not found")

    posts = db_service.list_account_posts(account_id)
    comments = db_service.list_account_comments(account_id)
    graph = db_service.get_account_graph(account_id)
    prompt_template = get_prompt_content(db_service, "graph_analysis")
    prompt = build_graph_analysis_prompt(
        instagram_username=str(detail.get("instagram_username") or ""),
        bio=str(detail.get("bio") or ""),
        account_profile_summary=str(detail.get("account_profile_summary") or ""),
        graph_summary=build_graph_analysis_summary(detail, posts, comments, graph),
        template_content=prompt_template,
    )
    payload = {
        "model": request.model or vllm_service.default_model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "stream": False,
    }
    if request.graph_image_data_url:
        payload = {
            "model": request.model or vllm_service.default_model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": request.graph_image_data_url}},
                    ],
                }
            ],
            "max_tokens": 500,
            "stream": False,
        }
    try:
        raw = vllm_service.create_chat_completion(payload)
        model, answer, _, _ = vllm_service.extract_answer(raw)
    except VLLMUpstreamError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "vLLM upstream request failed during graph analysis",
                "upstream_status": exc.status_code,
                "upstream_error": exc.message,
            },
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"Invalid vLLM response during graph analysis: {exc}") from exc

    final_model = model or (request.model or vllm_service.default_model)
    final_answer = answer.strip()
    db_service.update_account_graph_analysis(account_id, final_answer, final_model)
    saved = db_service.get_account_graph_analysis(account_id)
    return AnalyzeGraphResponse(
        account_id=account_id,
        model=final_model,
        analysis=final_answer,
        updated_at=saved.get("updated_at") or None,
    )


@router.post("/accounts/{account_id}/graph-capture", response_model=SaveGraphCaptureResponse)
def save_account_graph_capture(
    account_id: int,
    request: SaveGraphCaptureRequest,
    settings: Settings = Depends(get_settings),
    db_service=Depends(get_db_service),
) -> SaveGraphCaptureResponse:
    detail = db_service.get_account_detail(account_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Account not found")

    capture_url = save_graph_capture_from_data_url(account_id, request.graph_image_data_url, settings.sqlite_db_path)
    db_service.update_account_graph_capture(account_id, capture_url)
    saved = db_service.get_account_graph_capture(account_id)
    return SaveGraphCaptureResponse(
        account_id=account_id,
        capture_url=capture_url,
        updated_at=saved.get("updated_at") or None,
    )
