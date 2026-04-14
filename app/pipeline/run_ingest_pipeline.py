from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from app.config.review_rules import COMMENT_REVIEW_QUEUE_MIN_SCORE
from app.pipeline.helpers import (
    IngestTraceLogger,
    build_final_account_summary_fallback,
    build_media_observation_context,
    build_same_batch_commenter_history,
    build_graph_analysis_summary,
    discover_usernames_in_bucket,
    extract_embedded_comments,
    find_latest_run_id,
    generate_final_account_profile_summary,
    evaluate_media_deep_requirement,
    get_prompt_content,
    infer_post_media_type,
    parse_followup_candidate_analysis,
    read_json_object,
    read_json_value,
    read_jsonl_objects,
    resolve_archive_bucket,
    validate_followup_candidate_analysis,
)
from app.pipeline.helpers import collect_post_media_items as _collect_post_media_items
from app.pipeline.run_aggregation_stage import execute_aggregation_stage
from app.pipeline.run_comment_stage import execute_comment_stage
from app.pipeline.run_media_deep_stage import execute_media_deep_stage
from app.pipeline.run_media_stage import execute_media_stage
from app.pipeline.run_post_stage import execute_parent_post_stage, execute_post_stage
from app.prompts import build_followup_candidate_analysis_prompt
from app.prompts.builders import FOLLOWUP_CANDIDATE_ANALYSIS_JSON_SCHEMA
from app.schemas import IngestInstagramAccountLatestRequest, IngestInstagramAccountLatestResponse, IngestWatchScanRequest, IngestWatchScanResponse, IngestWorkersRunOnceResponse, IngestJobItem
from app.services.aggregation_service import AggregationService
from app.services.normalization_service import NormalizationService
from app.services.review_service import ReviewService
from app.services.scoring_service import ScoringService
from app.services.stage_executor import StageExecutor
from app.vllm_service import VLLMUpstreamError


def _sync_batch_target_for_job(
    *,
    db_service: object,
    job: dict[str, object],
    status: str,
    note: str | None = None,
) -> None:
    batch_target_id = job.get("batch_target_id")
    if batch_target_id is None:
        return
    db_service.update_batch_target_status(int(batch_target_id), status, note=note)
    batch_job_id = job.get("batch_job_id")
    if batch_job_id is not None:
        db_service.refresh_batch_job_status(int(batch_job_id))


def _build_followup_relationship_evidence(candidate: dict[str, object], commenter_history: list[dict[str, object]]) -> str:
    verdict_counter = candidate.get("verdicts") or Counter()
    verdict_text = ", ".join(f"{name}: {count}" for name, count in verdict_counter.most_common(5)) or "yok"
    reasons = [str(item).strip() for item in candidate.get("reasons", []) if str(item).strip()]
    history_lines = []
    for index, item in enumerate(commenter_history[-5:], 1):
        history_lines.append(
            f"[{index}] Post: {item.get('post_ozet') or '-'} | "
            f"Yorum: {item.get('comment_text') or '-'} | "
            f"Verdict: {item.get('verdict') or 'belirsiz'} | "
            f"Skor: {item.get('orgut_baglanti_skoru') or 0}"
        )
    lines = [
        f"Hesaba birakilan yorum sayisi: {candidate.get('comment_count') or 0}",
        f"Flagli yorum sayisi: {candidate.get('flagged_count') or 0}",
        f"Maksimum orgut baglanti skoru: {candidate.get('max_org_score') or 0}",
        f"Verdict dagilimi: {verdict_text}",
    ]
    if reasons:
        lines.append(f"Son nedenler: {' | '.join(reasons[:4])}")
    if history_lines:
        lines.append("Adayin onceki yorum gecmisi:")
        lines.extend(history_lines)
    return "\n".join(lines)


def _build_followup_interaction_snippets(candidate: dict[str, object], commenter_history: list[dict[str, object]]) -> str:
    snippets = [str(item).strip() for item in candidate.get("snippets", []) if str(item).strip()]
    history_snippets = [
        str(item.get("comment_text") or "").strip()
        for item in commenter_history[-3:]
        if str(item.get("comment_text") or "").strip()
    ]
    merged: list[str] = []
    for snippet in [*snippets[:4], *history_snippets]:
        if snippet and snippet not in merged:
            merged.append(snippet)
    if not merged:
        return "Etkilesim ornegi bulunmuyor."
    return "\n".join(f"- {snippet}" for snippet in merged[:6])


def _build_followup_graph_tie_summary(candidate: dict[str, object]) -> str:
    verdict_counter = candidate.get("verdicts") or Counter()
    dominant_verdict = verdict_counter.most_common(1)[0][0] if verdict_counter else "belirsiz"
    return (
        f"Aday dugum tipi: commenter\n"
        f"Seed hesaba yorum baglari: {candidate.get('comment_count') or 0}\n"
        f"Flagli bag sayisi: {candidate.get('flagged_count') or 0}\n"
        f"En baskin yorum verdicti: {dominant_verdict}\n"
        f"En yuksek baglanti skoru: {candidate.get('max_org_score') or 0}"
    )


def _build_followup_decision_note(decision: dict[str, object]) -> str:
    candidate = str(decision.get("candidate_username") or "").strip() or "aday"
    relationship = str(decision.get("relationship_to_seed") or "unclear")
    strength = str(decision.get("relationship_strength") or "low")
    risk_level = str(decision.get("risk_level") or "low")
    branch_recommended = str(decision.get("branch_recommended") or "no")
    reason = str(decision.get("reason_tr") or "").strip()
    return (
        f"LLM follow-up: @{candidate} iliski={relationship}, guc={strength}, risk={risk_level}, "
        f"takip={branch_recommended}. {reason}".strip()
    )


def _should_enqueue_followup_candidate(decision: dict[str, object]) -> bool:
    if str(decision.get("branch_recommended") or "no") == "yes":
        return True
    if str(decision.get("risk_level") or "low") in {"high", "critical"}:
        return True
    if str(decision.get("relationship_to_seed") or "unclear") == "possible_operator":
        return True
    return (
        str(decision.get("relationship_strength") or "low") == "high"
        and str(decision.get("relationship_to_seed") or "unclear") in {"supporter", "peer", "amplifier"}
    )


def _should_enqueue_followup_candidate_by_heuristic(candidate: dict[str, object], commenter_history: list[dict[str, object]]) -> bool:
    if int(candidate.get("flagged_count") or 0) > 0:
        return True
    if int(candidate.get("max_org_score") or 0) >= 7:
        return True
    risky_history = {
        str(item.get("verdict") or "belirsiz")
        for item in commenter_history
        if str(item.get("verdict") or "belirsiz") in {"destekci_aktif", "koordinasyon", "bilgi_ifsa", "tehdit", "nefret_soylemi"}
    }
    if risky_history:
        return True
    return int(candidate.get("comment_count") or 0) >= 2 and any(
        verdict in {"destekci_aktif", "destekci_pasif", "koordinasyon"}
        for verdict in (candidate.get("verdicts") or Counter()).keys()
    )


def _enqueue_followup_jobs(
    *,
    job: dict[str, object],
    response: IngestInstagramAccountLatestResponse,
    minio_service: object,
    vllm_service: object,
    db_service: object,
) -> None:
    batch_job_id = job.get("batch_job_id")
    if batch_job_id is None:
        return
    batch_job = db_service.get_batch_job(int(batch_job_id))
    if not batch_job or str(batch_job.get("mode") or "") != "all":
        return

    bucket = str(job["bucket"])
    auto_enqueue_followups = bool(batch_job.get("auto_enqueue_followups"))
    focus_entity = job.get("focus_entity")
    country = job.get("country")
    parent_username = str(job["target_username"])
    trace_logger = IngestTraceLogger(None)

    detail = db_service.get_account_detail(int(response.instagram_account_id)) or {}
    account_comments = db_service.list_account_comments(int(response.instagram_account_id))
    graph = db_service.get_account_graph(int(response.instagram_account_id))
    graph_summary = build_graph_analysis_summary(detail, db_service.list_account_posts(int(response.instagram_account_id)), account_comments, graph)
    followup_prompt_template = get_prompt_content(db_service, "followup_candidate_analysis")
    stage_executor = StageExecutor(vllm_service=vllm_service, db_service=db_service)

    candidate_map: dict[str, dict[str, object]] = {}
    for comment in account_comments:
        commenter_username = str(comment.get("commenter_username") or "").strip().lower()
        if not commenter_username or commenter_username == parent_username.strip().lower():
            continue
        candidate = candidate_map.setdefault(
            commenter_username,
            {
                "candidate_username": commenter_username,
                "comment_count": 0,
                "flagged_count": 0,
                "max_org_score": 0,
                "reasons": [],
                "verdicts": Counter(),
                "snippets": [],
            },
        )
        candidate["comment_count"] = int(candidate["comment_count"]) + 1
        if bool(comment.get("bayrak")):
            candidate["flagged_count"] = int(candidate["flagged_count"]) + 1
        try:
            candidate["max_org_score"] = max(int(candidate["max_org_score"]), int(comment.get("orgut_baglanti_skoru") or 0))
        except (TypeError, ValueError):
            pass
        verdict = str(comment.get("verdict") or "belirsiz").strip()
        candidate["verdicts"][verdict] += 1
        reason = str(comment.get("reason") or "").strip()
        if reason:
            candidate["reasons"].append(reason)
        snippet = str(comment.get("comment_text") or "").strip()
        if snippet:
            candidate["snippets"].append(snippet)

    candidate_usernames = {
        username
        for username, item in candidate_map.items()
        if username in {str(value or "").strip().lower() for value in response.flagged_usernames}
        or int(item["flagged_count"]) > 0
        or int(item["max_org_score"]) >= 7
        or int(item["comment_count"]) >= 2
    }

    for normalized_username in sorted(candidate_usernames):
        candidate = candidate_map.get(normalized_username) or {}
        commenter_history = db_service.get_commenter_history(normalized_username)
        relationship_evidence = _build_followup_relationship_evidence(candidate, commenter_history)
        interaction_snippets = _build_followup_interaction_snippets(candidate, commenter_history)
        candidate_graph_summary = _build_followup_graph_tie_summary(candidate)
        prompt = build_followup_candidate_analysis_prompt(
            username=str(detail.get("full_name") or detail.get("instagram_username") or parent_username),
            instagram_username=str(detail.get("instagram_username") or parent_username),
            candidate_username=normalized_username,
            seed_account_summary=str(detail.get("account_profile_summary") or "Özet bulunmuyor."),
            relationship_evidence=relationship_evidence,
            interaction_snippets=interaction_snippets,
            graph_tie_summary=f"{candidate_graph_summary}\n\nGenel seed graph özeti:\n{graph_summary}",
            focus_entity=str(focus_entity) if focus_entity else None,
            template_content=followup_prompt_template,
        )
        payload = {
            "model": getattr(vllm_service, "default_model", None),
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 320,
            "stream": False,
        }

        should_enqueue = False
        decision_note = "Follow-up kararı üretilemedi."
        try:
            result = stage_executor.execute(
                stage_name="followup_candidate_analysis",
                prompt_key="followup_candidate_analysis",
                prompt=prompt,
                payload=payload,
                validator=validate_followup_candidate_analysis,
                target_schema=FOLLOWUP_CANDIDATE_ANALYSIS_JSON_SCHEMA,
                related_account_id=int(response.instagram_account_id),
            )
            decision = result.value if result.value is not None else parse_followup_candidate_analysis(result.answer)
            should_enqueue = _should_enqueue_followup_candidate(decision)
            decision_note = _build_followup_decision_note(decision)
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=int(job["id"]),
                event_type="followup_evaluated",
                stage="followup",
                message=f"@{normalized_username} follow-up adayi degerlendirildi: {decision_note}",
            )
        except Exception:
            should_enqueue = _should_enqueue_followup_candidate_by_heuristic(candidate, commenter_history)
            decision_note = (
                "LLM follow-up karari alinamadi; sezgisel kuralla "
                + ("takibe alindi." if should_enqueue else "elenmis kabul edildi.")
            )

        target_row, created = db_service.create_or_get_batch_target(
            batch_job_id=int(batch_job_id),
            raw_target=normalized_username,
            normalized_username=normalized_username,
            source_kind="followup",
            parent_username=parent_username,
            note=decision_note,
        )
        if not should_enqueue:
            db_service.update_batch_target_status(int(target_row["id"]), "skipped", note=decision_note)
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=int(job["id"]),
                event_type="followup_skipped",
                stage="followup",
                message=f"@{normalized_username} follow-up icin uygun bulunmadi. {decision_note}",
            )
            continue

        if not auto_enqueue_followups:
            db_service.update_batch_target_status(int(target_row["id"]), "suggested", note=decision_note)
            candidate_person_id = db_service.get_or_create_person(normalized_username)
            db_service.upsert_review_queue(
                normalized_username,
                decision_note,
                "followup_suggested",
                person_id=candidate_person_id,
            )
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=int(job["id"]),
                event_type="followup_suggested",
                stage="followup",
                message=f"@{normalized_username} arastirma adayi olarak onerildi; otomatik ingest acilmadi. {decision_note}",
            )
            continue

        existing_ingest_job_id = target_row.get("ingest_job_id")
        if existing_ingest_job_id:
            existing_job = db_service.get_ingest_job(int(existing_ingest_job_id))
            if existing_job:
                db_service.attach_ingest_job_to_batch_target(
                    int(target_row["id"]),
                    int(existing_ingest_job_id),
                    status=str(existing_job.get("status") or "enqueued"),
                )
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=int(job["id"]),
                    event_type="followup_existing",
                    stage="followup",
                    message=f"@{normalized_username} zaten follow-up job ile kuyrukta. {decision_note}",
                )
            continue

        try:
            run_id = find_latest_run_id(minio_service, bucket, normalized_username)
        except HTTPException:
            db_service.update_batch_target_status(
                int(target_row["id"]),
                "missing_archive",
                note=f"{decision_note} MinIO arsivi bulunamadi.",
            )
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=int(job["id"]),
                event_type="followup_missing_archive",
                stage="followup",
                message=f"@{normalized_username} icin MinIO arsivi bulunamadi. {decision_note}",
            )
            continue
        db_service.upsert_ingest_source(username=normalized_username, bucket=bucket, last_seen_run_id=run_id)
        ingest_job_id, _ = db_service.enqueue_ingest_job(
            username=normalized_username,
            bucket=bucket,
            run_id=run_id,
            batch_job_id=int(batch_job_id),
            batch_target_id=int(target_row["id"]),
            source_kind="followup",
            parent_username=parent_username,
            focus_entity=str(focus_entity) if focus_entity else None,
            country=str(country) if country else None,
        )
        if ingest_job_id is None:
            if created:
                db_service.update_batch_target_status(int(target_row["id"]), "pending", note=decision_note)
            continue
        db_service.attach_ingest_job_to_batch_target(int(target_row["id"]), int(ingest_job_id), status="enqueued")
        _emit_ingest_event(
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=int(job["id"]),
            event_type="followup_enqueued",
            stage="followup",
            message=f"@{normalized_username} follow-up inceleme icin kuyruga alindi. {decision_note}",
        )

    db_service.refresh_batch_job_status(int(batch_job_id))


def _shorten_text(value: str | None, limit: int = 96) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _emit_ingest_event(
    *,
    db_service: object,
    trace_logger: IngestTraceLogger,
    job_id: int | None,
    event_type: str,
    stage: str,
    message: str,
    source_post_id: str | None = None,
    commenter_username: str | None = None,
    post_index: int | None = None,
    post_total: int | None = None,
    media_index: int | None = None,
    media_total: int | None = None,
    comment_index: int | None = None,
    comment_total: int | None = None,
    current_stage: str | None = None,
    current_post_id: str | None = None,
    current_media_index: int | None = None,
    total_media_items: int | None = None,
    current_comment_index: int | None = None,
    total_comments: int | None = None,
    current_commenter_username: str | None = None,
    total_posts: int | None = None,
) -> None:
    payload = {
        "event_type": event_type,
        "stage": stage,
        "message": message,
        "source_post_id": source_post_id,
        "commenter_username": commenter_username,
        "post_index": post_index,
        "post_total": post_total,
        "media_index": media_index,
        "media_total": media_total,
        "comment_index": comment_index,
        "comment_total": comment_total,
    }
    trace_logger.log(f"INGEST_EVENT_{event_type.upper()}", payload)
    if job_id is None:
        return
    recorder = getattr(db_service, "record_ingest_job_event", None)
    if callable(recorder):
        recorder({"ingest_job_id": job_id, **payload})
    updater = getattr(db_service, "update_ingest_job_progress", None)
    if callable(updater):
        progress: dict[str, object] = {
            "current_stage": current_stage or stage,
            "current_event": message,
        }
        if post_index is not None:
            progress["current_post_index"] = post_index
        if (total_posts if total_posts is not None else post_total) is not None:
            progress["total_posts"] = total_posts if total_posts is not None else post_total
        if (current_post_id if current_post_id is not None else source_post_id) is not None:
            progress["current_post_id"] = current_post_id if current_post_id is not None else source_post_id
        if (current_media_index if current_media_index is not None else media_index) is not None:
            progress["current_media_index"] = current_media_index if current_media_index is not None else media_index
        if (total_media_items if total_media_items is not None else media_total) is not None:
            progress["total_media_items"] = total_media_items if total_media_items is not None else media_total
        if (current_comment_index if current_comment_index is not None else comment_index) is not None:
            progress["current_comment_index"] = current_comment_index if current_comment_index is not None else comment_index
        if (total_comments if total_comments is not None else comment_total) is not None:
            progress["total_comments"] = total_comments if total_comments is not None else comment_total
        if (current_commenter_username if current_commenter_username is not None else commenter_username) is not None:
            progress["current_commenter_username"] = current_commenter_username if current_commenter_username is not None else commenter_username
        updater(job_id, **progress)


def _build_single_media_analysis_payload(
    *,
    media_index: int,
    media_item: dict[str, str],
    canonical_post: object,
    legacy_post: object,
) -> dict[str, Any]:
    return {
        "media_index": media_index,
        "media_type": str(media_item.get("media_type") or "image"),
        "analysis_mode": "single_media_post",
        "ozet": getattr(legacy_post, "ozet", ""),
        "icerik_kategorisi": list(getattr(legacy_post, "icerik_kategorisi", []) or []),
        "tehdit_seviyesi": getattr(getattr(legacy_post, "tehdit_degerlendirmesi", None), "tehdit_seviyesi", "belirsiz"),
        "orgut": getattr(getattr(legacy_post, "orgut_baglantisi", None), "tespit_edilen_orgut", "belirsiz"),
        "analist_notu": getattr(legacy_post, "analist_notu", ""),
        "canonical_post_analysis": canonical_post.model_dump(mode="json"),
        "legacy_post_analysis": legacy_post.model_dump(mode="json"),
    }


def _execute_single_media_pipeline(
    *,
    stage_executor: StageExecutor,
    person_name: str,
    instagram_username: str,
    bio: str | None,
    caption: str | None,
    account_profile_summary: str | None,
    media_index: int,
    media_count: int,
    media_item: dict[str, str],
    post_prompt_template: str | None,
    media_prompt_template: str | None,
    media_deep_prompt_template: str | None,
    normalized_focus_entity: str | None,
    request_model: str | None,
    request_post_max_tokens: int,
    normalization_service: NormalizationService,
    scoring_service: ScoringService,
    review_service: ReviewService,
    related_account_id: int,
    trace_logger: object | None,
    trace_prefix: str | None,
) -> dict[str, object]:
    canonical_media, _, _, _ = execute_media_stage(
        stage_executor=stage_executor,
        media_index=media_index,
        media_item=media_item,
        media_count=media_count,
        username=person_name,
        instagram_username=instagram_username,
        bio=bio,
        caption=caption,
        template_content=media_prompt_template,
        model=request_model,
        max_tokens=request_post_max_tokens,
    )
    observation_payload = canonical_media.legacy_payload
    deep_required, deep_reason = evaluate_media_deep_requirement(observation_payload)
    deep_payload: dict[str, object] = {}
    deep_status = "not_required"
    if deep_required:
        deep_status = "failed"
        deep_payload, _, _ = execute_media_deep_stage(
            stage_executor=stage_executor,
            media_index=media_index,
            media_item=media_item,
            media_count=media_count,
            username=person_name,
            instagram_username=instagram_username,
            bio=bio,
            caption=caption,
            media_observation_context=build_media_observation_context([observation_payload]),
            template_content=media_deep_prompt_template,
            model=request_model,
            max_tokens=request_post_max_tokens,
        )
        deep_status = "completed"

    location_confidence = str(
        ((deep_payload.get("location_assessment") or {}) if isinstance(deep_payload.get("location_assessment"), dict) else {}).get(
            "location_confidence"
        )
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
    canonical_media.deep_required = deep_required
    canonical_media.deep_status = deep_status
    canonical_media.deep_reason = deep_reason
    canonical_media.location_confidence = location_confidence
    canonical_media.contains_vehicle = contains_vehicle
    canonical_media.contains_plate = contains_plate
    canonical_media.deep_payload = deep_payload

    persisted_observation = {
        **dict(observation_payload),
        "deep_required": deep_required,
        "deep_status": deep_status,
        "deep_reason": deep_reason,
        "location_confidence": location_confidence,
        "contains_vehicle": contains_vehicle,
        "contains_plate": contains_plate,
        "deep_payload": deep_payload,
    }

    canonical_post, legacy_post, post_prompt, post_payload = execute_post_stage(
        stage_executor=stage_executor,
        username=person_name,
        instagram_username=instagram_username,
        bio=bio,
        caption=caption,
        media_type=str(media_item.get("media_type") or "image"),
        media_url=str(media_item.get("media_url") or ""),
        media_items=[media_item],
        media_observations=[persisted_observation],
        post_history_summaries=[],
        account_profile_summary=account_profile_summary or "",
        focus_entity=normalized_focus_entity,
        template_content=post_prompt_template,
        model=request_model,
        max_tokens=request_post_max_tokens,
        normalization_service=normalization_service,
        scoring_service=scoring_service,
        review_service=review_service,
        attach_media=True,
        related_account_id=related_account_id,
        trace_logger=trace_logger,
        trace_prefix=trace_prefix,
    )
    persisted_payload = _build_single_media_analysis_payload(
        media_index=media_index,
        media_item=media_item,
        canonical_post=canonical_post,
        legacy_post=legacy_post,
    )
    return {
        "canonical_post": canonical_post,
        "legacy_post": legacy_post,
        "prompt": post_prompt,
        "payload": post_payload,
        "persisted_payload": persisted_payload,
        "media_observation": persisted_observation,
    }


def _load_post_comments(
    *,
    minio_service: object,
    bucket: str,
    post_dir_prefix: str,
    post: dict[str, object],
) -> list[dict[str, object]]:
    comments_key = f"{post_dir_prefix}comments.jsonl"
    comments = read_jsonl_objects(minio_service, bucket, comments_key) if minio_service.object_exists(bucket, comments_key) else []
    if not comments:
        comments = extract_embedded_comments(post)
    return comments


def _resolve_archive_media_url(
    *,
    minio_service: object,
    bucket: str,
    object_key: str | None,
    fallback_url: str | None,
    expires_seconds: int,
) -> str | None:
    object_key_text = str(object_key or "").strip()
    if object_key_text and minio_service.object_exists(bucket, object_key_text):
        return minio_service.presigned_get_object(bucket, object_key_text, expires_seconds)
    fallback_text = str(fallback_url or "").strip()
    return fallback_text or None


def _collect_story_items(
    *,
    minio_service: object,
    bucket: str,
    base_prefix: str,
    expires_seconds: int,
) -> list[dict[str, Any]]:
    stories_key = f"{base_prefix}/stories/stories.json"
    if not minio_service.object_exists(bucket, stories_key):
        return []
    stories = read_json_value(minio_service, bucket, stories_key)
    if not isinstance(stories, list):
        return []
    items: list[dict[str, Any]] = []
    for index, story in enumerate(stories, 1):
        if not isinstance(story, dict):
            continue
        media_url = _resolve_archive_media_url(
            minio_service=minio_service,
            bucket=bucket,
            object_key=str(story.get("object_key") or "").strip() or None,
            fallback_url=str(story.get("media_url") or "").strip() or None,
            expires_seconds=expires_seconds,
        )
        media_kind = str(story.get("media_kind") or "image").strip().lower()
        media_type = "video" if media_kind == "video" else "image"
        if not media_url:
            continue
        story_id = str(story.get("story_id") or f"story-{index}").strip()
        items.append(
            {
                "content_kind": "story",
                "post_index_label": "story",
                "source_post_id": f"story:{story_id}",
                "source_post_url": None,
                "caption": None,
                "post_type": media_type,
                "post_media_items": [{"media_type": media_type, "media_url": media_url}],
                "media_type": media_type,
                "media_url": media_url,
                "comments": [],
                "source_container_id": None,
                "source_container_title": None,
                "source_created_at": str(story.get("created_at") or "") or None,
            }
        )
    return items


def _collect_highlight_items(
    *,
    minio_service: object,
    bucket: str,
    base_prefix: str,
    expires_seconds: int,
) -> list[dict[str, Any]]:
    highlights_key = f"{base_prefix}/highlights/highlights.json"
    if not minio_service.object_exists(bucket, highlights_key):
        return []
    highlights = read_json_value(minio_service, bucket, highlights_key)
    if not isinstance(highlights, list):
        return []
    items: list[dict[str, Any]] = []
    for highlight in highlights:
        if not isinstance(highlight, dict):
            continue
        highlight_id = str(highlight.get("highlight_id") or "").strip() or "highlight"
        highlight_title = str(highlight.get("title") or "Highlight").strip() or "Highlight"
        for index, entry in enumerate(highlight.get("items") or [], 1):
            if not isinstance(entry, dict):
                continue
            media_url = _resolve_archive_media_url(
                minio_service=minio_service,
                bucket=bucket,
                object_key=str(entry.get("object_key") or "").strip() or None,
                fallback_url=str(entry.get("media_url") or "").strip() or None,
                expires_seconds=expires_seconds,
            )
            media_kind = str(entry.get("media_kind") or "image").strip().lower()
            media_type = "video" if media_kind == "video" else "image"
            if not media_url:
                continue
            story_id = str(entry.get("story_id") or f"item-{index}").strip()
            items.append(
                {
                    "content_kind": "highlight",
                    "post_index_label": "highlight",
                    "source_post_id": f"highlight:{highlight_id}:{story_id}",
                    "source_post_url": None,
                    "caption": f"Highlight: {highlight_title}",
                    "post_type": media_type,
                    "post_media_items": [{"media_type": media_type, "media_url": media_url}],
                    "media_type": media_type,
                    "media_url": media_url,
                    "comments": [],
                    "source_container_id": highlight_id,
                    "source_container_title": highlight_title,
                    "source_created_at": str(entry.get("created_at") or "") or None,
                }
            )
    return items


def _record_post_failure(
    *,
    errors: list[str],
    message: str,
    db_service: object,
    trace_logger: IngestTraceLogger,
    job_id: int | None,
    source_post_id: str,
    post_index: int,
    post_total: int,
) -> None:
    errors.append(message)
    if job_id is not None:
        db_service.update_ingest_job_post(job_id, source_post_id, "failed", message)
    trace_logger.log("INGEST_ERROR", message)
    _emit_ingest_event(
        db_service=db_service,
        trace_logger=trace_logger,
        job_id=job_id,
        event_type="post_failed",
        stage="error",
        message=message,
        source_post_id=source_post_id,
        post_index=post_index,
        post_total=post_total,
        total_posts=post_total,
    )


def ingest_instagram_account_latest_impl(
    *,
    request: IngestInstagramAccountLatestRequest,
    settings: object,
    minio_service: object,
    vllm_service: object,
    db_service: object,
    trace_logger: IngestTraceLogger | None = None,
    job_id: int | None = None,
) -> IngestInstagramAccountLatestResponse:
    trace_logger = trace_logger or IngestTraceLogger(getattr(settings, "ingest_trace_log_path", None))
    bucket = resolve_archive_bucket(
        minio_service=minio_service,
        requested_bucket=request.bucket,
        default_bucket=settings.minio_bucket_default,
        fallback_bucket=settings.minio_bucket_fallback,
    )
    run_id = request.run_id or find_latest_run_id(minio_service, bucket, request.target_username)
    base_prefix = f"instagram/{request.target_username}/{run_id}"
    debug_first_post_only = bool(getattr(request, "debug_first_post_only", False))

    profile_key = f"{base_prefix}/profile/profile.json"
    if not minio_service.object_exists(bucket, profile_key):
        raise HTTPException(status_code=404, detail=f"profile.json not found: {profile_key}")
    profile = read_json_object(minio_service, bucket, profile_key)

    person_name = str(profile.get("full_name") or profile.get("username") or request.target_username)
    instagram_username = str(profile.get("username") or request.target_username)
    profile_photo_url = str(profile.get("profile_image_url") or "") or None
    bio = str(profile.get("bio") or "") or None

    person_id, account_id = db_service.get_or_create_person_account(
        person_name=person_name,
        instagram_username=instagram_username,
        profile_photo_url=profile_photo_url,
        bio=bio,
    )
    current_account_profile_summary = db_service.get_account_profile_summary(account_id)

    normalization_service = NormalizationService(db_service)
    normalized_focus_entity = normalization_service.normalize_focus_entity(request.focus_entity)
    scoring_service = ScoringService()
    review_service = ReviewService()
    aggregation_service = AggregationService()
    stage_executor = StageExecutor(vllm_service=vllm_service, db_service=db_service)

    all_post_json_keys = [
        k
        for k in sorted(minio_service.list_object_names(bucket, prefix=f"{base_prefix}/posts/", recursive=True))
        if k.endswith("/post.json")
    ]
    effective_max_posts = 1 if debug_first_post_only else request.max_posts
    post_json_keys = all_post_json_keys[:effective_max_posts] if effective_max_posts else all_post_json_keys
    story_items: list[dict[str, Any]] = []
    highlight_items: list[dict[str, Any]] = []
    if not debug_first_post_only:
        story_items = _collect_story_items(
            minio_service=minio_service,
            bucket=bucket,
            base_prefix=base_prefix,
            expires_seconds=request.expires_seconds,
        )
        highlight_items = _collect_highlight_items(
            minio_service=minio_service,
            bucket=bucket,
            base_prefix=base_prefix,
            expires_seconds=request.expires_seconds,
        )
    _emit_ingest_event(
        db_service=db_service,
        trace_logger=trace_logger,
        job_id=job_id,
        event_type="job_started",
        stage="ingest_start",
        message=(
            f"@{request.target_username} icin ingest basladi. "
            f"{len(post_json_keys)} post, {len(story_items)} story, {len(highlight_items)} highlight icerigi bulundu."
            + (" Debug modunda yalnizca ilk post ve yorumlari islenecek." if debug_first_post_only else "")
        ),
        post_total=len(post_json_keys) + len(story_items) + len(highlight_items),
        total_posts=len(post_json_keys) + len(story_items) + len(highlight_items),
    )

    counters = Counter(
        {
            "processed_posts": 0,
            "created_posts": 0,
            "updated_posts": 0,
            "processed_comments": 0,
            "created_comments": 0,
            "skipped_comments": 0,
        }
    )
    flagged_usernames: set[str] = set()
    errors: list[str] = []
    post_prompt_template = get_prompt_content(db_service, "post_analysis")
    media_prompt_template = get_prompt_content(db_service, "media_analysis")
    media_deep_prompt_template = get_prompt_content(db_service, "media_deep_analysis")
    parent_post_prompt_template = get_prompt_content(db_service, "post_analysis_parent_merge")
    comment_prompt_template = get_prompt_content(db_service, "comment_analysis")
    account_final_summary_template = get_prompt_content(db_service, "account_final_summary")
    enable_deep_media_analysis = bool(getattr(request, "enable_deep_media_analysis", False))
    prepared_posts: list[dict[str, Any]] = []
    post_total = len(post_json_keys) + len(story_items) + len(highlight_items)

    for post_index, post_json_key in enumerate(post_json_keys, 1):
        counters["processed_posts"] += 1
        post_storage_key = post_json_key.split("/")[-2]
        source_post_id = post_storage_key
        try:
            post = read_json_object(minio_service, bucket, post_json_key)
            source_post_id = str(post.get("post_id") or source_post_id)
            source_post_url = str(post.get("post_url") or "") or None
            caption = str(post.get("caption") or "") or None
            post_type = str(post.get("post_type") or "")
            # Storage klasoru genellikle "<tarih>_<post_id>" formatinda olur.
            # Analitikte source_post_id olarak yalin post_id kullansak da,
            # medya/yorum dosyalarini dogru bulmak icin post.json'in bulundugu
            # klasor adini (post_storage_key) kullanmaliyiz.
            post_dir_prefix = f"{base_prefix}/posts/{post_storage_key}/"
            if job_id is not None:
                db_service.update_ingest_job_post(job_id, source_post_id, "running")
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                event_type="post_started",
                stage="post_read",
                message=f"Icerik {post_index}/{post_total} isleniyor: {source_post_id}",
                source_post_id=source_post_id,
                post_index=post_index,
                post_total=post_total,
                total_posts=post_total,
            )

            post_media_items = _collect_post_media_items(
                minio_service=minio_service,
                bucket=bucket,
                post=post,
                post_dir_prefix=post_dir_prefix,
                post_type=post_type,
                expires_seconds=request.expires_seconds,
            )
            if request.max_media_items_per_post is not None:
                post_media_items = post_media_items[: request.max_media_items_per_post]
            media_type = infer_post_media_type(post_media_items, post_type)
            media_url = post_media_items[0]["media_url"] if post_media_items else None
            if not media_url:
                raise ValueError(f"No media URL found for post {source_post_id}")
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                event_type="post_media_discovered",
                stage="media_prepare",
                message=f"Icerik {post_index}/{post_total} icin {len(post_media_items)} medya bulundu.",
                source_post_id=source_post_id,
                post_index=post_index,
                post_total=post_total,
                media_total=len(post_media_items),
                total_posts=post_total,
                total_media_items=len(post_media_items),
            )

            comments = _load_post_comments(
                minio_service=minio_service,
                bucket=bucket,
                post_dir_prefix=post_dir_prefix,
                post=post,
            )
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                event_type="comments_loaded",
                stage="comment_prepare",
                message=f"Icerik {post_index}/{post_total} icin {len(comments)} yorum yüklendi.",
                source_post_id=source_post_id,
                post_index=post_index,
                post_total=post_total,
                comment_total=len(comments),
                total_posts=post_total,
                total_comments=len(comments),
            )

            prepared_posts.append(
                {
                    "post_json_key": post_json_key,
                    "content_kind": "post",
                    "post_index": post_index,
                    "source_post_id": source_post_id,
                    "source_post_url": source_post_url,
                    "caption": caption,
                    "post_type": post_type,
                    "post_media_items": post_media_items,
                    "media_type": media_type,
                    "media_url": media_url,
                    "comments": comments,
                    "single_media_results": [None] * len(post_media_items),
                    "final_canonical_post": None,
                    "final_legacy_post": None,
                    "post_result_id": None,
                    "source_container_id": None,
                    "source_container_title": None,
                    "source_created_at": str(post.get("created_at") or "") or None,
                    "failed": False,
                }
            )
        except HTTPException:
            raise
        except VLLMUpstreamError as exc:
            _record_post_failure(
                errors=errors,
                message=f"Post ingestion failed ({post_json_key}): vLLM {exc.status_code} - {exc.message}",
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                source_post_id=source_post_id,
                post_index=post_index,
                post_total=post_total,
            )
        except Exception as exc:  # pragma: no cover
            _record_post_failure(
                errors=errors,
                message=f"Content ingestion failed ({post_json_key}): {exc}",
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                source_post_id=source_post_id,
                post_index=post_index,
                post_total=post_total,
            )

    next_index = len(prepared_posts) + 1
    for content_item in [*story_items, *highlight_items]:
        content_item["post_json_key"] = f"{content_item['content_kind']}::{content_item['source_post_id']}"
        content_item["post_index"] = next_index
        content_item["single_media_results"] = [None] * len(content_item["post_media_items"])
        content_item["final_canonical_post"] = None
        content_item["final_legacy_post"] = None
        content_item["post_result_id"] = None
        content_item["failed"] = False
        counters["processed_posts"] += 1
        _emit_ingest_event(
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=job_id,
            event_type="post_started",
            stage="post_read",
            message=f"Icerik {next_index}/{post_total} isleniyor: {content_item['source_post_id']}",
            source_post_id=content_item["source_post_id"],
            post_index=next_index,
            post_total=post_total,
            total_posts=post_total,
        )
        _emit_ingest_event(
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=job_id,
            event_type="post_media_discovered",
            stage="media_prepare",
            message=f"Icerik {next_index}/{post_total} icin {len(content_item['post_media_items'])} medya bulundu.",
            source_post_id=content_item["source_post_id"],
            post_index=next_index,
            post_total=post_total,
            media_total=len(content_item["post_media_items"]),
            total_posts=post_total,
            total_media_items=len(content_item["post_media_items"]),
        )
        prepared_posts.append(content_item)
        next_index += 1

    max_post_workers = max(1, getattr(settings, "ingest_max_concurrent_posts_per_account", 4))
    with ThreadPoolExecutor(max_workers=max_post_workers) as pool:
        single_media_futures: dict[object, tuple[dict[str, Any], int, dict[str, str], bool]] = {}
        for post_item in prepared_posts:
            for media_index, media_item in enumerate(post_item["post_media_items"], 1):
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=job_id,
                    event_type="single_media_started",
                    stage="single_media_post_analysis",
                    message=(
                        f"Post {post_item['post_index']}/{post_total} tekil medya {media_index}/"
                        f"{len(post_item['post_media_items'])} analiz ediliyor."
                    ),
                    source_post_id=post_item["source_post_id"],
                    post_index=post_item["post_index"],
                    post_total=post_total,
                    media_index=media_index,
                    media_total=len(post_item["post_media_items"]),
                    total_posts=post_total,
                    total_media_items=len(post_item["post_media_items"]),
                )
                if enable_deep_media_analysis:
                    future = pool.submit(
                        _execute_single_media_pipeline,
                        stage_executor=stage_executor,
                        person_name=person_name,
                        instagram_username=instagram_username,
                        bio=bio,
                        caption=post_item["caption"],
                        account_profile_summary=current_account_profile_summary,
                        media_index=media_index,
                        media_count=len(post_item["post_media_items"]),
                        media_item=media_item,
                        post_prompt_template=post_prompt_template,
                        media_prompt_template=media_prompt_template,
                        media_deep_prompt_template=media_deep_prompt_template,
                        normalized_focus_entity=normalized_focus_entity,
                        request_model=request.model,
                        request_post_max_tokens=request.post_max_tokens,
                        normalization_service=normalization_service,
                        scoring_service=scoring_service,
                        review_service=review_service,
                        related_account_id=account_id,
                        trace_logger=trace_logger,
                        trace_prefix=f"POST_{post_item['post_index']}_SINGLE_{media_index}",
                    )
                    single_media_futures[future] = (post_item, media_index, media_item, True)
                else:
                    future = pool.submit(
                        execute_post_stage,
                        stage_executor=stage_executor,
                        username=person_name,
                        instagram_username=instagram_username,
                        bio=bio,
                        caption=post_item["caption"],
                        media_type=str(media_item.get("media_type") or "image"),
                        media_url=str(media_item.get("media_url") or ""),
                        media_items=[media_item],
                        media_observations=[],
                        post_history_summaries=[],
                        account_profile_summary=current_account_profile_summary,
                        focus_entity=normalized_focus_entity,
                        template_content=post_prompt_template,
                        model=request.model,
                        max_tokens=request.post_max_tokens,
                        normalization_service=normalization_service,
                        scoring_service=scoring_service,
                        review_service=review_service,
                        attach_media=True,
                        related_account_id=account_id,
                        trace_logger=trace_logger,
                        trace_prefix=f"POST_{post_item['post_index']}_SINGLE_{media_index}",
                    )
                    single_media_futures[future] = (post_item, media_index, media_item, False)

        for future in as_completed(single_media_futures):
            post_item, media_index, media_item, is_deep_mode = single_media_futures[future]
            try:
                if is_deep_mode:
                    post_item["single_media_results"][media_index - 1] = future.result()
                else:
                    canonical_post, legacy_post, post_prompt, post_payload = future.result()
                    persisted_payload = _build_single_media_analysis_payload(
                        media_index=media_index,
                        media_item=media_item,
                        canonical_post=canonical_post,
                        legacy_post=legacy_post,
                    )
                    post_item["single_media_results"][media_index - 1] = {
                        "canonical_post": canonical_post,
                        "legacy_post": legacy_post,
                        "prompt": post_prompt,
                        "payload": post_payload,
                        "persisted_payload": persisted_payload,
                        "media_observation": {
                            "media_index": media_index,
                            "media_type": str(media_item.get("media_type") or "image"),
                            "scene_summary": getattr(legacy_post, "ozet", "unclear"),
                            "raw_observation_note_tr": getattr(legacy_post, "analist_notu", ""),
                            "deep_required": False,
                            "deep_status": "not_required",
                            "deep_reason": "feature_disabled",
                            "location_confidence": "unclear",
                            "contains_vehicle": False,
                            "contains_plate": False,
                            "deep_payload": {},
                        },
                    }
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=job_id,
                    event_type="single_media_completed",
                    stage="single_media_post_analysis",
                    message=(
                        f"Post {post_item['post_index']}/{post_total} tekil medya {media_index}/"
                        f"{len(post_item['post_media_items'])} tamamlandi."
                    ),
                    source_post_id=post_item["source_post_id"],
                    post_index=post_item["post_index"],
                    post_total=post_total,
                    media_index=media_index,
                    media_total=len(post_item["post_media_items"]),
                    total_posts=post_total,
                    total_media_items=len(post_item["post_media_items"]),
                )
            except VLLMUpstreamError as exc:
                if not post_item["failed"]:
                    post_item["failed"] = True
                    _record_post_failure(
                        errors=errors,
                        message=(
                            f"Post ingestion failed ({post_item['post_json_key']}): "
                            f"vLLM {exc.status_code} - {exc.message}"
                        ),
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        source_post_id=post_item["source_post_id"],
                        post_index=post_item["post_index"],
                        post_total=post_total,
                    )
            except Exception as exc:  # pragma: no cover
                if not post_item["failed"]:
                    post_item["failed"] = True
                    _record_post_failure(
                        errors=errors,
                        message=f"Post ingestion failed ({post_item['post_json_key']}): {exc}",
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        source_post_id=post_item["source_post_id"],
                        post_index=post_item["post_index"],
                        post_total=post_total,
                    )

    parent_candidates: list[dict[str, Any]] = []
    for post_item in prepared_posts:
        if post_item["failed"]:
            continue
        if any(result is None for result in post_item["single_media_results"]):
            post_item["failed"] = True
            _record_post_failure(
                errors=errors,
                message=f"Post ingestion failed ({post_item['post_json_key']}): missing single-media analysis result.",
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                source_post_id=post_item["source_post_id"],
                post_index=post_item["post_index"],
                post_total=post_total,
            )
            continue
        if len(post_item["post_media_items"]) == 1:
            only_result = post_item["single_media_results"][0]
            post_item["final_canonical_post"] = only_result["canonical_post"]
            post_item["final_legacy_post"] = only_result["legacy_post"]
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                event_type="post_completed",
                stage="post_analysis",
                message=(
                    f"Post {post_item['post_index']}/{post_total} tamamlandi. "
                    f"Tehdit: {post_item['final_legacy_post'].tehdit_degerlendirmesi.tehdit_seviyesi or 'belirsiz'}."
                ),
                source_post_id=post_item["source_post_id"],
                post_index=post_item["post_index"],
                post_total=post_total,
                total_posts=post_total,
            )
        else:
            parent_candidates.append(post_item)

    with ThreadPoolExecutor(max_workers=max_post_workers) as pool:
        parent_futures: dict[object, dict[str, Any]] = {}
        for post_item in parent_candidates:
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                event_type="post_merge_started",
                stage="post_parent_merge",
                message=f"Post {post_item['post_index']}/{post_total} icin parent merge analizi basladi.",
                source_post_id=post_item["source_post_id"],
                post_index=post_item["post_index"],
                post_total=post_total,
                total_posts=post_total,
            )
            future = pool.submit(
                execute_parent_post_stage,
                stage_executor=stage_executor,
                username=person_name,
                instagram_username=instagram_username,
                bio=bio,
                caption=post_item["caption"],
                media_count=len(post_item["post_media_items"]),
                single_media_analyses=[
                    result["persisted_payload"] for result in post_item["single_media_results"] if result is not None
                ],
                template_content=parent_post_prompt_template,
                model=request.model,
                max_tokens=request.post_max_tokens,
                normalization_service=normalization_service,
                scoring_service=scoring_service,
                review_service=review_service,
                related_account_id=account_id,
                focus_entity=normalized_focus_entity,
                trace_logger=trace_logger,
                trace_prefix=f"POST_{post_item['post_index']}_PARENT",
            )
            parent_futures[future] = post_item

        for future in as_completed(parent_futures):
            post_item = parent_futures[future]
            try:
                canonical_post, legacy_post, post_prompt, post_payload = future.result()
                post_item["final_canonical_post"] = canonical_post
                post_item["final_legacy_post"] = legacy_post
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=job_id,
                    event_type="post_completed",
                    stage="post_parent_merge",
                    message=(
                        f"Post {post_item['post_index']}/{post_total} tamamlandi. "
                        f"Tehdit: {legacy_post.tehdit_degerlendirmesi.tehdit_seviyesi or 'belirsiz'}."
                    ),
                    source_post_id=post_item["source_post_id"],
                    post_index=post_item["post_index"],
                    post_total=post_total,
                    total_posts=post_total,
                )
            except VLLMUpstreamError as exc:
                if not post_item["failed"]:
                    post_item["failed"] = True
                    _record_post_failure(
                        errors=errors,
                        message=(
                            f"Post ingestion failed ({post_item['post_json_key']}): "
                            f"vLLM {exc.status_code} - {exc.message}"
                        ),
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        source_post_id=post_item["source_post_id"],
                        post_index=post_item["post_index"],
                        post_total=post_total,
                    )
            except Exception as exc:  # pragma: no cover
                if not post_item["failed"]:
                    post_item["failed"] = True
                    _record_post_failure(
                        errors=errors,
                        message=f"Post ingestion failed ({post_item['post_json_key']}): {exc}",
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        source_post_id=post_item["source_post_id"],
                        post_index=post_item["post_index"],
                        post_total=post_total,
                    )

    comment_ready_items: list[dict[str, Any]] = []
    for post_item in prepared_posts:
        if post_item["failed"] or post_item["final_legacy_post"] is None or post_item["final_canonical_post"] is None:
            continue
        try:
            final_legacy_post = post_item["final_legacy_post"]
            final_canonical_post = post_item["final_canonical_post"]
            post_result = db_service.upsert_post(
                instagram_account_id=account_id,
                source_kind=str(post_item.get("content_kind") or "post"),
                media_type=post_item["media_type"],
                media_url=post_item["media_url"],
                media_items=post_item["post_media_items"],
                caption=post_item["caption"],
                post_analysis=final_legacy_post.ozet or "Gonderi ozetlenemedi.",
                structured_analysis=final_legacy_post.model_dump(mode="json"),
                model=request.model or vllm_service.default_model,
                source_target_username=request.target_username,
                source_run_id=run_id,
                source_post_id=post_item["source_post_id"],
                source_post_url=post_item["source_post_url"],
                source_container_id=post_item.get("source_container_id"),
                source_container_title=post_item.get("source_container_title"),
                source_created_at=post_item.get("source_created_at"),
            )
            post_item["post_result_id"] = post_result.post_id
            counters["created_posts" if post_result.created else "updated_posts"] += 1
            db_service.save_media_observations(
                post_result.post_id,
                [result["media_observation"] for result in post_item["single_media_results"] if result is not None],
            )
            db_service.save_canonical_post_analysis(
                post_result.post_id,
                final_canonical_post.model_dump(mode="json"),
                final_canonical_post.model_dump(mode="json"),
            )
            valid_comments = [
                comment for comment in post_item["comments"] if str(comment.get("comment_text") or "").strip()
            ]
            if request.max_comments_per_post is not None:
                valid_comments = valid_comments[: request.max_comments_per_post]
            post_item["valid_comments"] = valid_comments
            if valid_comments:
                comment_ready_items.append(post_item)
            else:
                if job_id is not None:
                    db_service.update_ingest_job_post(job_id, post_item["source_post_id"], "completed")
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=job_id,
                    event_type="post_pipeline_completed",
                    stage="post_complete",
                    message=f"Icerik {post_item['post_index']}/{post_total} icin tum adimlar tamamlandi.",
                    source_post_id=post_item["source_post_id"],
                    post_index=post_item["post_index"],
                    post_total=post_total,
                    total_posts=post_total,
                )
        except HTTPException:
            raise
        except VLLMUpstreamError as exc:
            _record_post_failure(
                errors=errors,
                message=(
                    f"Content ingestion failed ({post_item['post_json_key']}): "
                    f"vLLM {exc.status_code} - {exc.message}"
                ),
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                source_post_id=post_item["source_post_id"],
                post_index=post_item["post_index"],
                post_total=post_total,
            )
        except Exception as exc:  # pragma: no cover
            _record_post_failure(
                errors=errors,
                message=f"Content ingestion failed ({post_item['post_json_key']}): {exc}",
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                source_post_id=post_item["source_post_id"],
                post_index=post_item["post_index"],
                post_total=post_total,
            )

    finalized_post_summaries = db_service.get_post_history_summaries(account_id)
    comment_context_profile_summary = build_final_account_summary_fallback(
        person_name,
        instagram_username,
        finalized_post_summaries,
    )
    db_service.update_account_profile_summary(account_id, comment_context_profile_summary)

    post_comment_totals: dict[str, int] = {}
    post_comment_processed: Counter[str] = Counter()
    same_batch_commenter_totals: Counter[str] = Counter()
    queued_commenters_this_run: set[str] = set()
    for post_item in comment_ready_items:
        for comment in list(post_item.get("valid_comments") or []):
            commenter_username = str(comment.get("commenter_username") or "").strip().lower()
            if commenter_username:
                same_batch_commenter_totals[commenter_username] += 1
    with ThreadPoolExecutor(max_workers=max(1, settings.ingest_max_concurrent_comments)) as pool:
        comment_futures: dict[object, dict[str, object]] = {}
        for post_item in comment_ready_items:
            valid_comments = list(post_item.get("valid_comments") or [])
            post_comment_totals[post_item["source_post_id"]] = len(valid_comments)
            for comment_index, comment in enumerate(valid_comments, 1):
                comment_text = str(comment.get("comment_text") or "").strip()
                commenter_username = str(comment.get("commenter_username") or "") or None
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=job_id,
                    event_type="comment_started",
                    stage="comment_analysis",
                    message=(
                        f"Icerik {post_item['post_index']}/{post_total} yorum {comment_index}/{len(valid_comments)} "
                        f"analiz ediliyor: @{commenter_username or 'unknown'} - {_shorten_text(comment_text, 72)}"
                    ),
                    source_post_id=post_item["source_post_id"],
                    commenter_username=commenter_username,
                    post_index=post_item["post_index"],
                    post_total=post_total,
                    comment_index=comment_index,
                    comment_total=len(valid_comments),
                    total_posts=post_total,
                    total_comments=len(valid_comments),
                    current_commenter_username=commenter_username,
                )
                commenter_history = db_service.get_commenter_history(commenter_username)
                commenter_history.extend(
                    build_same_batch_commenter_history(
                        valid_comments,
                        current_index=comment_index - 1,
                        commenter_username=commenter_username,
                    )
                )
                future = pool.submit(
                    execute_comment_stage,
                    stage_executor=stage_executor,
                    post_analysis=post_item["final_legacy_post"].model_dump_json(),
                    username=instagram_username,
                    bio=bio,
                    caption=post_item["caption"],
                    account_profile_summary=comment_context_profile_summary,
                    commenter_username=commenter_username,
                    comment_text=comment_text,
                    commenter_history=commenter_history,
                    post_summary=post_item["final_canonical_post"].summary,
                    post_categories=post_item["final_canonical_post"].categories,
                    post_detected_entities=post_item["final_canonical_post"].detected_entities,
                    post_role=post_item["final_canonical_post"].role,
                    post_organization_link_score=post_item["final_canonical_post"].organization_link_score,
                    post_threat_level=post_item["final_canonical_post"].threat_level,
                    investigated_aliases=[instagram_username, person_name],
                    same_batch_commenter_total=same_batch_commenter_totals.get(str(commenter_username or "").strip().lower(), 1),
                    focus_entity=normalized_focus_entity,
                    template_content=comment_prompt_template,
                    model=request.model,
                    max_tokens=request.comment_max_tokens,
                    scoring_service=scoring_service,
                    related_account_id=account_id,
                    related_post_id=post_item["post_result_id"],
                    trace_logger=trace_logger,
                    trace_prefix=f"POST_{post_item['post_index']}_COMMENT_{comment_index}",
                )
                comment_futures[future] = {
                    "post_item": post_item,
                    "source_comment": comment,
                    "comment_index": comment_index,
                    "comment_total": len(valid_comments),
                }

        for future in as_completed(comment_futures):
            payload = comment_futures[future]
            post_item = payload["post_item"]
            source_comment = payload["source_comment"]
            comment_index = int(payload["comment_index"])
            comment_total = int(payload["comment_total"])
            counters["processed_comments"] += 1
            try:
                canonical_comment, legacy_comment, comment_prompt, comment_payload = future.result()
            except VLLMUpstreamError as exc:
                errors.append(
                    f"Comment analysis failed ({post_item['source_post_id']} #{comment_index}): "
                    f"vLLM {exc.status_code} - {exc.message}"
                )
                post_comment_processed[post_item["source_post_id"]] += 1
                if post_comment_processed[post_item["source_post_id"]] >= post_comment_totals.get(post_item["source_post_id"], 0):
                    if job_id is not None:
                        db_service.update_ingest_job_post(job_id, post_item["source_post_id"], "completed")
                    _emit_ingest_event(
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        event_type="post_pipeline_completed",
                        stage="post_complete",
                        message=f"Icerik {post_item['post_index']}/{post_total} icin tum adimlar tamamlandi.",
                        source_post_id=post_item["source_post_id"],
                        post_index=post_item["post_index"],
                        post_total=post_total,
                        total_posts=post_total,
                    )
                continue
            except Exception as exc:  # pragma: no cover
                errors.append(f"Comment analysis failed ({post_item['source_post_id']} #{comment_index}): {exc}")
                post_comment_processed[post_item["source_post_id"]] += 1
                if post_comment_processed[post_item["source_post_id"]] >= post_comment_totals.get(post_item["source_post_id"], 0):
                    if job_id is not None:
                        db_service.update_ingest_job_post(job_id, post_item["source_post_id"], "completed")
                    _emit_ingest_event(
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        event_type="post_pipeline_completed",
                        stage="post_complete",
                        message=f"Icerik {post_item['post_index']}/{post_total} icin tum adimlar tamamlandi.",
                        source_post_id=post_item["source_post_id"],
                        post_index=post_item["post_index"],
                        post_total=post_total,
                        total_posts=post_total,
                    )
                continue
            _emit_ingest_event(
                db_service=db_service,
                trace_logger=trace_logger,
                job_id=job_id,
                event_type="comment_completed",
                stage="comment_analysis",
                message=(
                    f"Icerik {post_item['post_index']}/{post_total} yorum {comment_index}/{comment_total} "
                    f"tamamlandi: @{legacy_comment.commenter_username or 'unknown'} -> {legacy_comment.verdict}"
                ),
                source_post_id=post_item["source_post_id"],
                commenter_username=legacy_comment.commenter_username,
                post_index=post_item["post_index"],
                post_total=post_total,
                comment_index=comment_index,
                comment_total=comment_total,
                total_posts=post_total,
                total_comments=comment_total,
                current_commenter_username=legacy_comment.commenter_username,
            )
            comment_result = db_service.upsert_comment(
                instagram_post_id=post_item["post_result_id"],
                commenter_username=legacy_comment.commenter_username,
                commenter_profile_url=source_comment.get("commenter_profile_url"),
                comment_text=legacy_comment.text,
                verdict=legacy_comment.verdict,
                sentiment=legacy_comment.sentiment,
                orgut_baglanti_skoru=legacy_comment.orgut_baglanti_skoru,
                bayrak=legacy_comment.bayrak,
                reason=legacy_comment.reason,
                discovered_at=source_comment.get("discovered_at"),
                source_run_id=run_id,
                source_post_id=post_item["source_post_id"],
                source_post_url=source_comment.get("source_post_url") or post_item["source_post_url"],
            )
            if comment_result.created:
                counters["created_comments"] += 1
            else:
                counters["skipped_comments"] += 1
            db_service.save_canonical_comment_analysis(
                comment_result.comment_id,
                canonical_comment.model_dump(mode="json"),
                canonical_comment.model_dump(mode="json"),
            )
            if legacy_comment.commenter_username:
                commenter_username_normalized = str(legacy_comment.commenter_username or "").strip().lower()
                commenter_person_id = db_service.get_or_create_person(legacy_comment.commenter_username)
                db_service.upsert_person_link(
                    person_id=person_id,
                    related_username=legacy_comment.commenter_username,
                    related_person_id=commenter_person_id,
                    source_account_id=account_id,
                    source_post_id=post_item["source_post_id"],
                    source_comment_id=comment_result.comment_id,
                    link_reason="comment_interaction",
                )
                should_enqueue_review = (
                    bool(legacy_comment.bayrak)
                    or int(legacy_comment.orgut_baglanti_skoru or 0) >= COMMENT_REVIEW_QUEUE_MIN_SCORE
                    or same_batch_commenter_totals.get(commenter_username_normalized, 0) >= 5
                )
                if comment_result.created and should_enqueue_review and commenter_username_normalized not in queued_commenters_this_run:
                    db_service.upsert_review_queue(
                        legacy_comment.commenter_username,
                        legacy_comment.reason,
                        legacy_comment.verdict,
                        person_id=commenter_person_id,
                    )
                    queued_commenters_this_run.add(commenter_username_normalized)
                    flagged_usernames.add(legacy_comment.commenter_username)
                    _emit_ingest_event(
                        db_service=db_service,
                        trace_logger=trace_logger,
                        job_id=job_id,
                        event_type="review_queue_added",
                        stage="review_queue",
                        message=(
                            f"@{legacy_comment.commenter_username} review kuyruğuna alindi: "
                            f"{legacy_comment.verdict} - {_shorten_text(legacy_comment.reason, 88)}"
                        ),
                        source_post_id=post_item["source_post_id"],
                        commenter_username=legacy_comment.commenter_username,
                        post_index=post_item["post_index"],
                        post_total=post_total,
                        total_posts=post_total,
                    )

            post_comment_processed[post_item["source_post_id"]] += 1
            if post_comment_processed[post_item["source_post_id"]] >= post_comment_totals.get(post_item["source_post_id"], 0):
                if job_id is not None:
                    db_service.update_ingest_job_post(job_id, post_item["source_post_id"], "completed")
                _emit_ingest_event(
                    db_service=db_service,
                    trace_logger=trace_logger,
                    job_id=job_id,
                    event_type="post_pipeline_completed",
                    stage="post_complete",
                    message=f"Icerik {post_item['post_index']}/{post_total} icin tum adimlar tamamlandi.",
                    source_post_id=post_item["source_post_id"],
                    post_index=post_item["post_index"],
                    post_total=post_total,
                    total_posts=post_total,
                )

    finalized_post_summaries = db_service.get_post_history_summaries(account_id)
    if debug_first_post_only:
        final_account_summary = build_final_account_summary_fallback(
            person_name,
            instagram_username,
            finalized_post_summaries,
        )
        trace_logger.log(
            "ACCOUNT_FINAL_SUMMARY_SKIPPED",
            {
                "reason": "debug_first_post_only",
                "summary": final_account_summary,
            },
        )
    else:
        final_account_summary = generate_final_account_profile_summary(
            username=person_name,
            instagram_username=instagram_username,
            bio=bio,
            post_history_summaries=finalized_post_summaries,
            vllm_service=vllm_service,
            model=request.model,
            db_service=db_service,
            template_content=account_final_summary_template,
            trace_logger=trace_logger,
        )
    db_service.update_account_profile_summary(account_id, final_account_summary)

    canonical_posts_for_aggregate = db_service.list_canonical_post_analyses_for_account(account_id)

    aggregate = execute_aggregation_stage(
        aggregation_service=aggregation_service,
        account_id=account_id,
        post_payloads=canonical_posts_for_aggregate,
    )
    db_service.save_account_aggregate(account_id, aggregate.model_dump(mode="json"))
    db_service.refresh_account_ingest_aggregate(account_id, last_ingested_run_id=run_id)

    response = IngestInstagramAccountLatestResponse(
        target_username=request.target_username,
        run_id=run_id,
        bucket=bucket,
        person_id=person_id,
        instagram_account_id=account_id,
        processed_posts=int(counters["processed_posts"]),
        created_posts=int(counters["created_posts"]),
        updated_posts=int(counters["updated_posts"]),
        processed_comments=int(counters["processed_comments"]),
        created_comments=int(counters["created_comments"]),
        skipped_comments=int(counters["skipped_comments"]),
        flagged_users=len(flagged_usernames),
        flagged_usernames=sorted(flagged_usernames),
        errors=errors,
    )
    trace_logger.log("INGEST_DONE", response.model_dump(mode="json"))
    _emit_ingest_event(
        db_service=db_service,
        trace_logger=trace_logger,
        job_id=job_id,
        event_type="job_completed",
        stage="done",
        message=(
            f"@{request.target_username} ingest bitti. "
            f"{response.processed_posts} post, {response.processed_comments} yorum, {response.flagged_users} flag."
        ),
        post_total=len(post_json_keys),
        total_posts=len(post_json_keys),
    )
    return response


def run_discovery_scan(
    *,
    request: IngestWatchScanRequest,
    settings: object,
    minio_service: object,
    db_service: object,
) -> IngestWatchScanResponse:
    bucket = resolve_archive_bucket(
        minio_service=minio_service,
        requested_bucket=request.bucket,
        default_bucket=settings.minio_bucket_default,
        fallback_bucket=settings.minio_bucket_fallback,
    )
    usernames = request.usernames or discover_usernames_in_bucket(minio_service, bucket)
    discovered_sources = 0
    enqueued_jobs = 0
    skipped_jobs = 0
    for username in usernames:
        try:
            run_id = find_latest_run_id(minio_service, bucket, username)
        except HTTPException:
            skipped_jobs += 1
            continue
        db_service.upsert_ingest_source(username=username, bucket=bucket, last_seen_run_id=run_id)
        discovered_sources += 1
        _, created = db_service.enqueue_ingest_job(username=username, bucket=bucket, run_id=run_id)
        if created:
            enqueued_jobs += 1
        else:
            skipped_jobs += 1
    return IngestWatchScanResponse(
        bucket=bucket,
        discovered_sources=discovered_sources,
        enqueued_jobs=enqueued_jobs,
        skipped_jobs=skipped_jobs,
        usernames=usernames,
    )


def process_ingest_job(
    *,
    job: dict[str, object],
    settings: object,
    minio_service: object,
    vllm_service: object,
    db_service: object,
) -> dict:
    try:
        trace_logger = IngestTraceLogger(getattr(settings, "ingest_trace_log_path", None))
        _emit_ingest_event(
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=int(job["id"]),
            event_type="worker_claimed",
            stage="worker",
            message=f"Job #{job['id']} worker tarafindan claim edildi.",
        )
        request = IngestInstagramAccountLatestRequest(
            target_username=str(job["target_username"]),
            run_id=str(job["run_id"]),
            bucket=str(job["bucket"]),
            focus_entity=str(job["focus_entity"]) if job.get("focus_entity") else None,
            debug_first_post_only=True,
            max_posts=1,
            max_media_items_per_post=2,
            analyze_comments=True,
        )
        response = ingest_instagram_account_latest_impl(
            request=request,
            settings=settings,
            minio_service=minio_service,
            vllm_service=vllm_service,
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=int(job["id"]),
        )
        counters = response.model_dump(mode="json")
        summary = {
            "processed_posts": int(counters["processed_posts"]),
            "created_posts": int(counters["created_posts"]),
            "updated_posts": int(counters["updated_posts"]),
            "processed_comments": int(counters["processed_comments"]),
            "created_comments": int(counters["created_comments"]),
            "skipped_comments": int(counters["skipped_comments"]),
            "flagged_users": int(counters["flagged_users"]),
        }
        status = "completed" if not response.errors else "failed"
        result = db_service.complete_ingest_job(
            int(job["id"]),
            status=status,
            counters=summary,
            error_message="\n".join(response.errors) or None,
        )
        _sync_batch_target_for_job(
            db_service=db_service,
            job=job,
            status="completed" if status == "completed" else "failed",
            note="\n".join(response.errors) or None,
        )
        if status == "completed":
            _enqueue_followup_jobs(
                job=job,
                response=response,
                minio_service=minio_service,
                vllm_service=vllm_service,
                db_service=db_service,
            )
        _emit_ingest_event(
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=int(job["id"]),
            event_type="worker_finished",
            stage="worker",
            message=f"Job #{job['id']} worker tarafinda {status} olarak kapandi.",
        )
        return result
    except Exception as exc:
        trace_logger = IngestTraceLogger(getattr(settings, "ingest_trace_log_path", None))
        result = db_service.complete_ingest_job(
            int(job["id"]),
            status="failed",
            counters={},
            error_message=str(exc),
        )
        _sync_batch_target_for_job(
            db_service=db_service,
            job=job,
            status="failed",
            note=str(exc),
        )
        _emit_ingest_event(
            db_service=db_service,
            trace_logger=trace_logger,
            job_id=int(job["id"]),
            event_type="worker_failed",
            stage="worker",
            message=f"Job #{job['id']} worker hatasi: {exc}",
        )
        return result


def run_ingest_workers_once_impl(
    *,
    request: object,
    settings: object,
    minio_service: object,
    vllm_service: object,
    db_service: object,
) -> IngestWorkersRunOnceResponse:
    lease_owner = request.lease_owner or f"worker-{uuid4()}"
    max_jobs = request.max_jobs or settings.ingest_max_concurrent_accounts
    completed_jobs = 0
    failed_jobs = 0
    results: list[dict] = []
    total_claimed = 0
    while True:
        claimed_jobs = db_service.claim_pending_ingest_jobs(
            lease_owner=lease_owner,
            lease_seconds=settings.ingest_job_lease_seconds,
            limit=max_jobs,
        )
        if not claimed_jobs:
            break
        total_claimed += len(claimed_jobs)
        with ThreadPoolExecutor(max_workers=max(1, min(len(claimed_jobs), settings.ingest_max_concurrent_accounts))) as pool:
            futures = [
                pool.submit(
                    process_ingest_job,
                    job=job,
                    settings=settings,
                    minio_service=minio_service,
                    vllm_service=vllm_service,
                    db_service=db_service,
                )
                for job in claimed_jobs
            ]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if str(result.get("status")) == "completed":
                    completed_jobs += 1
                else:
                    failed_jobs += 1

    return IngestWorkersRunOnceResponse(
        lease_owner=lease_owner,
        claimed_jobs=total_claimed,
        completed_jobs=completed_jobs,
        failed_jobs=failed_jobs,
        items=[IngestJobItem(**item) for item in sorted(results, key=lambda row: int(row["id"]), reverse=True)],
    )
