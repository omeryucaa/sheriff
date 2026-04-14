from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect

from app.api.dependencies import get_db_service, get_minio_service
from app.pipeline.helpers import find_latest_run_id, resolve_archive_bucket
from app.schemas import (
    BatchJobCreateResponse,
    BatchJobItem,
    BatchJobTargetItem,
    IngestJobEventItem,
    BatchJobsCreateRequest,
    IngestJobItem,
    JobsOverviewResponse,
)
from app.settings import Settings, get_settings
from app.storage.database_service import DatabaseService


router = APIRouter()


def _normalize_target_username(raw_target: str) -> str:
    candidate = str(raw_target or "").strip()
    if not candidate:
        return ""
    if candidate.startswith("@"):
        candidate = candidate[1:]
    if "instagram.com" in candidate:
        parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
        path_parts = [part.strip() for part in parsed.path.split("/") if part.strip()]
        if not path_parts:
            return ""
        candidate = path_parts[0]
    return candidate.strip().strip("/").lower()


def _derive_target_status_from_job(job_row: dict[str, object] | None) -> str:
    if not job_row:
        return "pending"
    status = str(job_row.get("status") or "pending")
    if status in {"pending", "discovered", "retry_wait"}:
        return "enqueued"
    if status in {"running", "completed", "failed", "skipped"}:
        return status
    return "pending"


def _build_jobs_overview_payload(
    *,
    db_service: object,
    batch_job_id: int | None,
    limit: int,
    event_limit_multiplier: int = 10,
) -> JobsOverviewResponse:
    safe_limit = max(1, min(limit, 500))
    raw_batches = db_service.list_batch_jobs(limit=safe_limit)
    batches: list[dict[str, object]] = []
    for item in raw_batches:
        refreshed = db_service.refresh_batch_job_status(int(item["id"])) or item
        batches.append(refreshed)
    targets = db_service.list_batch_job_targets(batch_job_id=batch_job_id, limit=max(safe_limit * 5, 50))
    ingest_jobs = db_service.list_ingest_jobs(limit=max(safe_limit * 5, 50))
    ingest_job_ids = [int(item["id"]) for item in ingest_jobs]
    recent_events = db_service.list_ingest_job_events(
        limit=max(safe_limit * event_limit_multiplier, 120),
        ingest_job_ids=ingest_job_ids,
    )
    review_queue = db_service.list_review_queue_top(limit=safe_limit)
    return JobsOverviewResponse(
        batches=[BatchJobItem(**item) for item in batches],
        targets=[BatchJobTargetItem(**item) for item in targets],
        ingest_jobs=[IngestJobItem(**item) for item in ingest_jobs],
        review_queue=review_queue,
        recent_events=[IngestJobEventItem(**item) for item in recent_events],
    )


@router.post("/jobs/batch", response_model=BatchJobCreateResponse)
def create_batch_jobs(
    request: BatchJobsCreateRequest,
    settings: Settings = Depends(get_settings),
    minio_service=Depends(get_minio_service),
    db_service=Depends(get_db_service),
) -> BatchJobCreateResponse:
    bucket = resolve_archive_bucket(
        minio_service=minio_service,
        requested_bucket=request.bucket,
        default_bucket=settings.minio_bucket_default,
        fallback_bucket=settings.minio_bucket_fallback,
    )

    normalized_pairs: list[tuple[str, str]] = []
    seen_usernames: set[str] = set()
    for raw_target in request.targets:
        normalized_username = _normalize_target_username(raw_target)
        if not normalized_username or normalized_username in seen_usernames:
            continue
        seen_usernames.add(normalized_username)
        normalized_pairs.append((raw_target, normalized_username))

    if not normalized_pairs:
        raise HTTPException(status_code=400, detail="No valid targets were provided.")

    batch_job, target_rows = db_service.create_batch_job(
        mode=request.mode,
        bucket=bucket,
        requested_targets=[pair[0] for pair in normalized_pairs],
        normalized_targets=[pair[1] for pair in normalized_pairs],
        country=request.country,
        focus_entity=request.focus_entity,
        auto_enqueue_followups=request.auto_enqueue_followups,
    )

    ingest_jobs: list[dict[str, object]] = []
    for target_row in target_rows:
        normalized_username = str(target_row["normalized_username"])
        try:
            run_id = find_latest_run_id(minio_service, bucket, normalized_username)
        except HTTPException:
            db_service.update_batch_target_status(
                int(target_row["id"]),
                "missing_archive",
                note="MinIO archive not found for target.",
            )
            continue

        db_service.upsert_ingest_source(username=normalized_username, bucket=bucket, last_seen_run_id=run_id)
        ingest_job_id, _ = db_service.enqueue_ingest_job(
            username=normalized_username,
            bucket=bucket,
            run_id=run_id,
            batch_job_id=int(batch_job["id"]),
            batch_target_id=int(target_row["id"]),
            source_kind="initial",
            focus_entity=request.focus_entity,
            country=request.country,
        )
        if ingest_job_id is None:
            continue
        job_row = db_service.get_ingest_job(int(ingest_job_id))
        db_service.attach_ingest_job_to_batch_target(
            int(target_row["id"]),
            int(ingest_job_id),
            status=_derive_target_status_from_job(job_row),
        )
        if job_row:
            ingest_jobs.append(job_row)

    refreshed = db_service.refresh_batch_job_status(int(batch_job["id"])) or batch_job
    refreshed_targets = db_service.list_batch_job_targets(batch_job_id=int(batch_job["id"]), limit=500)
    return BatchJobCreateResponse(
        batch_job=BatchJobItem(**refreshed),
        targets=[BatchJobTargetItem(**item) for item in refreshed_targets],
        ingest_jobs=[IngestJobItem(**item) for item in ingest_jobs],
    )


@router.get("/jobs/overview", response_model=JobsOverviewResponse)
def get_jobs_overview(
    batch_job_id: int | None = None,
    limit: int = 50,
    db_service=Depends(get_db_service),
) -> JobsOverviewResponse:
    return _build_jobs_overview_payload(
        db_service=db_service,
        batch_job_id=batch_job_id,
        limit=limit,
    )


@router.websocket("/ws/jobs/overview")
async def stream_jobs_overview(websocket: WebSocket) -> None:
    await websocket.accept()
    query_params = websocket.query_params
    try:
        limit = int(query_params.get("limit") or "60")
    except ValueError:
        limit = 60
    try:
        batch_job_id = int(query_params.get("batch_job_id")) if query_params.get("batch_job_id") else None
    except ValueError:
        batch_job_id = None
    try:
        interval_ms = max(500, min(int(query_params.get("interval_ms") or "1000"), 10000))
    except ValueError:
        interval_ms = 1000

    settings = get_settings()
    db_service = DatabaseService(db_path=settings.sqlite_db_path)
    db_service.init_schema()
    try:
        while True:
            overview = _build_jobs_overview_payload(
                db_service=db_service,
                batch_job_id=batch_job_id,
                limit=limit,
                event_limit_multiplier=16,
            )
            await websocket.send_json(
                {
                    "overview": overview.model_dump(mode="json"),
                    "server_time": datetime.now(timezone.utc).isoformat(),
                }
            )
            await asyncio.sleep(interval_ms / 1000)
    except (WebSocketDisconnect, RuntimeError):
        return
