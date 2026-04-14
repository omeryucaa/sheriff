from __future__ import annotations

import os

from fastapi import APIRouter, Depends

from app.api.dependencies import get_db_service, get_minio_service, get_vllm_service
from app.pipeline.run_ingest_pipeline import (
    ingest_instagram_account_latest_impl,
    run_discovery_scan,
    run_ingest_workers_once_impl,
)
from app.pipeline.helpers import find_latest_run_id, resolve_archive_bucket
from app.schemas import (
    IngestInstagramAccountLatestRequest,
    IngestInstagramAccountLatestResponse,
    IngestJobsEnqueueRequest,
    IngestJobsListResponse,
    IngestJobItem,
    IngestWatchScanRequest,
    IngestWatchScanResponse,
    IngestWorkersRunOnceRequest,
    IngestWorkersRunOnceResponse,
)
from app.settings import Settings, get_settings


router = APIRouter()


@router.post("/ingest-instagram-account-latest", response_model=IngestInstagramAccountLatestResponse)
def ingest_instagram_account_latest(
    request: IngestInstagramAccountLatestRequest,
    settings: Settings = Depends(get_settings),
    minio_service=Depends(get_minio_service),
    vllm_service=Depends(get_vllm_service),
    db_service=Depends(get_db_service),
) -> IngestInstagramAccountLatestResponse:
    return ingest_instagram_account_latest_impl(
        request=request,
        settings=settings,
        minio_service=minio_service,
        vllm_service=vllm_service,
        db_service=db_service,
    )


@router.post("/ingest/run", response_model=IngestInstagramAccountLatestResponse)
def run_ingest(
    request: IngestInstagramAccountLatestRequest,
    settings: Settings = Depends(get_settings),
    minio_service=Depends(get_minio_service),
    vllm_service=Depends(get_vllm_service),
    db_service=Depends(get_db_service),
) -> IngestInstagramAccountLatestResponse:
    return ingest_instagram_account_latest(request, settings, minio_service, vllm_service, db_service)


@router.post("/ingest/watch/scan", response_model=IngestWatchScanResponse)
def scan_ingest_watch(
    request: IngestWatchScanRequest,
    settings: Settings = Depends(get_settings),
    minio_service=Depends(get_minio_service),
    db_service=Depends(get_db_service),
) -> IngestWatchScanResponse:
    return run_discovery_scan(
        request=request,
        settings=settings,
        minio_service=minio_service,
        db_service=db_service,
    )


@router.post("/ingest/jobs/enqueue", response_model=IngestJobsListResponse)
def enqueue_ingest_jobs(
    request: IngestJobsEnqueueRequest,
    settings: Settings = Depends(get_settings),
    minio_service=Depends(get_minio_service),
    db_service=Depends(get_db_service),
) -> IngestJobsListResponse:
    bucket = resolve_archive_bucket(
        minio_service=minio_service,
        requested_bucket=request.bucket,
        default_bucket=settings.minio_bucket_default,
        fallback_bucket=settings.minio_bucket_fallback,
    )
    job_ids: list[int] = []
    for username in request.usernames:
        run_id = request.run_ids.get(username) or find_latest_run_id(minio_service, bucket, username)
        db_service.upsert_ingest_source(username=username, bucket=bucket, last_seen_run_id=run_id)
        job_id, _ = db_service.enqueue_ingest_job(username=username, bucket=bucket, run_id=run_id)
        if job_id is not None:
            job_ids.append(job_id)

    items = [item for item in db_service.list_ingest_jobs(limit=max(100, len(job_ids) or 1)) if int(item["id"]) in set(job_ids)]
    return IngestJobsListResponse(items=[IngestJobItem(**item) for item in items])


@router.get("/ingest/jobs", response_model=IngestJobsListResponse)
def get_ingest_jobs(limit: int = 100, db_service=Depends(get_db_service)) -> IngestJobsListResponse:
    safe_limit = max(1, min(limit, 500))
    return IngestJobsListResponse(items=[IngestJobItem(**item) for item in db_service.list_ingest_jobs(limit=safe_limit)])


@router.post("/ingest/workers/run-once", response_model=IngestWorkersRunOnceResponse)
def run_ingest_workers_once(
    request: IngestWorkersRunOnceRequest,
    settings: Settings = Depends(get_settings),
    minio_service=Depends(get_minio_service),
    vllm_service=Depends(get_vllm_service),
    db_service=Depends(get_db_service),
) -> IngestWorkersRunOnceResponse:
    return run_ingest_workers_once_impl(
        request=request,
        settings=settings,
        minio_service=minio_service,
        vllm_service=vllm_service,
        db_service=db_service,
    )


@router.get("/ingest/trace")
def get_ingest_trace(settings: Settings = Depends(get_settings)) -> dict[str, object]:
    if not settings.ingest_trace_log_path:
        return {"path": None, "content": ""}
    if not os.path.exists(settings.ingest_trace_log_path):
        return {"path": settings.ingest_trace_log_path, "content": ""}
    with open(settings.ingest_trace_log_path, "r", encoding="utf-8") as handle:
        content = handle.read()
    return {"path": settings.ingest_trace_log_path, "content": content[-200000:]}
