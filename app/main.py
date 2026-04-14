from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.dependencies import graph_capture_dir
from app.api.routes_analysis import (
    analyze_account_graph,
    analyze_media,
    analyze_post_and_comments,
    router as analysis_router,
    save_account_graph_capture,
)
from app.api.routes_dashboard import (
    get_account,
    get_account_comments,
    get_account_graph,
    get_account_posts,
    get_dashboard_summary,
    get_review_queue,
    health,
    list_accounts,
    router as dashboard_router,
)
from app.api.routes_ingest import (
    enqueue_ingest_jobs,
    get_ingest_jobs,
    get_ingest_trace,
    ingest_instagram_account_latest,
    router as ingest_router,
    run_ingest,
    run_ingest_workers_once,
    scan_ingest_watch,
)
from app.api.routes_jobs import create_batch_jobs, get_jobs_overview, router as jobs_router
from app.api.routes_media import get_avatar, router as media_router
from app.api.routes_prompts import (
    get_prompt,
    list_prompts,
    router as prompts_router,
    update_prompt,
)
from app.pipeline.run_ingest_pipeline import process_ingest_job as _process_ingest_job
from app.pipeline.run_ingest_pipeline import run_discovery_scan as _run_discovery_scan
from app.settings import get_settings
from app.storage.database_service import DatabaseService


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    DatabaseService(db_path=settings.sqlite_db_path).init_schema()
    graph_capture_dir(settings)
    yield


app = FastAPI(title="RedKid Media Analyzer", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/captures", StaticFiles(directory=str(graph_capture_dir(get_settings()))), name="captures")
app.include_router(dashboard_router)
app.include_router(analysis_router)
app.include_router(prompts_router)
app.include_router(ingest_router)
app.include_router(jobs_router)
app.include_router(media_router)

__all__ = [
    "app",
    "analyze_account_graph",
    "analyze_media",
    "analyze_post_and_comments",
    "enqueue_ingest_jobs",
    "get_account",
    "get_account_comments",
    "get_account_graph",
    "get_account_posts",
    "get_dashboard_summary",
    "get_ingest_jobs",
    "get_ingest_trace",
    "get_jobs_overview",
    "get_prompt",
    "get_review_queue",
    "health",
    "ingest_instagram_account_latest",
    "list_accounts",
    "list_prompts",
    "create_batch_jobs",
    "get_avatar",
    "run_ingest",
    "run_ingest_workers_once",
    "save_account_graph_capture",
    "scan_ingest_watch",
    "update_prompt",
    "_process_ingest_job",
    "_run_discovery_scan",
]
