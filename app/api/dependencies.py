from __future__ import annotations

from pathlib import Path

from fastapi import Depends

from app.minio_service import MinioService
from app.settings import Settings, get_settings
from app.storage.database_service import DatabaseService
from app.vllm_service import VLLMService


def get_minio_service(settings: Settings = Depends(get_settings)) -> MinioService:
    return MinioService(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def get_vllm_service(settings: Settings = Depends(get_settings)) -> VLLMService:
    return VLLMService(
        base_url=settings.vllm_base_url,
        default_model=settings.vllm_model,
        timeout_seconds=settings.vllm_timeout_seconds,
    )


def get_db_service(settings: Settings = Depends(get_settings)) -> DatabaseService:
    service = DatabaseService(db_path=settings.sqlite_db_path)
    service.init_schema()
    return service


def graph_capture_dir(settings: Settings) -> Path:
    base_dir = Path(settings.sqlite_db_path).resolve().parent
    capture_dir = base_dir / "graph_captures"
    capture_dir.mkdir(parents=True, exist_ok=True)
    return capture_dir
