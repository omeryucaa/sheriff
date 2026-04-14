from dataclasses import dataclass
import os
import socket


def _default_minio_endpoint() -> str:
    public_endpoint = os.getenv("MINIO_PUBLIC_ENDPOINT")
    if public_endpoint:
        return public_endpoint

    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("8.8.8.8", 80))
            ip = probe.getsockname()[0]
            if ip and not ip.startswith("127."):
                return f"{ip}:9000"
        finally:
            probe.close()
    except OSError:
        pass

    return "127.0.0.1:9000"


@dataclass(frozen=True)
class Settings:
    vllm_base_url: str = os.getenv("VLLM_BASE_URL", "http://10.21.6.145:8007")
    vllm_model: str = os.getenv("VLLM_MODEL", "gemma-4-31b-it")
    vllm_timeout_seconds: int = int(os.getenv("VLLM_TIMEOUT_SECONDS", "120"))

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", _default_minio_endpoint())
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    minio_bucket_default: str = os.getenv("MINIO_BUCKET_DEFAULT", "instagram-archive")
    minio_bucket_fallback: str = os.getenv("MINIO_BUCKET_FALLBACK", "instagram_archive")
    sqlite_db_path: str = os.getenv("SQLITE_DB_PATH", "./data/redkid.db")
    ingest_trace_log_path: str | None = os.getenv("INGEST_TRACE_LOG_PATH", "./data/ingest_trace.log")
    ingest_watch_interval_seconds: int = int(os.getenv("INGEST_WATCH_INTERVAL_SECONDS", "60"))
    ingest_max_concurrent_accounts: int = int(os.getenv("INGEST_MAX_CONCURRENT_ACCOUNTS", "2"))
    ingest_max_concurrent_posts_per_account: int = int(os.getenv("INGEST_MAX_CONCURRENT_POSTS_PER_ACCOUNT", "10"))
    ingest_max_concurrent_media_per_post: int = int(os.getenv("INGEST_MAX_CONCURRENT_MEDIA_PER_POST", "2"))
    ingest_max_concurrent_comments: int = int(os.getenv("INGEST_MAX_CONCURRENT_COMMENTS", "10"))
    ingest_job_lease_seconds: int = int(os.getenv("INGEST_JOB_LEASE_SECONDS", "300"))


def get_settings() -> Settings:
    return Settings()
