#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

export SQLITE_DB_PATH="${SQLITE_DB_PATH:-./data/redkid.db}"
export MINIO_BUCKET_DEFAULT="${MINIO_BUCKET_DEFAULT:-instagram-archive}"
export MINIO_BUCKET_FALLBACK="${MINIO_BUCKET_FALLBACK:-instagram_archive}"
export VLLM_BASE_URL="${VLLM_BASE_URL:-http://10.21.6.145:8007}"

exec uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
