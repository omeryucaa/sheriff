from __future__ import annotations

import mimetypes
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response

from app.api.dependencies import get_minio_service


router = APIRouter()

ALLOWED_REMOTE_HOSTS = {
    "instagram.ftol1-1.fna.fbcdn.net",
    "instagram.fyei6-3.fna.fbcdn.net",
    "instagram.fna.fbcdn.net",
}


def _guess_media_type(path: str, fallback: str = "image/jpeg") -> str:
    guessed = mimetypes.guess_type(path)[0]
    return guessed or fallback


@router.get("/media/avatar")
def get_avatar(
    source: str = Query(..., min_length=1),
    minio_service=Depends(get_minio_service),
) -> Response:
    if source.startswith("minio://"):
        without_scheme = source[len("minio://") :]
        bucket, sep, object_key = without_scheme.partition("/")
        if not sep or not bucket or not object_key:
            raise HTTPException(status_code=400, detail="Invalid minio source")
        try:
            payload = minio_service.read_object_bytes(bucket=bucket, object_key=object_key)
            media_type = minio_service.object_content_type(bucket=bucket, object_key=object_key) or _guess_media_type(object_key)
            if media_type == "application/octet-stream":
                media_type = _guess_media_type(object_key)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=f"MinIO object could not be read: {exc}") from exc
        return Response(
            content=payload,
            media_type=media_type,
            headers={"Cache-Control": "public, max-age=3600"},
        )

    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Unsupported source scheme")
    if parsed.hostname not in ALLOWED_REMOTE_HOSTS:
        raise HTTPException(status_code=403, detail="Host not allowed")

    try:
        req = Request(
            source,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "image/*,*/*;q=0.8",
            },
        )
        with urlopen(req, timeout=15) as upstream:
            payload = upstream.read()
            media_type = upstream.headers.get_content_type() or _guess_media_type(parsed.path)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Remote avatar fetch failed: {exc}") from exc

    return Response(
        content=payload,
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=3600"},
    )
