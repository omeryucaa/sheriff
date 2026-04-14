#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from typing import TextIO

from app.main import (
    _build_media_observation_context,
    _build_post_history_entry,
    _collect_post_media_items,
    _extract_embedded_comments,
    _find_latest_run_id,
    _infer_post_media_type,
    _parse_comment_classification,
    _parse_media_observation,
    _parse_post_analysis,
    _read_json_object,
    _read_jsonl_objects,
    _resolve_archive_bucket,
)
from app.minio_service import MinioService
from app.prompts import (
    _build_commenter_history_context,
    _build_post_history_context,
    build_comment_analysis_prompt,
    build_media_analysis_prompt,
    build_post_analysis_prompt,
)
from app.settings import Settings
from app.vllm_service import VLLMService


def _print_block(title: str, content: object, stream: TextIO) -> None:
    print(f"\n===== {title} =====", file=stream)
    if isinstance(content, (dict, list)):
        print(json.dumps(content, ensure_ascii=False, indent=2), file=stream)
    else:
        print(str(content), file=stream)
    print(f"===== /{title} =====", file=stream)


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Instagram ingest akışını DB'ye yazmadan trace log ile çalıştırır."
    )
    parser.add_argument("--username", required=True, help="Hedef Instagram kullanıcı adı")
    parser.add_argument("--run-id", help="Belirli run_id kullan")
    parser.add_argument("--bucket", help="Belirli bucket kullan")
    parser.add_argument("--limit", type=int, default=5, help="İşlenecek post sayısı")
    parser.add_argument(
        "--max-comments-per-post",
        type=int,
        default=1,
        help="Her post için en fazla kaç yorum trace edilsin",
    )
    parser.add_argument(
        "--log-file",
        default="dry_run_trace.log",
        help="Trace çıktısının yazılacağı dosya yolu",
    )
    return parser


def main() -> int:
    parser = _build_argument_parser()
    args = parser.parse_args()

    settings = Settings()
    minio_service = MinioService(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    vllm_service = VLLMService(
        base_url=settings.vllm_base_url,
        default_model=settings.vllm_model,
        timeout_seconds=settings.vllm_timeout_seconds,
    )

    bucket = _resolve_archive_bucket(
        minio_service=minio_service,
        requested_bucket=args.bucket,
        default_bucket=settings.minio_bucket_default,
        fallback_bucket=settings.minio_bucket_fallback,
    )
    run_id = args.run_id or _find_latest_run_id(minio_service, bucket, args.username)
    base_prefix = f"instagram/{args.username}/{run_id}"

    profile = _read_json_object(minio_service, bucket, f"{base_prefix}/profile/profile.json")
    person_name = str(profile.get("full_name") or profile.get("username") or args.username)
    instagram_username = str(profile.get("username") or args.username)
    bio = str(profile.get("bio") or "") or None

    all_post_json_keys = [
        key
        for key in sorted(minio_service.list_object_names(bucket, prefix=f"{base_prefix}/posts/", recursive=True))
        if key.endswith("/post.json")
    ]
    post_json_keys = all_post_json_keys[: args.limit]
    post_history_summaries: list[dict[str, object]] = []
    commenter_histories: dict[str, list[dict[str, object]]] = {}

    with open(args.log_file, "w", encoding="utf-8") as handle:
        _print_block(
            "RUN_INFO",
            {
                "username": args.username,
                "bucket": bucket,
                "run_id": run_id,
                "limit": args.limit,
                "max_comments_per_post": args.max_comments_per_post,
                "minio_endpoint": settings.minio_endpoint,
                "vllm_base_url": settings.vllm_base_url,
                "vllm_model": settings.vllm_model,
            },
            handle,
        )

        for index, post_json_key in enumerate(post_json_keys, 1):
            post = _read_json_object(minio_service, bucket, post_json_key)
            source_post_id = str(post.get("post_id") or post_json_key.split("/")[-2])
            source_post_url = str(post.get("post_url") or "") or None
            caption = str(post.get("caption") or "") or None
            post_type = str(post.get("post_type") or "")
            post_dir_prefix = f"{base_prefix}/posts/{source_post_id}/"

            _print_block(
                f"POST_{index}_META",
                {
                    "source_post_id": source_post_id,
                    "source_post_url": source_post_url,
                    "post_type": post_type,
                    "caption": caption,
                    "history_count_before": len(post_history_summaries),
                },
                handle,
            )

            post_media_items = _collect_post_media_items(
                minio_service=minio_service,
                bucket=bucket,
                post=post,
                post_dir_prefix=post_dir_prefix,
                post_type=post_type,
                expires_seconds=3600,
            )
            media_type = _infer_post_media_type(post_media_items, post_type)
            media_observations: list[dict[str, object]] = []

            for media_index, media_item in enumerate(post_media_items, 1):
                media_prompt = build_media_analysis_prompt(
                    username=person_name,
                    instagram_username=instagram_username,
                    bio=bio,
                    caption=caption,
                    media_index=media_index,
                    media_count=len(post_media_items),
                    media_type=media_item["media_type"],
                )
                media_payload = vllm_service.build_payload(
                    description=media_prompt,
                    media_type=media_item["media_type"],
                    media_url=media_item["media_url"],
                    max_tokens=220,
                )
                media_raw = vllm_service.create_chat_completion(media_payload)
                media_model, media_answer, media_usage, media_finish_reason = vllm_service.extract_answer(media_raw)
                media_parsed = _parse_media_observation(media_answer, media_index, media_item["media_type"])
                media_observations.append(media_parsed)

                _print_block(
                    f"POST_{index}_MEDIA_{media_index}_PROMPT",
                    media_prompt,
                    handle,
                )
                _print_block(
                    f"POST_{index}_MEDIA_{media_index}_PAYLOAD",
                    media_payload,
                    handle,
                )
                _print_block(
                    f"POST_{index}_MEDIA_{media_index}_RAW_RESPONSE",
                    media_raw,
                    handle,
                )
                _print_block(
                    f"POST_{index}_MEDIA_{media_index}_PARSED",
                    {
                        "model": media_model,
                        "usage": media_usage,
                        "finish_reason": media_finish_reason,
                        "parsed": media_parsed,
                    },
                    handle,
                )

            history_context = _build_post_history_context(post_history_summaries)
            media_context = _build_media_observation_context(media_observations)
            post_prompt = build_post_analysis_prompt(
                username=person_name,
                instagram_username=instagram_username,
                bio=bio,
                caption=caption,
                post_history_context=history_context,
                media_context=media_context,
            )
            post_payload = {
                "model": vllm_service.default_model,
                "messages": [{"role": "user", "content": post_prompt}],
                "max_tokens": 1200,
                "stream": False,
            }
            post_raw = vllm_service.create_chat_completion(post_payload)
            post_model, post_answer, post_usage, post_finish_reason = vllm_service.extract_answer(post_raw)
            parsed_post_analysis = _parse_post_analysis(post_answer)
            post_history_summaries.append(_build_post_history_entry(source_post_id, parsed_post_analysis))

            _print_block(f"POST_{index}_PROMPT", post_prompt, handle)
            _print_block(f"POST_{index}_PAYLOAD", post_payload, handle)
            _print_block(f"POST_{index}_RAW_RESPONSE", post_raw, handle)
            _print_block(
                f"POST_{index}_PARSED",
                {
                    "model": post_model,
                    "usage": post_usage,
                    "finish_reason": post_finish_reason,
                    "parsed": parsed_post_analysis.model_dump(mode="json"),
                },
                handle,
            )

            comments_key = f"{post_dir_prefix}comments.jsonl"
            comments: list[dict[str, object]] = []
            if minio_service.object_exists(bucket, comments_key):
                comments = _read_jsonl_objects(minio_service, bucket, comments_key)
            if not comments:
                comments = _extract_embedded_comments(post)
            if args.max_comments_per_post is not None:
                comments = comments[: args.max_comments_per_post]

            for comment_index, comment in enumerate(comments, 1):
                commenter_username = str(comment.get("commenter_username") or "") or None
                comment_text = str(comment.get("comment_text") or "").strip()
                if not comment_text:
                    continue

                commenter_history = commenter_histories.get(commenter_username or "", [])
                commenter_history_context = _build_commenter_history_context(commenter_history)
                comment_prompt = build_comment_analysis_prompt(
                    post_analysis=json.dumps(parsed_post_analysis.model_dump(mode="json"), ensure_ascii=False),
                    username=instagram_username,
                    bio=bio,
                    caption=caption,
                    commenter_username=commenter_username,
                    comment_text=comment_text,
                    commenter_history_context=commenter_history_context,
                )
                comment_payload = {
                    "model": vllm_service.default_model,
                    "messages": [{"role": "user", "content": comment_prompt}],
                    "max_tokens": 180,
                    "stream": False,
                }
                comment_raw = vllm_service.create_chat_completion(comment_payload)
                comment_model, comment_answer, comment_usage, comment_finish_reason = vllm_service.extract_answer(
                    comment_raw
                )
                verdict, sentiment, score, bayrak, reason = _parse_comment_classification(comment_answer)
                parsed_comment = {
                    "commenter_username": commenter_username,
                    "comment_text": comment_text,
                    "verdict": verdict,
                    "sentiment": sentiment,
                    "orgut_baglanti_skoru": score,
                    "bayrak": bayrak,
                    "reason": reason,
                }
                if commenter_username:
                    commenter_histories.setdefault(commenter_username, []).append(
                        {
                            "post_ozet": parsed_post_analysis.ozet,
                            "comment_text": comment_text,
                            "verdict": verdict,
                            "orgut_baglanti_skoru": score,
                            "bayrak": bayrak,
                        }
                    )

                _print_block(f"POST_{index}_COMMENT_{comment_index}_PROMPT", comment_prompt, handle)
                _print_block(f"POST_{index}_COMMENT_{comment_index}_PAYLOAD", comment_payload, handle)
                _print_block(f"POST_{index}_COMMENT_{comment_index}_RAW_RESPONSE", comment_raw, handle)
                _print_block(
                    f"POST_{index}_COMMENT_{comment_index}_PARSED",
                    {
                        "model": comment_model,
                        "usage": comment_usage,
                        "finish_reason": comment_finish_reason,
                        "parsed": parsed_comment,
                    },
                    handle,
                )

        _print_block(
            "RUN_DONE",
            {"processed_posts": len(post_json_keys), "log_file": args.log_file},
            handle,
        )

    with open(args.log_file, "r", encoding="utf-8") as handle:
        sys.stdout.write(handle.read())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
