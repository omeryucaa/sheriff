from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.pipeline.run_comment_stage import execute_comment_stage
from app.prompts import _build_commenter_history_context, build_comment_analysis_prompt
from app.services.scoring_service import ScoringService
from app.services.stage_executor import StageExecutor
from app.settings import get_settings
from app.storage.database_service import DatabaseService
from app.vllm_service import VLLMService, VLLMUpstreamError


class RecordingTraceLogger:
    def __init__(self) -> None:
        self.entries: list[tuple[str, object]] = []

    def log(self, title: str, content: object) -> None:
        self.entries.append((title, content))


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _load_target_row(db_path: str, post_id: int | None, comment_id: int | None) -> sqlite3.Row:
    sql = """
        SELECT
            p.id AS post_id,
            a.id AS account_id,
            a.person_id AS person_id,
            a.instagram_username,
            a.bio,
            p.caption,
            p.post_analysis,
            p.structured_analysis,
            c.id AS comment_id,
            c.commenter_username,
            c.comment_text
        FROM instagram_posts p
        JOIN instagram_accounts a ON a.id = p.instagram_account_id
        JOIN instagram_comments c ON c.instagram_post_id = p.id
    """
    clauses: list[str] = []
    params: list[object] = []
    if post_id is not None:
        clauses.append("p.id = ?")
        params.append(post_id)
    if comment_id is not None:
        clauses.append("c.id = ?")
        params.append(comment_id)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY p.id DESC, c.id ASC LIMIT 1"
    with _connect(db_path) as conn:
        row = conn.execute(sql, params).fetchone()
    if row is None:
        raise SystemExit("Secilen post/yorum bulunamadi.")
    return row


def _load_commenter_history(db_path: str, commenter_username: str | None, exclude_comment_id: int) -> list[dict[str, Any]]:
    if not commenter_username:
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                c.comment_text,
                c.verdict,
                c.sentiment,
                c.reason,
                c.orgut_baglanti_skoru,
                c.bayrak,
                p.structured_analysis
            FROM instagram_comments c
            JOIN instagram_posts p ON p.id = c.instagram_post_id
            WHERE c.commenter_username = ? AND c.id != ?
            ORDER BY c.id
            """,
            (commenter_username, exclude_comment_id),
        ).fetchall()
    history: list[dict[str, Any]] = []
    for row in rows:
        post_ozet = ""
        structured = row["structured_analysis"]
        if structured:
            try:
                parsed = json.loads(str(structured))
                if isinstance(parsed, dict):
                    post_ozet = str(parsed.get("ozet") or "")
            except json.JSONDecodeError:
                pass
        history.append(
            {
                "comment_text": row["comment_text"],
                "verdict": row["verdict"],
                "sentiment": row["sentiment"],
                "reason": row["reason"],
                "orgut_baglanti_skoru": row["orgut_baglanti_skoru"],
                "bayrak": bool(row["bayrak"]),
                "post_ozet": post_ozet,
            }
        )
    return history


def _parse_structured_analysis(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_canonical_post_context(db_path: str, post_id: int) -> dict[str, Any]:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT corrected_payload
            FROM canonical_post_analyses
            WHERE instagram_post_id = ?
            """,
            (post_id,),
        ).fetchone()
    if row is None or not row["corrected_payload"]:
        return {}
    try:
        parsed = json.loads(str(row["corrected_payload"]))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_post_context(structured: dict[str, Any], canonical: dict[str, Any]) -> dict[str, Any]:
    if canonical:
        detected_entities = canonical.get("detected_entities")
        detected_entities = detected_entities if isinstance(detected_entities, list) else None
        return {
            "post_summary": str(canonical.get("summary") or "").strip() or None,
            "post_categories": canonical.get("categories") if isinstance(canonical.get("categories"), list) else None,
            "post_detected_entities": detected_entities,
            "post_role": str(canonical.get("role") or "").strip() or None,
            "post_organization_link_score": canonical.get("organization_link_score"),
            "post_threat_level": str(canonical.get("threat_level") or "").strip() or None,
            "focus_entity": str(canonical.get("focus_entity") or "").strip() or (detected_entities[0] if detected_entities else None),
        }
    org = structured.get("orgut_baglantisi")
    org = org if isinstance(org, dict) else {}
    threat = structured.get("tehdit_degerlendirmesi")
    threat = threat if isinstance(threat, dict) else {}
    detected_entities: list[str] = []
    for value in [org.get("tespit_edilen_orgut"), org.get("baglanti_gostergesi")]:
        item = str(value or "").strip()
        if item and item not in detected_entities:
            detected_entities.append(item)
    organization_link_score = None
    raw_score = org.get("orgut_baglanti_skoru")
    if raw_score is not None:
        try:
            organization_link_score = int(raw_score)
        except (TypeError, ValueError):
            organization_link_score = None
    if organization_link_score is None and detected_entities:
        organization_link_score = 2
    return {
        "post_summary": str(structured.get("ozet") or "").strip() or None,
        "post_categories": structured.get("icerik_kategorisi") if isinstance(structured.get("icerik_kategorisi"), list) else None,
        "post_detected_entities": detected_entities or None,
        "post_role": str(org.get("muhtemel_rol") or "").strip() or None,
        "post_organization_link_score": organization_link_score,
        "post_threat_level": str(threat.get("tehdit_seviyesi") or "").strip() or None,
        "focus_entity": detected_entities[0] if detected_entities else None,
    }


def _load_stored_attempts(
    db_path: str,
    post_id: int,
    commenter_username: str | None,
    comment_text: str,
    limit: int = 3,
) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, validation_status, rendered_prompt, raw_output, created_at
            FROM llm_stage_attempts
            WHERE stage_name = 'comment_analysis'
              AND related_post_id = ?
              AND rendered_prompt LIKE ?
              AND rendered_prompt LIKE ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (
                post_id,
                f"%Comment owner: {commenter_username or '-'}%",
                f"%Comment text: {comment_text}%",
                limit,
            ),
        ).fetchall()
    return rows


def _print_section(title: str, content: object) -> None:
    print(f"\n===== {title} =====")
    if isinstance(content, (dict, list)):
        print(json.dumps(content, ensure_ascii=False, indent=2))
        return
    print(str(content))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mevcut DB'den tek bir yorum secip comment-analysis promptu ve LLM cevabini gosterir."
    )
    parser.add_argument("--db-path", default=str(ROOT / "data" / "redkid.db"))
    parser.add_argument("--post-id", type=int)
    parser.add_argument("--comment-id", type=int)
    parser.add_argument("--show-stored", action="store_true", help="DB'deki onceki kayitli attempt'leri de goster.")
    parser.add_argument("--no-llm", action="store_true", help="LLM'e tekrar gitme; sadece prompt ve mevcut DB context'ini goster.")
    parser.add_argument("--max-tokens", type=int, default=768)
    args = parser.parse_args()

    row = _load_target_row(args.db_path, args.post_id, args.comment_id)
    structured = _parse_structured_analysis(row["structured_analysis"])
    canonical_post = _load_canonical_post_context(args.db_path, int(row["post_id"]))
    post_context = _extract_post_context(structured, canonical_post)
    history = _load_commenter_history(args.db_path, row["commenter_username"], int(row["comment_id"]))

    _print_section(
        "SECILEN ORNEK",
        {
            "post_id": row["post_id"],
            "comment_id": row["comment_id"],
            "instagram_username": row["instagram_username"],
            "commenter_username": row["commenter_username"],
            "comment_text": row["comment_text"],
            "history_count_excluding_current": len(history),
            "focus_entity": post_context["focus_entity"],
        },
    )

    if args.show_stored:
        stored = _load_stored_attempts(
            args.db_path,
            int(row["post_id"]),
            row["commenter_username"],
            str(row["comment_text"]),
        )
        if stored:
            for item in stored:
                _print_section(
                    f"DBDE KAYITLI ATTEMPT #{item['id']} ({item['validation_status']}, {item['created_at']}) PROMPT",
                    item["rendered_prompt"],
                )
                _print_section(
                    f"DBDE KAYITLI ATTEMPT #{item['id']} RAW OUTPUT",
                    item["raw_output"],
                )
        else:
            _print_section("DBDE KAYITLI ATTEMPT", "Bu yorum icin prompt metninden eslesen bir attempt bulunamadi.")

    db_service = DatabaseService(args.db_path)
    stage_executor = StageExecutor(
        vllm_service=VLLMService(
            base_url=get_settings().vllm_base_url,
            default_model=get_settings().vllm_model,
            timeout_seconds=get_settings().vllm_timeout_seconds,
        ),
        db_service=db_service,
    )
    trace_logger = RecordingTraceLogger()

    if args.no_llm:
        prompt = build_comment_analysis_prompt(
            post_analysis=str(row["post_analysis"] or post_context["post_summary"] or ""),
            username=str(row["instagram_username"]),
            bio=row["bio"],
            caption=row["caption"],
            commenter_username=row["commenter_username"],
            comment_text=str(row["comment_text"]),
            commenter_history_context=_build_commenter_history_context(history),
            focus_entity=post_context["focus_entity"],
            template_content=db_service.get_prompt_content("comment_analysis"),
            post_summary=post_context["post_summary"],
            post_categories=post_context["post_categories"],
            post_detected_entities=post_context["post_detected_entities"],
            post_role=post_context["post_role"],
            post_organization_link_score=post_context["post_organization_link_score"],
            post_threat_level=post_context["post_threat_level"],
        )
        _print_section("RENDER EDILEN PROMPT", prompt)
        return

    try:
        canonical, legacy, prompt, payload = execute_comment_stage(
            stage_executor=stage_executor,
            post_analysis=str(row["post_analysis"] or post_context["post_summary"] or ""),
            username=str(row["instagram_username"]),
            bio=row["bio"],
            caption=row["caption"],
            commenter_username=row["commenter_username"],
            comment_text=str(row["comment_text"]),
            commenter_history=history,
            template_content=db_service.get_prompt_content("comment_analysis"),
            model=None,
            max_tokens=args.max_tokens,
            scoring_service=ScoringService(),
            related_account_id=int(row["account_id"]),
            related_post_id=int(row["post_id"]),
            related_comment_id=int(row["comment_id"]),
            focus_entity=post_context["focus_entity"],
            post_summary=post_context["post_summary"],
            post_categories=post_context["post_categories"],
            post_detected_entities=post_context["post_detected_entities"],
            post_role=post_context["post_role"],
            post_organization_link_score=post_context["post_organization_link_score"],
            post_threat_level=post_context["post_threat_level"],
            trace_logger=trace_logger,
            trace_prefix="DEBUG_COMMENT",
        )
    except VLLMUpstreamError as exc:
        prompt = next((content for title, content in trace_logger.entries if title.endswith("_PROMPT")), "")
        payload = next((content for title, content in trace_logger.entries if title.endswith("_PAYLOAD")), {})
        _print_section("RENDER EDILEN PROMPT", prompt)
        _print_section("REQUEST PAYLOAD", payload)
        _print_section(
            "LLM HATASI",
            {
                "status_code": exc.status_code,
                "message": exc.message,
            },
        )
        raise SystemExit(1)

    _print_section("RENDER EDILEN PROMPT", prompt)
    _print_section("REQUEST PAYLOAD", payload)
    for title, content in trace_logger.entries:
        if title.endswith("_RAW_RESPONSE"):
            _print_section(title, content)
    _print_section("PARSED CANONICAL RESULT", canonical.model_dump(mode="json"))
    _print_section("LEGACY RESULT", legacy.model_dump(mode="json"))


if __name__ == "__main__":
    main()
