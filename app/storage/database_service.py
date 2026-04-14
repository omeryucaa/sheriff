from __future__ import annotations

import os
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass

from app.config.org_groups import get_seed_org_group_rows
from app.config.review_rules import COMMENT_REVIEW_QUEUE_MIN_SCORE
from app.prompts import get_default_prompt_template, get_default_prompt_templates
from app.utils.text_normalize import normalize_match_text


@dataclass(frozen=True)
class PersistedAnalysisIds:
    person_id: int
    instagram_account_id: int
    post_id: int
    comment_ids: list[int]


@dataclass(frozen=True)
class UpsertPostResult:
    post_id: int
    created: bool


@dataclass(frozen=True)
class UpsertCommentResult:
    comment_id: int
    created: bool


class DatabaseService:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    @contextmanager
    def _connect(self):
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column_name: str, column_def: str) -> None:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {str(r[1]) for r in rows}
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_def}")

    def _has_column(self, conn: sqlite3.Connection, table: str, column_name: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return column_name in {str(r[1]) for r in rows}

    @staticmethod
    def _normalize_media_items(media_items: list[dict[str, object]] | None) -> list[dict[str, str]]:
        normalized: list[dict[str, str]] = []
        for item in media_items or []:
            if not isinstance(item, dict):
                continue
            media_type = str(item.get("media_type") or "").strip()
            media_url = str(item.get("media_url") or "").strip()
            if media_type not in {"image", "video"} or not media_url:
                continue
            normalized.append({"media_type": media_type, "media_url": media_url})
        return normalized

    def init_schema(self) -> None:
        schema_sql = """
        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS instagram_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            instagram_username TEXT NOT NULL,
            profile_photo_url TEXT,
            bio TEXT,
            account_profile_summary TEXT,
            account_profile_summary_updated_at TEXT,
            dominant_detected_org TEXT,
            dominant_threat_level TEXT,
            last_ingested_run_id TEXT,
            last_ingested_at TEXT,
            graph_ai_analysis TEXT,
            graph_ai_analysis_model TEXT,
            graph_ai_analysis_updated_at TEXT,
            graph_capture_path TEXT,
            graph_capture_updated_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, instagram_username),
            FOREIGN KEY (person_id) REFERENCES persons(id)
        );

        CREATE TABLE IF NOT EXISTS instagram_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instagram_account_id INTEGER NOT NULL,
            source_kind TEXT NOT NULL DEFAULT 'post',
            source_container_id TEXT,
            source_container_title TEXT,
            source_created_at TEXT,
            media_type TEXT NOT NULL,
            media_url TEXT NOT NULL,
            media_items TEXT,
            caption TEXT,
            post_analysis TEXT,
            post_ozet TEXT,
            structured_analysis TEXT,
            icerik_kategorisi TEXT,
            tehdit_seviyesi TEXT,
            onem_skoru INTEGER,
            orgut_baglantisi TEXT,
            tespit_edilen_orgut TEXT,
            model TEXT,
            source_target_username TEXT,
            source_run_id TEXT,
            source_post_id TEXT,
            source_post_url TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (instagram_account_id) REFERENCES instagram_accounts(id)
        );

        CREATE TABLE IF NOT EXISTS instagram_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instagram_post_id INTEGER NOT NULL,
            commenter_username TEXT,
            commenter_profile_url TEXT,
            comment_text TEXT NOT NULL,
            verdict TEXT NOT NULL,
            sentiment TEXT NOT NULL,
            orgut_baglanti_skoru INTEGER NOT NULL DEFAULT 0,
            bayrak INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            discovered_at TEXT,
            source_run_id TEXT,
            source_post_id TEXT,
            source_post_url TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (instagram_post_id) REFERENCES instagram_posts(id)
        );

        CREATE TABLE IF NOT EXISTS review_queue (
            commenter_username TEXT PRIMARY KEY,
            person_id INTEGER,
            trigger_count INTEGER NOT NULL DEFAULT 1,
            first_triggered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_triggered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_reason TEXT,
            flag_reason_type TEXT,
            status TEXT NOT NULL DEFAULT 'open'
        );

        CREATE TABLE IF NOT EXISTS ingest_sources (
            username TEXT PRIMARY KEY,
            bucket TEXT NOT NULL,
            last_seen_run_id TEXT,
            last_enqueued_run_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ingest_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_username TEXT NOT NULL,
            bucket TEXT NOT NULL,
            run_id TEXT NOT NULL,
            batch_job_id INTEGER,
            batch_target_id INTEGER,
            source_kind TEXT,
            parent_username TEXT,
            focus_entity TEXT,
            country TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            processed_posts INTEGER NOT NULL DEFAULT 0,
            created_posts INTEGER NOT NULL DEFAULT 0,
            updated_posts INTEGER NOT NULL DEFAULT 0,
            processed_comments INTEGER NOT NULL DEFAULT 0,
            created_comments INTEGER NOT NULL DEFAULT 0,
            skipped_comments INTEGER NOT NULL DEFAULT 0,
            flagged_users INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            lease_owner TEXT,
            lease_expires_at TEXT,
            started_at TEXT,
            finished_at TEXT,
            current_stage TEXT,
            current_event TEXT,
            current_post_index INTEGER,
            total_posts INTEGER,
            current_post_id TEXT,
            current_media_index INTEGER,
            total_media_items INTEGER,
            current_comment_index INTEGER,
            total_comments INTEGER,
            current_commenter_username TEXT,
            last_event_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(target_username, run_id)
        );

        CREATE TABLE IF NOT EXISTS batch_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            bucket TEXT NOT NULL,
            country TEXT,
            focus_entity TEXT,
            auto_enqueue_followups INTEGER NOT NULL DEFAULT 0,
            requested_targets TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS batch_job_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_job_id INTEGER NOT NULL,
            raw_target TEXT NOT NULL,
            normalized_username TEXT NOT NULL,
            source_kind TEXT NOT NULL DEFAULT 'initial',
            parent_username TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            ingest_job_id INTEGER,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(batch_job_id, normalized_username),
            FOREIGN KEY (batch_job_id) REFERENCES batch_jobs(id)
        );

        CREATE TABLE IF NOT EXISTS ingest_job_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingest_job_id INTEGER NOT NULL,
            source_post_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ingest_job_id, source_post_id),
            FOREIGN KEY (ingest_job_id) REFERENCES ingest_jobs(id)
        );

        CREATE TABLE IF NOT EXISTS ingest_job_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingest_job_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            stage TEXT,
            message TEXT NOT NULL,
            source_post_id TEXT,
            commenter_username TEXT,
            post_index INTEGER,
            post_total INTEGER,
            media_index INTEGER,
            media_total INTEGER,
            comment_index INTEGER,
            comment_total INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ingest_job_id) REFERENCES ingest_jobs(id)
        );

        CREATE TABLE IF NOT EXISTS person_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            related_username TEXT NOT NULL,
            related_person_id INTEGER,
            source_account_id INTEGER,
            source_post_id TEXT,
            source_comment_id INTEGER,
            link_reason TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, related_username, source_post_id, link_reason)
        );

        CREATE TABLE IF NOT EXISTS prompt_templates (
            key TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            description TEXT,
            content TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            version INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS org_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_type TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(group_type, canonical_name)
        );

        CREATE TABLE IF NOT EXISTS org_group_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_group_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            normalized_alias TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(org_group_id, normalized_alias),
            FOREIGN KEY (org_group_id) REFERENCES org_groups(id)
        );

        CREATE TABLE IF NOT EXISTS llm_stage_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_name TEXT NOT NULL,
            prompt_key TEXT NOT NULL,
            prompt_version INTEGER NOT NULL DEFAULT 1,
            rendered_prompt TEXT NOT NULL,
            model TEXT NOT NULL,
            raw_output TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            validation_error TEXT,
            repair_attempted INTEGER NOT NULL DEFAULT 0,
            related_account_id INTEGER,
            related_post_id INTEGER,
            related_comment_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS media_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instagram_post_id INTEGER NOT NULL,
            media_index INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            deep_required INTEGER NOT NULL DEFAULT 0,
            deep_status TEXT NOT NULL DEFAULT 'not_required',
            deep_reason TEXT,
            location_confidence TEXT,
            contains_vehicle INTEGER NOT NULL DEFAULT 0,
            contains_plate INTEGER NOT NULL DEFAULT 0,
            deep_payload TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(instagram_post_id, media_index),
            FOREIGN KEY (instagram_post_id) REFERENCES instagram_posts(id)
        );

        CREATE TABLE IF NOT EXISTS canonical_post_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instagram_post_id INTEGER NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            corrected_payload TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (instagram_post_id) REFERENCES instagram_posts(id)
        );

        CREATE TABLE IF NOT EXISTS canonical_comment_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instagram_comment_id INTEGER NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            corrected_payload TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (instagram_comment_id) REFERENCES instagram_comments(id)
        );

        CREATE TABLE IF NOT EXISTS account_aggregates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instagram_account_id INTEGER NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (instagram_account_id) REFERENCES instagram_accounts(id)
        );

        CREATE INDEX IF NOT EXISTS idx_instagram_accounts_person_id
            ON instagram_accounts(person_id);

        CREATE INDEX IF NOT EXISTS idx_instagram_posts_account_id
            ON instagram_posts(instagram_account_id);

        CREATE INDEX IF NOT EXISTS idx_instagram_comments_post_id
            ON instagram_comments(instagram_post_id);
        """
        with self._connect() as conn:
            conn.executescript(schema_sql)

            # Lightweight migration path for already-created databases.
            self._ensure_column(conn, "instagram_posts", "source_target_username", "TEXT")
            self._ensure_column(conn, "instagram_posts", "source_run_id", "TEXT")
            self._ensure_column(conn, "instagram_posts", "source_post_id", "TEXT")
            self._ensure_column(conn, "instagram_posts", "source_post_url", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "account_profile_summary", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "account_profile_summary_updated_at", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "dominant_detected_org", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "dominant_threat_level", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "last_ingested_run_id", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "last_ingested_at", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "graph_ai_analysis", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "graph_ai_analysis_model", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "graph_ai_analysis_updated_at", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "graph_capture_path", "TEXT")
            self._ensure_column(conn, "instagram_accounts", "graph_capture_updated_at", "TEXT")
            self._ensure_column(conn, "instagram_posts", "source_kind", "TEXT NOT NULL DEFAULT 'post'")
            self._ensure_column(conn, "instagram_posts", "source_container_id", "TEXT")
            self._ensure_column(conn, "instagram_posts", "source_container_title", "TEXT")
            self._ensure_column(conn, "instagram_posts", "source_created_at", "TEXT")
            self._ensure_column(conn, "instagram_posts", "post_ozet", "TEXT")
            self._ensure_column(conn, "instagram_posts", "structured_analysis", "TEXT")
            self._ensure_column(conn, "instagram_posts", "icerik_kategorisi", "TEXT")
            self._ensure_column(conn, "instagram_posts", "tehdit_seviyesi", "TEXT")
            self._ensure_column(conn, "instagram_posts", "onem_skoru", "INTEGER")
            self._ensure_column(conn, "instagram_posts", "orgut_baglantisi", "TEXT")
            self._ensure_column(conn, "instagram_posts", "tespit_edilen_orgut", "TEXT")
            self._ensure_column(conn, "instagram_posts", "media_items", "TEXT")

            self._ensure_column(conn, "instagram_comments", "commenter_profile_url", "TEXT")
            self._ensure_column(conn, "instagram_comments", "discovered_at", "TEXT")
            self._ensure_column(conn, "instagram_comments", "source_run_id", "TEXT")
            self._ensure_column(conn, "instagram_comments", "source_post_id", "TEXT")
            self._ensure_column(conn, "instagram_comments", "source_post_url", "TEXT")
            self._ensure_column(conn, "instagram_comments", "orgut_baglanti_skoru", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "instagram_comments", "bayrak", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "media_observations", "deep_required", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "media_observations", "deep_status", "TEXT NOT NULL DEFAULT 'not_required'")
            self._ensure_column(conn, "media_observations", "deep_reason", "TEXT")
            self._ensure_column(conn, "media_observations", "location_confidence", "TEXT")
            self._ensure_column(conn, "media_observations", "contains_vehicle", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "media_observations", "contains_plate", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "media_observations", "deep_payload", "TEXT")
            self._ensure_column(conn, "review_queue", "person_id", "INTEGER")
            self._ensure_column(conn, "review_queue", "flag_reason_type", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "batch_job_id", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "batch_target_id", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "source_kind", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "parent_username", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "focus_entity", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "country", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "current_stage", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "current_event", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "current_post_index", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "total_posts", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "current_post_id", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "current_media_index", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "total_media_items", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "current_comment_index", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "total_comments", "INTEGER")
            self._ensure_column(conn, "ingest_jobs", "current_commenter_username", "TEXT")
            self._ensure_column(conn, "ingest_jobs", "last_event_at", "TEXT")
            self._ensure_column(conn, "batch_jobs", "auto_enqueue_followups", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "prompt_templates", "description", "TEXT")
            self._ensure_column(conn, "prompt_templates", "is_enabled", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(conn, "prompt_templates", "version", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(conn, "prompt_templates", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")

            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_instagram_posts_source
                ON instagram_posts(instagram_account_id, source_run_id, source_post_id)
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_instagram_comments_source
                ON instagram_comments(instagram_post_id, commenter_username, comment_text, discovered_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ingest_jobs_status_lease
                ON ingest_jobs(status, lease_expires_at, created_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ingest_job_posts_job
                ON ingest_job_posts(ingest_job_id, status)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ingest_job_events_job_created
                ON ingest_job_events(ingest_job_id, created_at DESC, id DESC)
                """
            )
            self._seed_prompt_templates(conn)
            self._seed_org_groups(conn)
            self._backfill_review_queue_from_comments(conn)

            conn.commit()

    def _seed_prompt_templates(self, conn: sqlite3.Connection) -> None:
        for item in get_default_prompt_templates():
            existing = conn.execute("SELECT key FROM prompt_templates WHERE key = ?", (item["key"],)).fetchone()
            if existing:
                continue
            conn.execute(
                """
                INSERT INTO prompt_templates(key, display_name, description, content, is_enabled, version)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    item["key"],
                    item["display_name"],
                    item["description"],
                    item["content"],
                    1 if item.get("is_enabled") else 0,
                    int(item.get("version") or 1),
                ),
            )

    def _seed_org_groups(self, conn: sqlite3.Connection) -> None:
        for item in get_seed_org_group_rows():
            existing = conn.execute(
                "SELECT id FROM org_groups WHERE group_type = ? AND canonical_name = ?",
                (item["group_type"], item["canonical_name"]),
            ).fetchone()
            if existing:
                org_group_id = int(existing["id"])
            else:
                cur = conn.execute(
                    """
                    INSERT INTO org_groups(group_type, canonical_name, is_enabled, notes)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        item["group_type"],
                        item["canonical_name"],
                        1 if item.get("is_enabled", True) else 0,
                        item.get("notes"),
                    ),
                )
                org_group_id = int(cur.lastrowid)

            for alias in item.get("aliases", []) or []:
                normalized_alias = normalize_match_text(str(alias))
                conn.execute(
                    """
                    INSERT OR IGNORE INTO org_group_aliases(org_group_id, alias, normalized_alias)
                    VALUES (?, ?, ?)
                    """,
                    (org_group_id, str(alias), normalized_alias),
                )

    def _backfill_review_queue_from_comments(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """
            SELECT ic.commenter_username, ic.reason, ic.verdict
            FROM instagram_comments ic
            WHERE ic.commenter_username IS NOT NULL
              AND TRIM(ic.commenter_username) <> ''
              AND (COALESCE(ic.bayrak, 0) = 1 OR COALESCE(ic.orgut_baglanti_skoru, 0) >= ?)
              AND ic.id = (
                SELECT MAX(ic2.id)
                FROM instagram_comments ic2
                WHERE ic2.commenter_username = ic.commenter_username
                  AND (COALESCE(ic2.bayrak, 0) = 1 OR COALESCE(ic2.orgut_baglanti_skoru, 0) >= ?)
              )
            """,
            (COMMENT_REVIEW_QUEUE_MIN_SCORE, COMMENT_REVIEW_QUEUE_MIN_SCORE),
        ).fetchall()

        for row in rows:
            commenter_username = str(row["commenter_username"] or "").strip()
            if not commenter_username:
                continue
            conn.execute(
                """
                INSERT OR IGNORE INTO review_queue(commenter_username, last_reason, flag_reason_type, status)
                VALUES (?, ?, ?, 'open')
                """,
                (
                    commenter_username,
                    str(row["reason"] or ""),
                    str(row["verdict"] or "belirsiz"),
                ),
            )

    def _get_or_create_person(self, conn: sqlite3.Connection, full_name: str) -> int:
        row = conn.execute(
            "SELECT id FROM persons WHERE full_name = ? ORDER BY id LIMIT 1",
            (full_name,),
        ).fetchone()
        if row:
            return int(row["id"])

        cur = conn.execute("INSERT INTO persons(full_name) VALUES (?)", (full_name,))
        return int(cur.lastrowid)

    def _upsert_instagram_account(
        self,
        conn: sqlite3.Connection,
        person_id: int,
        instagram_username: str,
        profile_photo_url: str | None,
        bio: str | None,
    ) -> int:
        row = conn.execute(
            "SELECT id FROM instagram_accounts WHERE person_id = ? AND instagram_username = ?",
            (person_id, instagram_username),
        ).fetchone()
        if row:
            account_id = int(row["id"])
            conn.execute(
                """
                UPDATE instagram_accounts
                SET profile_photo_url = ?,
                    bio = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (profile_photo_url, bio, account_id),
            )
            return account_id

        cur = conn.execute(
            """
            INSERT INTO instagram_accounts(person_id, instagram_username, profile_photo_url, bio)
            VALUES (?, ?, ?, ?)
            """,
            (person_id, instagram_username, profile_photo_url, bio),
        )
        return int(cur.lastrowid)

    def get_or_create_person_account(
        self,
        person_name: str,
        instagram_username: str,
        profile_photo_url: str | None,
        bio: str | None,
    ) -> tuple[int, int]:
        with self._connect() as conn:
            person_id = self._get_or_create_person(conn, person_name)
            account_id = self._upsert_instagram_account(
                conn,
                person_id=person_id,
                instagram_username=instagram_username,
                profile_photo_url=profile_photo_url,
                bio=bio,
            )
            conn.commit()
            return person_id, account_id

    def get_or_create_person(self, full_name: str) -> int:
        with self._connect() as conn:
            person_id = self._get_or_create_person(conn, full_name)
            conn.commit()
            return person_id

    def update_account_ingest_aggregate(
        self,
        instagram_account_id: int,
        dominant_detected_org: str | None,
        dominant_threat_level: str | None,
        last_ingested_run_id: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE instagram_accounts
                SET dominant_detected_org = ?,
                    dominant_threat_level = ?,
                    last_ingested_run_id = ?,
                    last_ingested_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (dominant_detected_org, dominant_threat_level, last_ingested_run_id, instagram_account_id),
            )
            conn.commit()

    def refresh_account_ingest_aggregate(self, instagram_account_id: int, last_ingested_run_id: str | None = None) -> dict[str, str]:
        with self._connect() as conn:
            org_map, threat_map = self._get_account_signal_maps(conn, [instagram_account_id])
        dominant_detected_org = org_map.get(instagram_account_id, "belirsiz")
        dominant_threat_level = threat_map.get(instagram_account_id, "belirsiz")
        self.update_account_ingest_aggregate(
            instagram_account_id=instagram_account_id,
            dominant_detected_org=dominant_detected_org,
            dominant_threat_level=dominant_threat_level,
            last_ingested_run_id=last_ingested_run_id,
        )
        return {
            "dominant_detected_org": dominant_detected_org,
            "dominant_threat_level": dominant_threat_level,
        }

    def upsert_person_link(
        self,
        person_id: int,
        related_username: str,
        related_person_id: int | None,
        source_account_id: int | None,
        source_post_id: str | None,
        source_comment_id: int | None,
        link_reason: str,
    ) -> None:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT id
                FROM person_links
                WHERE person_id = ?
                  AND related_username = ?
                  AND COALESCE(source_post_id, '') = COALESCE(?, '')
                  AND link_reason = ?
                """,
                (person_id, related_username, source_post_id, link_reason),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE person_links
                    SET related_person_id = ?,
                        source_account_id = ?,
                        source_comment_id = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (related_person_id, source_account_id, source_comment_id, int(existing["id"])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO person_links(
                        person_id, related_username, related_person_id, source_account_id,
                        source_post_id, source_comment_id, link_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (person_id, related_username, related_person_id, source_account_id, source_post_id, source_comment_id, link_reason),
                )
            conn.commit()

    def upsert_ingest_source(
        self,
        username: str,
        bucket: str,
        last_seen_run_id: str | None = None,
        last_enqueued_run_id: str | None = None,
    ) -> None:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT username FROM ingest_sources WHERE username = ?",
                (username,),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE ingest_sources
                    SET bucket = ?,
                        last_seen_run_id = COALESCE(?, last_seen_run_id),
                        last_enqueued_run_id = COALESCE(?, last_enqueued_run_id),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE username = ?
                    """,
                    (bucket, last_seen_run_id, last_enqueued_run_id, username),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO ingest_sources(username, bucket, last_seen_run_id, last_enqueued_run_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (username, bucket, last_seen_run_id, last_enqueued_run_id),
                )
            conn.commit()

    def enqueue_ingest_job(
        self,
        username: str,
        bucket: str,
        run_id: str,
        batch_job_id: int | None = None,
        batch_target_id: int | None = None,
        source_kind: str | None = None,
        parent_username: str | None = None,
        focus_entity: str | None = None,
        country: str | None = None,
    ) -> tuple[int | None, bool]:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM ingest_jobs WHERE target_username = ? AND run_id = ?",
                (username, run_id),
            ).fetchone()
            if existing:
                if batch_job_id is not None or batch_target_id is not None or source_kind or parent_username or focus_entity or country:
                    conn.execute(
                        """
                        UPDATE ingest_jobs
                        SET batch_job_id = COALESCE(?, batch_job_id),
                            batch_target_id = COALESCE(?, batch_target_id),
                            source_kind = COALESCE(?, source_kind),
                            parent_username = COALESCE(?, parent_username),
                            focus_entity = COALESCE(?, focus_entity),
                            country = COALESCE(?, country),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country, int(existing["id"])),
                    )
                conn.execute(
                    """
                    UPDATE ingest_sources
                    SET bucket = ?, last_seen_run_id = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE username = ?
                    """,
                    (bucket, run_id, username),
                )
                conn.commit()
                return int(existing["id"]), False

            cur = conn.execute(
                """
                INSERT INTO ingest_jobs(target_username, bucket, run_id, batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (username, bucket, run_id, batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country),
            )
            conn.execute(
                """
                INSERT INTO ingest_sources(username, bucket, last_seen_run_id, last_enqueued_run_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    bucket = excluded.bucket,
                    last_seen_run_id = excluded.last_seen_run_id,
                    last_enqueued_run_id = excluded.last_enqueued_run_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (username, bucket, run_id, run_id),
            )
            conn.commit()
            return int(cur.lastrowid), True

    def list_ingest_jobs(self, limit: int = 100) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, target_username, bucket, run_id, status, attempts,
                       batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country,
                       processed_posts, created_posts, updated_posts,
                       processed_comments, created_comments, skipped_comments,
                       flagged_users, error_message, lease_owner, lease_expires_at,
                       started_at, finished_at,
                       current_stage, current_event, current_post_index, total_posts,
                       current_post_id, current_media_index, total_media_items,
                       current_comment_index, total_comments, current_commenter_username, last_event_at,
                       created_at, updated_at
                FROM ingest_jobs
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_ingest_job_progress(self, job_id: int, **fields: object) -> None:
        allowed_fields = {
            "current_stage",
            "current_event",
            "current_post_index",
            "total_posts",
            "current_post_id",
            "current_media_index",
            "total_media_items",
            "current_comment_index",
            "total_comments",
            "current_commenter_username",
            "last_event_at",
        }
        updates = {key: value for key, value in fields.items() if key in allowed_fields}
        if not updates:
            return
        assignments = [f"{key} = ?" for key in updates]
        params = list(updates.values())
        if "current_event" in updates and "last_event_at" not in updates:
            assignments.append("last_event_at = CURRENT_TIMESTAMP")
        assignments.append("updated_at = CURRENT_TIMESTAMP")
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE ingest_jobs
                SET {", ".join(assignments)}
                WHERE id = ?
                """,
                (*params, job_id),
            )
            conn.commit()

    def record_ingest_job_event(self, payload: dict[str, object]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingest_job_events(
                    ingest_job_id, event_type, stage, message, source_post_id, commenter_username,
                    post_index, post_total, media_index, media_total, comment_index, comment_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(payload["ingest_job_id"]),
                    payload.get("event_type"),
                    payload.get("stage"),
                    payload.get("message"),
                    payload.get("source_post_id"),
                    payload.get("commenter_username"),
                    payload.get("post_index"),
                    payload.get("post_total"),
                    payload.get("media_index"),
                    payload.get("media_total"),
                    payload.get("comment_index"),
                    payload.get("comment_total"),
                ),
            )
            conn.execute(
                """
                DELETE FROM ingest_job_events
                WHERE id NOT IN (
                    SELECT id
                    FROM ingest_job_events
                    WHERE ingest_job_id = ?
                    ORDER BY id DESC
                    LIMIT 250
                )
                AND ingest_job_id = ?
                """,
                (int(payload["ingest_job_id"]), int(payload["ingest_job_id"])),
            )
            conn.commit()

    def list_ingest_job_events(self, limit: int = 200, ingest_job_ids: list[int] | None = None) -> list[dict]:
        params: list[object] = []
        query = """
            SELECT id, ingest_job_id, event_type, stage, message, source_post_id, commenter_username,
                   post_index, post_total, media_index, media_total, comment_index, comment_total, created_at
            FROM ingest_job_events
        """
        if ingest_job_ids:
            placeholders = ", ".join("?" for _ in ingest_job_ids)
            query += f" WHERE ingest_job_id IN ({placeholders})"
            params.extend(ingest_job_ids)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def create_batch_job(
        self,
        *,
        mode: str,
        bucket: str,
        requested_targets: list[str],
        normalized_targets: list[str],
        country: str | None = None,
        focus_entity: str | None = None,
        auto_enqueue_followups: bool = False,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO batch_jobs(mode, status, bucket, country, focus_entity, auto_enqueue_followups, requested_targets)
                VALUES (?, 'queued', ?, ?, ?, ?, ?)
                """,
                (mode, bucket, country, focus_entity, 1 if auto_enqueue_followups else 0, json.dumps(requested_targets, ensure_ascii=True)),
            )
            batch_job_id = int(cur.lastrowid)
            target_rows: list[dict[str, object]] = []
            for raw_target, normalized_username in zip(requested_targets, normalized_targets):
                target_cur = conn.execute(
                    """
                    INSERT INTO batch_job_targets(batch_job_id, raw_target, normalized_username, source_kind, status)
                    VALUES (?, ?, ?, 'initial', 'pending')
                    """,
                    (batch_job_id, raw_target, normalized_username),
                )
                row = conn.execute(
                    """
                    SELECT id, batch_job_id, raw_target, normalized_username, source_kind, parent_username, status, ingest_job_id, note, created_at, updated_at
                    FROM batch_job_targets WHERE id = ?
                    """,
                    (int(target_cur.lastrowid),),
                ).fetchone()
                target_rows.append(dict(row))
            conn.commit()
        return self.get_batch_job(batch_job_id) or {}, target_rows

    def create_or_get_batch_target(
        self,
        *,
        batch_job_id: int,
        raw_target: str,
        normalized_username: str,
        source_kind: str,
        parent_username: str | None = None,
        note: str | None = None,
    ) -> tuple[dict[str, object], bool]:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT id, batch_job_id, raw_target, normalized_username, source_kind, parent_username, status, ingest_job_id, note, created_at, updated_at
                FROM batch_job_targets
                WHERE batch_job_id = ? AND normalized_username = ?
                """,
                (batch_job_id, normalized_username),
            ).fetchone()
            if existing:
                return dict(existing), False
            cur = conn.execute(
                """
                INSERT INTO batch_job_targets(batch_job_id, raw_target, normalized_username, source_kind, parent_username, status, note)
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (batch_job_id, raw_target, normalized_username, source_kind, parent_username, note),
            )
            row = conn.execute(
                """
                SELECT id, batch_job_id, raw_target, normalized_username, source_kind, parent_username, status, ingest_job_id, note, created_at, updated_at
                FROM batch_job_targets WHERE id = ?
                """,
                (int(cur.lastrowid),),
            ).fetchone()
            conn.commit()
        return dict(row), True

    def attach_ingest_job_to_batch_target(self, batch_target_id: int, ingest_job_id: int, status: str = "enqueued") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE batch_job_targets
                SET ingest_job_id = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (ingest_job_id, status, batch_target_id),
            )
            conn.commit()

    def update_batch_target_status(self, batch_target_id: int, status: str, note: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE batch_job_targets
                SET status = ?, note = COALESCE(?, note), updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (status, note, batch_target_id),
            )
            conn.commit()

    def get_batch_target(self, batch_target_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, batch_job_id, raw_target, normalized_username, source_kind, parent_username, status, ingest_job_id, note, created_at, updated_at
                FROM batch_job_targets
                WHERE id = ?
                """,
                (batch_target_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_batch_job_targets(self, batch_job_id: int | None = None, limit: int = 200) -> list[dict]:
        query = """
            SELECT id, batch_job_id, raw_target, normalized_username, source_kind, parent_username, status, ingest_job_id, note, created_at, updated_at
            FROM batch_job_targets
        """
        params: list[object] = []
        if batch_job_id is not None:
            query += " WHERE batch_job_id = ?"
            params.append(batch_job_id)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_batch_job(self, batch_job_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, mode, status, bucket, country, focus_entity, auto_enqueue_followups, requested_targets, created_at, updated_at
                FROM batch_jobs WHERE id = ?
                """,
                (batch_job_id,),
            ).fetchone()
            if not row:
                return None
            counts = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_targets,
                    SUM(CASE WHEN source_kind = 'initial' THEN 1 ELSE 0 END) AS initial_targets,
                    SUM(CASE WHEN source_kind = 'followup' THEN 1 ELSE 0 END) AS discovered_followups,
                    SUM(CASE WHEN status IN ('completed', 'skipped', 'suggested') THEN 1 ELSE 0 END) AS completed_targets,
                    SUM(CASE WHEN status IN ('failed', 'missing_archive') THEN 1 ELSE 0 END) AS failed_targets,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_targets,
                    SUM(CASE WHEN status IN ('pending', 'enqueued') THEN 1 ELSE 0 END) AS pending_targets
                FROM batch_job_targets
                WHERE batch_job_id = ?
                """,
                (batch_job_id,),
            ).fetchone()
        item = dict(row)
        try:
            item["requested_targets"] = json.loads(str(item.get("requested_targets") or "[]"))
        except json.JSONDecodeError:
            item["requested_targets"] = []
        item["auto_enqueue_followups"] = bool(item.get("auto_enqueue_followups"))
        item.update({key: int(counts[key] or 0) for key in counts.keys()})
        return item

    def list_batch_jobs(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM batch_jobs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items: list[dict] = []
        for row in rows:
            item = self.get_batch_job(int(row["id"]))
            if item:
                items.append(item)
        return items

    def update_batch_job_status(self, batch_job_id: int, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE batch_jobs
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (status, batch_job_id),
            )
            conn.commit()

    def refresh_batch_job_status(self, batch_job_id: int) -> dict[str, object] | None:
        item = self.get_batch_job(batch_job_id)
        if not item:
            return None
        status = "queued"
        if item["running_targets"] > 0:
            status = "running"
        elif item["failed_targets"] > 0 and item["completed_targets"] > 0:
            status = "partial"
        elif item["failed_targets"] > 0 and item["completed_targets"] == 0 and item["pending_targets"] == 0:
            status = "failed"
        elif item["pending_targets"] == 0 and item["running_targets"] == 0 and item["total_targets"] > 0:
            status = "completed" if item["failed_targets"] == 0 else "partial"
        self.update_batch_job_status(batch_job_id, status)
        return self.get_batch_job(batch_job_id)

    def get_ingest_job(self, job_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, target_username, bucket, run_id, status, attempts,
                       batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country,
                       processed_posts, created_posts, updated_posts,
                       processed_comments, created_comments, skipped_comments,
                       flagged_users, error_message, lease_owner, lease_expires_at,
                       started_at, finished_at,
                       current_stage, current_event, current_post_index, total_posts,
                       current_post_id, current_media_index, total_media_items,
                       current_comment_index, total_comments, current_commenter_username, last_event_at,
                       created_at, updated_at
                FROM ingest_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_review_queue_top(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT commenter_username, trigger_count, first_triggered_at, last_triggered_at,
                       last_reason, flag_reason_type, status
                FROM review_queue
                ORDER BY trigger_count DESC, last_triggered_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_pending_ingest_jobs(self, lease_owner: str, lease_seconds: int, limit: int) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, target_username, bucket, run_id, status, attempts,
                       batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country,
                       processed_posts, created_posts, updated_posts,
                       processed_comments, created_comments, skipped_comments,
                       flagged_users, error_message, lease_owner, lease_expires_at,
                       started_at, finished_at,
                       current_stage, current_event, current_post_index, total_posts,
                       current_post_id, current_media_index, total_media_items,
                       current_comment_index, total_comments, current_commenter_username, last_event_at,
                       created_at, updated_at
                FROM ingest_jobs
                WHERE status IN ('pending', 'retry_wait')
                  AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP)
                  AND NOT EXISTS (
                    SELECT 1
                    FROM ingest_jobs j2
                    WHERE j2.target_username = ingest_jobs.target_username
                      AND j2.status = 'running'
                      AND (j2.lease_expires_at IS NULL OR j2.lease_expires_at > CURRENT_TIMESTAMP)
                  )
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            claimed: list[dict] = []
            for row in rows:
                conn.execute(
                    """
                    UPDATE ingest_jobs
                    SET status = 'running',
                        attempts = attempts + 1,
                        lease_owner = ?,
                        lease_expires_at = datetime('now', '+' || ? || ' seconds'),
                        started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                        current_stage = 'queued',
                        current_event = 'Worker tarafindan claim edildi.',
                        last_event_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP,
                        error_message = NULL
                    WHERE id = ?
                    """,
                    (lease_owner, lease_seconds, int(row["id"])),
                )
                if row["batch_target_id"] is not None:
                    conn.execute(
                        """
                        UPDATE batch_job_targets
                        SET status = 'running',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (int(row["batch_target_id"]),),
                    )
                refreshed = conn.execute(
                    """
                    SELECT id, target_username, bucket, run_id, status, attempts,
                           batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country,
                           processed_posts, created_posts, updated_posts,
                           processed_comments, created_comments, skipped_comments,
                           flagged_users, error_message, lease_owner, lease_expires_at,
                           started_at, finished_at,
                           current_stage, current_event, current_post_index, total_posts,
                           current_post_id, current_media_index, total_media_items,
                           current_comment_index, total_comments, current_commenter_username, last_event_at,
                           created_at, updated_at
                    FROM ingest_jobs
                    WHERE id = ?
                    """,
                    (int(row["id"]),),
                ).fetchone()
                claimed.append(dict(refreshed))
            conn.commit()
        return claimed

    def heartbeat_ingest_job(self, job_id: int, lease_owner: str, lease_seconds: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingest_jobs
                SET lease_expires_at = datetime('now', '+' || ? || ' seconds'),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND lease_owner = ?
                """,
                (lease_seconds, job_id, lease_owner),
            )
            conn.commit()

    def update_ingest_job_post(self, ingest_job_id: int, source_post_id: str, status: str, error_message: str | None = None) -> None:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT id FROM ingest_job_posts
                WHERE ingest_job_id = ? AND source_post_id = ?
                """,
                (ingest_job_id, source_post_id),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE ingest_job_posts
                    SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (status, error_message, int(existing["id"])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO ingest_job_posts(ingest_job_id, source_post_id, status, error_message)
                    VALUES (?, ?, ?, ?)
                    """,
                    (ingest_job_id, source_post_id, status, error_message),
                )
            conn.commit()

    def complete_ingest_job(self, job_id: int, status: str, counters: dict[str, int], error_message: str | None = None) -> dict:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingest_jobs
                SET status = ?,
                    processed_posts = ?,
                    created_posts = ?,
                    updated_posts = ?,
                    processed_comments = ?,
                    created_comments = ?,
                    skipped_comments = ?,
                    flagged_users = ?,
                    error_message = ?,
                    finished_at = CURRENT_TIMESTAMP,
                    current_stage = NULL,
                    current_media_index = NULL,
                    total_media_items = NULL,
                    current_comment_index = NULL,
                    total_comments = NULL,
                    current_commenter_username = NULL,
                    current_event = CASE WHEN ? IS NULL THEN 'Islem tamamlandi.' ELSE ? END,
                    last_event_at = CURRENT_TIMESTAMP,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    status,
                    int(counters.get("processed_posts", 0)),
                    int(counters.get("created_posts", 0)),
                    int(counters.get("updated_posts", 0)),
                    int(counters.get("processed_comments", 0)),
                    int(counters.get("created_comments", 0)),
                    int(counters.get("skipped_comments", 0)),
                    int(counters.get("flagged_users", 0)),
                    error_message,
                    error_message,
                    error_message,
                    job_id,
                ),
            )
            row = conn.execute(
                """
                SELECT id, target_username, bucket, run_id, status, attempts,
                       batch_job_id, batch_target_id, source_kind, parent_username, focus_entity, country,
                       processed_posts, created_posts, updated_posts,
                       processed_comments, created_comments, skipped_comments,
                       flagged_users, error_message, lease_owner, lease_expires_at,
                       started_at, finished_at,
                       current_stage, current_event, current_post_index, total_posts,
                       current_post_id, current_media_index, total_media_items,
                       current_comment_index, total_comments, current_commenter_username, last_event_at,
                       created_at, updated_at
                FROM ingest_jobs WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
            conn.commit()
        return dict(row) if row else {}

    def get_account_profile_summary(self, instagram_account_id: int) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT account_profile_summary FROM instagram_accounts WHERE id = ?",
                (instagram_account_id,),
            ).fetchone()
        if not row:
            return ""
        return str(row["account_profile_summary"] or "").strip()

    def update_account_profile_summary(self, instagram_account_id: int, summary: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE instagram_accounts
                SET account_profile_summary = ?,
                    account_profile_summary_updated_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (summary, instagram_account_id),
            )
            conn.commit()

    def get_account_graph_analysis(self, instagram_account_id: int) -> dict[str, str]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT graph_ai_analysis, graph_ai_analysis_model, graph_ai_analysis_updated_at
                FROM instagram_accounts
                WHERE id = ?
                """,
                (instagram_account_id,),
            ).fetchone()
        if not row:
            return {"analysis": "", "model": "", "updated_at": ""}
        return {
            "analysis": str(row["graph_ai_analysis"] or "").strip(),
            "model": str(row["graph_ai_analysis_model"] or "").strip(),
            "updated_at": str(row["graph_ai_analysis_updated_at"] or "").strip(),
        }

    def update_account_graph_analysis(self, instagram_account_id: int, analysis: str | None, model: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE instagram_accounts
                SET graph_ai_analysis = ?,
                    graph_ai_analysis_model = ?,
                    graph_ai_analysis_updated_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (analysis, model, instagram_account_id),
            )
            conn.commit()

    def get_account_graph_capture(self, instagram_account_id: int) -> dict[str, str]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT graph_capture_path, graph_capture_updated_at
                FROM instagram_accounts
                WHERE id = ?
                """,
                (instagram_account_id,),
            ).fetchone()
        if not row:
            return {"path": "", "updated_at": ""}
        return {
            "path": str(row["graph_capture_path"] or "").strip(),
            "updated_at": str(row["graph_capture_updated_at"] or "").strip(),
        }

    def update_account_graph_capture(self, instagram_account_id: int, path: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE instagram_accounts
                SET graph_capture_path = ?,
                    graph_capture_updated_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (path, instagram_account_id),
            )
            conn.commit()

    def list_prompt_templates(self) -> list[dict]:
        defaults = {item["key"]: item for item in get_default_prompt_templates()}
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT key, display_name, description, content, is_enabled, version, updated_at
                FROM prompt_templates
                ORDER BY key
                """
            ).fetchall()
        items: list[dict] = []
        for row in rows:
            default = defaults.get(str(row["key"])) or {}
            items.append(
                {
                    "key": row["key"],
                    "display_name": row["display_name"],
                    "description": row["description"],
                    "content": row["content"],
                    "default_content": default.get("content", ""),
                    "is_enabled": bool(row["is_enabled"]),
                    "version": row["version"],
                    "updated_at": row["updated_at"],
                    "is_overridden": row["content"] != default.get("content", ""),
                }
            )
        return items

    def get_prompt_template(self, key: str) -> dict | None:
        default = get_default_prompt_template(key)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT key, display_name, description, content, is_enabled, version, updated_at
                FROM prompt_templates
                WHERE key = ?
                """,
                (key,),
            ).fetchone()
        if not row:
            return None
        return {
            "key": row["key"],
            "display_name": row["display_name"],
            "description": row["description"],
            "content": row["content"],
            "default_content": default["content"] if default else "",
            "is_enabled": bool(row["is_enabled"]),
            "version": row["version"],
            "updated_at": row["updated_at"],
            "is_overridden": bool(default and row["content"] != default["content"]),
        }

    def get_prompt_content(self, key: str) -> str | None:
        item = self.get_prompt_template(key)
        if not item or not item["is_enabled"]:
            return None
        return str(item["content"])

    def update_prompt_template(self, key: str, content: str, is_enabled: bool = True) -> dict | None:
        with self._connect() as conn:
            existing = conn.execute("SELECT version FROM prompt_templates WHERE key = ?", (key,)).fetchone()
            if not existing:
                default = get_default_prompt_template(key)
                if not default:
                    return None
                conn.execute(
                    """
                    INSERT INTO prompt_templates(key, display_name, description, content, is_enabled, version)
                    VALUES (?, ?, ?, ?, ?, 1)
                    """,
                    (key, default["display_name"], default["description"], content, 1 if is_enabled else 0),
                )
            else:
                conn.execute(
                    """
                    UPDATE prompt_templates
                    SET content = ?,
                        is_enabled = ?,
                        version = version + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE key = ?
                    """,
                    (content, 1 if is_enabled else 0, key),
                )
            conn.commit()
        return self.get_prompt_template(key)

    def reset_prompt_template(self, key: str) -> dict | None:
        default = get_default_prompt_template(key)
        if not default:
            return None
        return self.update_prompt_template(key, str(default["content"]), True)

    def list_enabled_org_groups(self) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT og.id, og.group_type, og.canonical_name, og.is_enabled, og.notes, oga.alias
                FROM org_groups og
                LEFT JOIN org_group_aliases oga ON oga.org_group_id = og.id
                WHERE og.is_enabled = 1
                ORDER BY og.group_type, og.canonical_name, oga.alias
                """
            ).fetchall()
        grouped: dict[int, dict[str, object]] = {}
        for row in rows:
            row_id = int(row["id"])
            item = grouped.setdefault(
                row_id,
                {
                    "id": row_id,
                    "group_type": row["group_type"],
                    "canonical_name": row["canonical_name"],
                    "is_enabled": bool(row["is_enabled"]),
                    "notes": row["notes"],
                    "aliases": [],
                },
            )
            alias = str(row["alias"] or "").strip()
            if alias:
                item["aliases"].append(alias)
        return list(grouped.values())

    def record_llm_stage_attempt(self, payload: dict[str, object]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO llm_stage_attempts(
                    stage_name, prompt_key, prompt_version, rendered_prompt, model,
                    raw_output, validation_status, validation_error, repair_attempted,
                    related_account_id, related_post_id, related_comment_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("stage_name"),
                    payload.get("prompt_key"),
                    int(payload.get("prompt_version") or 1),
                    payload.get("rendered_prompt"),
                    payload.get("model"),
                    payload.get("raw_output"),
                    payload.get("validation_status"),
                    payload.get("validation_error"),
                    1 if payload.get("repair_attempted") else 0,
                    payload.get("related_account_id"),
                    payload.get("related_post_id"),
                    payload.get("related_comment_id"),
                ),
            )
            conn.commit()

    def save_media_observations(self, instagram_post_id: int, observations: list[dict[str, object]]) -> None:
        with self._connect() as conn:
            for observation in observations:
                deep_payload_raw = observation.get("deep_payload")
                deep_payload_text = (
                    json.dumps(deep_payload_raw, ensure_ascii=True)
                    if isinstance(deep_payload_raw, dict)
                    else None
                )
                conn.execute(
                    """
                    INSERT INTO media_observations(
                        instagram_post_id, media_index, media_type, payload,
                        deep_required, deep_status, deep_reason, location_confidence,
                        contains_vehicle, contains_plate, deep_payload, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(instagram_post_id, media_index) DO UPDATE SET
                        media_type = excluded.media_type,
                        payload = excluded.payload,
                        deep_required = excluded.deep_required,
                        deep_status = excluded.deep_status,
                        deep_reason = excluded.deep_reason,
                        location_confidence = excluded.location_confidence,
                        contains_vehicle = excluded.contains_vehicle,
                        contains_plate = excluded.contains_plate,
                        deep_payload = excluded.deep_payload,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        instagram_post_id,
                        int(observation.get("media_index") or observation.get("medya_no") or 0),
                        str(observation.get("media_type") or observation.get("medya_turu") or "image"),
                        json.dumps(observation, ensure_ascii=True),
                        1 if bool(observation.get("deep_required")) else 0,
                        str(observation.get("deep_status") or "not_required"),
                        str(observation.get("deep_reason") or ""),
                        str(observation.get("location_confidence") or "unclear"),
                        1 if bool(observation.get("contains_vehicle")) else 0,
                        1 if bool(observation.get("contains_plate")) else 0,
                        deep_payload_text,
                    ),
                )
            conn.commit()

    def save_canonical_post_analysis(self, instagram_post_id: int, payload: dict[str, object], corrected_payload: dict[str, object] | None = None) -> None:
        corrected = corrected_payload or payload
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO canonical_post_analyses(instagram_post_id, payload, corrected_payload, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(instagram_post_id) DO UPDATE SET
                    payload = excluded.payload,
                    corrected_payload = excluded.corrected_payload,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    instagram_post_id,
                    json.dumps(payload, ensure_ascii=True),
                    json.dumps(corrected, ensure_ascii=True),
                ),
            )
            conn.commit()

    def save_canonical_comment_analysis(
        self,
        instagram_comment_id: int,
        payload: dict[str, object],
        corrected_payload: dict[str, object] | None = None,
    ) -> None:
        corrected = corrected_payload or payload
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO canonical_comment_analyses(instagram_comment_id, payload, corrected_payload, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(instagram_comment_id) DO UPDATE SET
                    payload = excluded.payload,
                    corrected_payload = excluded.corrected_payload,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    instagram_comment_id,
                    json.dumps(payload, ensure_ascii=True),
                    json.dumps(corrected, ensure_ascii=True),
                ),
            )
            conn.commit()

    def list_canonical_post_analyses_for_account(self, instagram_account_id: int) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.corrected_payload
                FROM canonical_post_analyses c
                JOIN instagram_posts p ON p.id = c.instagram_post_id
                WHERE p.instagram_account_id = ?
                ORDER BY p.id
                """,
                (instagram_account_id,),
            ).fetchall()
        items: list[dict[str, object]] = []
        for row in rows:
            try:
                parsed = json.loads(str(row["corrected_payload"]))
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                items.append(parsed)
        return items

    def save_account_aggregate(self, instagram_account_id: int, payload: dict[str, object]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO account_aggregates(instagram_account_id, payload, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(instagram_account_id) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (instagram_account_id, json.dumps(payload, ensure_ascii=True)),
            )
            conn.commit()

    def get_account_aggregate(self, instagram_account_id: int) -> dict[str, object]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM account_aggregates WHERE instagram_account_id = ?",
                (instagram_account_id,),
            ).fetchone()
        if not row:
            return {}
        try:
            parsed = json.loads(str(row["payload"]))
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _parse_structured_analysis(self, raw: str | None) -> dict:
        if not raw:
            return {}
        try:
            parsed = json.loads(str(raw))
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _parse_json_list(self, raw: str | None) -> list[object]:
        if not raw:
            return []
        try:
            parsed = json.loads(str(raw))
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []

    def _format_org_summary(self, counter: dict[str, int], limit: int = 3) -> str:
        meaningful = [(name, count) for name, count in counter.items() if name and name != "belirsiz"]
        if meaningful:
            meaningful.sort(key=lambda item: (-item[1], item[0]))
            return ", ".join(name for name, _ in meaningful[:limit])
        return "belirsiz"

    def _infer_orgs_from_summary(self, summary: str | None) -> str:
        text = str(summary or "").upper()
        if not text:
            return "belirsiz"

        alias_map = {
            "PKK/KCK": ["PKK/KCK", "PKK", "KCK"],
            "DHKP-C": ["DHKP-C", "DHKPC"],
            "FETÖ": ["FETÖ", "FETO"],
            "DEAŞ/IŞİD": ["DEAŞ", "DEAS", "IŞİD", "ISID", "ISIS"],
        }
        detected: list[str] = []
        for canonical, aliases in alias_map.items():
            if any(alias in text for alias in aliases):
                detected.append(canonical)

        return ", ".join(detected) if detected else "belirsiz"

    def _resolve_detected_org(self, org_summary: str | None, account_profile_summary: str | None) -> str:
        normalized = str(org_summary or "").strip() or "belirsiz"
        if normalized != "belirsiz":
            return normalized
        return self._infer_orgs_from_summary(account_profile_summary)

    def _format_threat_summary(self, counter: dict[str, int]) -> str:
        if not counter:
            return "belirsiz"
        priority = {"kritik": 4, "yuksek": 3, "orta": 2, "dusuk": 1, "yok": 0, "belirsiz": -1}
        return sorted(counter.items(), key=lambda item: (-priority.get(item[0], -1), -item[1], item[0]))[0][0]

    def _get_account_signal_maps(
        self, conn: sqlite3.Connection, account_ids: list[int]
    ) -> tuple[dict[int, str], dict[int, str]]:
        if not account_ids:
            return {}, {}

        placeholders = ",".join("?" for _ in account_ids)
        rows = conn.execute(
            f"""
            SELECT instagram_account_id,
                   COALESCE(tespit_edilen_orgut, 'belirsiz') AS orgut,
                   COALESCE(tehdit_seviyesi, 'belirsiz') AS threat
            FROM instagram_posts
            WHERE instagram_account_id IN ({placeholders})
            """,
            account_ids,
        ).fetchall()

        org_counters: dict[int, dict[str, int]] = {account_id: {} for account_id in account_ids}
        threat_counters: dict[int, dict[str, int]] = {account_id: {} for account_id in account_ids}
        for row in rows:
            account_id = int(row["instagram_account_id"])
            orgut = str(row["orgut"] or "belirsiz").strip() or "belirsiz"
            threat = str(row["threat"] or "belirsiz").strip() or "belirsiz"
            org_counters[account_id][orgut] = org_counters[account_id].get(orgut, 0) + 1
            threat_counters[account_id][threat] = threat_counters[account_id].get(threat, 0) + 1

        org_map = {account_id: self._format_org_summary(counter) for account_id, counter in org_counters.items()}
        threat_map = {account_id: self._format_threat_summary(counter) for account_id, counter in threat_counters.items()}
        return org_map, threat_map

    def get_dashboard_summary(self) -> dict:
        with self._connect() as conn:
            account_count = conn.execute("SELECT COUNT(*) FROM instagram_accounts").fetchone()[0]
            post_count = conn.execute("SELECT COUNT(*) FROM instagram_posts").fetchone()[0]
            comment_count = conn.execute("SELECT COUNT(*) FROM instagram_comments").fetchone()[0]
            flagged_commenters = conn.execute("SELECT COUNT(*) FROM review_queue WHERE status = 'open'").fetchone()[0]
            dominant_org = conn.execute(
                """
                SELECT COALESCE(tespit_edilen_orgut, 'belirsiz') AS orgut, COUNT(*) AS total
                FROM instagram_posts
                GROUP BY COALESCE(tespit_edilen_orgut, 'belirsiz')
                ORDER BY total DESC, orgut
                LIMIT 1
                """
            ).fetchone()
            top_accounts = conn.execute(
                """
                SELECT ia.id,
                       ia.instagram_username,
                       ia.account_profile_summary,
                       MAX(COALESCE(ip.onem_skoru, 0)) AS max_onem_skoru,
                       MAX(COALESCE(ip.tehdit_seviyesi, 'yok')) AS tehdit_seviyesi,
                       COUNT(DISTINCT ip.id) AS post_count,
                       SUM(CASE WHEN ic.bayrak = 1 THEN 1 ELSE 0 END) AS flagged_comment_count
                FROM instagram_accounts ia
                LEFT JOIN instagram_posts ip ON ip.instagram_account_id = ia.id
                LEFT JOIN instagram_comments ic ON ic.instagram_post_id = ip.id
                GROUP BY ia.id
                ORDER BY max_onem_skoru DESC, flagged_comment_count DESC, post_count DESC
                LIMIT 6
                """
            ).fetchall()
            top_account_ids = [int(row["id"]) for row in top_accounts]
            top_account_orgs, top_account_threats = self._get_account_signal_maps(conn, top_account_ids)
            latest_comments = conn.execute(
                """
                SELECT ic.commenter_username, ic.comment_text, ic.verdict, ic.orgut_baglanti_skoru, ic.bayrak,
                       ia.instagram_username
                FROM instagram_comments ic
                JOIN instagram_posts ip ON ip.id = ic.instagram_post_id
                JOIN instagram_accounts ia ON ia.id = ip.instagram_account_id
                ORDER BY ic.id DESC
                LIMIT 8
                """
            ).fetchall()
            category_rows = conn.execute(
                """
                SELECT icerik_kategorisi
                FROM instagram_posts
                WHERE COALESCE(icerik_kategorisi, '') != ''
                """
            ).fetchall()
            threat_rows = conn.execute(
                """
                SELECT COALESCE(tehdit_seviyesi, 'belirsiz') AS threat, COUNT(*) AS total
                FROM instagram_posts
                GROUP BY COALESCE(tehdit_seviyesi, 'belirsiz')
                ORDER BY total DESC
                """
            ).fetchall()

        category_counter: dict[str, int] = {}
        for row in category_rows:
            for item in str(row["icerik_kategorisi"]).split(","):
                cleaned = item.strip()
                if cleaned:
                    category_counter[cleaned] = category_counter.get(cleaned, 0) + 1

        return {
            "kpis": {
                "incelenen_hesap": int(account_count),
                "incelenen_post": int(post_count),
                "incelenen_yorum": int(comment_count),
                "acik_review_queue": int(flagged_commenters),
                "baskin_orgut": dominant_org["orgut"] if dominant_org else "belirsiz",
            },
            "riskli_hesaplar": [
                {
                    "id": row["id"],
                    "instagram_username": row["instagram_username"],
                    "profil_ozeti": row["account_profile_summary"] or "",
                    "max_onem_skoru": row["max_onem_skoru"] or 0,
                    "tehdit_seviyesi": top_account_threats.get(int(row["id"]), row["tehdit_seviyesi"] or "belirsiz"),
                    "tespit_edilen_orgut": top_account_orgs.get(int(row["id"]), "belirsiz"),
                    "post_count": row["post_count"] or 0,
                    "flagged_comment_count": row["flagged_comment_count"] or 0,
                }
                for row in top_accounts
            ],
            "son_bayrakli_yorumlar": [
                {
                    "instagram_username": row["instagram_username"],
                    "commenter_username": row["commenter_username"],
                    "comment_text": row["comment_text"],
                    "verdict": row["verdict"],
                    "orgut_baglanti_skoru": row["orgut_baglanti_skoru"],
                    "bayrak": bool(row["bayrak"]),
                }
                for row in latest_comments
            ],
            "kategori_dagilimi": [{"name": key, "value": value} for key, value in sorted(category_counter.items(), key=lambda x: x[1], reverse=True)[:8]],
            "tehdit_dagilimi": [{"name": row["threat"], "value": row["total"]} for row in threat_rows],
        }

    def list_accounts(
        self,
        search: str | None = None,
        orgut: str | None = None,
        threat: str | None = None,
        flagged_only: bool = False,
    ) -> list[dict]:
        filters: list[str] = []
        params: list[object] = []
        if search:
            filters.append("(ia.instagram_username LIKE ? OR ia.bio LIKE ? OR ia.account_profile_summary LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like, like])
        if orgut:
            filters.append("EXISTS (SELECT 1 FROM instagram_posts ip2 WHERE ip2.instagram_account_id = ia.id AND COALESCE(ip2.tespit_edilen_orgut, 'belirsiz') = ?)")
            params.append(orgut)
        if threat:
            filters.append("EXISTS (SELECT 1 FROM instagram_posts ip2 WHERE ip2.instagram_account_id = ia.id AND COALESCE(ip2.tehdit_seviyesi, 'belirsiz') = ?)")
            params.append(threat)
        if flagged_only:
            filters.append("COALESCE(ic.bayrak, 0) = 1")

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f"""
            SELECT ia.id,
                   ia.instagram_username,
                   ia.profile_photo_url,
                   ia.bio,
                   ia.account_profile_summary,
                   ia.dominant_detected_org,
                   ia.dominant_threat_level,
                   MAX(COALESCE(ip.tehdit_seviyesi, 'yok')) AS tehdit_seviyesi,
                   MAX(COALESCE(ip.onem_skoru, 0)) AS max_onem_skoru,
                   MAX(COALESCE(ip.tespit_edilen_orgut, 'belirsiz')) AS tespit_edilen_orgut,
                   COUNT(DISTINCT ip.id) AS post_count,
                   COUNT(DISTINCT ic.id) AS comment_count,
                   SUM(CASE WHEN ic.bayrak = 1 THEN 1 ELSE 0 END) AS flagged_comment_count
            FROM instagram_accounts ia
            LEFT JOIN instagram_posts ip ON ip.instagram_account_id = ia.id
            LEFT JOIN instagram_comments ic ON ic.instagram_post_id = ip.id
            {where_clause}
            GROUP BY ia.id
            ORDER BY max_onem_skoru DESC, flagged_comment_count DESC, post_count DESC, ia.instagram_username
        """
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            account_ids = [int(row["id"]) for row in rows]
            org_map, threat_map = self._get_account_signal_maps(conn, account_ids)
        items = [dict(row) for row in rows]
        for item in items:
            account_id = int(item["id"])
            item["tespit_edilen_orgut"] = self._resolve_detected_org(
                item.get("dominant_detected_org") or org_map.get(account_id, "belirsiz"),
                item.get("account_profile_summary"),
            )
            item["tehdit_seviyesi"] = item.get("dominant_threat_level") or threat_map.get(account_id, item.get("tehdit_seviyesi") or "belirsiz")
        return items

    def get_account_detail(self, account_id: int) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT ia.id,
                       ia.instagram_username,
                       ia.profile_photo_url,
                       ia.bio,
                       ia.account_profile_summary,
                       ia.dominant_detected_org,
                       ia.dominant_threat_level,
                       ia.graph_ai_analysis,
                       ia.graph_ai_analysis_model,
                       ia.graph_ai_analysis_updated_at,
                       ia.graph_capture_path,
                       ia.graph_capture_updated_at,
                       MAX(COALESCE(ip.tehdit_seviyesi, 'yok')) AS tehdit_seviyesi,
                       MAX(COALESCE(ip.tespit_edilen_orgut, 'belirsiz')) AS tespit_edilen_orgut,
                       COUNT(DISTINCT ip.id) AS post_count,
                       COUNT(DISTINCT ic.id) AS comment_count,
                       SUM(CASE WHEN ic.bayrak = 1 THEN 1 ELSE 0 END) AS flagged_comment_count,
                       AVG(COALESCE(ip.onem_skoru, 0)) AS ortalama_onem_skoru
                FROM instagram_accounts ia
                LEFT JOIN instagram_posts ip ON ip.instagram_account_id = ia.id
                LEFT JOIN instagram_comments ic ON ic.instagram_post_id = ip.id
                WHERE ia.id = ?
                GROUP BY ia.id
                """,
                (account_id,),
            ).fetchone()
            if not row:
                return None
            org_map, threat_map = self._get_account_signal_maps(conn, [account_id])
            dominant_category_rows = conn.execute(
                """
                SELECT icerik_kategorisi
                FROM instagram_posts
                WHERE instagram_account_id = ?
                """,
                (account_id,),
            ).fetchall()
        category_counter: dict[str, int] = {}
        for item in dominant_category_rows:
            for raw in str(item["icerik_kategorisi"] or "").split(","):
                cleaned = raw.strip()
                if cleaned:
                    category_counter[cleaned] = category_counter.get(cleaned, 0) + 1
        dominant_category = sorted(category_counter.items(), key=lambda x: x[1], reverse=True)[0][0] if category_counter else "belirsiz"
        detail = dict(row)
        detail["ortalama_onem_skoru"] = round(float(detail["ortalama_onem_skoru"] or 0), 1)
        detail["baskin_kategori"] = dominant_category
        detail["tespit_edilen_orgut"] = self._resolve_detected_org(
            detail.get("dominant_detected_org") or org_map.get(account_id, "belirsiz"),
            detail.get("account_profile_summary"),
        )
        detail["tehdit_seviyesi"] = detail.get("dominant_threat_level") or threat_map.get(account_id, detail.get("tehdit_seviyesi") or "belirsiz")
        return detail

    def list_account_posts(self, account_id: int) -> list[dict]:
        with self._connect() as conn:
            has_media_items = self._has_column(conn, "instagram_posts", "media_items")
            media_items_select = "media_items," if has_media_items else "NULL AS media_items,"
            rows = conn.execute(
                f"""
                SELECT id, source_kind, source_container_id, source_container_title, source_created_at,
                       media_type, media_url, media_items, caption, post_analysis, post_ozet, structured_analysis,
                       icerik_kategorisi, tehdit_seviyesi, onem_skoru, tespit_edilen_orgut,
                       source_post_id, source_post_url, created_at
                FROM instagram_posts
                WHERE instagram_account_id = ?
                ORDER BY COALESCE(source_created_at, created_at), id
                """.replace("media_items,", media_items_select),
                (account_id,),
            ).fetchall()
            observation_rows = conn.execute(
                """
                SELECT mo.instagram_post_id,
                       mo.media_index,
                       mo.media_type,
                       mo.payload,
                       mo.deep_required,
                       mo.deep_status,
                       mo.deep_reason,
                       mo.location_confidence,
                       mo.contains_vehicle,
                       mo.contains_plate,
                       mo.deep_payload
                FROM media_observations mo
                JOIN instagram_posts ip ON ip.id = mo.instagram_post_id
                WHERE ip.instagram_account_id = ?
                ORDER BY mo.instagram_post_id, mo.media_index
                """,
                (account_id,),
            ).fetchall()
        observations_by_post: dict[int, list[dict[str, object]]] = {}
        for row in observation_rows:
            observation = self._parse_structured_analysis(row["payload"])
            deep_payload = self._parse_structured_analysis(row["deep_payload"])
            observation["deep_required"] = bool(row["deep_required"])
            observation["deep_status"] = str(row["deep_status"] or "not_required")
            observation["deep_reason"] = str(row["deep_reason"] or "")
            observation["location_confidence"] = str(row["location_confidence"] or "unclear")
            observation["contains_vehicle"] = bool(row["contains_vehicle"])
            observation["contains_plate"] = bool(row["contains_plate"])
            observation["deep_payload"] = deep_payload if deep_payload else {}
            observations_by_post.setdefault(int(row["instagram_post_id"]), []).append(observation)
        items: list[dict] = []
        for row in rows:
            item = dict(row)
            item["structured_analysis"] = self._parse_structured_analysis(item.get("structured_analysis"))
            item["media_items"] = self._normalize_media_items(self._parse_json_list(item.get("media_items")))
            if not item["media_items"] and item.get("media_url"):
                item["media_items"] = [
                    {
                        "media_type": str(item.get("media_type") or "image"),
                        "media_url": str(item["media_url"]),
                    }
                ]
            item["icerik_kategorisi"] = [x.strip() for x in str(item.get("icerik_kategorisi") or "").split(",") if x.strip()]
            item["media_observations"] = observations_by_post.get(int(item["id"]), [])
            items.append(item)
        return items

    def list_account_comments(self, account_id: int, verdict: str | None = None, flagged_only: bool = False) -> list[dict]:
        filters = ["ip.instagram_account_id = ?"]
        params: list[object] = [account_id]
        if verdict:
            filters.append("ic.verdict = ?")
            params.append(verdict)
        if flagged_only:
            filters.append("ic.bayrak = 1")
        where_clause = " AND ".join(filters)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ic.id, ic.commenter_username, ic.commenter_profile_url, ic.comment_text, ic.verdict,
                       ic.sentiment, ic.orgut_baglanti_skoru, ic.bayrak, ic.reason, ic.created_at,
                       ip.source_post_id, ip.post_ozet
                FROM instagram_comments ic
                JOIN instagram_posts ip ON ip.id = ic.instagram_post_id
                WHERE {where_clause}
                ORDER BY ic.id DESC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def get_account_graph(self, account_id: int) -> dict:
        detail = self.get_account_detail(account_id)
        if not detail:
            return {"nodes": [], "edges": []}

        posts = self.list_account_posts(account_id)
        comments = self.list_account_comments(account_id)
        nodes: list[dict] = [
            {
                "id": f"account-{account_id}",
                "type": "account",
                "label": detail["instagram_username"],
                "weight": detail["post_count"] or 1,
                "avatar_url": detail.get("profile_photo_url"),
            }
        ]
        edges: list[dict] = []
        seen_nodes = {f"account-{account_id}"}

        for post in posts:
            org = post.get("tespit_edilen_orgut") or "belirsiz"
            org_node = f"org-{org}"
            if org_node not in seen_nodes:
                nodes.append({"id": org_node, "type": "organization", "label": org, "weight": 2})
                seen_nodes.add(org_node)
            edges.append({"id": f"edge-account-org-{post['id']}", "source": f"account-{account_id}", "target": org_node, "type": "linked_to"})

            threat = post.get("tehdit_seviyesi") or "belirsiz"
            threat_node = f"threat-{threat}"
            if threat_node not in seen_nodes:
                nodes.append({"id": threat_node, "type": "threat", "label": threat, "weight": 1})
                seen_nodes.add(threat_node)
            edges.append({"id": f"edge-post-threat-{post['id']}", "source": f"account-{account_id}", "target": threat_node, "type": "posted_about"})

            for category in post.get("icerik_kategorisi", []):
                category_node = f"category-{category}"
                if category_node not in seen_nodes:
                    nodes.append({"id": category_node, "type": "category", "label": category, "weight": 1})
                    seen_nodes.add(category_node)
                edges.append({"id": f"edge-post-category-{post['id']}-{category}", "source": f"account-{account_id}", "target": category_node, "type": "matches_category"})

        for comment in comments:
            commenter = comment.get("commenter_username") or "anonim"
            commenter_node = f"commenter-{commenter}"
            if commenter_node not in seen_nodes:
                nodes.append(
                    {
                        "id": commenter_node,
                        "type": "commenter",
                        "label": commenter,
                        "weight": int(comment.get("orgut_baglanti_skoru") or 1),
                        "avatar_url": comment.get("commenter_profile_url"),
                    }
                )
                seen_nodes.add(commenter_node)
            edges.append(
                {
                    "id": f"edge-commenter-{comment['id']}",
                    "source": commenter_node,
                    "target": f"account-{account_id}",
                    "type": "commented_on",
                }
            )
            verdict_node = f"verdict-{comment['verdict']}"
            if verdict_node not in seen_nodes:
                nodes.append({"id": verdict_node, "type": "verdict", "label": comment["verdict"], "weight": 1})
                seen_nodes.add(verdict_node)
            edges.append(
                {
                    "id": f"edge-verdict-{comment['id']}",
                    "source": commenter_node,
                    "target": verdict_node,
                    "type": "flagged_by" if comment["bayrak"] else "matches_category",
                }
            )

        # Second-hop branching: commenter -> commenter links from person_links.
        with self._connect() as conn:
            commenter_labels = [
                str(node.get("label") or "").strip()
                for node in nodes
                if str(node.get("type") or "") == "commenter"
            ]
            commenter_person_ids: set[int] = set()
            if commenter_labels:
                placeholders = ",".join("?" for _ in commenter_labels)
                person_rows = conn.execute(
                    f"SELECT id, full_name FROM persons WHERE full_name IN ({placeholders})",
                    commenter_labels,
                ).fetchall()
                commenter_person_ids.update(int(row["id"]) for row in person_rows if row["id"] is not None)

            candidate_person_ids = set(commenter_person_ids)
            if candidate_person_ids:
                placeholders = ",".join("?" for _ in candidate_person_ids)
                link_rows = conn.execute(
                    f"""
                    SELECT person_id, related_username, source_account_id, link_reason
                    FROM person_links
                    WHERE person_id IN ({placeholders})
                    """,
                    tuple(candidate_person_ids),
                ).fetchall()

                parent_person_map: dict[int, str] = {}
                parent_rows = conn.execute(
                    f"SELECT id, full_name FROM persons WHERE id IN ({placeholders})",
                    tuple(candidate_person_ids),
                ).fetchall()
                for row in parent_rows:
                    parent_person_map[int(row["id"])] = str(row["full_name"] or "").strip()

                for link in link_rows:
                    parent_person_id = int(link["person_id"])
                    child_username = str(link["related_username"] or "").strip()
                    if not child_username:
                        continue
                    if link["source_account_id"] is not None and int(link["source_account_id"]) != account_id:
                        continue
                    if str(link["link_reason"] or "").strip() == "comment_interaction":
                        continue

                    parent_username = parent_person_map.get(parent_person_id, "")
                    possible_parent = f"commenter-{parent_username}" if parent_username else ""
                    parent_node = possible_parent if possible_parent in seen_nodes else None
                    if parent_node is None:
                        continue

                    child_node = f"commenter-{child_username}"
                    if child_node not in seen_nodes:
                        nodes.append(
                            {
                                "id": child_node,
                                "type": "commenter",
                                "label": child_username,
                                "weight": 6,
                            }
                        )
                        seen_nodes.add(child_node)

                    edge_id = f"edge-related-{parent_node}-{child_node}"
                    if not any(item.get("id") == edge_id for item in edges):
                        edges.append(
                            {
                                "id": edge_id,
                                "source": parent_node,
                                "target": child_node,
                                "type": "related_to",
                            }
                        )
        return {"nodes": nodes, "edges": edges}

    def list_review_queue(self, search: str | None = None) -> list[dict]:
        params: list[object] = []
        where_clause = ""
        if search:
            where_clause = "WHERE commenter_username LIKE ? OR last_reason LIKE ?"
            like = f"%{search}%"
            params.extend([like, like])
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT commenter_username, trigger_count, first_triggered_at, last_triggered_at,
                       last_reason, flag_reason_type, status
                FROM review_queue
                {where_clause}
                ORDER BY trigger_count DESC, last_triggered_at DESC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_post(
        self,
        instagram_account_id: int,
        media_type: str,
        media_url: str,
        media_items: list[dict[str, str]] | None,
        caption: str | None,
        post_analysis: str,
        structured_analysis: dict | None,
        model: str,
        source_kind: str = "post",
        source_target_username: str | None = None,
        source_run_id: str | None = None,
        source_post_id: str | None = None,
        source_post_url: str | None = None,
        source_container_id: str | None = None,
        source_container_title: str | None = None,
        source_created_at: str | None = None,
    ) -> UpsertPostResult:
        structured_analysis_text = json.dumps(structured_analysis, ensure_ascii=True) if structured_analysis else None
        normalized_media_items = self._normalize_media_items(media_items)
        media_items_text = json.dumps(normalized_media_items, ensure_ascii=True) if normalized_media_items else None
        post_ozet = None
        content_categories = None
        threat_level = None
        importance_score = None
        organization_link = None
        detected_organization = None
        if structured_analysis:
            post_ozet = str(structured_analysis.get("ozet") or "") or None
            raw_categories = structured_analysis.get("icerik_kategorisi")
            if isinstance(raw_categories, list):
                content_categories = ",".join(str(item) for item in raw_categories if item)
            raw_threat = structured_analysis.get("tehdit_degerlendirmesi")
            if isinstance(raw_threat, dict):
                threat_level = str(raw_threat.get("tehdit_seviyesi") or "") or None
            raw_org = structured_analysis.get("orgut_baglantisi")
            if isinstance(raw_org, dict):
                organization_link = str(raw_org.get("tespit_edilen_orgut") or "") or None
                detected_organization = str(raw_org.get("tespit_edilen_orgut") or "") or None
            try:
                importance_score = int(structured_analysis.get("onem_skoru")) if structured_analysis.get("onem_skoru") is not None else None
            except (TypeError, ValueError):
                importance_score = None

        with self._connect() as conn:
            if source_run_id and source_post_id:
                existing = conn.execute(
                    """
                    SELECT id
                    FROM instagram_posts
                    WHERE instagram_account_id = ?
                      AND source_run_id = ?
                      AND source_post_id = ?
                    """,
                    (instagram_account_id, source_run_id, source_post_id),
                ).fetchone()
                if existing:
                    post_id = int(existing["id"])
                    conn.execute(
                        """
                        UPDATE instagram_posts
                        SET source_kind = ?,
                            source_container_id = ?,
                            source_container_title = ?,
                            source_created_at = ?,
                            media_type = ?,
                            media_url = ?,
                            media_items = ?,
                            caption = ?,
                            post_analysis = ?,
                            post_ozet = ?,
                            structured_analysis = ?,
                            icerik_kategorisi = ?,
                            tehdit_seviyesi = ?,
                            onem_skoru = ?,
                            orgut_baglantisi = ?,
                            tespit_edilen_orgut = ?,
                            model = ?,
                            source_target_username = ?,
                            source_post_url = ?
                        WHERE id = ?
                        """,
                        (
                            source_kind,
                            source_container_id,
                            source_container_title,
                            source_created_at,
                            media_type,
                            media_url,
                            media_items_text,
                            caption,
                            post_analysis,
                            post_ozet,
                            structured_analysis_text,
                            content_categories,
                            threat_level,
                            importance_score,
                            organization_link,
                            detected_organization,
                            model,
                            source_target_username,
                            source_post_url,
                            post_id,
                        ),
                    )
                    conn.commit()
                    return UpsertPostResult(post_id=post_id, created=False)

            cur = conn.execute(
                """
                INSERT INTO instagram_posts(
                    instagram_account_id,
                    source_kind,
                    source_container_id,
                    source_container_title,
                    source_created_at,
                    media_type,
                    media_url,
                    media_items,
                    caption,
                    post_analysis,
                    post_ozet,
                    structured_analysis,
                    icerik_kategorisi,
                    tehdit_seviyesi,
                    onem_skoru,
                    orgut_baglantisi,
                    tespit_edilen_orgut,
                    model,
                    source_target_username,
                    source_run_id,
                    source_post_id,
                    source_post_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    instagram_account_id,
                    source_kind,
                    source_container_id,
                    source_container_title,
                    source_created_at,
                    media_type,
                    media_url,
                    media_items_text,
                    caption,
                    post_analysis,
                    post_ozet,
                    structured_analysis_text,
                    content_categories,
                    threat_level,
                    importance_score,
                    organization_link,
                    detected_organization,
                    model,
                    source_target_username,
                    source_run_id,
                    source_post_id,
                    source_post_url,
                ),
            )
            conn.commit()
            return UpsertPostResult(post_id=int(cur.lastrowid), created=True)

    def upsert_comment(
        self,
        instagram_post_id: int,
        commenter_username: str | None,
        commenter_profile_url: str | None,
        comment_text: str,
        verdict: str,
        sentiment: str,
        orgut_baglanti_skoru: int,
        bayrak: bool,
        reason: str,
        discovered_at: str | None = None,
        source_run_id: str | None = None,
        source_post_id: str | None = None,
        source_post_url: str | None = None,
    ) -> UpsertCommentResult:
        with self._connect() as conn:
            if discovered_at:
                existing = conn.execute(
                    """
                    SELECT id
                    FROM instagram_comments
                    WHERE instagram_post_id = ?
                      AND COALESCE(commenter_username, '') = COALESCE(?, '')
                      AND comment_text = ?
                      AND discovered_at = ?
                    """,
                    (instagram_post_id, commenter_username, comment_text, discovered_at),
                ).fetchone()
                if existing:
                    comment_id = int(existing["id"])
                    conn.execute(
                        """
                        UPDATE instagram_comments
                        SET commenter_profile_url = ?,
                            verdict = ?,
                            sentiment = ?,
                            orgut_baglanti_skoru = ?,
                            bayrak = ?,
                            reason = ?,
                            source_run_id = ?,
                            source_post_id = ?,
                            source_post_url = ?
                        WHERE id = ?
                        """,
                        (
                            commenter_profile_url,
                            verdict,
                            sentiment,
                            orgut_baglanti_skoru,
                            1 if bayrak else 0,
                            reason,
                            source_run_id,
                            source_post_id,
                            source_post_url,
                            comment_id,
                        ),
                    )
                    conn.commit()
                    return UpsertCommentResult(comment_id=comment_id, created=False)

            cur = conn.execute(
                """
                INSERT INTO instagram_comments(
                    instagram_post_id,
                    commenter_username,
                    commenter_profile_url,
                    comment_text,
                    verdict,
                    sentiment,
                    orgut_baglanti_skoru,
                    bayrak,
                    reason,
                    discovered_at,
                    source_run_id,
                    source_post_id,
                    source_post_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    instagram_post_id,
                    commenter_username,
                    commenter_profile_url,
                    comment_text,
                    verdict,
                    sentiment,
                    orgut_baglanti_skoru,
                    1 if bayrak else 0,
                    reason,
                    discovered_at,
                    source_run_id,
                    source_post_id,
                    source_post_url,
                ),
            )
            conn.commit()
            return UpsertCommentResult(comment_id=int(cur.lastrowid), created=True)

    def upsert_review_queue(
        self,
        commenter_username: str,
        last_reason: str | None,
        flag_reason_type: str | None,
        person_id: int | None = None,
    ) -> None:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT commenter_username FROM review_queue WHERE commenter_username = ?",
                (commenter_username,),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE review_queue
                    SET trigger_count = trigger_count + 1,
                        person_id = COALESCE(?, person_id),
                        last_triggered_at = CURRENT_TIMESTAMP,
                        last_reason = ?,
                        flag_reason_type = ?,
                        status = 'open'
                    WHERE commenter_username = ?
                    """,
                    (person_id, last_reason, flag_reason_type, commenter_username),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO review_queue(commenter_username, person_id, last_reason, flag_reason_type, status)
                    VALUES (?, ?, ?, ?, 'open')
                    """,
                    (commenter_username, person_id, last_reason, flag_reason_type),
                )
            conn.commit()

    def get_post_history_summaries(self, instagram_account_id: int) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT source_kind,
                       source_post_id,
                       source_container_title,
                       source_created_at,
                       post_ozet,
                       structured_analysis,
                       created_at,
                       tespit_edilen_orgut
                FROM instagram_posts
                WHERE instagram_account_id = ?
                ORDER BY COALESCE(source_created_at, created_at), id
                """,
                (instagram_account_id,),
            ).fetchall()

        summaries: list[dict] = []
        for row in rows:
            structured = row["structured_analysis"]
            data: dict = {}
            if structured:
                try:
                    parsed = json.loads(str(structured))
                    if isinstance(parsed, dict):
                        data = parsed
                except json.JSONDecodeError:
                    data = {}
            threat_data = data.get("tehdit_degerlendirmesi", {}) if isinstance(data.get("tehdit_degerlendirmesi"), dict) else {}
            org_data = data.get("orgut_baglantisi", {}) if isinstance(data.get("orgut_baglantisi"), dict) else {}
            categories = data.get("icerik_kategorisi", [])
            source_kind = str(row["source_kind"] or "post").strip() or "post"
            source_post_id = str(row["source_post_id"] or "").strip()
            source_created_at = str(row["source_created_at"] or "").strip()
            source_container_title = str(row["source_container_title"] or "").strip()
            summaries.append(
                {
                    "tarih": source_created_at or source_post_id or row["created_at"],
                    "source_kind": source_kind,
                    "source_post_id": source_post_id,
                    "source_container_title": source_container_title,
                    "ozet": row["post_ozet"] or data.get("ozet") or "",
                    "icerik_kategorisi": categories if isinstance(categories, list) else [],
                    "tehdit_seviyesi": threat_data.get("tehdit_seviyesi") or "belirsiz",
                    "orgut": row["tespit_edilen_orgut"] or org_data.get("tespit_edilen_orgut") or "belirsiz",
                    "onem_skoru": data.get("onem_skoru") or 0,
                }
            )
        return summaries

    def get_commenter_history(self, commenter_username: str | None) -> list[dict]:
        if not commenter_username:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.comment_text,
                       c.verdict,
                       c.sentiment,
                       c.reason,
                       c.orgut_baglanti_skoru,
                       c.bayrak,
                       p.structured_analysis
                FROM instagram_comments c
                JOIN instagram_posts p ON p.id = c.instagram_post_id
                WHERE c.commenter_username = ?
                ORDER BY c.id
                """,
                (commenter_username,),
            ).fetchall()

        history: list[dict] = []
        for row in rows:
            post_ozet = ""
            structured = row["structured_analysis"]
            if structured:
                try:
                    parsed = json.loads(str(structured))
                    if isinstance(parsed, dict):
                        post_ozet = str(parsed.get("ozet") or "")
                except json.JSONDecodeError:
                    post_ozet = ""
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

    def persist_post_and_comments(
        self,
        person_name: str,
        instagram_username: str,
        profile_photo_url: str | None,
        bio: str | None,
        media_type: str,
        media_url: str,
        media_items: list[dict[str, str]] | None,
        caption: str | None,
        post_analysis: str,
        structured_analysis: dict | None,
        model: str,
        comment_analyses: list[dict[str, str | None]],
        media_observations: list[dict[str, object]] | None = None,
    ) -> PersistedAnalysisIds:
        person_id, account_id = self.get_or_create_person_account(
            person_name=person_name,
            instagram_username=instagram_username,
            profile_photo_url=profile_photo_url,
            bio=bio,
        )

        post_result = self.upsert_post(
            instagram_account_id=account_id,
            media_type=media_type,
            media_url=media_url,
            media_items=media_items,
            caption=caption,
            post_analysis=post_analysis,
            structured_analysis=structured_analysis,
            model=model,
        )
        if media_observations:
            self.save_media_observations(post_result.post_id, media_observations)

        comment_ids: list[int] = []
        for item in comment_analyses:
            comment_result = self.upsert_comment(
                instagram_post_id=post_result.post_id,
                commenter_username=item.get("commenter_username"),
                commenter_profile_url=None,
                comment_text=str(item.get("text") or ""),
                verdict=str(item.get("verdict") or "unclear"),
                sentiment=str(item.get("sentiment") or "neutral"),
                orgut_baglanti_skoru=int(item.get("orgut_baglanti_skoru") or 0),
                bayrak=bool(item.get("bayrak")),
                reason=str(item.get("reason") or ""),
            )
            comment_ids.append(comment_result.comment_id)
            score = int(item.get("orgut_baglanti_skoru") or 0)
            should_enqueue_review = bool(item.get("bayrak")) or score >= COMMENT_REVIEW_QUEUE_MIN_SCORE
            if should_enqueue_review and item.get("commenter_username"):
                commenter_person_id = self.get_or_create_person(str(item["commenter_username"]))
                self.upsert_review_queue(
                    str(item["commenter_username"]),
                    str(item.get("reason") or ""),
                    str(item.get("verdict") or "belirsiz"),
                    person_id=commenter_person_id,
                )

        return PersistedAnalysisIds(
            person_id=person_id,
            instagram_account_id=account_id,
            post_id=post_result.post_id,
            comment_ids=comment_ids,
        )

    def reset_runtime_state(self) -> dict[str, int]:
        runtime_tables = [
            "account_aggregates",
            "canonical_comment_analyses",
            "canonical_post_analyses",
            "media_observations",
            "llm_stage_attempts",
            "person_links",
            "ingest_job_events",
            "ingest_job_posts",
            "batch_job_targets",
            "batch_jobs",
            "ingest_jobs",
            "ingest_sources",
            "review_queue",
            "instagram_comments",
            "instagram_posts",
            "instagram_accounts",
            "persons",
        ]
        deleted_rows: dict[str, int] = {}
        with self._connect() as conn:
            for table_name in runtime_tables:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
                deleted_rows[table_name] = int(row["count"]) if row else 0
                conn.execute(f"DELETE FROM {table_name}")
            conn.execute(
                """
                DELETE FROM sqlite_sequence
                WHERE name IN ({placeholders})
                """.format(placeholders=", ".join("?" for _ in runtime_tables)),
                runtime_tables,
            )
            conn.commit()
            conn.execute("VACUUM")
        return deleted_rows
