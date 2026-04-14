import json
import sqlite3
from pathlib import Path
from threading import Lock

from app.database_service import DatabaseService
from app.main import (
    _run_discovery_scan,
    create_batch_jobs,
    get_jobs_overview,
    ingest_instagram_account_latest,
    run_ingest_workers_once,
)
from app.schemas import (
    BatchJobsCreateRequest,
    IngestInstagramAccountLatestRequest,
    IngestWatchScanRequest,
    IngestWorkersRunOnceRequest,
)
from app.settings import Settings


TEST_SETTINGS = Settings(
    minio_bucket_default="instagram-archive",
    minio_bucket_fallback="instagram_archive",
    ingest_max_concurrent_posts_per_account=1,
    ingest_max_concurrent_comments=1,
    ingest_max_concurrent_media_per_post=1,
)


class FakeMinioArchiveService:
    def __init__(self, bucket: str, objects: dict[str, str]) -> None:
        self.bucket = bucket
        self.objects = objects

    def bucket_exists(self, bucket: str) -> bool:
        return bucket == self.bucket

    def list_object_names(self, bucket: str, prefix: str, recursive: bool = True) -> list[str]:
        if bucket != self.bucket:
            return []
        return sorted([k for k in self.objects if k.startswith(prefix)])

    def read_object_text(self, bucket: str, object_key: str, encoding: str = "utf-8") -> str:
        if bucket != self.bucket or object_key not in self.objects:
            raise FileNotFoundError(object_key)
        return self.objects[object_key]

    def object_exists(self, bucket: str, object_key: str) -> bool:
        return bucket == self.bucket and object_key in self.objects

    def presigned_get_object(self, bucket: str, object_key: str, expires_seconds: int) -> str:
        return f"http://minio.local/{bucket}/{object_key}?exp={expires_seconds}"


class SequencedVLLMService:
    default_model = "gemma-4-31b-it"

    def __init__(self, replies: list[str]) -> None:
        self.replies = replies[:]
        self.payloads: list[dict] = []
        self._lock = Lock()

    def build_payload(
        self,
        description: str,
        media_type: str,
        media_url: str,
        max_tokens: int,
        model: str | None = None,
        media_items: list[dict[str, str]] | None = None,
    ):
        return {
            "model": model or self.default_model,
            "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
            "max_tokens": max_tokens,
            "stream": False,
            "_media_type": media_type,
            "_media_url": media_url,
            "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
        }

    def create_chat_completion(self, payload):
        with self._lock:
            if not self.replies:
                raise AssertionError("No reply left")
            self.payloads.append(payload)
            text = self.replies.pop(0)
        return {
            "model": payload.get("model", self.default_model),
            "choices": [{"message": {"content": text}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10},
        }

    @staticmethod
    def extract_answer(chat_response):
        return (
            chat_response["model"],
            chat_response["choices"][0]["message"]["content"],
            chat_response.get("usage"),
            chat_response["choices"][0].get("finish_reason"),
        )


def _build_fake_archive_objects() -> dict[str, str]:
    username = "kurdistan24tv.official"
    old_run = "20260401T000000Z"
    new_run = "20260408T105713Z"

    profile = {
        "username": username,
        "full_name": "Kurdistan24",
        "bio": "news",
        "profile_image_url": "https://cdn/p.jpg",
    }
    post = {
        "post_id": "POST123",
        "post_url": f"https://www.instagram.com/{username}/p/POST123/",
        "post_type": "image",
        "caption": "caption text",
        "media": [{"url": "https://fallback.example/img.jpg"}],
    }
    comments = [
        {
            "commenter_username": "u_support",
            "commenter_profile_url": "https://ig/u_support",
            "comment_text": "great",
            "source_post_id": "POST123",
            "source_post_url": post["post_url"],
            "discovered_at": "2026-04-08T10:59:51+00:00",
            "run_id": new_run,
        },
        {
            "commenter_username": "u_other",
            "commenter_profile_url": "https://ig/u_other",
            "comment_text": "off-topic",
            "source_post_id": "POST123",
            "source_post_url": post["post_url"],
            "discovered_at": "2026-04-08T10:59:52+00:00",
            "run_id": new_run,
        },
    ]

    objects = {
        f"instagram/{username}/{old_run}/manifests/run.json": json.dumps({"run_id": old_run}),
        f"instagram/{username}/{new_run}/manifests/run.json": json.dumps({"run_id": new_run}),
        f"instagram/{username}/{new_run}/profile/profile.json": json.dumps(profile),
        f"instagram/{username}/{new_run}/posts/POST123/post.json": json.dumps(post),
        f"instagram/{username}/{new_run}/posts/POST123/comments.jsonl": "\n".join(json.dumps(c) for c in comments),
        f"instagram/{username}/{new_run}/posts/POST123/media/1-image.jpg": "binary-jpg-placeholder",
    }
    return objects


def _build_account_archive_objects(username: str, run_id: str, post_id: str, commenter_username: str) -> dict[str, str]:
    profile = {
        "username": username,
        "full_name": username,
        "bio": "news",
        "profile_image_url": "https://cdn/p.jpg",
    }
    post = {
        "post_id": post_id,
        "post_url": f"https://www.instagram.com/{username}/p/{post_id}/",
        "post_type": "image",
        "caption": f"caption for {username}",
        "media": [{"url": f"https://fallback.example/{post_id}.jpg"}],
    }
    comments = [
        {
            "commenter_username": commenter_username,
            "commenter_profile_url": f"https://ig/{commenter_username}",
            "comment_text": "great",
            "source_post_id": post_id,
            "source_post_url": post["post_url"],
            "discovered_at": "2026-04-08T10:59:51+00:00",
            "run_id": run_id,
        }
    ]
    return {
        f"instagram/{username}/{run_id}/manifests/run.json": json.dumps({"run_id": run_id}),
        f"instagram/{username}/{run_id}/profile/profile.json": json.dumps(profile),
        f"instagram/{username}/{run_id}/posts/{post_id}/post.json": json.dumps(post),
        f"instagram/{username}/{run_id}/posts/{post_id}/comments.jsonl": "\n".join(json.dumps(c) for c in comments),
        f"instagram/{username}/{run_id}/posts/{post_id}/media/1-image.jpg": "binary-jpg-placeholder",
    }


def test_ingest_instagram_account_latest_writes_db_and_flags(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = _build_fake_archive_objects()
    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    vllm = SequencedVLLMService(
        replies=[
            '{"ozet":"Post analysis text","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
            '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"destekliyor"}',
            '{"verdict":"alakasiz","sentiment":"neutral","orgut_baglanti_skoru":0,"bayrak":false,"reason":"alakasiz"}',
            "Hesap propaganda agirlikli icerikler paylasiyor ve orta riskli propaganda izleri sergiliyor.",
        ]
    )

    response = ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username="kurdistan24tv.official"),
        settings=TEST_SETTINGS,
        minio_service=minio,
        vllm_service=vllm,
        db_service=db_service,
    )

    assert response.run_id == "20260408T105713Z"
    assert response.processed_posts == 1
    assert response.created_posts == 1
    assert response.updated_posts == 0
    assert response.processed_comments == 2
    assert response.created_comments == 2
    assert response.skipped_comments == 0
    assert response.flagged_users == 1
    assert response.flagged_usernames == ["u_support"]

    conn = sqlite3.connect(str(db_path))
    rq = conn.execute(
        "SELECT commenter_username, trigger_count, flag_reason_type, status FROM review_queue"
    ).fetchall()
    assert rq == [("u_support", 1, "destekci_aktif", "open")]
    posts = conn.execute(
        "SELECT source_run_id, source_post_id, tehdit_seviyesi, orgut_baglantisi FROM instagram_posts"
    ).fetchall()
    assert posts == [("20260408T105713Z", "POST123", "orta", "PKK/KCK")]
    account_summary = conn.execute("SELECT account_profile_summary FROM instagram_accounts").fetchone()[0]
    assert account_summary == "Hesap propaganda agirlikli icerikler paylasiyor ve orta riskli propaganda izleri sergiliyor."
    conn.close()
    summary_payloads = []
    for payload in vllm.payloads:
        user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
        content = user_message["content"]
        text = content[0]["text"] if isinstance(content, list) else str(content)
        if "final persistent profile summary" in text:
            summary_payloads.append(text)
    assert len(summary_payloads) == 1


def test_ingest_instagram_account_latest_is_idempotent_for_posts_and_comments(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()
    objects = _build_fake_archive_objects()

    # first run
    response1 = ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username="kurdistan24tv.official"),
        settings=TEST_SETTINGS,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=SequencedVLLMService(
            replies=[
                '{"ozet":"Post analysis text","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
                '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"destekliyor"}',
                '{"verdict":"alakasiz","sentiment":"neutral","orgut_baglanti_skoru":0,"bayrak":false,"reason":"alakasiz"}',
                "Hesap propaganda agirlikli icerikler paylasiyor ve orta riskli propaganda izleri sergiliyor.",
            ]
        ),
        db_service=db_service,
    )

    # second run, same source data
    response2 = ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username="kurdistan24tv.official"),
        settings=TEST_SETTINGS,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=SequencedVLLMService(
            replies=[
                '{"ozet":"Post analysis text v2","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
                '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"destekliyor"}',
                '{"verdict":"alakasiz","sentiment":"neutral","orgut_baglanti_skoru":0,"bayrak":false,"reason":"alakasiz"}',
                "Hesap propaganda agirlikli icerikler paylasmayi surduruyor ve risk sinyallerini koruyor.",
            ]
        ),
        db_service=db_service,
    )

    assert response1.created_posts == 1
    assert response2.created_posts == 0
    assert response2.updated_posts == 1
    assert response2.created_comments == 0
    assert response2.skipped_comments == 2

    conn = sqlite3.connect(str(db_path))
    review = conn.execute("SELECT commenter_username, trigger_count FROM review_queue").fetchall()
    # idempotent rerun should not re-trigger the same supportive comment
    assert review == [("u_support", 1)]
    comments_count = conn.execute("SELECT COUNT(*) FROM instagram_comments").fetchone()[0]
    assert comments_count == 2
    conn.close()


def test_ingest_instagram_account_latest_sends_all_post_media(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = _build_fake_archive_objects()
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/post.json"] = json.dumps(
        {
            "post_id": "POST123",
            "post_url": "https://www.instagram.com/kurdistan24tv.official/p/POST123/",
            "post_type": "carousel",
            "caption": "caption text",
            "media": [
                {"url": "https://fallback.example/1.jpg", "kind": "image"},
                {"url": "https://fallback.example/2.mp4", "kind": "video"},
            ],
        }
    )
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/media/2-video.mp4"] = "binary-mp4-placeholder"

    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    vllm = SequencedVLLMService(
        replies=[
            '{"ozet":"Ilk medya tekil analiz","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"dusuk"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":4,"analist_notu":"ilk not"}',
            '{"ozet":"Ikinci medya tekil analiz","icerik_kategorisi":["haber_paylasim"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"yok"},"orgut_baglantisi":{"tespit_edilen_orgut":"belirsiz"},"onem_skoru":2,"analist_notu":"ikinci not"}',
            '{"ozet":"Birlesik post analysis text","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
            '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"destekliyor"}',
            '{"verdict":"alakasiz","sentiment":"neutral","orgut_baglanti_skoru":0,"bayrak":false,"reason":"alakasiz"}',
            "Hesap propaganda agirlikli icerikler paylasiyor ve birlesik postlarda risk artiyor.",
        ]
    )

    ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username="kurdistan24tv.official"),
        settings=TEST_SETTINGS,
        minio_service=minio,
        vllm_service=vllm,
        db_service=db_service,
    )

    first_media_payload = vllm.payloads[0]
    second_media_payload = vllm.payloads[1]
    final_post_payload = vllm.payloads[2]
    assert first_media_payload["_media_type"] == "image"
    assert second_media_payload["_media_type"] == "video"
    first_user = next(message for message in reversed(first_media_payload["messages"]) if message["role"] == "user")
    second_user = next(message for message in reversed(second_media_payload["messages"]) if message["role"] == "user")
    post_user = next(message for message in reversed(final_post_payload["messages"]) if message["role"] == "user")
    first_text = first_user["content"][0]["text"] if isinstance(first_user["content"], list) else str(first_user["content"])
    second_text = second_user["content"][0]["text"] if isinstance(second_user["content"], list) else str(second_user["content"])
    post_text = post_user["content"][0]["text"] if isinstance(post_user["content"], list) else str(post_user["content"])
    assert "KULLANICININ ÖNCEKİ GÖNDERİLERİ" not in first_text
    assert "KULLANICI PROFİL ÖZETİ" not in first_text
    assert "KULLANICININ ÖNCEKİ GÖNDERİLERİ" not in second_text
    assert "KULLANICI PROFİL ÖZETİ" not in second_text
    assert "Standalone media analyses" in post_text
    assert "Ilk medya tekil analiz" in post_text
    assert "Ikinci medya tekil analiz" in post_text


def test_ingest_instagram_account_latest_runs_deep_media_stage_when_enabled(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()
    objects = _build_fake_archive_objects()
    minio = FakeMinioArchiveService("instagram-archive", objects)
    vllm = SequencedVLLMService(
        replies=[
            '{"media_type":"image","scene_summary":"street","setting_type":"street","visible_person_count":"2","face_visibility":"open","clothing_types":["civilian"],"notable_objects":["car"],"weapon_presence":{"status":"no","types":[]},"symbols_or_logos":[],"visible_text_items":[{"text":"34 ABC 123","language":"tr"}],"activity_type":["daily_life"],"crowd_level":"small_group","audio_elements":{"speech":"absent","music":"absent","chanting":"absent","gunfire_or_blast":"absent"},"child_presence":"no","institutional_markers":[],"vehicles":["car"],"license_or_signage":["34 ABC 123"],"deep_review_hint":{"run_deep_analysis":"yes","confidence":"high","reason_tr":"plaka var"}}',
            '{"location_assessment":{"location_identifiable":"yes","location_confidence":"high","candidate_location_text":"Istanbul","evidence":["street sign"]},"vehicle_plate_assessment":{"vehicle_present":"yes","vehicles":["car"],"plate_visible":"yes","plate_text_candidates":["34 ABC 123"],"evidence":["plate"]},"sensitive_information":[{"type":"location","value":"Istanbul","confidence":"high","reason_tr":"sign"}],"followup_priority":"high","analyst_note_tr":"detayli bilgi"}',
            '{"ozet":"Tek medya post","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
            "Final hesap özeti.",
        ]
    )

    ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(
            target_username="kurdistan24tv.official",
            analyze_comments=False,
            enable_deep_media_analysis=True,
        ),
        settings=TEST_SETTINGS,
        minio_service=minio,
        vllm_service=vllm,
        db_service=db_service,
    )

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT deep_required, deep_status, contains_vehicle, contains_plate, location_confidence, deep_payload FROM media_observations LIMIT 1"
    ).fetchone()
    conn.close()
    assert row is not None
    assert int(row[0]) == 1
    assert str(row[1]) == "completed"
    assert int(row[2]) == 1
    assert int(row[3]) == 1
    assert str(row[4]) == "high"
    assert "vehicle_plate_assessment" in str(row[5] or "")


def test_ingest_parallel_single_media_runs_before_parent_merge_and_without_history_context(tmp_path: Path) -> None:
    db_path = tmp_path / "parallel.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    username = "parallel.account"
    run_id = "20260408T120000Z"
    profile = {
        "username": username,
        "full_name": "Parallel Account",
        "bio": "news",
        "profile_image_url": "https://cdn/p.jpg",
    }
    post_a = {
        "post_id": "POSTA",
        "post_url": f"https://www.instagram.com/{username}/p/POSTA/",
        "post_type": "carousel",
        "caption": "caption A",
        "media": [
            {"url": "https://fallback.example/a1.jpg", "kind": "image"},
            {"url": "https://fallback.example/a2.jpg", "kind": "image"},
        ],
    }
    post_b = {
        "post_id": "POSTB",
        "post_url": f"https://www.instagram.com/{username}/p/POSTB/",
        "post_type": "image",
        "caption": "caption B",
        "media": [{"url": "https://fallback.example/b1.jpg", "kind": "image"}],
    }
    objects = {
        f"instagram/{username}/{run_id}/manifests/run.json": json.dumps({"run_id": run_id}),
        f"instagram/{username}/{run_id}/profile/profile.json": json.dumps(profile),
        f"instagram/{username}/{run_id}/posts/POSTA/post.json": json.dumps(post_a),
        f"instagram/{username}/{run_id}/posts/POSTB/post.json": json.dumps(post_b),
    }

    class InspectingParallelVLLMService:
        default_model = "gemma-4-31b-it"

        def __init__(self) -> None:
            self.payloads: list[dict] = []

        def build_payload(
            self,
            description: str,
            media_type: str,
            media_url: str,
            max_tokens: int,
            model: str | None = None,
            media_items: list[dict[str, str]] | None = None,
        ):
            return {
                "model": model or self.default_model,
                "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
                "max_tokens": max_tokens,
                "stream": False,
                "_media_type": media_type,
                "_media_url": media_url,
                "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
            }

        def create_chat_completion(self, payload):
            self.payloads.append(payload)
            user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
            content = user_message["content"]
            text = content[0]["text"] if isinstance(content, list) else str(content)
            if "final persistent profile summary" in text:
                body = "Parallel account summary."
            elif "Standalone media analyses" in text and "caption A" in text:
                body = '{"ozet":"Parent A","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}'
            elif "caption A" in text:
                body = '{"ozet":"Child A","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"dusuk"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":4}'
            elif "caption B" in text:
                body = '{"ozet":"Child B","icerik_kategorisi":["haber_paylasim"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"yok"},"orgut_baglantisi":{"tespit_edilen_orgut":"belirsiz"},"onem_skoru":2}'
            else:
                raise AssertionError(f"Unexpected payload: {text}")
            return {
                "model": payload.get("model", self.default_model),
                "choices": [{"message": {"content": body}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            }

        @staticmethod
        def extract_answer(chat_response):
            return (
                chat_response["model"],
                chat_response["choices"][0]["message"]["content"],
                chat_response.get("usage"),
                chat_response["choices"][0].get("finish_reason"),
            )

    settings = Settings(
        minio_bucket_default="instagram-archive",
        minio_bucket_fallback="instagram_archive",
        ingest_max_concurrent_posts_per_account=4,
        ingest_max_concurrent_comments=1,
    )
    vllm = InspectingParallelVLLMService()

    ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username=username),
        settings=settings,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=vllm,
        db_service=db_service,
    )

    texts: list[str] = []
    for payload in vllm.payloads:
        user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
        content = user_message["content"]
        texts.append(content[0]["text"] if isinstance(content, list) else str(content))

    child_b_index = next(index for index, text in enumerate(texts) if "caption B" in text)
    parent_a_index = next(index for index, text in enumerate(texts) if "Standalone media analyses" in text and "caption A" in text)
    assert child_b_index < parent_a_index

    child_texts = [text for text in texts if "final persistent profile summary" not in text and "Standalone media analyses" not in text]
    assert child_texts
    assert all("KULLANICININ ÖNCEKİ GÖNDERİLERİ" not in text for text in child_texts)
    assert all("KULLANICI PROFİL ÖZETİ" not in text for text in child_texts)


def test_ingest_instagram_account_latest_falls_back_to_embedded_comments(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = _build_fake_archive_objects()
    post = json.loads(objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/post.json"])
    post["comments"] = [
        {
            "commenter_username": "u_embedded",
            "commenter_profile_url": "https://ig/u_embedded",
            "comment_text": "embedded comment",
            "source_post_id": "POST123",
            "source_post_url": post["post_url"],
            "discovered_at": "2026-04-08T10:59:53+00:00",
            "run_id": "20260408T105713Z",
        }
    ]
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/post.json"] = json.dumps(post)
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/comments.jsonl"] = ""

    response = ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username="kurdistan24tv.official"),
        settings=TEST_SETTINGS,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=SequencedVLLMService(
            replies=[
                '{"ozet":"Post analysis text","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
                '{"verdict":"destekci_pasif","sentiment":"positive","orgut_baglanti_skoru":5,"bayrak":false,"reason":"yorum mevcut"}',
                "Hesap propaganda agirlikli icerikler paylasiyor ve yorumlarla birlikte izlenmeli.",
            ]
        ),
        db_service=db_service,
    )

    assert response.processed_comments == 1
    assert response.created_comments == 1

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT commenter_username, trigger_count, flag_reason_type FROM review_queue WHERE commenter_username = ?",
        ("u_embedded",),
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "u_embedded"
    assert row[1] == 1
    assert row[2] == "destekci_pasif"


def test_ingest_debug_first_post_only_limits_posts_comments_and_skips_summary_llm(tmp_path: Path) -> None:
    db_path = tmp_path / "debug_first_post.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = _build_fake_archive_objects()
    second_post = {
        "post_id": "POST999",
        "post_url": "https://www.instagram.com/kurdistan24tv.official/p/POST999/",
        "post_type": "image",
        "caption": "second caption",
        "media": [{"url": "https://fallback.example/second.jpg"}],
    }
    second_comments = [
        {
            "commenter_username": "u_second",
            "commenter_profile_url": "https://ig/u_second",
            "comment_text": "second comment",
            "source_post_id": "POST999",
            "source_post_url": second_post["post_url"],
            "discovered_at": "2026-04-08T11:00:10+00:00",
            "run_id": "20260408T105713Z",
        }
    ]
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST999/post.json"] = json.dumps(second_post)
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST999/comments.jsonl"] = "\n".join(
        json.dumps(item) for item in second_comments
    )
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST999/media/1-image.jpg"] = "binary-jpg-placeholder"

    vllm = SequencedVLLMService(
        replies=[
            '{"ozet":"Ilk post analysis","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
            '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"destekliyor"}',
        ]
    )

    response = ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, 
            target_username="kurdistan24tv.official",
            debug_first_post_only=True,
            max_comments_per_post=1,
        ),
        settings=TEST_SETTINGS,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=vllm,
        db_service=db_service,
    )

    assert response.processed_posts == 1
    assert response.processed_comments == 1
    assert len(vllm.payloads) == 2

    conn = sqlite3.connect(str(db_path))
    stored_posts = conn.execute("SELECT source_post_id FROM instagram_posts ORDER BY source_post_id").fetchall()
    account_summary = conn.execute("SELECT account_profile_summary FROM instagram_accounts").fetchone()[0]
    conn.close()

    assert stored_posts == [("POST123",)]
    assert "Toplam gönderi: 1" in account_summary


def test_ingest_debug_first_post_only_can_limit_media_items(tmp_path: Path) -> None:
    db_path = tmp_path / "debug_media_limit.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = _build_fake_archive_objects()
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/post.json"] = json.dumps(
        {
            "post_id": "POST123",
            "post_url": "https://www.instagram.com/kurdistan24tv.official/p/POST123/",
            "post_type": "carousel",
            "caption": "caption text",
            "media": [
                {"url": "https://fallback.example/1.jpg", "kind": "image"},
                {"url": "https://fallback.example/2.mp4", "kind": "video"},
                {"url": "https://fallback.example/3.jpg", "kind": "image"},
            ],
        }
    )
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/media/2-video.mp4"] = "binary-mp4-placeholder"
    objects["instagram/kurdistan24tv.official/20260408T105713Z/posts/POST123/media/3-image.jpg"] = "binary-jpg-placeholder"

    vllm = SequencedVLLMService(
        replies=[
            '{"ozet":"Ilk medya","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"dusuk"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":4}',
            '{"ozet":"Ikinci medya","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"dusuk"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":4}',
            '{"ozet":"Parent","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}',
            '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"destekliyor"}',
        ]
    )

    response = ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, 
            target_username="kurdistan24tv.official",
            debug_first_post_only=True,
            max_media_items_per_post=2,
            max_comments_per_post=1,
        ),
        settings=TEST_SETTINGS,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=vllm,
        db_service=db_service,
    )

    assert response.processed_posts == 1
    assert response.processed_comments == 1
    assert len(vllm.payloads) == 4
    assert vllm.payloads[0]["_media_items"] == [{"media_type": "image", "media_url": vllm.payloads[0]["_media_url"]}]
    assert vllm.payloads[1]["_media_items"] == [{"media_type": "video", "media_url": vllm.payloads[1]["_media_url"]}]


def test_ingest_final_account_summary_falls_back_when_llm_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "summary_fallback.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()
    objects = _build_fake_archive_objects()

    class FinalSummaryFailingVLLMService:
        default_model = "gemma-4-31b-it"

        def build_payload(
            self,
            description: str,
            media_type: str,
            media_url: str,
            max_tokens: int,
            model: str | None = None,
            media_items: list[dict[str, str]] | None = None,
        ):
            return {
                "model": model or self.default_model,
                "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
                "max_tokens": max_tokens,
                "stream": False,
                "_media_type": media_type,
                "_media_url": media_url,
                "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
            }

        def create_chat_completion(self, payload):
            user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
            content = user_message["content"]
            text = content[0]["text"] if isinstance(content, list) else str(content)
            if "final persistent profile summary" in text:
                raise RuntimeError("summary failed")
            if "Comment owner: u_support" in text:
                body = '{"comment_type":"support","content_summary_tr":"destekliyor","sentiment":"positive","flags":{"active_supporter":{"flag":true,"reason_tr":"açık destek"},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":8,"confidence":"medium","reason_tr":"destek"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"yes","reason_tr":"tekil"},"overall_risk":{"level":"high","human_review_required":"yes"},"review":{"importance_score":8,"priority_level":"high","human_review_required":"yes","confidence":"medium","reason":"destek"}}'
            elif "Comment owner: u_other" in text:
                body = '{"comment_type":"neutral","content_summary_tr":"nötr","sentiment":"neutral","flags":{"active_supporter":{"flag":false,"reason_tr":""},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":0,"confidence":"low","reason_tr":"nötr"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"no","reason_tr":"tekil"},"overall_risk":{"level":"low","human_review_required":"no"},"review":{"importance_score":1,"priority_level":"low","human_review_required":"no","confidence":"low","reason":"nötr"}}'
            else:
                body = '{"ozet":"Post analysis text","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}'
            return {
                "model": payload.get("model", self.default_model),
                "choices": [{"message": {"content": body}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            }

        @staticmethod
        def extract_answer(chat_response):
            return (
                chat_response["model"],
                chat_response["choices"][0]["message"]["content"],
                chat_response.get("usage"),
                chat_response["choices"][0].get("finish_reason"),
            )

    ingest_instagram_account_latest(
        request=IngestInstagramAccountLatestRequest(enable_deep_media_analysis=False, target_username="kurdistan24tv.official"),
        settings=TEST_SETTINGS,
        minio_service=FakeMinioArchiveService(bucket="instagram-archive", objects=objects),
        vllm_service=FinalSummaryFailingVLLMService(),
        db_service=db_service,
    )

    conn = sqlite3.connect(str(db_path))
    account_summary = conn.execute("SELECT account_profile_summary FROM instagram_accounts").fetchone()[0]
    conn.close()
    assert "kurdistan24tv.official" in account_summary
    assert "Toplam gönderi: 1" in account_summary


def test_discovery_scan_enqueues_latest_run_once(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = _build_fake_archive_objects()
    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    settings = TEST_SETTINGS

    first = _run_discovery_scan(
        request=IngestWatchScanRequest(usernames=["kurdistan24tv.official"]),
        settings=settings,
        minio_service=minio,
        db_service=db_service,
    )
    second = _run_discovery_scan(
        request=IngestWatchScanRequest(usernames=["kurdistan24tv.official"]),
        settings=settings,
        minio_service=minio,
        db_service=db_service,
    )

    assert first.discovered_sources == 1
    assert first.enqueued_jobs == 1
    assert second.discovered_sources == 1
    assert second.enqueued_jobs == 0
    assert second.skipped_jobs == 1

    conn = sqlite3.connect(str(db_path))
    jobs = conn.execute("SELECT target_username, run_id, status FROM ingest_jobs").fetchall()
    assert jobs == [("kurdistan24tv.official", "20260408T105713Z", "pending")]
    conn.close()


def test_run_ingest_workers_once_processes_multiple_accounts(tmp_path: Path) -> None:
    db_path = tmp_path / "ingest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = {}
    objects.update(_build_account_archive_objects("alpha.account", "20260408T100000Z", "POSTA", "alpha_fan"))
    objects.update(_build_account_archive_objects("beta.account", "20260408T110000Z", "POSTB", "beta_fan"))
    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    settings = Settings(
        minio_bucket_default="instagram-archive",
        minio_bucket_fallback="instagram_archive",
        ingest_max_concurrent_accounts=2,
        ingest_max_concurrent_comments=2,
        ingest_max_concurrent_media_per_post=2,
    )

    scan = _run_discovery_scan(
        request=IngestWatchScanRequest(usernames=["alpha.account", "beta.account"]),
        settings=settings,
        minio_service=minio,
        db_service=db_service,
    )
    assert scan.enqueued_jobs == 2

    class RoutingVLLMService:
        default_model = "gemma-4-31b-it"

        def build_payload(
            self,
            description: str,
            media_type: str,
            media_url: str,
            max_tokens: int,
            model: str | None = None,
            media_items: list[dict[str, str]] | None = None,
        ):
            return {
                "model": model or self.default_model,
                "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
                "max_tokens": max_tokens,
                "stream": False,
                "_media_type": media_type,
                "_media_url": media_url,
                "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
            }

        def create_chat_completion(self, payload):
            user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
            message = user_message["content"]
            if isinstance(message, list):
                text = message[0]["text"]
            else:
                text = str(message)

            if "Comment owner: alpha_fan" in text:
                content = '{"comment_type":"support","content_summary_tr":"destekliyor","sentiment":"positive","flags":{"active_supporter":{"flag":true,"reason_tr":"açık destek"},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":8,"confidence":"medium","reason_tr":"destekliyor"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"yes","reason_tr":"destek dili"},"overall_risk":{"level":"high","human_review_required":"yes"},"review":{"importance_score":8,"priority_level":"high","human_review_required":"yes","confidence":"medium","reason":"destekliyor"}}'
            elif "Comment owner: beta_fan" in text:
                content = '{"comment_type":"support","content_summary_tr":"izleyici destek","sentiment":"positive","flags":{"active_supporter":{"flag":false,"reason_tr":"pasif destek"},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":5,"confidence":"low","reason_tr":"izleyici destek"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"no","reason_tr":"tekil"},"overall_risk":{"level":"medium","human_review_required":"no"},"review":{"importance_score":3,"priority_level":"medium","human_review_required":"no","confidence":"low","reason":"izleyici destek"}}'
            elif "final persistent profile summary" in text and "alpha.account" in text:
                content = "Alpha summary."
            elif "final persistent profile summary" in text and "beta.account" in text:
                content = "Beta summary."
            elif "caption for alpha.account" in text:
                content = '{"content_types":["propaganda"],"primary_theme":["organizational_symbolism"],"summary_tr":"Alpha post","language_and_tone":{"dominant_language":"tr","tone":"mobilizing","sloganized_language":"yes"},"risk_indicators":{"direct_support_expression":{"status":"present","evidence":["destek"]},"organizational_symbol_use":{"status":"present","evidence":["simge"]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[{"entity":"PKK/KCK","relationship_type":"direct_support","confidence":"medium","reason_tr":"destek dili"}],"organization_link_score":6,"confidence":"medium"},"profile_role_estimate":{"role":"supporter","reason_tr":"açık destek"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tek post"},"review_priority":{"importance_score":6,"priority_level":"medium","human_review_required":"no","reason_tr":"inceleme"},"analyst_note_tr":"alpha note"}'
            elif "caption for beta.account" in text:
                content = '{"content_types":["news"],"primary_theme":["information_sharing"],"summary_tr":"Beta post","language_and_tone":{"dominant_language":"tr","tone":"neutral","sloganized_language":"no"},"risk_indicators":{"direct_support_expression":{"status":"absent","evidence":[]},"organizational_symbol_use":{"status":"present","evidence":["logo"]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[{"entity":"YPG/PYD","relationship_type":"weak_signal","confidence":"low","reason_tr":"zayıf sinyal"}],"organization_link_score":4,"confidence":"low"},"profile_role_estimate":{"role":"news_sharer","reason_tr":"haber paylaşımı"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tekil"},"review_priority":{"importance_score":4,"priority_level":"low","human_review_required":"no","reason_tr":"düşük"},"analyst_note_tr":"beta note"}'
            else:
                raise AssertionError(f"Unexpected payload: {text}")

            return {
                "model": payload.get("model", self.default_model),
                "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            }

        @staticmethod
        def extract_answer(chat_response):
            return (
                chat_response["model"],
                chat_response["choices"][0]["message"]["content"],
                chat_response.get("usage"),
                chat_response["choices"][0].get("finish_reason"),
            )

    vllm = RoutingVLLMService()

    result = run_ingest_workers_once(
        request=IngestWorkersRunOnceRequest(max_jobs=2, lease_owner="test-worker"),
        settings=settings,
        minio_service=minio,
        vllm_service=vllm,
        db_service=db_service,
    )

    assert result.claimed_jobs == 2
    assert result.completed_jobs == 2
    assert result.failed_jobs == 0
    assert {item.target_username for item in result.items} == {"alpha.account", "beta.account"}
    assert all(item.status == "completed" for item in result.items)

    conn = sqlite3.connect(str(db_path))
    job_rows = conn.execute("SELECT target_username, status FROM ingest_jobs ORDER BY target_username").fetchall()
    assert job_rows == [("alpha.account", "completed"), ("beta.account", "completed")]
    account_rows = conn.execute(
        "SELECT instagram_username, dominant_detected_org, dominant_threat_level, last_ingested_run_id "
        "FROM instagram_accounts ORDER BY instagram_username"
    ).fetchall()
    assert account_rows == [
        ("alpha.account", "PKK/KCK", "orta", "20260408T100000Z"),
        ("beta.account", "YPG/PYD", "dusuk", "20260408T110000Z"),
    ]
    conn.close()


def test_batch_jobs_create_overview_and_auto_follow_up(tmp_path: Path) -> None:
    db_path = tmp_path / "batch.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = {}
    objects.update(_build_account_archive_objects("cdk.liege", "20260408T130000Z", "ROOTPOST", "flagged.user"))
    objects.update(_build_account_archive_objects("flagged.user", "20260408T131500Z", "FOLLOWPOST", "neutral.friend"))
    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    settings = Settings(
        minio_bucket_default="instagram-archive",
        minio_bucket_fallback="instagram_archive",
        ingest_max_concurrent_accounts=2,
        ingest_max_concurrent_comments=2,
        ingest_max_concurrent_media_per_post=2,
    )

    class BatchRoutingVLLMService:
        default_model = "gemma-4-31b-it"

        def __init__(self) -> None:
            self.followup_prompts: list[str] = []

        def build_payload(
            self,
            description: str,
            media_type: str,
            media_url: str,
            max_tokens: int,
            model: str | None = None,
            media_items: list[dict[str, str]] | None = None,
        ):
            return {
                "model": model or self.default_model,
                "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
                "max_tokens": max_tokens,
                "stream": False,
                "_media_type": media_type,
                "_media_url": media_url,
                "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
            }

        def create_chat_completion(self, payload):
            user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
            message = user_message["content"]
            if isinstance(message, list):
                text = message[0]["text"]
            else:
                text = str(message)

            if "Comment owner: flagged.user" in text:
                content = '{"comment_type":"support","content_summary_tr":"destekliyor","sentiment":"positive","flags":{"active_supporter":{"flag":true,"reason_tr":"açık destek"},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":8,"confidence":"medium","reason_tr":"destekliyor"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"yes","reason_tr":"destek dili"},"overall_risk":{"level":"high","human_review_required":"yes"},"review":{"importance_score":8,"priority_level":"high","human_review_required":"yes","confidence":"medium","reason":"destekliyor"}}'
            elif "Comment owner: neutral.friend" in text:
                content = '{"comment_type":"neutral","content_summary_tr":"nötr","sentiment":"neutral","flags":{"active_supporter":{"flag":false,"reason_tr":""},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":0,"confidence":"low","reason_tr":"nötr"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"no","reason_tr":"tekil"},"overall_risk":{"level":"low","human_review_required":"no"},"review":{"importance_score":1,"priority_level":"low","human_review_required":"no","confidence":"low","reason":"nötr"}}'
            elif "final persistent profile summary" in text and "cdk.liege" in text:
                content = "cdk liege summary"
            elif "final persistent profile summary" in text and "flagged.user" in text:
                content = "flagged user summary"
            elif "Candidate under review:" in text and "Candidate username: flagged.user" in text:
                self.followup_prompts.append(text)
                content = '{"candidate_username":"flagged.user","relationship_to_seed":"supporter","relationship_strength":"high","risk_level":"high","primary_entity":"PKK","secondary_entities":["PKK/KCK"],"trigger_signals":["acik destek","flagli yorum"],"branch_recommended":"yes","priority_rank":1,"reason_tr":"Seed hesaba acik destek veren ve takibe deger bir aday."}'
            elif "caption for cdk.liege" in text:
                content = '{"content_types":["propaganda"],"primary_theme":["organizational_symbolism"],"summary_tr":"Root post","language_and_tone":{"dominant_language":"tr","tone":"mobilizing","sloganized_language":"yes"},"risk_indicators":{"direct_support_expression":{"status":"present","evidence":["destek"]},"organizational_symbol_use":{"status":"present","evidence":["simge"]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[{"entity":"PKK/KCK","relationship_type":"direct_support","confidence":"medium","reason_tr":"destek dili"}],"organization_link_score":7,"confidence":"medium"},"profile_role_estimate":{"role":"supporter","reason_tr":"açık destek"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tek post"},"review_priority":{"importance_score":7,"priority_level":"high","human_review_required":"yes","reason_tr":"inceleme gerekli"},"analyst_note_tr":"root note"}'
            elif "caption for flagged.user" in text:
                content = '{"content_types":["personal_daily"],"primary_theme":["unclear"],"summary_tr":"Follow up post","language_and_tone":{"dominant_language":"tr","tone":"neutral","sloganized_language":"no"},"risk_indicators":{"direct_support_expression":{"status":"absent","evidence":[]},"organizational_symbol_use":{"status":"absent","evidence":[]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[],"organization_link_score":0,"confidence":"low"},"profile_role_estimate":{"role":"unclear","reason_tr":"belirsiz"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tekil"},"review_priority":{"importance_score":3,"priority_level":"low","human_review_required":"no","reason_tr":"düşük"},"analyst_note_tr":"follow note"}'
            else:
                raise AssertionError(f"Unexpected payload: {text}")

            return {
                "model": payload.get("model", self.default_model),
                "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            }

        @staticmethod
        def extract_answer(chat_response):
            return (
                chat_response["model"],
                chat_response["choices"][0]["message"]["content"],
                chat_response.get("usage"),
                chat_response["choices"][0].get("finish_reason"),
            )

    batch = create_batch_jobs(
        request=BatchJobsCreateRequest(
            targets=["https://www.instagram.com/cdk.liege/", "cdk.liege"],
            mode="all",
            country="be",
            focus_entity="PKK",
            auto_enqueue_followups=True,
        ),
        settings=settings,
        minio_service=minio,
        db_service=db_service,
    )

    assert batch.batch_job.mode == "all"
    assert batch.batch_job.total_targets == 1
    assert [item.normalized_username for item in batch.targets] == ["cdk.liege"]
    assert [item.target_username for item in batch.ingest_jobs] == ["cdk.liege"]

    vllm = BatchRoutingVLLMService()
    result = run_ingest_workers_once(
        request=IngestWorkersRunOnceRequest(max_jobs=2, lease_owner="batch-worker"),
        settings=settings,
        minio_service=minio,
        vllm_service=vllm,
        db_service=db_service,
    )

    assert result.claimed_jobs == 2
    assert result.completed_jobs == 2
    assert result.failed_jobs == 0
    assert {item.target_username for item in result.items} == {"cdk.liege", "flagged.user"}

    overview = get_jobs_overview(limit=10, db_service=db_service)
    assert overview.batches[0].status == "completed"
    assert {item.normalized_username for item in overview.targets} == {"cdk.liege", "flagged.user"}
    assert any(item.source_kind == "followup" for item in overview.targets)
    assert any(item["commenter_username"] == "flagged.user" for item in overview.review_queue)
    assert len(vllm.followup_prompts) == 1
    assert "flagged.user" in vllm.followup_prompts[0]

    conn = sqlite3.connect(str(db_path))
    job_rows = conn.execute(
        "SELECT target_username, source_kind, parent_username, focus_entity, country, status "
        "FROM ingest_jobs ORDER BY id"
    ).fetchall()
    assert job_rows == [
        ("cdk.liege", "initial", None, "PKK", "be", "completed"),
        ("flagged.user", "followup", "cdk.liege", "PKK", "be", "completed"),
    ]
    target_rows = conn.execute(
        "SELECT normalized_username, source_kind, parent_username, status "
        "FROM batch_job_targets ORDER BY id"
    ).fetchall()
    assert target_rows == [
        ("cdk.liege", "initial", None, "completed"),
        ("flagged.user", "followup", "cdk.liege", "completed"),
    ]
    batch_status = conn.execute("SELECT status FROM batch_jobs").fetchone()[0]
    assert batch_status == "completed"
    conn.close()


def test_batch_followup_candidates_are_suggested_without_auto_enqueue(tmp_path: Path) -> None:
    db_path = tmp_path / "batch_suggest.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    objects = {}
    objects.update(_build_account_archive_objects("cdk.liege", "20260408T130000Z", "ROOTPOST", "flagged.user"))
    objects.update(_build_account_archive_objects("flagged.user", "20260408T131500Z", "FOLLOWPOST", "neutral.friend"))
    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    settings = Settings(
        minio_bucket_default="instagram-archive",
        minio_bucket_fallback="instagram_archive",
        ingest_max_concurrent_accounts=2,
        ingest_max_concurrent_comments=2,
        ingest_max_concurrent_media_per_post=2,
    )

    class SuggestOnlyVLLMService:
        default_model = "gemma-4-31b-it"

        def __init__(self) -> None:
            self.followup_prompts: list[str] = []

        def build_payload(
            self,
            description: str,
            media_type: str,
            media_url: str,
            max_tokens: int,
            model: str | None = None,
            media_items: list[dict[str, str]] | None = None,
        ):
            return {
                "model": model or self.default_model,
                "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
                "max_tokens": max_tokens,
                "stream": False,
                "_media_type": media_type,
                "_media_url": media_url,
                "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
            }

        def create_chat_completion(self, payload):
            user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
            message = user_message["content"]
            text = message[0]["text"] if isinstance(message, list) else str(message)

            if "Comment owner: flagged.user" in text:
                content = '{"comment_type":"support","content_summary_tr":"destekliyor","sentiment":"positive","flags":{"active_supporter":{"flag":true,"reason_tr":"açık destek"},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":8,"confidence":"medium","reason_tr":"destekliyor"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"yes","reason_tr":"destek dili"},"overall_risk":{"level":"high","human_review_required":"yes"},"review":{"importance_score":8,"priority_level":"high","human_review_required":"yes","confidence":"medium","reason":"destekliyor"}}'
            elif "final persistent profile summary" in text and "cdk.liege" in text:
                content = "cdk liege summary"
            elif "Candidate under review:" in text and "Candidate username: flagged.user" in text:
                self.followup_prompts.append(text)
                content = '{"candidate_username":"flagged.user","relationship_to_seed":"supporter","relationship_strength":"high","risk_level":"high","primary_entity":"PKK","secondary_entities":["PKK/KCK"],"trigger_signals":["acik destek","flagli yorum"],"branch_recommended":"yes","priority_rank":1,"reason_tr":"Seed hesaba acik destek veren ve takibe deger bir aday."}'
            elif "caption for cdk.liege" in text:
                content = '{"content_types":["propaganda"],"primary_theme":["organizational_symbolism"],"summary_tr":"Root post","language_and_tone":{"dominant_language":"tr","tone":"mobilizing","sloganized_language":"yes"},"risk_indicators":{"direct_support_expression":{"status":"present","evidence":["destek"]},"organizational_symbol_use":{"status":"present","evidence":["simge"]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[{"entity":"PKK/KCK","relationship_type":"direct_support","confidence":"medium","reason_tr":"destek dili"}],"organization_link_score":7,"confidence":"medium"},"profile_role_estimate":{"role":"supporter","reason_tr":"açık destek"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tek post"},"review_priority":{"importance_score":7,"priority_level":"high","human_review_required":"yes","reason_tr":"inceleme gerekli"},"analyst_note_tr":"root note"}'
            else:
                raise AssertionError(f"Unexpected payload: {text}")

            return {
                "model": payload.get("model", self.default_model),
                "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            }

        @staticmethod
        def extract_answer(chat_response):
            return (
                chat_response["model"],
                chat_response["choices"][0]["message"]["content"],
                chat_response.get("usage"),
                chat_response["choices"][0].get("finish_reason"),
            )

    create_batch_jobs(
        request=BatchJobsCreateRequest(
            targets=["cdk.liege"],
            mode="all",
            country="be",
            focus_entity="PKK",
        ),
        settings=settings,
        minio_service=minio,
        db_service=db_service,
    )

    vllm = SuggestOnlyVLLMService()
    result = run_ingest_workers_once(
        request=IngestWorkersRunOnceRequest(max_jobs=2, lease_owner="suggest-worker"),
        settings=settings,
        minio_service=minio,
        vllm_service=vllm,
        db_service=db_service,
    )

    assert result.claimed_jobs == 1
    assert result.completed_jobs == 1
    assert {item.target_username for item in result.items} == {"cdk.liege"}
    assert len(vllm.followup_prompts) == 1

    overview = get_jobs_overview(limit=10, db_service=db_service)
    followup_target = next(item for item in overview.targets if item.normalized_username == "flagged.user")
    assert followup_target.status == "suggested"

    conn = sqlite3.connect(str(db_path))
    ingest_rows = conn.execute(
        "SELECT target_username, source_kind, status FROM ingest_jobs ORDER BY id"
    ).fetchall()
    target_rows = conn.execute(
        "SELECT normalized_username, source_kind, status, note FROM batch_job_targets ORDER BY id"
    ).fetchall()
    review_rows = conn.execute(
        "SELECT commenter_username, flag_reason_type, last_reason FROM review_queue ORDER BY commenter_username"
    ).fetchall()
    conn.close()

    assert ingest_rows == [("cdk.liege", "initial", "completed")]
    assert target_rows[0][0:3] == ("cdk.liege", "initial", "completed")
    assert target_rows[1][0:3] == ("flagged.user", "followup", "suggested")
    assert "LLM follow-up" in target_rows[1][3]
    assert ("flagged.user", "followup_suggested", target_rows[1][3]) in review_rows


def test_batch_followup_candidate_can_be_skipped_by_llm_decision(tmp_path: Path) -> None:
    db_path = tmp_path / "batch_skip.db"
    db_service = DatabaseService(str(db_path))
    db_service.init_schema()

    username = "seed.account"
    run_id = "20260408T140000Z"
    post_id = "ROOTPOST2"
    objects = _build_account_archive_objects(username, run_id, post_id, "repeat.friend")
    comments_key = f"instagram/{username}/{run_id}/posts/{post_id}/comments.jsonl"
    comments = [
        {
            "commenter_username": "repeat.friend",
            "commenter_profile_url": "https://ig/repeat.friend",
            "comment_text": "merhaba",
            "source_post_id": post_id,
            "source_post_url": f"https://www.instagram.com/{username}/p/{post_id}/",
            "discovered_at": "2026-04-08T10:59:51+00:00",
            "run_id": run_id,
        },
        {
            "commenter_username": "repeat.friend",
            "commenter_profile_url": "https://ig/repeat.friend",
            "comment_text": "nasılsın",
            "source_post_id": post_id,
            "source_post_url": f"https://www.instagram.com/{username}/p/{post_id}/",
            "discovered_at": "2026-04-08T10:59:52+00:00",
            "run_id": run_id,
        },
    ]
    objects[comments_key] = "\n".join(json.dumps(item) for item in comments)
    minio = FakeMinioArchiveService(bucket="instagram-archive", objects=objects)
    settings = Settings(
        minio_bucket_default="instagram-archive",
        minio_bucket_fallback="instagram_archive",
        ingest_max_concurrent_accounts=1,
        ingest_max_concurrent_comments=2,
        ingest_max_concurrent_media_per_post=1,
    )

    class SkipFollowupVLLMService:
        default_model = "gemma-4-31b-it"

        def build_payload(
            self,
            description: str,
            media_type: str,
            media_url: str,
            max_tokens: int,
            model: str | None = None,
            media_items: list[dict[str, str]] | None = None,
        ):
            return {
                "model": model or self.default_model,
                "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
                "max_tokens": max_tokens,
                "stream": False,
                "_media_type": media_type,
                "_media_url": media_url,
                "_media_items": media_items or [{"media_type": media_type, "media_url": media_url}],
            }

        def create_chat_completion(self, payload):
            user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
            content = user_message["content"]
            text = content[0]["text"] if isinstance(content, list) else str(content)
            if "Comment owner: repeat.friend" in text:
                body = '{"comment_type":"neutral","content_summary_tr":"selamlasma","sentiment":"neutral","flags":{"active_supporter":{"flag":false,"reason_tr":""},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":0,"confidence":"low","reason_tr":"nötr"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"no","reason_tr":"gündelik"},"overall_risk":{"level":"low","human_review_required":"no"},"review":{"importance_score":1,"priority_level":"low","human_review_required":"no","confidence":"low","reason":"nötr"}}'
            elif "Candidate under review:" in text and "Candidate username: repeat.friend" in text:
                body = '{"candidate_username":"repeat.friend","relationship_to_seed":"unclear","relationship_strength":"low","risk_level":"low","primary_entity":"unclear","secondary_entities":[],"trigger_signals":["tekrarlayan nötr yorum"],"branch_recommended":"no","priority_rank":5,"reason_tr":"Yalnızca gündelik ve nötr tekrar eden yorumlar görülüyor."}'
            elif "final persistent profile summary" in text and "seed.account" in text:
                body = "seed summary"
            elif "caption for seed.account" in text:
                body = '{"content_types":["personal_daily"],"primary_theme":["unclear"],"summary_tr":"Seed post","language_and_tone":{"dominant_language":"tr","tone":"neutral","sloganized_language":"no"},"risk_indicators":{"direct_support_expression":{"status":"absent","evidence":[]},"organizational_symbol_use":{"status":"absent","evidence":[]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[],"organization_link_score":0,"confidence":"low"},"profile_role_estimate":{"role":"unclear","reason_tr":"belirsiz"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tekil"},"review_priority":{"importance_score":2,"priority_level":"low","human_review_required":"no","reason_tr":"düşük"},"analyst_note_tr":"seed note"}'
            else:
                raise AssertionError(f"Unexpected payload: {text}")
            return {
                "model": payload.get("model", self.default_model),
                "choices": [{"message": {"content": body}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10},
            }

        @staticmethod
        def extract_answer(chat_response):
            return (
                chat_response["model"],
                chat_response["choices"][0]["message"]["content"],
                chat_response.get("usage"),
                chat_response["choices"][0].get("finish_reason"),
            )

    create_batch_jobs(
        request=BatchJobsCreateRequest(
            targets=[username],
            mode="all",
            country="be",
            focus_entity="PKK",
        ),
        settings=settings,
        minio_service=minio,
        db_service=db_service,
    )

    result = run_ingest_workers_once(
        request=IngestWorkersRunOnceRequest(max_jobs=1, lease_owner="skip-worker"),
        settings=settings,
        minio_service=minio,
        vllm_service=SkipFollowupVLLMService(),
        db_service=db_service,
    )

    assert result.claimed_jobs == 1
    assert result.completed_jobs == 1

    conn = sqlite3.connect(str(db_path))
    target_rows = conn.execute(
        "SELECT normalized_username, source_kind, status, note FROM batch_job_targets ORDER BY id"
    ).fetchall()
    ingest_rows = conn.execute(
        "SELECT target_username, source_kind, status FROM ingest_jobs ORDER BY id"
    ).fetchall()
    conn.close()

    assert ingest_rows == [("seed.account", "initial", "completed")]
    assert target_rows[0][0:3] == ("seed.account", "initial", "completed")
    assert target_rows[1][0:3] == ("repeat.friend", "followup", "skipped")
    assert "LLM follow-up" in target_rows[1][3]
