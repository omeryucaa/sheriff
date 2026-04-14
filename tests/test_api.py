import pytest
from fastapi import HTTPException

from app.main import analyze_account_graph, analyze_media, analyze_post_and_comments, save_account_graph_capture
from app.schemas import AnalyzeGraphRequest, AnalyzeMediaRequest, AnalyzePostAndCommentsRequest, CommentInput, SaveGraphCaptureRequest
from app.vllm_service import VLLMUpstreamError


class FakeMinioService:
    def presigned_get_object(self, bucket: str, object_key: str, expires_seconds: int) -> str:
        return f"https://minio.local/{bucket}/{object_key}?exp={expires_seconds}"


class FakeVLLMService:
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
        return {
            "model": payload["model"],
            "choices": [{"message": {"content": "analysis done"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 20, "completion_tokens": 10},
        }

    @staticmethod
    def extract_answer(chat_response):
        return (
            chat_response["model"],
            chat_response["choices"][0]["message"]["content"],
            chat_response.get("usage"),
            chat_response["choices"][0].get("finish_reason"),
        )


class FailingVLLMService(FakeVLLMService):
    def create_chat_completion(self, payload):
        raise VLLMUpstreamError(status_code=403, message="Forbidden presigned URL")


class SequencedVLLMService(FakeVLLMService):
    def __init__(self, replies: list[str]) -> None:
        self._replies = replies[:]

    def create_chat_completion(self, payload):
        if not self._replies:
            raise AssertionError("No reply left in SequencedVLLMService")
        content = self._replies.pop(0)
        return {
            "model": payload.get("model", self.default_model),
            "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 30, "completion_tokens": 20},
        }


class PromptInspectingVLLMService(FakeVLLMService):
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def create_chat_completion(self, payload):
        self.payloads.append(payload)
        user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
        content = user_message["content"]
        text = content[0]["text"] if isinstance(content, list) else str(content)
        if "Comment owner:" in text:
            body = '{"comment_type":"support","content_summary_tr":"yorum mevcut","sentiment":"positive","flags":{"active_supporter":{"flag":false,"reason_tr":"pasif destek"},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":5,"confidence":"low","reason_tr":"yorum mevcut"},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"no","reason_tr":"tekil yorum"},"overall_risk":{"level":"medium","human_review_required":"no"},"review":{"importance_score":3,"priority_level":"medium","human_review_required":"no","confidence":"low","reason":"yorum mevcut"}}'
        elif "MEVCUT PROFİL ÖZETİ" in text:
            body = "Profil özeti."
        else:
            body = '{"content_types":["propaganda"],"primary_theme":["organizational_symbolism"],"summary_tr":"Gonderi ozeti X","language_and_tone":{"dominant_language":"tr","tone":"mobilizing","sloganized_language":"yes"},"risk_indicators":{"direct_support_expression":{"status":"present","evidence":["slogan"]},"organizational_symbol_use":{"status":"present","evidence":["logo"]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"present","evidence":["Daltons"]}},"organization_assessment":{"aligned_entities":[{"entity":"Daltons","relationship_type":"symbolic_affinity","confidence":"medium","reason_tr":"tekrar eden aidiyet"}],"organization_link_score":7,"confidence":"medium"},"profile_role_estimate":{"role":"sympathizer","reason_tr":"tekrar eden aidiyet"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"tek post"},"review_priority":{"importance_score":7,"priority_level":"high","human_review_required":"yes","reason_tr":"inceleme gerekli"},"analyst_note_tr":"analist notu"}'
        return {
            "model": payload.get("model", self.default_model),
            "choices": [{"message": {"content": body}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 30, "completion_tokens": 20},
        }


class FakeDatabaseService:
    def __init__(self) -> None:
        self._summary = ""
        self._graph_analysis = ""
        self._graph_model = ""
        self._graph_updated_at = ""
        self._graph_capture_path = ""
        self._graph_capture_updated_at = ""

    def get_or_create_person_account(
        self,
        person_name: str,
        instagram_username: str,
        profile_photo_url: str | None,
        bio: str | None,
    ):
        return 101, 201

    def get_account_profile_summary(self, instagram_account_id: int) -> str:
        return self._summary

    def get_post_history_summaries(self, instagram_account_id: int) -> list[dict]:
        return []

    def get_commenter_history(self, commenter_username: str | None) -> list[dict]:
        return []

    def update_account_profile_summary(self, instagram_account_id: int, summary: str | None) -> None:
        self._summary = summary or ""

    def get_account_detail(self, account_id: int) -> dict | None:
        return {
            "id": account_id,
            "instagram_username": "ali.ig",
            "bio": "bio",
            "account_profile_summary": self._summary,
            "post_count": 2,
            "comment_count": 3,
            "flagged_comment_count": 1,
            "baskin_kategori": "propaganda",
            "tehdit_seviyesi": "orta",
            "tespit_edilen_orgut": "PKK/KCK",
        }

    def list_account_posts(self, account_id: int) -> list[dict]:
        return [
            {"icerik_kategorisi": ["propaganda"], "tehdit_seviyesi": "orta", "tespit_edilen_orgut": "PKK/KCK"},
            {"icerik_kategorisi": ["medya_kultur"], "tehdit_seviyesi": "dusuk", "tespit_edilen_orgut": "belirsiz"},
        ]

    def list_account_comments(self, account_id: int, verdict: str | None = None, flagged_only: bool = False) -> list[dict]:
        return [
            {"commenter_username": "u1", "verdict": "destekci_aktif", "bayrak": True, "orgut_baglanti_skoru": 8},
            {"commenter_username": "u1", "verdict": "destekci_pasif", "bayrak": False, "orgut_baglanti_skoru": 4},
            {"commenter_username": "u2", "verdict": "alakasiz", "bayrak": False, "orgut_baglanti_skoru": 1},
        ]

    def get_account_graph(self, account_id: int) -> dict:
        return {"nodes": [{"id": "account-1", "type": "account", "label": "ali.ig"}], "edges": [{"id": "e1", "source": "a", "target": "b", "type": "linked_to"}]}

    def update_account_graph_analysis(self, instagram_account_id: int, analysis: str | None, model: str | None) -> None:
        self._graph_analysis = analysis or ""
        self._graph_model = model or ""
        self._graph_updated_at = "2026-04-09 10:00:00"

    def get_account_graph_analysis(self, instagram_account_id: int) -> dict[str, str]:
        return {
            "analysis": self._graph_analysis,
            "model": self._graph_model,
            "updated_at": self._graph_updated_at,
        }

    def update_account_graph_capture(self, instagram_account_id: int, path: str | None) -> None:
        self._graph_capture_path = path or ""
        self._graph_capture_updated_at = "2026-04-09 10:05:00"

    def get_account_graph_capture(self, instagram_account_id: int) -> dict[str, str]:
        return {
          "path": self._graph_capture_path,
          "updated_at": self._graph_capture_updated_at,
        }

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
        comment_analyses: list[dict[str, object | None]],
    ):
        class _Result:
            person_id = 101
            instagram_account_id = 201
            post_id = 301
            comment_ids = [401 + i for i in range(len(comment_analyses))]

        return _Result()


def test_analyze_media_image_success() -> None:
    response = analyze_media(
        request=AnalyzeMediaRequest(
            bucket="uploads",
            object_key="img/1.png",
            description="Bu görseli analiz et",
            media_type="image",
            expires_seconds=900,
            max_tokens=256,
        ),
        minio_service=FakeMinioService(),
        vllm_service=FakeVLLMService(),
    )

    assert response.model == "gemma-4-31b-it"
    assert response.answer == "analysis done"
    assert response.media_url.startswith("https://minio.local/uploads/img/1.png")


def test_analyze_media_video_success() -> None:
    response = analyze_media(
        request=AnalyzeMediaRequest(
            bucket="uploads",
            object_key="video/1.mp4",
            description="Bu videoyu analiz et",
            media_type="video",
            expires_seconds=900,
            max_tokens=256,
        ),
        minio_service=FakeMinioService(),
        vllm_service=FakeVLLMService(),
    )

    assert response.answer == "analysis done"
    assert response.media_url.startswith("https://minio.local/uploads/video/1.mp4")


def test_analyze_media_expired_or_invalid_url_error() -> None:
    with pytest.raises(HTTPException) as exc:
        analyze_media(
            request=AnalyzeMediaRequest(
                bucket="uploads",
                object_key="video/expired.mp4",
                description="Bu videoyu analiz et",
                media_type="video",
                expires_seconds=60,
                max_tokens=256,
            ),
            minio_service=FakeMinioService(),
            vllm_service=FailingVLLMService(),
        )

    assert exc.value.status_code == 502
    assert exc.value.detail["upstream_status"] == 403
    assert "Forbidden" in exc.value.detail["upstream_error"]


def test_analyze_post_and_comments_success() -> None:
    vllm = SequencedVLLMService(
        replies=[
            '{"ozet":"Gonderi ozeti X","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":7}',
            '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"Gonderiyi acikca destekliyor."}',
            '{"verdict":"alakasiz","sentiment":"neutral","orgut_baglanti_skoru":0,"bayrak":false,"reason":"Konu disi yorum."}',
            "Hesap son donemde propaganda nitelikli paylasimlarini artirmistir.",
        ]
    )
    response = analyze_post_and_comments(
        request=AnalyzePostAndCommentsRequest(enable_deep_media_analysis=False, 
            username="ali",
            instagram_username="ali.ig",
            bio="Test bio",
            caption="Test caption",
            media_type="video",
            media_url="https://example.com/video.mp4",
            comments=[
                CommentInput(commenter_username="u1", text="Helal olsun"),
                CommentInput(commenter_username="u2", text="Bugün hava güzel"),
            ],
        ),
        minio_service=FakeMinioService(),
        vllm_service=vllm,
        db_service=FakeDatabaseService(),
    )

    assert response.model == "gemma-4-31b-it"
    assert response.person_id == 101
    assert response.instagram_account_id == 201
    assert response.post_id == 301
    assert response.comment_ids == [401, 402]
    assert response.media_url == "https://example.com/video.mp4"
    assert '"ozet": "Gonderi ozeti X"' in response.post_analysis
    assert len(response.comment_analyses) == 2
    assert response.comment_analyses[0].verdict == "destekci_aktif"
    assert response.comment_analyses[0].bayrak is True
    assert response.comment_analyses[1].verdict == "alakasiz"
    assert response.summary["destekci_aktif"] == 1
    assert response.summary["alakasiz"] == 1
    assert response.summary["belirsiz"] == 0


def test_analyze_post_and_comments_deep_media_mode_persists_observation() -> None:
    class CapturingDatabaseService(FakeDatabaseService):
        def __init__(self) -> None:
            super().__init__()
            self.captured_media_observations: list[dict[str, object]] = []

        def persist_post_and_comments(self, **kwargs):
            self.captured_media_observations = list(kwargs.get("media_observations") or [])

            class _Result:
                person_id = 101
                instagram_account_id = 201
                post_id = 301
                comment_ids = [401]

            return _Result()

    vllm = SequencedVLLMService(
        replies=[
            '{"media_type":"image","scene_summary":"street scene","setting_type":"street","visible_person_count":"2","face_visibility":"open","clothing_types":["civilian"],"notable_objects":["car"],"weapon_presence":{"status":"no","types":[]},"symbols_or_logos":[],"visible_text_items":[{"text":"34 ABC 123","language":"tr"}],"activity_type":["daily_life"],"crowd_level":"small_group","audio_elements":{"speech":"absent","music":"absent","chanting":"absent","gunfire_or_blast":"absent"},"child_presence":"no","institutional_markers":[],"vehicles":["car"],"license_or_signage":["34 ABC 123"],"deep_review_hint":{"run_deep_analysis":"yes","confidence":"high","reason_tr":"plaka ve konum sinyali"}}',
            '{"location_assessment":{"location_identifiable":"yes","location_confidence":"high","candidate_location_text":"Istanbul","evidence":["sokak tabelasi"]},"vehicle_plate_assessment":{"vehicle_present":"yes","vehicles":["car"],"plate_visible":"yes","plate_text_candidates":["34 ABC 123"],"evidence":["plaka"]},"sensitive_information":[{"type":"location","value":"Istanbul","confidence":"high","reason_tr":"tabela"}],"followup_priority":"high","analyst_note_tr":"detaylar mevcut"}',
            '{"ozet":"Gonderi ozeti X","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":7}',
            '{"verdict":"destekci_aktif","sentiment":"positive","orgut_baglanti_skoru":8,"bayrak":true,"reason":"Gonderiyi acikca destekliyor."}',
            "Hesap son donemde propaganda nitelikli paylasimlarini artirmistir.",
        ]
    )
    db = CapturingDatabaseService()
    response = analyze_post_and_comments(
        request=AnalyzePostAndCommentsRequest(
            username="ali",
            instagram_username="ali.ig",
            bio="Test bio",
            caption="Test caption",
            media_type="video",
            media_url="https://example.com/video.mp4",
            comments=[CommentInput(commenter_username="u1", text="Helal olsun")],
            enable_deep_media_analysis=True,
        ),
        minio_service=FakeMinioService(),
        vllm_service=vllm,
        db_service=db,
    )

    assert response.summary["destekci_aktif"] == 1
    assert len(db.captured_media_observations) == 1
    saved = db.captured_media_observations[0]
    assert saved["deep_required"] is True
    assert saved["deep_status"] == "completed"
    assert saved["contains_plate"] is True


def test_analyze_post_and_comments_non_json_fallback() -> None:
    vllm = SequencedVLLMService(
        replies=[
            "Gonderi ozeti: X",
            "Bu yorum biraz karisik.",
            "Bu hesap icin yeni bir profil ozeti yok.",
        ]
    )
    response = analyze_post_and_comments(
        request=AnalyzePostAndCommentsRequest(enable_deep_media_analysis=False, 
            username="ali",
            media_type="image",
            bucket="uploads",
            object_key="img/1.png",
            comments=[CommentInput(commenter_username="u1", text="hmm")],
        ),
        minio_service=FakeMinioService(),
        vllm_service=vllm,
        db_service=FakeDatabaseService(),
    )

    assert response.media_url.startswith("https://minio.local/uploads/img/1.png")
    assert response.comment_analyses[0].verdict == "belirsiz"
    assert response.comment_analyses[0].sentiment == "neutral"
    assert response.comment_analyses[0].orgut_baglanti_skoru == 0
    assert response.summary["belirsiz"] == 1


def test_analyze_post_and_comments_does_not_refine_unclear_comment_app_side() -> None:
    vllm = SequencedVLLMService(
        replies=[
            '{"ozet":"Gonderi ozeti X","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":7}',
            '{"comment_type":"unclear","content_summary_tr":"","sentiment":"neutral","flags":{"active_supporter":{"flag":false,"reason_tr":""},"threat":{"flag":false,"reason_tr":""},"information_leak":{"flag":false,"reason_tr":""},"coordination":{"flag":false,"reason_tr":""},"hate_speech":{"flag":false,"reason_tr":""}},"organization_link_assessment":{"organization_link_score":0,"confidence":"low","reason_tr":""},"behavior_pattern":{"consistent_with_history":"unclear","repeated_support_language":"unclear","reason_tr":""},"overall_risk":{"level":"low","human_review_required":"no"},"review":{"importance_score":1,"priority_level":"low","human_review_required":"no","confidence":"low","reason":""}}',
            "Profil özeti.",
        ]
    )
    response = analyze_post_and_comments(
        request=AnalyzePostAndCommentsRequest(enable_deep_media_analysis=False, 
            username="ali",
            media_type="image",
            media_url="https://example.com/img.jpg",
            comments=[CommentInput(commenter_username="u1", text="Helal olsun")],
        ),
        minio_service=FakeMinioService(),
        vllm_service=vllm,
        db_service=FakeDatabaseService(),
    )

    assert response.comment_analyses[0].verdict == "belirsiz"
    assert response.comment_analyses[0].sentiment == "neutral"
    assert response.summary["belirsiz"] == 1


def test_analyze_post_and_comments_supports_focus_entity_and_same_commenter_batch_context() -> None:
    vllm = PromptInspectingVLLMService()
    response = analyze_post_and_comments(
        request=AnalyzePostAndCommentsRequest(enable_deep_media_analysis=False, 
            username="ali",
            instagram_username="ali.ig",
            bio="Test bio",
            caption="Test caption",
            focus_entity="Daltonlar",
            media_type="video",
            media_url="https://example.com/video.mp4",
            comments=[
                CommentInput(commenter_username="u1", text="ilk yorum"),
                CommentInput(commenter_username="u1", text="ikinci yorum"),
            ],
        ),
        minio_service=FakeMinioService(),
        vllm_service=vllm,
        db_service=FakeDatabaseService(),
    )

    assert len(response.comment_analyses) == 2
    comment_texts = []
    for payload in vllm.payloads:
        user_message = next(message for message in reversed(payload["messages"]) if message["role"] == "user")
        content = user_message["content"]
        text = content[0]["text"] if isinstance(content, list) else str(content)
        if "Comment owner:" in text:
            comment_texts.append(text)
    assert any("Investigative focus entity: Daltons" in text for text in comment_texts)
    assert any("Aynı gönderideki diğer yorum" in text for text in comment_texts)


def test_analyze_account_graph_persists_result() -> None:
    db = FakeDatabaseService()
    response = analyze_account_graph(
        account_id=201,
        request=AnalyzeGraphRequest(),
        vllm_service=SequencedVLLMService(["Graf ana olarak PKK/KCK sinyallerinin propaganda ve destekci yorumlarla yogunlastigini gosteriyor."]),
        db_service=db,
    )

    assert response.account_id == 201
    assert response.model == "gemma-4-31b-it"
    assert "PKK/KCK" in response.analysis
    assert response.updated_at == "2026-04-09 10:00:00"
    assert db.get_account_graph_analysis(201)["analysis"] == response.analysis


def test_save_account_graph_capture_persists_file_reference(tmp_path) -> None:
    from app.settings import Settings

    db = FakeDatabaseService()
    png_data_url = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wn7L8sAAAAASUVORK5CYII="
    response = save_account_graph_capture(
        account_id=201,
        request=SaveGraphCaptureRequest(graph_image_data_url=png_data_url),
        settings=Settings(sqlite_db_path=str(tmp_path / "app.db")),
        db_service=db,
    )

    assert response.account_id == 201
    assert response.capture_url == "/captures/account_201_graph.png"
    assert response.updated_at == "2026-04-09 10:05:00"
    assert db.get_account_graph_capture(201)["path"] == "/captures/account_201_graph.png"
