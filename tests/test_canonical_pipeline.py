import json
import sqlite3
from pathlib import Path

import pytest

from app.adapters.legacy_projection import legacy_comment_from_canonical
from app.pipeline.helpers import (
    evaluate_media_deep_requirement,
    parse_comment_analysis_canonical,
    parse_media_observation,
    validate_comment_analysis_canonical,
)
from app.pipeline.run_comment_stage import execute_comment_stage
from app.pipeline.run_post_stage import execute_post_stage
from app.prompts import get_default_prompt_templates
from app.services.normalization_service import NormalizationService
from app.services.review_service import ReviewService
from app.services.scoring_service import ScoringService
from app.services.stage_executor import StageExecutor
from app.storage.database_service import DatabaseService
from app.utils.json_extract import extract_json_fragment


class SequencedVLLMService:
    default_model = "gemma-4-31b-it"

    def __init__(self, replies: list[str]) -> None:
        self.replies = replies[:]

    def build_payload(self, description: str, media_type: str, media_url: str, max_tokens: int, model: str | None = None, media_items=None):
        return {
            "model": model or self.default_model,
            "messages": [{"role": "user", "content": [{"type": "text", "text": description}]}],
            "max_tokens": max_tokens,
            "stream": False,
        }

    def create_chat_completion(self, payload):
        if not self.replies:
            raise AssertionError("No reply left")
        content = self.replies.pop(0)
        return {
            "model": payload.get("model", self.default_model),
            "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }

    @staticmethod
    def extract_answer(chat_response):
        return (
            chat_response["model"],
            chat_response["choices"][0]["message"]["content"],
            chat_response.get("usage"),
            chat_response["choices"][0].get("finish_reason"),
        )


class RecordingDB:
    def __init__(self) -> None:
        self.records = []

    def record_llm_stage_attempt(self, payload: dict[str, object]) -> None:
        self.records.append(payload)


class RecordingTraceLogger:
    def __init__(self) -> None:
        self.entries: list[tuple[str, object]] = []

    def log(self, title: str, content: object) -> None:
        self.entries.append((title, content))


class PayloadRecordingVLLMService(SequencedVLLMService):
    def __init__(self, replies: list[str]) -> None:
        super().__init__(replies)
        self.payloads = []

    def create_chat_completion(self, payload):
        self.payloads.append(payload)
        return super().create_chat_completion(payload)


def test_prompt_registry_preserves_legacy_keys_and_adds_new_keys() -> None:
    keys = {item["key"] for item in get_default_prompt_templates()}
    assert {"post_analysis", "media_analysis", "comment_analysis", "account_profile_update", "graph_analysis"} <= keys
    assert {"shared_system", "json_repair", "post_analysis_parent_merge", "account_final_summary", "followup_candidate_analysis"} <= keys
    assert "media_deep_analysis" in keys


def test_parse_media_observation_normalizes_deep_review_hint_and_hybrid_trigger() -> None:
    payload = {
        "scene_summary": "Street view",
        "setting_type": "street",
        "vehicles": ["car"],
        "license_or_signage": ["34 ABC 123"],
        "visible_text_items": [{"text": "Karakol Sk."}],
        "deep_review_hint": {
            "run_deep_analysis": "yes",
            "confidence": "medium",
            "reason_tr": "Konum ve plaka benzeri bulgu var.",
        },
    }
    parsed = parse_media_observation(json.dumps(payload), media_index=1, media_type="image")
    assert parsed["deep_review_hint"]["run_deep_analysis"] == "yes"
    assert parsed["deep_review_hint"]["confidence"] == "medium"
    required, reason = evaluate_media_deep_requirement(parsed)
    assert required is True
    assert reason in {"model_decision_true", "model_hint"}



def test_database_schema_adds_canonical_tables_and_seeds_org_groups(tmp_path: Path) -> None:
    db_path = tmp_path / "canonical.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    conn = sqlite3.connect(str(db_path))
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    assert {"org_groups", "org_group_aliases", "llm_stage_attempts", "media_observations", "canonical_post_analyses", "canonical_comment_analyses", "account_aggregates"} <= tables
    org_rows = conn.execute("SELECT canonical_name FROM org_groups ORDER BY canonical_name").fetchall()
    assert ("RedKitler",) in org_rows
    conn.close()


def test_normalization_service_dedupes_aliases_and_avoids_simple_false_positive(tmp_path: Path) -> None:
    svc = DatabaseService(str(tmp_path / "norm.db"))
    svc.init_schema()
    normalizer = NormalizationService(svc)

    assert normalizer.normalize_entities(["Red Kitler support", "RedKitler video"]) == ["RedKitler"]
    assert normalizer.normalize_entities(["sarpkkli kullanici"]) == []


def test_scoring_service_enforces_signal_thresholds_and_ambiguity_downgrade() -> None:
    from app.models.canonical import CanonicalSignal

    scoring = ScoringService()
    assert scoring.determine_confidence([CanonicalSignal(family="alias", strength="weak")], []) == "low"
    assert scoring.determine_confidence(
        [
            CanonicalSignal(family="alias", strength="weak"),
            CanonicalSignal(family="propaganda", strength="moderate"),
        ],
        [],
    ) == "medium"
    assert scoring.determine_confidence(
        [
            CanonicalSignal(family="a", strength="strong"),
            CanonicalSignal(family="b", strength="strong"),
            CanonicalSignal(family="c", strength="strong"),
        ],
        [],
    ) == "high"
    assert scoring.determine_confidence(
        [
            CanonicalSignal(family="a", strength="strong"),
            CanonicalSignal(family="b", strength="strong"),
            CanonicalSignal(family="c", strength="strong"),
        ],
        ["mourning"],
    ) == "medium"


def test_stage_executor_runs_one_repair_pass_and_records_attempts() -> None:
    db = RecordingDB()
    executor = StageExecutor(SequencedVLLMService(["not-json", '{"ok": true}']), db_service=db)

    result = executor.execute(
        stage_name="comment_analysis",
        prompt_key="comment_analysis",
        prompt="prompt",
        payload={"model": "gemma-4-31b-it", "messages": [{"role": "user", "content": "prompt"}], "max_tokens": 20, "stream": False},
        validator=lambda answer: json.loads(answer),
        target_schema='{"ok": true}',
    )

    assert result.repair_attempted is True
    assert result.value == {"ok": True}
    assert [record["validation_status"] for record in db.records] == ["invalid_first_pass", "repair_success"]


def test_stage_executor_uses_larger_token_budget_for_repair() -> None:
    vllm = PayloadRecordingVLLMService(["not-json", '{"ok": true}'])
    executor = StageExecutor(vllm)

    result = executor.execute(
        stage_name="media_analysis",
        prompt_key="media_analysis",
        prompt="prompt",
        payload={"model": "gemma-4-31b-it", "messages": [{"role": "user", "content": "prompt"}], "max_tokens": 220, "stream": False},
        validator=lambda answer: json.loads(answer),
        target_schema='{"ok": true}',
    )

    assert result.repair_attempted is True
    assert len(vllm.payloads) == 2
    assert vllm.payloads[1]["max_tokens"] == 768


def test_stage_executor_traces_full_payload_and_raw_response() -> None:
    trace_logger = RecordingTraceLogger()
    executor = StageExecutor(SequencedVLLMService(['{"ok": true}']))

    result = executor.execute(
        stage_name="comment_analysis",
        prompt_key="comment_analysis",
        prompt="prompt-body",
        payload={"model": "gemma-4-31b-it", "messages": [{"role": "user", "content": "prompt-body"}], "max_tokens": 20, "stream": False},
        validator=lambda answer: json.loads(answer),
        target_schema='{"ok": true}',
        trace_logger=trace_logger,
        trace_prefix="TRACE_SAMPLE",
    )

    assert result.value == {"ok": True}
    titles = [title for title, _ in trace_logger.entries]
    assert titles == [
        "TRACE_SAMPLE_PROMPT",
        "TRACE_SAMPLE_PAYLOAD",
        "TRACE_SAMPLE_RAW_RESPONSE",
        "TRACE_SAMPLE_PARSED_RESULT",
    ]
    payload = trace_logger.entries[1][1]
    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][-1]["content"] == "prompt-body"


def test_post_stage_uses_focus_entity_as_reference_context(tmp_path: Path) -> None:
    vllm = PayloadRecordingVLLMService(
        [
            '{"content_types":["propaganda"],"primary_theme":["organizational_symbolism"],"summary_tr":"Test","language_and_tone":{"dominant_language":"tr","tone":"neutral","sloganized_language":"no"},"risk_indicators":{"direct_support_expression":{"status":"absent","evidence":[]},"organizational_symbol_use":{"status":"absent","evidence":[]},"leader_or_cadre_praise":{"status":"absent","evidence":[]},"violence_praise_or_justification":{"status":"absent","evidence":[]},"call_to_action_or_gathering":{"status":"absent","evidence":[]},"coordination_signal":{"status":"absent","evidence":[]},"fundraising_or_resource_request":{"status":"absent","evidence":[]},"targeting_or_threat":{"status":"absent","evidence":[]},"organized_crime_indicator":{"status":"absent","evidence":[]}},"organization_assessment":{"aligned_entities":[],"organization_link_score":0,"confidence":"low"},"profile_role_estimate":{"role":"unclear","reason_tr":"-"},"behavior_pattern":{"single_instance":"yes","repeated_theme":"no","escalation_signal":"no","reason_tr":"-"},"review_priority":{"importance_score":1,"priority_level":"low","human_review_required":"no","reason_tr":"-"},"analyst_note_tr":"-"}'
        ]
    )
    db_service = DatabaseService(str(tmp_path / "focus.db"))
    db_service.init_schema()
    executor = StageExecutor(vllm, db_service=db_service)
    normalization_service = NormalizationService()
    scoring_service = ScoringService()
    review_service = ReviewService()

    execute_post_stage(
        stage_executor=executor,
        username="name",
        instagram_username="ig",
        bio=None,
        caption="caption",
        media_type="image",
        media_url="http://media",
        media_items=[{"media_type": "image", "media_url": "http://media"}],
        media_observations=[],
        post_history_summaries=[],
        account_profile_summary="",
        template_content=None,
        model=None,
        max_tokens=128,
        normalization_service=normalization_service,
        scoring_service=scoring_service,
        review_service=review_service,
        attach_media=True,
        focus_entity="Daltonlar",
    )

    user_message = next(message for message in reversed(vllm.payloads[0]["messages"]) if message["role"] == "user")
    text = user_message["content"][0]["text"] if isinstance(user_message["content"], list) else str(user_message["content"])
    assert "Reference organizations:\nDaltons" in text
    assert "Sarallar" not in text


def test_extract_json_fragment_accepts_valid_json_with_trailing_noise() -> None:
    payload = extract_json_fragment('prefix {"ok": true, "nested": {"value": 1}} trailing noise')
    assert payload == {"ok": True, "nested": {"value": 1}}


def test_comment_parser_preserves_llm_top_level_payload() -> None:
    raw = json.dumps(
        {
            "comment_type": "threat",
            "content_summary_tr": "Olum temali tehdit iceren yorum.",
            "flags": {
                "active_supporter": {"flag": False, "reason_tr": ""},
                "threat": {"flag": True, "reason_tr": "Acik tehdit ifadesi."},
                "information_leak": {"flag": False, "reason_tr": ""},
                "coordination": {"flag": False, "reason_tr": ""},
                "hate_speech": {"flag": False, "reason_tr": ""},
            },
            "organization_link_assessment": {
                "organization_link_score": 6,
                "confidence": "medium",
                "reason_tr": "Seed hesapla uyumlu tehdit dili.",
            },
            "behavior_pattern": {
                "consistent_with_history": "unclear",
                "repeated_support_language": "no",
                "reason_tr": "Tekil gorunum.",
            },
            "overall_risk": {"level": "high", "human_review_required": "yes"},
        }
    )

    canonical = parse_comment_analysis_canonical(raw)
    legacy = legacy_comment_from_canonical(canonical)

    assert canonical.comment_type == "threat"
    assert canonical.organization_link_score == 6
    assert canonical.flagged is True
    assert canonical.reason == "Seed hesapla uyumlu tehdit dili."
    assert canonical.legacy_payload["comment_type"] == "threat"
    assert legacy.verdict == "tehdit"
    assert legacy.bayrak is True
    assert legacy.reason == "Seed hesapla uyumlu tehdit dili."


def test_comment_validator_rejects_nested_flag_object_only() -> None:
    with pytest.raises(ValueError, match="top-level schema"):
        validate_comment_analysis_canonical('{"flag": true, "reason_tr": "Ic nested obje"}')


def test_legacy_projection_does_not_upgrade_support_verdict_from_flags() -> None:
    canonical = parse_comment_analysis_canonical(
        json.dumps(
            {
                "comment_type": "support",
                "content_summary_tr": "Destek ifadesi.",
                "flags": {
                    "active_supporter": {"flag": True, "reason_tr": "Acik destek."},
                    "threat": {"flag": False, "reason_tr": ""},
                    "information_leak": {"flag": False, "reason_tr": ""},
                    "coordination": {"flag": False, "reason_tr": ""},
                    "hate_speech": {"flag": False, "reason_tr": ""},
                },
                "organization_link_assessment": {
                    "organization_link_score": 8,
                    "confidence": "medium",
                    "reason_tr": "Destek dili.",
                },
                "behavior_pattern": {
                    "consistent_with_history": "yes",
                    "repeated_support_language": "yes",
                    "reason_tr": "Tekrarlayan destek.",
                },
                "overall_risk": {"level": "high", "human_review_required": "yes"},
            }
        )
    )

    legacy = legacy_comment_from_canonical(canonical)

    assert legacy.verdict == "destekci_pasif"
    assert legacy.bayrak is True


def test_post_stage_preserves_explicit_legacy_entity_in_projection() -> None:
    executor = StageExecutor(SequencedVLLMService(['{"ozet":"Alpha post","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"PKK/KCK"},"onem_skoru":6}']))
    canonical, legacy, _, _ = execute_post_stage(
        stage_executor=executor,
        username="alpha",
        instagram_username="alpha.account",
        bio="bio",
        caption="caption",
        media_type="image",
        media_url="https://example.com/x.jpg",
        media_items=[{"media_type": "image", "media_url": "https://example.com/x.jpg"}],
        media_observations=[],
        post_history_summaries=[],
        account_profile_summary="",
        template_content=None,
        model=None,
        max_tokens=200,
        normalization_service=NormalizationService(),
        scoring_service=ScoringService(),
        review_service=ReviewService(),
        attach_media=False,
    )

    assert canonical.detected_entities[0] == "PKK/KCK"
    assert legacy.orgut_baglantisi.tespit_edilen_orgut == "PKK/KCK"


def test_post_stage_prioritizes_focus_entity_in_legacy_projection() -> None:
    executor = StageExecutor(SequencedVLLMService(['{"ozet":"Alpha post","icerik_kategorisi":["propaganda"],"tehdit_degerlendirmesi":{"tehdit_seviyesi":"orta"},"orgut_baglantisi":{"tespit_edilen_orgut":"YPG/PYD"},"onem_skoru":6}']))
    canonical, legacy, _, _ = execute_post_stage(
        stage_executor=executor,
        username="alpha",
        instagram_username="alpha.account",
        bio="bio",
        caption="caption",
        media_type="image",
        media_url="https://example.com/x.jpg",
        media_items=[{"media_type": "image", "media_url": "https://example.com/x.jpg"}],
        media_observations=[],
        post_history_summaries=[],
        account_profile_summary="",
        template_content=None,
        model=None,
        max_tokens=200,
        normalization_service=NormalizationService(),
        scoring_service=ScoringService(),
        review_service=ReviewService(),
        attach_media=False,
        focus_entity="PKK",
    )

    assert canonical.focus_entity == "PKK"
    assert legacy.orgut_baglantisi.tespit_edilen_orgut == "PKK, YPG/PYD"
