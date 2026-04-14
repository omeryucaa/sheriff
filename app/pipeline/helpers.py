from __future__ import annotations

import base64
import json
import logging
import os
from collections import Counter
from pathlib import Path

logger = logging.getLogger(__name__)

from fastapi import HTTPException

from app.adapters.legacy_projection import legacy_comment_from_canonical, legacy_post_from_canonical
from app.config.scoring import PROFILE_ROLES
from app.models.canonical import CanonicalCommentAnalysis, CanonicalMediaObservation, CanonicalPostAnalysis, CanonicalReviewDecision, CanonicalSignal
from app.prompts import (
    PROFILE_SUMMARY_MAX_WORDS,
    _build_account_profile_stats_context,
    build_account_final_summary_prompt,
    build_account_profile_update_prompt,
    get_shared_system_prompt,
)
from app.schemas import CommentSentiment, CommentVerdict, PostStructuredAnalysis
from app.utils.json_extract import extract_fenced_json_fragment, extract_json_fragment
from app.vllm_service import VLLMService


COMMENT_VERDICTS = {
    "destekci_aktif",
    "destekci_pasif",
    "karsit",
    "tehdit",
    "bilgi_ifsa",
    "koordinasyon",
    "nefret_soylemi",
    "alakasiz",
    "belirsiz",
}
FLAGGED_VERDICTS = {"destekci_aktif", "tehdit", "bilgi_ifsa", "koordinasyon", "nefret_soylemi"}
FOLLOWUP_RELATIONSHIP_TYPES = {"supporter", "peer", "amplifier", "possible_operator", "unclear"}
FOLLOWUP_RELATIONSHIP_STRENGTHS = {"low", "medium", "high"}
FOLLOWUP_RISK_LEVELS = {"low", "medium", "high", "critical"}
POST_CATEGORY_ALIASES = {
    "ideolojik": "dini_ideolojik",
    "ideolojik_soylem": "dini_ideolojik",
    "kultur_medya": "medya_kultur",
    "tehdit_gozdagi": "tehdit_gozdag",
}
PROFILE_SUMMARY_MAX_TOKENS = 700
LEGACY_CATEGORY_BY_CONTENT_TYPE = {
    "news": "haber_paylasim",
    "announcement": "haber_paylasim",
    "propaganda": "propaganda",
    "commemoration": "cenaze_anma_sehit",
    "activity_march": "yuruyus_gosteri",
    "violence_conflict": "askeri_operasyon",
    "political_message": "hukuki_savunma",
    "religious_message": "dini_ideolojik",
    "personal_daily": "kisisel_gunluk",
    "fundraising": "lojistik_koordinasyon",
    "unclear": "belirsiz",
}
LEGACY_ROLE_BY_PROFILE_ROLE = {
    "supporter": "sempatizan",
    "propaganda_distributor": "propaganda_sorumlusu",
    "sympathizer": "sempatizan",
    "news_sharer": "belirsiz",
    "event_participant": "belirsiz",
    "possible_organizer": "lojistik",
    "possible_network_node": "lojistik",
    "unclear": "belirsiz",
}


class IngestTraceLogger:
    def __init__(self, path: str | None) -> None:
        self.path = path
        if self.path:
            log_dir = os.path.dirname(os.path.abspath(self.path))
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)

    def log(self, title: str, content: object) -> None:
        if not self.path:
            return
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(f"\n===== {title} =====\n")
            if isinstance(content, (dict, list)):
                handle.write(json.dumps(content, ensure_ascii=False, indent=2))
            else:
                handle.write(str(content))
            handle.write(f"\n===== /{title} =====\n")


def resolve_media_url(
    media_url: str | None,
    bucket: str | None,
    object_key: str | None,
    expires_seconds: int,
    minio_service: object,
) -> str:
    if media_url:
        return media_url
    if bucket and object_key:
        try:
            return minio_service.presigned_get_object(
                bucket=bucket,
                object_key=object_key,
                expires_seconds=expires_seconds,
            )
        except Exception as exc:  # pragma: no cover
            raise HTTPException(status_code=500, detail=f"Failed to create presigned URL: {exc}") from exc
    raise HTTPException(status_code=400, detail="Either media_url or bucket+object_key must be provided.")


def get_prompt_content(db_service: object, key: str) -> str | None:
    getter = getattr(db_service, "get_prompt_content", None)
    if not callable(getter):
        return None
    return getter(key)


def should_flag_comment(verdict: str, orgut_baglanti_skoru: int, bayrak: bool) -> bool:
    return bool(bayrak)


def _is_yes_like(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"yes", "true", "1"}


def sanitize_post_analysis_payload(data: dict[str, object]) -> dict[str, object]:
    sanitized = dict(data)

    categories = sanitized.get("icerik_kategorisi")
    if isinstance(categories, list):
        normalized_categories: list[str] = []
        for item in categories:
            if not isinstance(item, str):
                continue
            cleaned = POST_CATEGORY_ALIASES.get(item.strip().lower(), item.strip().lower())
            normalized_categories.append(cleaned)
        sanitized["icerik_kategorisi"] = normalized_categories

    tone = sanitized.get("icerik_tonu")
    if isinstance(tone, str):
        sanitized["icerik_tonu"] = tone.strip().lower()

    threat = sanitized.get("tehdit_degerlendirmesi")
    if isinstance(threat, dict):
        raw_level = threat.get("tehdit_seviyesi")
        if isinstance(raw_level, str):
            threat = dict(threat)
            threat["tehdit_seviyesi"] = raw_level.strip().lower()
            sanitized["tehdit_degerlendirmesi"] = threat

    nested_candidate = sanitized.get("ozet")
    if isinstance(nested_candidate, str):
        nested_data = extract_fenced_json_fragment(nested_candidate) or extract_json_fragment(nested_candidate)
        if nested_data:
            return sanitize_post_analysis_payload(nested_data)

    return sanitized


def parse_post_analysis(text: str) -> PostStructuredAnalysis:
    return legacy_post_from_canonical(parse_post_analysis_canonical(text))


def validate_post_analysis(text: str) -> PostStructuredAnalysis:
    return legacy_post_from_canonical(validate_post_analysis_canonical(text))


def parse_comment_classification(text: str) -> tuple[CommentVerdict, CommentSentiment, int, bool, str]:
    canonical = parse_comment_analysis_canonical(text)
    legacy = legacy_comment_from_canonical(canonical)
    return (
        legacy.verdict,
        legacy.sentiment,
        legacy.orgut_baglanti_skoru,
        bool(legacy.bayrak),
        legacy.reason,
    )


def validate_comment_classification(text: str) -> tuple[CommentVerdict, CommentSentiment, int, bool, str]:
    canonical = validate_comment_analysis_canonical(text)
    legacy = legacy_comment_from_canonical(canonical)
    return (
        legacy.verdict,
        legacy.sentiment,
        legacy.orgut_baglanti_skoru,
        bool(legacy.bayrak),
        legacy.reason,
    )


def serialize_post_analysis(analysis: PostStructuredAnalysis) -> str:
    return json.dumps(analysis.model_dump(mode="json"), ensure_ascii=True)


def _to_list_of_strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _status_present(entry: object) -> bool:
    if not isinstance(entry, dict):
        return False
    return str(entry.get("status") or "unclear").strip().lower() == "present"


def _extract_evidence(entry: object) -> list[str]:
    if not isinstance(entry, dict):
        return []
    return _to_list_of_strings(entry.get("evidence"))


def _ambiguity_flags_from_text(*texts: str | None) -> list[str]:
    combined = " ".join(text or "" for text in texts).lower()
    flags: list[str] = []
    if any(token in combined for token in ["haber", "report", "reporting", "news"]):
        flags.append("reporting")
    if any(token in combined for token in ["elestir", "eleştir", "criticism", "karsi", "karşı", "opposition"]):
        flags.append("criticism")
    if any(token in combined for token in ["yas", "anma", "mourning", "commemoration"]):
        flags.append("mourning")
    if any(token in combined for token in ["satire", "irony", "ironi"]):
        flags.append("satire")
    return sorted(set(flags))


def _derive_threat_level(priority_level: str, risk_indicators: dict[str, object]) -> str:
    if priority_level == "critical":
        return "kritik"
    threat_present = _status_present(risk_indicators.get("targeting_or_threat"))
    coordination_present = _status_present(risk_indicators.get("coordination_signal"))
    violence_present = _status_present(risk_indicators.get("violence_praise_or_justification"))
    direct_support_present = _status_present(risk_indicators.get("direct_support_expression"))
    symbolic_affinity_present = _status_present(risk_indicators.get("organizational_symbol_use"))
    if threat_present:
        return "yuksek" if priority_level in {"high", "critical"} else "orta"
    if coordination_present or violence_present:
        return "orta" if priority_level in {"medium", "high", "critical"} else "dusuk"
    if direct_support_present:
        return "orta" if priority_level in {"medium", "high", "critical"} else "dusuk"
    if symbolic_affinity_present:
        return "dusuk" if priority_level in {"low", "medium", "high", "critical"} else "yok"
    return "dusuk" if priority_level == "medium" else "yok"


def _ambiguity_flags_from_canonical_post(payload: dict[str, object]) -> list[str]:
    flags: list[str] = []
    content_types = set(_to_list_of_strings(payload.get("content_types")))
    primary_theme = set(_to_list_of_strings(payload.get("primary_theme")))
    tone_block = payload.get("language_and_tone")
    tone = ""
    if isinstance(tone_block, dict):
        tone = str(tone_block.get("tone") or "").strip().lower()
    if "news" in content_types:
        flags.append("reporting")
    if "mourning" in primary_theme or tone == "mourning":
        flags.append("mourning")
    analyst_note = str(payload.get("analyst_note_tr") or "").lower()
    summary = str(payload.get("summary_tr") or "").lower()
    combined = f"{summary} {analyst_note}"
    if any(token in combined for token in ["criticism", "elestir", "eleştir", "opposition", "karşıt"]):
        flags.append("criticism")
    if any(token in combined for token in ["satire", "irony", "ironi"]):
        flags.append("satire")
    return sorted(set(flags))


def _signals_from_post_payload(payload: dict[str, object]) -> list[CanonicalSignal]:
    risk_indicators = payload.get("risk_indicators")
    if not isinstance(risk_indicators, dict):
        return []
    mapping = {
        "direct_support_expression": ("direct_support", "strong"),
        "organizational_symbol_use": ("symbolic_affinity", "weak"),
        "leader_or_cadre_praise": ("leader_praise", "strong"),
        "violence_praise_or_justification": ("violence_praise", "strong"),
        "call_to_action_or_gathering": ("mobilization", "strong"),
        "coordination_signal": ("coordination", "strong"),
        "fundraising_or_resource_request": ("fundraising", "strong"),
        "targeting_or_threat": ("threat", "strong"),
        "organized_crime_indicator": ("organized_crime", "moderate"),
    }
    signals: list[CanonicalSignal] = []
    for key, (family, strength) in mapping.items():
        entry = risk_indicators.get(key)
        if _status_present(entry):
            signals.append(CanonicalSignal(family=family, strength=strength, evidence=_extract_evidence(entry)))
    return signals


def _normalize_profile_role(payload: dict[str, object]) -> str:
    role_block = payload.get("profile_role_estimate")
    if not isinstance(role_block, dict):
        return "unclear"
    role = str(role_block.get("role") or "unclear").strip()
    if role not in PROFILE_ROLES:
        role = "unclear"
    risk_indicators = payload.get("risk_indicators")
    coordination_present = isinstance(risk_indicators, dict) and _status_present(risk_indicators.get("coordination_signal"))
    if role == "possible_organizer" and not coordination_present:
        return "possible_network_node"
    return role


def parse_post_analysis_canonical(text: str) -> CanonicalPostAnalysis:
    data = extract_fenced_json_fragment(text) or extract_json_fragment(text)
    if not data:
        return CanonicalPostAnalysis(
            summary=text.strip() or "Model JSON disinda yanit verdi.",
            content_types=["unclear"],
            categories=["belirsiz"],
            primary_themes=["unclear"],
            analyst_note="Model JSON disinda yanit verdi.",
            legacy_payload={},
        )
    if "content_types" not in data and "organization_assessment" not in data:
        legacy = PostStructuredAnalysis.model_validate(sanitize_post_analysis_payload(data))
        if not legacy.icerik_kategorisi:
            legacy.icerik_kategorisi = ["belirsiz"]
        detected_entity = legacy.orgut_baglantisi.tespit_edilen_orgut
        signals: list[CanonicalSignal] = []
        if detected_entity and detected_entity != "belirsiz":
            signals.append(CanonicalSignal(family="organization_affinity", strength="weak", evidence=[detected_entity]))
        if "propaganda" in legacy.icerik_kategorisi:
            signals.append(CanonicalSignal(family="propaganda", strength="strong", evidence=["propaganda"]))
        if legacy.tehdit_degerlendirmesi.tehdit_seviyesi in {"orta", "yuksek", "kritik"}:
            signals.append(CanonicalSignal(family="threat", strength="strong", evidence=[legacy.tehdit_degerlendirmesi.tehdit_seviyesi]))
        role = "unclear"
        if legacy.orgut_baglantisi.muhtemel_rol == "propaganda_sorumlusu":
            role = "propaganda_distributor"
        elif legacy.orgut_baglantisi.muhtemel_rol == "sempatizan":
            role = "sympathizer"
        elif legacy.orgut_baglantisi.muhtemel_rol == "lojistik":
            role = "possible_network_node"
        return CanonicalPostAnalysis(
            summary=legacy.ozet,
            content_types=["unclear"],
            categories=legacy.icerik_kategorisi or ["belirsiz"],
            primary_themes=["unclear"],
            tone=legacy.icerik_tonu or "neutral",
            detected_entities=[detected_entity] if detected_entity and detected_entity != "belirsiz" else [],
            threat_level=legacy.tehdit_degerlendirmesi.tehdit_seviyesi,
            role=role,
            signals=signals,
            ambiguity_flags=_ambiguity_flags_from_text(legacy.ozet, legacy.analist_notu),
            organization_link_score=max(0, min(10, legacy.onem_skoru if detected_entity and detected_entity != "belirsiz" else max(legacy.onem_skoru - 2, 0))),
            analyst_note=legacy.analist_notu,
            review=CanonicalReviewDecision(
                importance_score=legacy.onem_skoru,
                priority_level="high" if legacy.onem_skoru >= 7 else "medium" if legacy.onem_skoru >= 5 else "low",
                human_review_required=False,
                confidence="low",
                reason=legacy.analist_notu or legacy.ozet,
            ),
            legacy_payload=legacy.model_dump(mode="json"),
        )

    content_types = _to_list_of_strings(data.get("content_types")) or ["unclear"]
    primary_themes = _to_list_of_strings(data.get("primary_theme")) or ["unclear"]
    language_and_tone = data.get("language_and_tone")
    if not isinstance(language_and_tone, dict):
        language_and_tone = {}
    org_assessment = data.get("organization_assessment")
    if not isinstance(org_assessment, dict):
        org_assessment = {}
    aligned_entities = org_assessment.get("aligned_entities")
    detected_entities: list[str] = []
    if isinstance(aligned_entities, list):
        for item in aligned_entities:
            if not isinstance(item, dict):
                continue
            entity = str(item.get("entity") or "").strip()
            if entity and entity not in detected_entities:
                detected_entities.append(entity)

    review_priority = data.get("review_priority")
    if not isinstance(review_priority, dict):
        review_priority = {}

    behavior_pattern = data.get("behavior_pattern")
    if not isinstance(behavior_pattern, dict):
        behavior_pattern = {}

    importance_score = int(review_priority.get("importance_score") or 1)
    importance_score = max(1, min(10, importance_score))
    priority_level = str(review_priority.get("priority_level") or "low").strip().lower()
    if priority_level not in {"low", "medium", "high", "critical"}:
        priority_level = "low"
    confidence = str(org_assessment.get("confidence") or "low").strip().lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "low"

    return CanonicalPostAnalysis(
        summary=str(data.get("summary_tr") or "").strip() or "unclear",
        content_types=content_types,
        categories=[LEGACY_CATEGORY_BY_CONTENT_TYPE.get(item, "belirsiz") for item in content_types] or ["belirsiz"],
        primary_themes=primary_themes,
        dominant_language=str(language_and_tone.get("dominant_language") or "unclear").strip().lower() or "unclear",
        tone=str(language_and_tone.get("tone") or "unclear").strip().lower() or "unclear",
        sloganized_language=str(language_and_tone.get("sloganized_language") or "unclear").strip().lower() or "unclear",
        detected_entities=detected_entities,
        threat_level=_derive_threat_level(priority_level, data.get("risk_indicators") if isinstance(data.get("risk_indicators"), dict) else {}),
        role=_normalize_profile_role(data),
        signals=_signals_from_post_payload(data),
        ambiguity_flags=_ambiguity_flags_from_canonical_post(data),
        organization_link_score=max(0, min(10, int(org_assessment.get("organization_link_score") or 0))),
        organization_confidence=confidence,
        behavior_single_instance=str(behavior_pattern.get("single_instance") or "unclear").strip().lower(),
        repeated_theme=str(behavior_pattern.get("repeated_theme") or "unclear").strip().lower(),
        escalation_signal=str(behavior_pattern.get("escalation_signal") or "unclear").strip().lower(),
        analyst_note=str(data.get("analyst_note_tr") or "").strip(),
        review=CanonicalReviewDecision(
            importance_score=importance_score,
            priority_level=priority_level,
            human_review_required=str(review_priority.get("human_review_required") or "no").strip().lower() == "yes",
            confidence=confidence,
            reason=str(review_priority.get("reason_tr") or "").strip(),
        ),
        legacy_payload=dict(data),
    )


def validate_post_analysis_canonical(text: str) -> CanonicalPostAnalysis:
    data = extract_fenced_json_fragment(text) or extract_json_fragment(text)
    if not data:
        raise ValueError("Post analysis output is not valid JSON.")
    return parse_post_analysis_canonical(text)


def parse_comment_analysis_canonical(text: str) -> CanonicalCommentAnalysis:
    data = extract_fenced_json_fragment(text) or extract_json_fragment(text)
    if not data:
        return CanonicalCommentAnalysis(
            text="",
            comment_type="unclear",
            content_summary_tr="Model JSON disinda yanit verdi.",
            reason="Model JSON disinda yanit verdi.",
        )
    if "comment_type" not in data:
        raw_verdict = str(data.get("verdict", "")).strip().lower()
        raw_sentiment = str(data.get("sentiment", "")).strip().lower()
        raw_score = data.get("orgut_baglanti_skoru", 0)
        raw_flag = bool(data.get("bayrak", False))
        raw_reason = str(data.get("reason", "")).strip() or "Gerekce verilmedi."
        try:
            score = max(0, min(10, int(raw_score)))
        except (TypeError, ValueError):
            score = 0
        legacy_type_map = {
            "destekci_aktif": "support",
            "destekci_pasif": "support",
            "karsit": "opposition",
            "tehdit": "threat",
            "bilgi_ifsa": "information_sharing",
            "koordinasyon": "coordination",
            "nefret_soylemi": "insult",
            "alakasiz": "neutral",
            "belirsiz": "unclear",
        }
        comment_type = legacy_type_map.get(raw_verdict, "unclear")
        return CanonicalCommentAnalysis(
            text="",
            comment_type=comment_type,
            content_summary_tr=raw_reason,
            sentiment=raw_sentiment if raw_sentiment in {"positive", "negative", "neutral"} else "neutral",
            organization_link_score=score,
            signals=[],
            organization_confidence="low",
            active_supporter_flag=raw_verdict == "destekci_aktif",
            threat_flag=raw_verdict == "tehdit",
            information_leak_flag=raw_verdict == "bilgi_ifsa",
            coordination_flag=raw_verdict == "koordinasyon",
            hate_speech_flag=raw_verdict == "nefret_soylemi",
            overall_risk_level="low",
            flagged=raw_flag,
            reason=raw_reason,
            review=CanonicalReviewDecision(
                importance_score=1,
                priority_level="low",
                human_review_required=raw_flag,
                confidence="low",
                reason=raw_reason,
            ),
            legacy_payload=dict(data),
        )

    comment_type = str(data.get("comment_type") or "unclear").strip().lower()
    if comment_type not in {"support", "opposition", "neutral", "slogan", "coordination", "threat", "insult", "information_sharing", "unclear"}:
        comment_type = "unclear"
    flags = data.get("flags")
    if not isinstance(flags, dict):
        flags = {}
    org_assessment = data.get("organization_link_assessment")
    if not isinstance(org_assessment, dict):
        org_assessment = {}
    behavior_pattern = data.get("behavior_pattern")
    if not isinstance(behavior_pattern, dict):
        behavior_pattern = {}
    overall_risk = data.get("overall_risk")
    if not isinstance(overall_risk, dict):
        overall_risk = {}
    try:
        organization_link_score = max(0, min(10, int(org_assessment.get("organization_link_score") or 0)))
    except (TypeError, ValueError):
        organization_link_score = 0
    confidence = str(org_assessment.get("confidence") or "low").strip().lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "low"
    overall_level = str(overall_risk.get("level") or "low").strip().lower()
    if overall_level not in {"low", "medium", "high", "critical"}:
        overall_level = "low"
    active_supporter_flag = bool((flags.get("active_supporter") or {}).get("flag")) if isinstance(flags.get("active_supporter"), dict) else False
    threat_flag = bool((flags.get("threat") or {}).get("flag")) if isinstance(flags.get("threat"), dict) else False
    information_leak_flag = bool((flags.get("information_leak") or {}).get("flag")) if isinstance(flags.get("information_leak"), dict) else False
    coordination_flag = bool((flags.get("coordination") or {}).get("flag")) if isinstance(flags.get("coordination"), dict) else False
    hate_speech_flag = bool((flags.get("hate_speech") or {}).get("flag")) if isinstance(flags.get("hate_speech"), dict) else False
    sentiment_value = str(data.get("sentiment") or "").strip().lower()
    if sentiment_value not in {"positive", "negative", "neutral"}:
        logger.warning("LLM response missing/invalid 'sentiment' (got %r), falling back to 'neutral'", data.get("sentiment"))
    sentiment: CommentSentiment = sentiment_value if sentiment_value in {"positive", "negative", "neutral"} else "neutral"
    review_data = data.get("review")
    if not isinstance(review_data, dict):
        logger.warning("LLM response missing/invalid 'review' block (got %r), using empty defaults", type(review_data).__name__)
        review_data = {}
    review_priority = str(review_data.get("priority_level") or overall_level).strip().lower()
    if review_priority not in {"low", "medium", "high", "critical"}:
        review_priority = overall_level
    review_confidence = str(review_data.get("confidence") or confidence).strip().lower()
    if review_confidence not in {"low", "medium", "high"}:
        review_confidence = confidence
    raw_importance = review_data.get("importance_score")
    try:
        review_importance = max(1, int(raw_importance or 1))
    except (TypeError, ValueError):
        review_importance = 1
    if raw_importance is None:
        logger.warning("LLM response missing 'review.importance_score', falling back to %d", review_importance)
    flagged = _is_yes_like(review_data.get("human_review_required") or overall_risk.get("human_review_required"))
    raw_reason = review_data.get("reason")
    reason = str(raw_reason or org_assessment.get("reason_tr") or data.get("content_summary_tr") or "").strip()
    if not raw_reason:
        logger.warning("LLM response missing 'review.reason', falling back to %r", reason[:80] if reason else "(empty)")
    return CanonicalCommentAnalysis(
        text="",
        comment_type=comment_type,
        content_summary_tr=str(data.get("content_summary_tr") or "").strip(),
        sentiment=sentiment,
        organization_link_score=organization_link_score,
        signals=[],
        organization_confidence=confidence,
        consistent_with_history=str(behavior_pattern.get("consistent_with_history") or "unclear").strip().lower(),
        repeated_support_language=str(behavior_pattern.get("repeated_support_language") or "unclear").strip().lower(),
        active_supporter_flag=active_supporter_flag,
        threat_flag=threat_flag,
        information_leak_flag=information_leak_flag,
        coordination_flag=coordination_flag,
        hate_speech_flag=hate_speech_flag,
        overall_risk_level=overall_level,
        flagged=flagged,
        reason=reason,
        review=CanonicalReviewDecision(
            importance_score=review_importance,
            priority_level=review_priority,
            human_review_required=flagged,
            confidence=review_confidence,
            reason=reason,
        ),
        legacy_payload=dict(data),
    )


def validate_comment_analysis_canonical(text: str) -> CanonicalCommentAnalysis:
    data = extract_fenced_json_fragment(text) or extract_json_fragment(text)
    if not data:
        raise ValueError("Comment analysis output is not valid JSON.")
    if "comment_type" in data:
        required_keys = {
            "comment_type",
            "content_summary_tr",
            "sentiment",
            "flags",
            "organization_link_assessment",
            "behavior_pattern",
            "overall_risk",
            "review",
        }
        missing = sorted(key for key in required_keys if key not in data)
        if missing:
            raise ValueError(f"Comment analysis output is missing top-level keys: {', '.join(missing)}")
    elif "verdict" in data:
        required_keys = {"verdict", "sentiment", "orgut_baglanti_skoru", "bayrak", "reason"}
        missing = sorted(key for key in required_keys if key not in data)
        if missing:
            raise ValueError(f"Legacy comment analysis output is missing keys: {', '.join(missing)}")
    else:
        raise ValueError("Comment analysis output does not match the expected top-level schema.")
    return parse_comment_analysis_canonical(text)


def parse_followup_candidate_analysis(text: str) -> dict[str, object]:
    data = extract_json_fragment(text)
    if not isinstance(data, dict):
        raise ValueError("Follow-up candidate analysis output is not valid JSON.")

    relationship_to_seed = str(data.get("relationship_to_seed") or "unclear").strip().lower()
    if relationship_to_seed not in FOLLOWUP_RELATIONSHIP_TYPES:
        relationship_to_seed = "unclear"

    relationship_strength = str(data.get("relationship_strength") or "low").strip().lower()
    if relationship_strength not in FOLLOWUP_RELATIONSHIP_STRENGTHS:
        relationship_strength = "low"

    risk_level = str(data.get("risk_level") or "low").strip().lower()
    if risk_level not in FOLLOWUP_RISK_LEVELS:
        risk_level = "low"

    branch_recommended = str(data.get("branch_recommended") or "no").strip().lower()
    if branch_recommended not in {"yes", "no"}:
        branch_recommended = "no"

    try:
        priority_rank = int(data.get("priority_rank") or 5)
    except (TypeError, ValueError):
        priority_rank = 5
    priority_rank = max(1, min(priority_rank, 5))

    return {
        "candidate_username": str(data.get("candidate_username") or "").strip(),
        "relationship_to_seed": relationship_to_seed,
        "relationship_strength": relationship_strength,
        "risk_level": risk_level,
        "primary_entity": str(data.get("primary_entity") or "unclear").strip() or "unclear",
        "secondary_entities": _to_list_of_strings(data.get("secondary_entities")),
        "trigger_signals": _to_list_of_strings(data.get("trigger_signals")),
        "branch_recommended": branch_recommended,
        "priority_rank": priority_rank,
        "reason_tr": str(data.get("reason_tr") or "").strip(),
    }


def validate_followup_candidate_analysis(text: str) -> dict[str, object]:
    return parse_followup_candidate_analysis(text)


def build_graph_analysis_summary(
    detail: dict[str, object],
    posts: list[dict[str, object]],
    comments: list[dict[str, object]],
    graph: dict[str, object],
) -> str:
    category_counter: Counter[str] = Counter()
    threat_counter: Counter[str] = Counter()
    org_counter: Counter[str] = Counter()
    verdict_counter: Counter[str] = Counter()
    commenter_counter: Counter[str] = Counter()
    high_score_commenters: list[str] = []

    for post in posts:
        for category in post.get("icerik_kategorisi", []) or []:
            if isinstance(category, str) and category.strip():
                category_counter[category.strip()] += 1
        threat_counter[str(post.get("tehdit_seviyesi") or "belirsiz")] += 1
        org_counter[str(post.get("tespit_edilen_orgut") or "belirsiz")] += 1

    for comment in comments:
        commenter = str(comment.get("commenter_username") or "anonim")
        commenter_counter[commenter] += 1
        verdict_counter[str(comment.get("verdict") or "belirsiz")] += 1
        try:
            score = int(comment.get("orgut_baglanti_skoru") or 0)
        except (TypeError, ValueError):
            score = 0
        if score >= 7:
            high_score_commenters.append(f"{commenter}({score})")

    top_categories = ", ".join(f"{name}: {count}" for name, count in category_counter.most_common(5)) or "yok"
    top_threats = ", ".join(f"{name}: {count}" for name, count in threat_counter.most_common(5)) or "yok"
    top_orgs = ", ".join(f"{name}: {count}" for name, count in org_counter.most_common(5)) or "yok"
    top_verdicts = ", ".join(f"{name}: {count}" for name, count in verdict_counter.most_common(5)) or "yok"
    top_commenters = ", ".join(f"{name}: {count}" for name, count in commenter_counter.most_common(5)) or "yok"
    elevated_commenters = ", ".join(high_score_commenters[:8]) or "yok"

    node_count = len(graph.get("nodes") or [])
    edge_count = len(graph.get("edges") or [])

    return (
        f"Hesap: @{detail.get('instagram_username') or '-'}\n"
        f"Toplam post: {detail.get('post_count') or 0} | Toplam yorum: {detail.get('comment_count') or 0} | "
        f"Bayrakli yorum: {detail.get('flagged_comment_count') or 0}\n"
        f"Baskin kategori: {detail.get('baskin_kategori') or 'belirsiz'} | "
        f"Genel tehdit seviyesi: {detail.get('tehdit_seviyesi') or 'belirsiz'} | "
        f"Tespit edilen orgut: {detail.get('tespit_edilen_orgut') or 'belirsiz'}\n"
        f"Graf boyutu: {node_count} dugum, {edge_count} kenar\n"
        f"Kategori dagilimi: {top_categories}\n"
        f"Tehdit dagilimi: {top_threats}\n"
        f"Orgut sinyalleri: {top_orgs}\n"
        f"Yorum verdict dagilimi: {top_verdicts}\n"
        f"En aktif yorumcular: {top_commenters}\n"
        f"Yuksek baglanti skorlu yorumcular: {elevated_commenters}\n"
        f"Not: linked_to orgut iliskisini, posted_about tehdit sinyalini, matches_category kategori tekrarini, "
        f"commented_on hesaba gelen yorum etkisini, flagged_by ise yorum verdict veya risk isaretini temsil eder."
    )


def save_graph_capture_from_data_url(account_id: int, data_url: str, sqlite_db_path: str) -> str:
    if not data_url.startswith("data:image/png;base64,"):
        raise HTTPException(status_code=400, detail="Only PNG data URLs are supported.")
    try:
        encoded = data_url.split(",", 1)[1]
        binary = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid graph image payload: {exc}") from exc

    base_dir = Path(sqlite_db_path).resolve().parent
    capture_dir = base_dir / "graph_captures"
    capture_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"account_{account_id}_graph.png"
    output_path = capture_dir / file_name
    output_path.write_bytes(binary)
    return f"/captures/{file_name}"


def limit_words(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text.strip()
    return " ".join(words[:max_words]).strip()


def normalize_profile_summary_text(text: str, fallback: str) -> str:
    collapsed = " ".join(text.split()).strip()
    selected = collapsed or fallback.strip()
    return limit_words(selected, PROFILE_SUMMARY_MAX_WORDS)


def build_profile_summary_fallback(
    current_summary: str,
    parsed_post_analysis: PostStructuredAnalysis,
) -> str:
    fallback = parsed_post_analysis.ozet.strip() or current_summary.strip() or "Profil özeti üretilemedi."
    if current_summary.strip():
        return normalize_profile_summary_text(f"{current_summary.strip()} {fallback}", current_summary.strip())
    return normalize_profile_summary_text(fallback, "Profil özeti üretilemedi.")


def build_final_account_summary_fallback(
    username: str,
    instagram_username: str | None,
    post_history_summaries: list[dict[str, object]],
) -> str:
    if not post_history_summaries:
        handle = instagram_username or username
        return normalize_profile_summary_text(
            f"{handle} hesabı için henüz çözümlenmiş gönderi bulunmuyor.",
            "Profil özeti üretilemedi.",
        )

    stats_context = _build_account_profile_stats_context(post_history_summaries)
    latest_items = post_history_summaries[-3:]
    recent_summary = " | ".join(str(item.get("ozet") or "").strip() for item in latest_items if str(item.get("ozet") or "").strip())
    handle = instagram_username or username
    fallback = f"{handle} hesabında {stats_context.replace(chr(10), '. ')}"
    if recent_summary:
        fallback = f"{fallback}. Son gönderi özetleri: {recent_summary}"
    return normalize_profile_summary_text(fallback, "Profil özeti üretilemedi.")


def update_account_profile_summary(
    username: str,
    instagram_username: str | None,
    current_summary: str,
    parsed_post_analysis: PostStructuredAnalysis,
    post_history_summaries: list[dict[str, object]],
    vllm_service: VLLMService,
    model: str | None,
    history_stats_context: str,
    template_content: str | None = None,
    trace_logger: IngestTraceLogger | None = None,
    trace_prefix: str = "ACCOUNT_PROFILE",
) -> str:
    prompt = build_account_profile_update_prompt(
        username=username,
        instagram_username=instagram_username,
        current_summary=current_summary,
        latest_post_summary=parsed_post_analysis.ozet,
        latest_post_categories=list(parsed_post_analysis.icerik_kategorisi),
        latest_threat_level=parsed_post_analysis.tehdit_degerlendirmesi.tehdit_seviyesi,
        latest_detected_org=parsed_post_analysis.orgut_baglantisi.tespit_edilen_orgut,
        latest_importance_score=parsed_post_analysis.onem_skoru,
        history_stats_context=history_stats_context,
        template_content=template_content,
    )
    payload = {
        "model": model or vllm_service.default_model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": PROFILE_SUMMARY_MAX_TOKENS,
        "stream": False,
    }
    fallback = build_profile_summary_fallback(current_summary, parsed_post_analysis)
    try:
        raw = vllm_service.create_chat_completion(payload)
        _, answer, _, _ = vllm_service.extract_answer(raw)
        if trace_logger:
            trace_logger.log(f"{trace_prefix}_PROMPT", prompt)
            trace_logger.log(f"{trace_prefix}_PAYLOAD", payload)
            trace_logger.log(f"{trace_prefix}_RAW_RESPONSE", raw)
    except Exception:
        return fallback
    return normalize_profile_summary_text(answer, fallback)


def generate_final_account_profile_summary(
    *,
    username: str,
    instagram_username: str | None,
    bio: str | None,
    post_history_summaries: list[dict[str, object]],
    vllm_service: VLLMService,
    model: str | None,
    db_service: object | None = None,
    template_content: str | None = None,
    trace_logger: IngestTraceLogger | None = None,
    trace_prefix: str = "ACCOUNT_FINAL_SUMMARY",
) -> str:
    fallback = build_final_account_summary_fallback(username, instagram_username, post_history_summaries)
    prompt = build_account_final_summary_prompt(
        username=username,
        instagram_username=instagram_username,
        bio=bio,
        post_history_summaries=post_history_summaries,
        history_stats_context=_build_account_profile_stats_context(post_history_summaries),
        template_content=template_content,
    )
    payload = {
        "model": model or vllm_service.default_model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": PROFILE_SUMMARY_MAX_TOKENS,
        "stream": False,
    }
    shared_system_prompt = get_shared_system_prompt(get_prompt_content(db_service, "shared_system") if db_service is not None else None)
    if shared_system_prompt.strip():
        payload["messages"] = [{"role": "system", "content": shared_system_prompt}, *payload["messages"]]

    recorder = getattr(db_service, "record_llm_stage_attempt", None)
    try:
        raw = vllm_service.create_chat_completion(payload)
        model_name, answer, _, _ = vllm_service.extract_answer(raw)
        if trace_logger:
            trace_logger.log(f"{trace_prefix}_PROMPT", prompt)
            trace_logger.log(f"{trace_prefix}_PAYLOAD", payload)
            trace_logger.log(f"{trace_prefix}_RAW_RESPONSE", raw)
        if callable(recorder):
            recorder(
                {
                    "stage_name": "account_final_summary",
                    "prompt_key": "account_final_summary",
                    "prompt_version": 1,
                    "rendered_prompt": prompt,
                    "model": model_name or vllm_service.default_model,
                    "raw_output": answer,
                    "validation_status": "success",
                    "validation_error": None,
                    "repair_attempted": False,
                }
            )
        return normalize_profile_summary_text(answer, fallback)
    except Exception as exc:
        if trace_logger:
            trace_logger.log(f"{trace_prefix}_PROMPT", prompt)
            trace_logger.log(f"{trace_prefix}_PAYLOAD", payload)
            trace_logger.log(f"{trace_prefix}_ERROR", str(exc))
        if callable(recorder):
            recorder(
                {
                    "stage_name": "account_final_summary",
                    "prompt_key": "account_final_summary",
                    "prompt_version": 1,
                    "rendered_prompt": prompt,
                    "model": model or vllm_service.default_model,
                    "raw_output": "",
                    "validation_status": "failed",
                    "validation_error": str(exc),
                    "repair_attempted": False,
                }
            )
        return fallback


def build_media_observation_context(observations: list[dict[str, object]]) -> str:
    if not observations:
        return "MEDIA SUMMARY: unavailable"

    lines: list[str] = []
    for item in observations:
        symbols = item.get("symbols_or_logos")
        symbol_bits: list[str] = []
        if isinstance(symbols, list):
            for symbol in symbols[:3]:
                if isinstance(symbol, dict):
                    description = str(symbol.get("description") or symbol.get("type") or "unclear").strip()
                    visible_text = str(symbol.get("visible_text") or "").strip()
                    symbol_bits.append(f"{description}{f' ({visible_text})' if visible_text else ''}")
        visible_text_items = item.get("visible_text_items")
        text_bits: list[str] = []
        if isinstance(visible_text_items, list):
            for text_item in visible_text_items[:3]:
                if isinstance(text_item, dict):
                    text_value = str(text_item.get("text") or "").strip()
                    if text_value:
                        text_bits.append(text_value)
        lines.append(
            f"{item.get('media_index', item.get('medya_no', '?'))}. "
            f"{item.get('media_type', item.get('medya_turu', 'unclear'))}: "
            f"{item.get('scene_summary', item.get('sahne_tanimi', 'unclear'))}, "
            f"setting {item.get('setting_type', item.get('konum_tahmini', 'unclear'))}, "
            f"objects {', '.join(_to_list_of_strings(item.get('notable_objects'))) or item.get('dikkat_ceken_unsurlar', 'unclear')}, "
            f"symbols {', '.join(symbol_bits) or item.get('bayrak_sembol_amblam', 'unclear')}, "
            f"text {', '.join(text_bits) or 'unclear'}, "
            f"weapon {'yes' if item.get('weapon_present', item.get('silah_patlayici_var_mi')) else 'no'}, "
            f"deep_required {'yes' if bool(item.get('deep_required')) else 'no'}, "
            f"deep_status {item.get('deep_status') or 'not_required'}"
        )
    return "\n".join(lines)


def parse_media_observation(text: str, media_index: int, media_type: str) -> dict[str, object]:
    data = extract_json_fragment(text) or {}
    deep_review_required_raw = data.get("deep_review_required")
    deep_review_required = False
    if isinstance(deep_review_required_raw, bool):
        deep_review_required = deep_review_required_raw
    elif isinstance(deep_review_required_raw, str):
        deep_review_required = deep_review_required_raw.strip().lower() in {"true", "yes", "1"}

    deep_review_hint = data.get("deep_review_hint")
    if not isinstance(deep_review_hint, dict):
        deep_review_hint = {}
    deep_run = str(deep_review_hint.get("run_deep_analysis") or "no").strip().lower()
    if deep_run not in {"yes", "no", "unclear"}:
        deep_run = "no"
    deep_confidence = str(deep_review_hint.get("confidence") or "low").strip().lower()
    if deep_confidence not in {"low", "medium", "high"}:
        deep_confidence = "low"
    deep_hint_payload = {
        "run_deep_analysis": deep_run,
        "confidence": deep_confidence,
        "reason_tr": str(deep_review_hint.get("reason_tr") or "").strip(),
    }
    # Keep backward-compatible shape while preferring explicit boolean flag.
    if deep_review_required and deep_hint_payload["run_deep_analysis"] != "yes":
        deep_hint_payload["run_deep_analysis"] = "yes"
    if not deep_review_required and deep_hint_payload["run_deep_analysis"] == "yes":
        deep_review_required = True
    if "scene_summary" in data or "setting_type" in data:
        return {
            "media_index": media_index,
            "media_type": str(data.get("media_type") or media_type),
            "scene_summary": str(data.get("scene_summary") or text.strip() or "unclear"),
            "setting_type": str(data.get("setting_type") or "unclear"),
            "visible_person_count": str(data.get("visible_person_count") or "unclear"),
            "face_visibility": str(data.get("face_visibility") or "unclear"),
            "clothing_types": _to_list_of_strings(data.get("clothing_types")) or ["unclear"],
            "notable_objects": _to_list_of_strings(data.get("notable_objects")),
            "weapon_presence": data.get("weapon_presence") if isinstance(data.get("weapon_presence"), dict) else {"status": "unclear", "types": ["unclear"]},
            "symbols_or_logos": data.get("symbols_or_logos") if isinstance(data.get("symbols_or_logos"), list) else [],
            "visible_text_items": data.get("visible_text_items") if isinstance(data.get("visible_text_items"), list) else [],
            "activity_type": _to_list_of_strings(data.get("activity_type")) or ["unclear"],
            "crowd_level": str(data.get("crowd_level") or "unclear"),
            "audio_elements": data.get("audio_elements") if isinstance(data.get("audio_elements"), dict) else {},
            "child_presence": str(data.get("child_presence") or "unclear"),
            "institutional_markers": _to_list_of_strings(data.get("institutional_markers")) or ["unclear"],
            "vehicles": _to_list_of_strings(data.get("vehicles")) or ["unclear"],
            "license_or_signage": _to_list_of_strings(data.get("license_or_signage")),
            "deep_review_required": deep_review_required,
            "deep_review_hint": deep_hint_payload,
            "raw_observation_note_tr": str(data.get("raw_observation_note_tr") or "unclear"),
        }
    return {
        "media_index": int(data.get("medya_no") or media_index),
        "media_type": str(data.get("medya_turu") or media_type),
        "scene_summary": str(data.get("sahne_tanimi") or text.strip() or "unclear"),
        "setting_type": str(data.get("konum_tahmini") or "unclear"),
        "visible_person_count": "unclear",
        "face_visibility": "unclear",
        "clothing_types": [str(data.get("uniforma_kiyafet") or "unclear")],
        "notable_objects": [str(data.get("dikkat_ceken_unsurlar") or "unclear")],
        "weapon_presence": {"status": "yes" if bool(data.get("silah_patlayici_var_mi", False)) else "no", "types": ["unclear"]},
        "symbols_or_logos": [{"type": "unclear", "description": str(data.get("bayrak_sembol_amblam") or "unclear"), "visible_text": ""}],
        "visible_text_items": [],
        "activity_type": ["unclear"],
        "crowd_level": "unclear",
        "audio_elements": {},
        "child_presence": "unclear",
        "institutional_markers": ["unclear"],
        "vehicles": ["unclear"],
        "license_or_signage": [],
        "deep_review_required": deep_review_required,
        "deep_review_hint": deep_hint_payload,
        "raw_observation_note_tr": str(data.get("risk_notu") or "unclear"),
    }


def validate_media_observation(text: str, media_index: int, media_type: str) -> dict[str, object]:
    data = extract_json_fragment(text)
    if not data:
        raise ValueError("Media observation output is not valid JSON.")
    return parse_media_observation(text, media_index=media_index, media_type=media_type)


def parse_media_deep_analysis(text: str) -> dict[str, object]:
    data = extract_json_fragment(text) or {}
    location = data.get("location_assessment")
    if not isinstance(location, dict):
        location = {}
    location_confidence = str(location.get("location_confidence") or "low").strip().lower()
    if location_confidence not in {"low", "medium", "high"}:
        location_confidence = "low"

    vehicle = data.get("vehicle_plate_assessment")
    if not isinstance(vehicle, dict):
        vehicle = {}
    vehicle_present = str(vehicle.get("vehicle_present") or "unclear").strip().lower()
    if vehicle_present not in {"yes", "no", "unclear"}:
        vehicle_present = "unclear"
    plate_visible = str(vehicle.get("plate_visible") or "unclear").strip().lower()
    if plate_visible not in {"yes", "no", "unclear"}:
        plate_visible = "unclear"

    priority = str(data.get("followup_priority") or "low").strip().lower()
    if priority not in {"low", "medium", "high", "critical"}:
        priority = "low"

    sensitive_information = data.get("sensitive_information")
    if not isinstance(sensitive_information, list):
        sensitive_information = []

    return {
        "location_assessment": {
            "location_identifiable": str(location.get("location_identifiable") or "unclear").strip().lower(),
            "location_confidence": location_confidence,
            "candidate_location_text": str(location.get("candidate_location_text") or "").strip(),
            "evidence": _to_list_of_strings(location.get("evidence")),
        },
        "vehicle_plate_assessment": {
            "vehicle_present": vehicle_present,
            "vehicles": _to_list_of_strings(vehicle.get("vehicles")),
            "plate_visible": plate_visible,
            "plate_text_candidates": _to_list_of_strings(vehicle.get("plate_text_candidates")),
            "evidence": _to_list_of_strings(vehicle.get("evidence")),
        },
        "sensitive_information": [item for item in sensitive_information if isinstance(item, dict)],
        "followup_priority": priority,
        "analyst_note_tr": str(data.get("analyst_note_tr") or "").strip(),
    }


def validate_media_deep_analysis(text: str) -> dict[str, object]:
    data = extract_json_fragment(text)
    if not data:
        raise ValueError("Media deep analysis output is not valid JSON.")
    return parse_media_deep_analysis(text)


def evaluate_media_deep_requirement(observation: dict[str, object]) -> tuple[bool, str]:
    explicit_required = observation.get("deep_review_required")
    if isinstance(explicit_required, bool):
        return (True, "model_decision_true") if explicit_required else (False, "model_decision_false")
    if isinstance(explicit_required, str):
        required = explicit_required.strip().lower() in {"true", "yes", "1"}
        return (True, "model_decision_true") if required else (False, "model_decision_false")

    # Backward-compatible fallback for legacy payloads.
    hint = observation.get("deep_review_hint")
    if isinstance(hint, dict):
        run_deep = str(hint.get("run_deep_analysis") or "no").strip().lower()
        confidence = str(hint.get("confidence") or "low").strip().lower()
        if run_deep == "yes" and confidence in {"medium", "high"}:
            return True, "model_hint"
    return False, "trigger_not_met"


def empty_comment_summary() -> dict[str, int]:
    return {verdict: 0 for verdict in sorted(COMMENT_VERDICTS)}


def read_json_object(minio_service: object, bucket: str, key: str) -> dict[str, object]:
    raw = minio_service.read_object_text(bucket, key)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in {key}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=500, detail=f"Expected JSON object in {key}")
    return parsed


def read_json_value(minio_service: object, bucket: str, key: str) -> object:
    raw = minio_service.read_object_text(bucket, key)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in {key}: {exc}") from exc


def read_jsonl_objects(minio_service: object, bucket: str, key: str) -> list[dict[str, object]]:
    raw = minio_service.read_object_text(bucket, key)
    rows: list[dict[str, object]] = []
    for line_no, line in enumerate(raw.splitlines(), 1):
        cleaned = line.strip()
        if not cleaned:
            continue
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
        else:
            raise HTTPException(status_code=500, detail=f"Invalid JSONL object in {key}:{line_no}")
    return rows


def extract_embedded_comments(post: dict[str, object]) -> list[dict[str, object]]:
    raw_comments = post.get("comments")
    if not isinstance(raw_comments, list):
        return []
    return [item for item in raw_comments if isinstance(item, dict)]


def resolve_archive_bucket(
    minio_service: object,
    requested_bucket: str | None,
    default_bucket: str,
    fallback_bucket: str,
) -> str:
    if requested_bucket:
        if minio_service.bucket_exists(requested_bucket):
            return requested_bucket
        raise HTTPException(status_code=404, detail=f"Bucket not found: {requested_bucket}")

    for candidate in [default_bucket, fallback_bucket]:
        if candidate and minio_service.bucket_exists(candidate):
            return candidate
    raise HTTPException(status_code=404, detail=f"Bucket not found. Tried: {default_bucket}, {fallback_bucket}")


def find_latest_run_id(minio_service: object, bucket: str, target_username: str) -> str:
    prefix = f"instagram/{target_username}/"
    object_names = minio_service.list_object_names(bucket, prefix=prefix, recursive=True)
    run_ids: set[str] = set()
    for name in object_names:
        parts = name.split("/")
        if len(parts) >= 3 and parts[0] == "instagram" and parts[1] == target_username:
            run_ids.add(parts[2])
    if not run_ids:
        raise HTTPException(status_code=404, detail=f"No runs found for username: {target_username}")
    return sorted(run_ids)[-1]


def normalize_media_type(post_type: str | None, media_object_key: str | None) -> str:
    if post_type and "video" in post_type.lower():
        return "video"
    if media_object_key and media_object_key.lower().endswith((".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v")):
        return "video"
    return "image"


def normalize_media_kind(value: str | None) -> str:
    if value and "video" in value.lower():
        return "video"
    return "image"


def collect_post_media_items(
    minio_service: object,
    bucket: str,
    post: dict[str, object],
    post_dir_prefix: str,
    post_type: str,
    expires_seconds: int,
) -> list[dict[str, str]]:
    media_items: list[dict[str, str]] = []
    media_keys = [
        k
        for k in sorted(minio_service.list_object_names(bucket, prefix=f"{post_dir_prefix}media/", recursive=True))
        if "/media/" in k
    ]
    if media_keys:
        for key in media_keys:
            media_items.append(
                {
                    "media_type": normalize_media_type(post_type, key),
                    "media_url": minio_service.presigned_get_object(
                        bucket=bucket,
                        object_key=key,
                        expires_seconds=expires_seconds,
                    ),
                }
            )
        return media_items

    raw_media_items = post.get("media") if isinstance(post.get("media"), list) else []
    for item in raw_media_items:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        media_items.append(
            {
                "media_type": normalize_media_kind(str(item.get("kind") or post_type)),
                "media_url": url,
            }
        )
    return media_items


def infer_post_media_type(media_items: list[dict[str, str]], fallback_post_type: str) -> str:
    if any(item.get("media_type") == "video" for item in media_items):
        return "video"
    return normalize_media_kind(fallback_post_type)


def build_post_history_entry(source_post_id: str, parsed_analysis: PostStructuredAnalysis) -> dict[str, object]:
    return {
        "tarih": source_post_id,
        "ozet": parsed_analysis.ozet,
        "icerik_kategorisi": parsed_analysis.icerik_kategorisi,
        "tehdit_seviyesi": parsed_analysis.tehdit_degerlendirmesi.tehdit_seviyesi,
        "orgut": parsed_analysis.orgut_baglantisi.tespit_edilen_orgut,
        "onem_skoru": parsed_analysis.onem_skoru,
    }


def discover_usernames_in_bucket(minio_service: object, bucket: str) -> list[str]:
    object_names = minio_service.list_object_names(bucket, prefix="instagram/", recursive=True)
    usernames: set[str] = set()
    for name in object_names:
        parts = name.split("/")
        if len(parts) >= 3 and parts[0] == "instagram" and parts[1]:
            usernames.add(parts[1])
    return sorted(usernames)


def build_same_batch_commenter_history(
    comments: list[dict[str, object]],
    current_index: int,
    commenter_username: str | None,
) -> list[dict[str, object]]:
    if not commenter_username:
        return []
    history: list[dict[str, object]] = []
    for index, item in enumerate(comments):
        if index == current_index:
            continue
        sibling_username = str(item.get("commenter_username") or "").strip()
        comment_text = str(item.get("comment_text") or item.get("text") or "").strip()
        if not comment_text or sibling_username != commenter_username:
            continue
        history.append(
            {
                "comment_text": comment_text,
                "verdict": "ayni_post_diger_yorum",
                "sentiment": "neutral",
                "reason": "Aynı gönderide aynı kullanıcıya ait başka yorum.",
                "orgut_baglanti_skoru": 0,
                "bayrak": False,
                "post_ozet": "Aynı gönderideki diğer yorum",
            }
        )
    return history
