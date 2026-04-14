from __future__ import annotations

from app.models.canonical import CanonicalCommentAnalysis, CanonicalPostAnalysis, CanonicalSignal
from app.schemas import (
    CommentAnalysis,
    PostCrimeAssessment,
    PostOrganizationLink,
    PostStructuredAnalysis,
    PostThreatAssessment,
    PostVisualAnalysis,
)


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

LEGACY_COMMENT_TYPE_TO_VERDICT = {
    "support": "destekci_pasif",
    "opposition": "karsit",
    "neutral": "alakasiz",
    "slogan": "destekci_pasif",
    "coordination": "koordinasyon",
    "threat": "tehdit",
    "insult": "nefret_soylemi",
    "information_sharing": "bilgi_ifsa",
    "unclear": "belirsiz",
}

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


def _ambiguity_flags_from_text(*texts: str | None) -> list[str]:
    combined = " ".join(text or "" for text in texts).lower()
    flags: list[str] = []
    if any(token in combined for token in ["haber", "report", "reporting", "news"]):
        flags.append("reporting")
    if any(token in combined for token in ["elestir", "criticism", "karsi", "opposition"]):
        flags.append("criticism")
    if any(token in combined for token in ["yas", "anma", "mourning", "commemoration"]):
        flags.append("mourning")
    return flags


def _ordered_display_entities(canonical: CanonicalPostAnalysis) -> list[str]:
    ordered: list[str] = []
    if canonical.focus_entity and canonical.focus_entity != "belirsiz":
        ordered.append(canonical.focus_entity)
    for entity in canonical.detected_entities:
        if not entity or entity == "belirsiz":
            continue
        if entity in ordered:
            continue
        entity_folded = entity.casefold()
        covered_by_existing = False
        for existing in ordered:
            existing_folded = existing.casefold()
            if entity_folded in existing_folded or existing_folded in entity_folded:
                covered_by_existing = True
                if len(entity_folded) > len(existing_folded):
                    ordered[ordered.index(existing)] = entity
                break
        if not covered_by_existing:
            ordered.append(entity)
    return ordered


def canonical_post_from_legacy(parsed: PostStructuredAnalysis) -> CanonicalPostAnalysis:
    detected_entity = parsed.orgut_baglantisi.tespit_edilen_orgut
    signals: list[CanonicalSignal] = []
    categories = list(parsed.icerik_kategorisi)

    if detected_entity and detected_entity != "belirsiz":
        strength = "weak"
        if "propaganda" in categories or parsed.tehdit_degerlendirmesi.tehdit_seviyesi in {"orta", "yuksek", "kritik"}:
            strength = "strong"
        signals.append(CanonicalSignal(family="organization_affinity", strength=strength, evidence=[detected_entity]))
    if "propaganda" in categories:
        signals.append(CanonicalSignal(family="propaganda", strength="strong", evidence=["propaganda"]))
    if parsed.tehdit_degerlendirmesi.tehdit_seviyesi in {"orta", "yuksek", "kritik"}:
        signals.append(
            CanonicalSignal(
                family="threat",
                strength="strong" if parsed.tehdit_degerlendirmesi.tehdit_seviyesi in {"yuksek", "kritik"} else "moderate",
                evidence=[parsed.tehdit_degerlendirmesi.tehdit_seviyesi],
            )
        )
    if parsed.onem_skoru >= 5:
        signals.append(CanonicalSignal(family="importance", strength="moderate", evidence=[str(parsed.onem_skoru)]))

    return CanonicalPostAnalysis(
        focus_entity=None,
        summary=parsed.ozet,
        categories=categories or ["belirsiz"],
        tone=parsed.icerik_tonu,
        detected_entities=[detected_entity] if detected_entity and detected_entity != "belirsiz" else [],
        threat_level=parsed.tehdit_degerlendirmesi.tehdit_seviyesi,
        role=parsed.orgut_baglantisi.muhtemel_rol,
        signals=signals,
        ambiguity_flags=_ambiguity_flags_from_text(parsed.ozet, parsed.analist_notu),
        organization_link_score=max(0, min(10, parsed.onem_skoru if detected_entity and detected_entity != "belirsiz" else max(parsed.onem_skoru - 2, 0))),
        analyst_note=parsed.analist_notu,
        legacy_payload=parsed.model_dump(mode="json"),
    )


def legacy_post_from_canonical(canonical: CanonicalPostAnalysis) -> PostStructuredAnalysis:
    display_entities = _ordered_display_entities(canonical)
    display_entity_text = ", ".join(display_entities) if display_entities else "belirsiz"
    legacy = dict(canonical.legacy_payload)
    if not legacy or "content_types" in legacy or "organization_assessment" in legacy:
        legacy = {
            "ozet": canonical.summary,
            "gorsel_analiz": PostVisualAnalysis().model_dump(mode="json"),
            "icerik_tonu": canonical.tone,
            "icerik_kategorisi": canonical.categories or [LEGACY_CATEGORY_BY_CONTENT_TYPE.get(item, "belirsiz") for item in canonical.content_types] or ["belirsiz"],
            "orgut_baglantisi": PostOrganizationLink(
                tespit_edilen_orgut=display_entity_text,
                baglanti_gostergesi=display_entity_text if display_entity_text != "belirsiz" else canonical.analyst_note or "belirsiz",
                muhtemel_rol=LEGACY_ROLE_BY_PROFILE_ROLE.get(canonical.role or "unclear", "belirsiz"),
            ).model_dump(mode="json"),
            "tehdit_degerlendirmesi": PostThreatAssessment(tehdit_seviyesi=canonical.threat_level or "yok").model_dump(mode="json"),
            "suc_unsuru": PostCrimeAssessment().model_dump(mode="json"),
            "onem_skoru": canonical.review.importance_score,
            "analist_notu": canonical.analyst_note,
        }
    legacy["ozet"] = canonical.summary
    legacy["icerik_kategorisi"] = canonical.categories or [LEGACY_CATEGORY_BY_CONTENT_TYPE.get(item, "belirsiz") for item in canonical.content_types] or ["belirsiz"]
    legacy["icerik_tonu"] = canonical.tone or "notral"
    legacy["analist_notu"] = canonical.analyst_note
    legacy["onem_skoru"] = canonical.review.importance_score
    orgut = dict(legacy.get("orgut_baglantisi") or {})
    orgut["tespit_edilen_orgut"] = display_entity_text
    orgut["baglanti_gostergesi"] = display_entity_text if display_entity_text != "belirsiz" else canonical.analyst_note or orgut.get("baglanti_gostergesi", "belirsiz")
    orgut["muhtemel_rol"] = LEGACY_ROLE_BY_PROFILE_ROLE.get(canonical.role or "unclear", orgut.get("muhtemel_rol", "belirsiz"))
    legacy["orgut_baglantisi"] = orgut
    threat = dict(legacy.get("tehdit_degerlendirmesi") or {})
    threat["tehdit_seviyesi"] = canonical.threat_level or threat.get("tehdit_seviyesi", "yok")
    legacy["tehdit_degerlendirmesi"] = threat
    return PostStructuredAnalysis.model_validate(legacy)


def canonical_comment_from_legacy(
    *,
    commenter_username: str | None,
    text: str,
    focus_entity: str | None = None,
    verdict: str,
    sentiment: str,
    orgut_baglanti_skoru: int,
    bayrak: bool,
    reason: str,
) -> CanonicalCommentAnalysis:
    signals: list[CanonicalSignal] = []
    if verdict in {"destekci_aktif", "destekci_pasif"}:
        signals.append(CanonicalSignal(family="support", strength="strong" if verdict == "destekci_aktif" else "moderate", evidence=[verdict]))
    if verdict in {"tehdit", "bilgi_ifsa", "koordinasyon"}:
        signals.append(CanonicalSignal(family=verdict, strength="strong", evidence=[verdict]))
    if orgut_baglanti_skoru >= 7:
        signals.append(CanonicalSignal(family="org_score", strength="strong", evidence=[str(orgut_baglanti_skoru)]))
    return CanonicalCommentAnalysis(
        focus_entity=focus_entity,
        commenter_username=commenter_username,
        text=text,
        comment_type=verdict,
        sentiment=sentiment,
        organization_link_score=orgut_baglanti_skoru,
        signals=signals,
        flagged=bayrak,
        reason=reason,
        legacy_payload={
            "verdict": verdict,
            "sentiment": sentiment,
            "orgut_baglanti_skoru": orgut_baglanti_skoru,
            "bayrak": bayrak,
            "reason": reason,
        },
    )


def legacy_comment_from_canonical(canonical: CanonicalCommentAnalysis) -> CommentAnalysis:
    legacy = dict(canonical.legacy_payload)
    verdict = str(legacy.get("verdict") or "").strip().lower()
    if verdict not in {
        "destekci_aktif",
        "destekci_pasif",
        "karsit",
        "tehdit",
        "bilgi_ifsa",
        "koordinasyon",
        "nefret_soylemi",
        "alakasiz",
        "belirsiz",
    }:
        verdict = LEGACY_COMMENT_TYPE_TO_VERDICT.get(canonical.comment_type or "unclear", "belirsiz")
    sentiment = str(legacy.get("sentiment") or canonical.sentiment or "neutral")
    if sentiment not in {"positive", "negative", "neutral"}:
        sentiment = "neutral"
    return CommentAnalysis(
        commenter_username=canonical.commenter_username,
        text=canonical.text,
        verdict=verdict,
        sentiment=sentiment,
        orgut_baglanti_skoru=int(legacy.get("orgut_baglanti_skoru") or canonical.organization_link_score),
        bayrak=bool(legacy.get("bayrak") if "bayrak" in legacy else canonical.flagged),
        reason=str(legacy.get("reason") or canonical.reason or canonical.content_summary_tr or ""),
    )
