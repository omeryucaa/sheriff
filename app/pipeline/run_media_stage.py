from __future__ import annotations

from app.models.canonical import CanonicalMediaObservation
from app.pipeline.helpers import parse_media_observation, validate_media_observation
from app.prompts import build_media_analysis_prompt
from app.prompts.builders import MEDIA_OBSERVATION_JSON_SCHEMA
from app.services.stage_executor import StageExecutor


def execute_media_stage(
    *,
    stage_executor: StageExecutor,
    media_index: int,
    media_item: dict[str, str],
    media_count: int,
    username: str,
    instagram_username: str,
    bio: str | None,
    caption: str | None,
    template_content: str | None,
    model: str | None,
    max_tokens: int,
) -> tuple[CanonicalMediaObservation, dict[str, object], str, dict[str, object]]:
    prompt = build_media_analysis_prompt(
        username=username,
        instagram_username=instagram_username,
        bio=bio,
        caption=caption,
        media_index=media_index,
        media_count=media_count,
        media_type=media_item["media_type"],
        template_content=template_content,
    )
    payload = stage_executor.vllm_service.build_payload(
        description=prompt,
        media_type=media_item["media_type"],
        media_url=media_item["media_url"],
        max_tokens=max_tokens,
        model=model,
    )
    result = stage_executor.execute(
        stage_name="media_analysis",
        prompt_key="media_analysis",
        prompt=prompt,
        payload=payload,
        validator=lambda answer: validate_media_observation(answer, media_index=media_index, media_type=media_item["media_type"]),
        target_schema=MEDIA_OBSERVATION_JSON_SCHEMA,
    )
    observation = result.value if result.value is not None else parse_media_observation(result.answer, media_index, media_item["media_type"])
    canonical = CanonicalMediaObservation(
        media_index=int(observation["media_index"]),
        media_type="video" if str(observation["media_type"]).startswith("video") else "image",
        scene_summary=str(observation["scene_summary"]),
        setting=str(observation["setting_type"]),
        visible_person_count=str(observation.get("visible_person_count") or "unclear"),
        face_visibility=str(observation.get("face_visibility") or "unclear"),
        visible_symbols=[
            str(item.get("description") or item.get("type") or "unclear")
            for item in observation.get("symbols_or_logos", [])
            if isinstance(item, dict)
        ],
        visible_text_items=[
            str(item.get("text") or "").strip()
            for item in observation.get("visible_text_items", [])
            if isinstance(item, dict) and str(item.get("text") or "").strip()
        ],
        notable_objects=[str(item) for item in observation.get("notable_objects", []) if str(item).strip()],
        weapon_present=str((observation.get("weapon_presence") or {}).get("status") or "unclear") == "yes",
        weapon_types=[str(item) for item in (observation.get("weapon_presence") or {}).get("types", []) if str(item).strip()],
        clothing=", ".join(str(item) for item in observation.get("clothing_types", []) if str(item).strip()) or "unclear",
        activity_types=[str(item) for item in observation.get("activity_type", []) if str(item).strip()],
        crowd_level=str(observation.get("crowd_level") or "unclear"),
        audio_summary={str(key): str(value) for key, value in dict(observation.get("audio_elements") or {}).items()},
        child_presence=str(observation.get("child_presence") or "unclear"),
        institutional_markers=[str(item) for item in observation.get("institutional_markers", []) if str(item).strip()],
        vehicles=[str(item) for item in observation.get("vehicles", []) if str(item).strip()],
        license_or_signage=[str(item) for item in observation.get("license_or_signage", []) if str(item).strip()],
        deep_review_hint={
            "run_deep_analysis": str((observation.get("deep_review_hint") or {}).get("run_deep_analysis") or "no"),
            "confidence": str((observation.get("deep_review_hint") or {}).get("confidence") or "low"),
            "reason_tr": str((observation.get("deep_review_hint") or {}).get("reason_tr") or ""),
        },
        raw_note=str(observation.get("raw_observation_note_tr") or "unclear"),
        legacy_payload=observation,
    )
    legacy = {
        "medya_no": canonical.media_index,
        "medya_turu": canonical.media_type,
        "sahne_tanimi": canonical.scene_summary,
        "dikkat_ceken_unsurlar": ", ".join(canonical.notable_objects) or "unclear",
        "konum_tahmini": canonical.setting,
        "silah_patlayici_var_mi": canonical.weapon_present,
        "bayrak_sembol_amblam": ", ".join(canonical.visible_symbols) or "unclear",
        "uniforma_kiyafet": canonical.clothing,
        "risk_notu": canonical.raw_note,
        "visible_text_items": canonical.visible_text_items,
        "activity_type": canonical.activity_types,
        "crowd_level": canonical.crowd_level,
    }
    return canonical, legacy, prompt, payload
