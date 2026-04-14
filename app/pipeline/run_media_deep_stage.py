from __future__ import annotations

from app.pipeline.helpers import parse_media_deep_analysis, validate_media_deep_analysis
from app.prompts import build_media_deep_analysis_prompt
from app.prompts.builders import MEDIA_DEEP_ANALYSIS_JSON_SCHEMA
from app.services.stage_executor import StageExecutor


def execute_media_deep_stage(
    *,
    stage_executor: StageExecutor,
    media_index: int,
    media_item: dict[str, str],
    media_count: int,
    username: str,
    instagram_username: str,
    bio: str | None,
    caption: str | None,
    media_observation_context: str,
    template_content: str | None,
    model: str | None,
    max_tokens: int,
) -> tuple[dict[str, object], str, dict[str, object]]:
    prompt = build_media_deep_analysis_prompt(
        username=username,
        instagram_username=instagram_username,
        bio=bio,
        caption=caption,
        media_index=media_index,
        media_count=media_count,
        media_type=media_item["media_type"],
        media_observation_context=media_observation_context,
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
        stage_name="media_deep_analysis",
        prompt_key="media_deep_analysis",
        prompt=prompt,
        payload=payload,
        validator=validate_media_deep_analysis,
        target_schema=MEDIA_DEEP_ANALYSIS_JSON_SCHEMA,
    )
    deep_payload = result.value if result.value is not None else parse_media_deep_analysis(result.answer)
    return deep_payload, prompt, payload
