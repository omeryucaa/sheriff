from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.models.canonical import LLMStageAttemptRecord
from app.prompts import build_json_repair_prompt, get_shared_system_prompt
from app.services.validation_service import ValidationService
from app.vllm_service import VLLMService


@dataclass
class StageExecutionResult:
    model: str
    answer: str
    value: Any | None
    repair_attempted: bool
    validation_error: str | None = None


class StageExecutor:
    def __init__(self, vllm_service: VLLMService, validation_service: ValidationService | None = None, db_service: object | None = None) -> None:
        self.vllm_service = vllm_service
        self.validation_service = validation_service or ValidationService()
        self.db_service = db_service

    def execute(
        self,
        *,
        stage_name: str,
        prompt_key: str,
        prompt: str,
        payload: dict[str, Any],
        validator: Callable[[str], Any],
        target_schema: str,
        repair_prompt_template: str | None = None,
        related_account_id: int | None = None,
        related_post_id: int | None = None,
        related_comment_id: int | None = None,
        trace_logger: object | None = None,
        trace_prefix: str | None = None,
    ) -> StageExecutionResult:
        payload = self._attach_shared_system_prompt(payload)
        trace_label = trace_prefix or stage_name.upper()
        self._trace(trace_logger, f"{trace_label}_PROMPT", prompt)
        self._trace(trace_logger, f"{trace_label}_PAYLOAD", payload)
        raw = self.vllm_service.create_chat_completion(payload)
        self._trace(trace_logger, f"{trace_label}_RAW_RESPONSE", raw)
        model, answer, _, _ = self.vllm_service.extract_answer(raw)
        try:
            value = self.validation_service.validate(answer, validator)
            self._trace(trace_logger, f"{trace_label}_PARSED_RESULT", value)
            self._record(
                LLMStageAttemptRecord(
                    stage_name=stage_name,
                    prompt_key=prompt_key,
                    rendered_prompt=prompt,
                    model=model or self.vllm_service.default_model,
                    raw_output=answer,
                    validation_status="success",
                    related_account_id=related_account_id,
                    related_post_id=related_post_id,
                    related_comment_id=related_comment_id,
                )
            )
            return StageExecutionResult(model=model or self.vllm_service.default_model, answer=answer, value=value, repair_attempted=False)
        except Exception as exc:
            self._trace(trace_logger, f"{trace_label}_VALIDATION_ERROR", str(exc))
            self._record(
                LLMStageAttemptRecord(
                    stage_name=stage_name,
                    prompt_key=prompt_key,
                    rendered_prompt=prompt,
                    model=model or self.vllm_service.default_model,
                    raw_output=answer,
                    validation_status="invalid_first_pass",
                    validation_error=str(exc),
                    repair_attempted=True,
                    related_account_id=related_account_id,
                    related_post_id=related_post_id,
                    related_comment_id=related_comment_id,
                )
            )
            repair_prompt = build_json_repair_prompt(
                answer,
                target_schema,
                template_content=repair_prompt_template or self._get_prompt_content("json_repair"),
            )
            repair_payload = {
                "model": payload.get("model", self.vllm_service.default_model),
                "messages": [{"role": "user", "content": repair_prompt}],
                "max_tokens": self._resolve_repair_max_tokens(payload),
                "stream": False,
            }
            repair_payload = self._attach_shared_system_prompt(repair_payload)
            self._trace(trace_logger, f"{trace_label}_REPAIR_PROMPT", repair_prompt)
            self._trace(trace_logger, f"{trace_label}_REPAIR_PAYLOAD", repair_payload)
            repair_raw = self.vllm_service.create_chat_completion(repair_payload)
            self._trace(trace_logger, f"{trace_label}_REPAIR_RAW_RESPONSE", repair_raw)
            repair_model, repair_answer, _, _ = self.vllm_service.extract_answer(repair_raw)
            try:
                value = self.validation_service.validate(repair_answer, validator)
                self._trace(trace_logger, f"{trace_label}_REPAIR_PARSED_RESULT", value)
                self._record(
                    LLMStageAttemptRecord(
                        stage_name=stage_name,
                        prompt_key=prompt_key,
                        rendered_prompt=repair_prompt,
                        model=repair_model or self.vllm_service.default_model,
                        raw_output=repair_answer,
                        validation_status="repair_success",
                        repair_attempted=True,
                        related_account_id=related_account_id,
                        related_post_id=related_post_id,
                        related_comment_id=related_comment_id,
                    )
                )
                return StageExecutionResult(
                    model=repair_model or self.vllm_service.default_model,
                    answer=repair_answer,
                    value=value,
                    repair_attempted=True,
                )
            except Exception as repair_exc:
                self._trace(trace_logger, f"{trace_label}_REPAIR_VALIDATION_ERROR", str(repair_exc))
                self._record(
                    LLMStageAttemptRecord(
                        stage_name=stage_name,
                        prompt_key=prompt_key,
                        rendered_prompt=repair_prompt,
                        model=repair_model or self.vllm_service.default_model,
                        raw_output=repair_answer,
                        validation_status="failed",
                        validation_error=str(repair_exc),
                        repair_attempted=True,
                        related_account_id=related_account_id,
                        related_post_id=related_post_id,
                        related_comment_id=related_comment_id,
                    )
                )
                return StageExecutionResult(
                    model=repair_model or self.vllm_service.default_model,
                    answer=repair_answer,
                    value=None,
                    repair_attempted=True,
                    validation_error=str(repair_exc),
                )

    def _record(self, record: LLMStageAttemptRecord) -> None:
        recorder = getattr(self.db_service, "record_llm_stage_attempt", None)
        if callable(recorder):
            recorder(record.model_dump(mode="json"))

    @staticmethod
    def _trace(trace_logger: object | None, title: str, content: object) -> None:
        logger = getattr(trace_logger, "log", None)
        if callable(logger):
            logger(title, content)

    def _get_prompt_content(self, key: str) -> str | None:
        getter = getattr(self.db_service, "get_prompt_content", None)
        if callable(getter):
            return getter(key)
        return None

    def _attach_shared_system_prompt(self, payload: dict[str, Any]) -> dict[str, Any]:
        messages = list(payload.get("messages") or [])
        if not messages:
            return payload
        if any(str(message.get("role") or "") == "system" for message in messages if isinstance(message, dict)):
            return payload
        shared_system_prompt = get_shared_system_prompt(self._get_prompt_content("shared_system"))
        if not shared_system_prompt.strip():
            return payload
        return {
            **payload,
            "messages": [{"role": "system", "content": shared_system_prompt}, *messages],
        }

    def _resolve_repair_max_tokens(self, payload: dict[str, Any]) -> int:
        try:
            requested = int(payload.get("max_tokens") or 0)
        except (TypeError, ValueError):
            requested = 0
        # Repairing truncated JSON needs more room than the initial pass.
        return max(requested, 768)
