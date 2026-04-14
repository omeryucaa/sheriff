from typing import Any, Literal

import requests


class VLLMUpstreamError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


MediaType = Literal["image", "video"]


class VLLMService:
    def __init__(self, base_url: str, default_model: str, timeout_seconds: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model
        self.timeout_seconds = timeout_seconds

    def build_payload(
        self,
        description: str,
        media_type: MediaType,
        media_url: str,
        max_tokens: int,
        model: str | None = None,
        media_items: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        content: list[dict[str, Any]] = [{"type": "text", "text": description}]

        normalized_media_items = media_items or [{"media_type": media_type, "media_url": media_url}]
        for item in normalized_media_items:
            item_type = str(item.get("media_type") or media_type)
            item_url = str(item.get("media_url") or media_url)
            if item_type == "image":
                media_part: dict[str, Any] = {"type": "image_url", "image_url": {"url": item_url}}
            else:
                media_part = {"type": "video_url", "video_url": {"url": item_url}}
            content.append(media_part)

        return {
            "model": model or self.default_model,
            "messages": [
                {
                    "role": "user",
                    "content": content,
                }
            ],
            "max_tokens": max_tokens,
            "stream": False,
        }

    def create_chat_completion(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/v1/chat/completions"
        try:
            response = requests.post(url, json=payload, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise VLLMUpstreamError(status_code=502, message=str(exc)) from exc

        if response.status_code >= 400:
            message = response.text
            try:
                body = response.json()
                if isinstance(body, dict):
                    error = body.get("error")
                    if isinstance(error, dict) and error.get("message"):
                        message = str(error["message"])
            except ValueError:
                pass
            raise VLLMUpstreamError(status_code=response.status_code, message=message)

        return response.json()

    @staticmethod
    def extract_answer(chat_response: dict[str, Any]) -> tuple[str, str | None, dict[str, Any] | None, str | None]:
        model = str(chat_response.get("model", ""))
        choices = chat_response.get("choices") or []
        if not choices:
            raise ValueError("vLLM response has no choices")

        first = choices[0]
        message = first.get("message") or {}
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("vLLM response does not include message content")

        finish_reason = first.get("finish_reason")
        usage = chat_response.get("usage")
        return model, content, usage, finish_reason
