import pytest

from app.vllm_service import VLLMService, VLLMUpstreamError


class DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("No JSON payload")
        return self._payload


def test_build_payload_image() -> None:
    service = VLLMService(base_url="http://x", default_model="gemma-4-31b-it")
    payload = service.build_payload(
        description="describe",
        media_type="image",
        media_url="https://minio/presigned-image",
        max_tokens=128,
    )

    assert payload["model"] == "gemma-4-31b-it"
    assert payload["messages"][0]["content"][0] == {"type": "text", "text": "describe"}
    assert payload["messages"][0]["content"][1] == {
        "type": "image_url",
        "image_url": {"url": "https://minio/presigned-image"},
    }


def test_build_payload_video() -> None:
    service = VLLMService(base_url="http://x", default_model="gemma-4-31b-it")
    payload = service.build_payload(
        description="describe",
        media_type="video",
        media_url="https://minio/presigned-video",
        max_tokens=128,
    )

    assert payload["messages"][0]["content"][1] == {
        "type": "video_url",
        "video_url": {"url": "https://minio/presigned-video"},
    }


def test_build_payload_multiple_media_items() -> None:
    service = VLLMService(base_url="http://x", default_model="gemma-4-31b-it")
    payload = service.build_payload(
        description="describe",
        media_type="image",
        media_url="https://minio/presigned-image",
        max_tokens=128,
        media_items=[
            {"media_type": "image", "media_url": "https://minio/1.jpg"},
            {"media_type": "video", "media_url": "https://minio/2.mp4"},
        ],
    )

    content = payload["messages"][0]["content"]
    assert content[0] == {"type": "text", "text": "describe"}
    assert content[1] == {"type": "image_url", "image_url": {"url": "https://minio/1.jpg"}}
    assert content[2] == {"type": "video_url", "video_url": {"url": "https://minio/2.mp4"}}


def test_extract_answer_success() -> None:
    raw = {
        "model": "gemma-4-31b-it",
        "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }
    model, answer, usage, finish_reason = VLLMService.extract_answer(raw)

    assert model == "gemma-4-31b-it"
    assert answer == "ok"
    assert usage == {"prompt_tokens": 10, "completion_tokens": 5}
    assert finish_reason == "stop"


def test_create_chat_completion_surfaces_error(monkeypatch: pytest.MonkeyPatch) -> None:
    service = VLLMService(base_url="http://x", default_model="gemma-4-31b-it")

    def _fake_post(*args, **kwargs):
        return DummyResponse(
            status_code=500,
            payload={"error": {"message": "upstream exploded"}},
            text="boom",
        )

    monkeypatch.setattr("app.vllm_service.requests.post", _fake_post)

    with pytest.raises(VLLMUpstreamError) as exc:
        service.create_chat_completion({"hello": "world"})

    assert exc.value.status_code == 500
    assert "upstream exploded" in exc.value.message
