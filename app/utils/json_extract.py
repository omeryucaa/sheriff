from __future__ import annotations

import json


def extract_json_fragment(text: str) -> dict[str, object] | None:
    decoder = json.JSONDecoder()
    search_from = 0
    best_match: tuple[int, int, dict[str, object]] | None = None
    while True:
        start = text.find("{", search_from)
        if start == -1:
            if best_match is None:
                return None
            return best_match[2]
        try:
            payload, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            search_from = start + 1
            continue
        if isinstance(payload, dict):
            span = end
            if best_match is None or start < best_match[0] or (start == best_match[0] and span > best_match[1]):
                best_match = (start, span, payload)
        search_from = start + 1


def extract_fenced_json_fragment(text: str) -> dict[str, object] | None:
    start_marker = "```json"
    start = text.find(start_marker)
    if start == -1:
        start = text.find("```")
        if start == -1:
            return None
        start += 3
    else:
        start += len(start_marker)
    end = text.find("```", start)
    if end == -1:
        return None
    candidate = text[start:end].strip()
    return extract_json_fragment(candidate)
