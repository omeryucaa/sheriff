from __future__ import annotations

import re


_PUNCT_RE = re.compile(r"[\-_/.,:;(){}\[\]!?'\"`]+")
_SPACE_RE = re.compile(r"\s+")


def collapse_whitespace(text: str) -> str:
    return _SPACE_RE.sub(" ", text).strip()


def normalize_match_text(text: str) -> str:
    lowered = text.casefold()
    without_punct = _PUNCT_RE.sub(" ", lowered)
    return collapse_whitespace(without_punct)
