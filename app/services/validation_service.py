from __future__ import annotations

from typing import Any, Callable


class ValidationService:
    def validate(self, answer: str, validator: Callable[[str], Any]) -> Any:
        return validator(answer)
