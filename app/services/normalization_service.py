from __future__ import annotations

import re

from app.config.org_groups import get_seed_org_group_rows
from app.utils.text_normalize import normalize_match_text


class NormalizationService:
    def __init__(self, db_service: object | None = None) -> None:
        self.db_service = db_service

    def _load_org_groups(self) -> list[dict[str, object]]:
        getter = getattr(self.db_service, "list_enabled_org_groups", None)
        if callable(getter):
            rows = getter()
            if rows:
                return rows
        return get_seed_org_group_rows()

    def render_known_organizations(self, focus_entity: str | None = None) -> str:
        normalized_focus = self.normalize_focus_entity(focus_entity)
        if normalized_focus:
            return normalized_focus
        items = self._load_org_groups()
        return ", ".join(str(item["canonical_name"]) for item in items if item.get("is_enabled", True))

    def normalize_focus_entity(self, focus_entity: str | None) -> str | None:
        if not focus_entity:
            return None
        normalized_target = normalize_match_text(focus_entity)
        for item in self._load_org_groups():
            if normalize_match_text(str(item["canonical_name"])) == normalized_target:
                return str(item["canonical_name"])
            for alias in item.get("aliases", []) or []:
                if normalize_match_text(str(alias)) == normalized_target:
                    return str(item["canonical_name"])
        return focus_entity.strip() or None

    def normalize_entities(self, texts: list[str]) -> list[str]:
        normalized_text = " ".join(normalize_match_text(text) for text in texts if text).strip()
        if not normalized_text:
            return []

        entities: list[str] = []
        for item in self._load_org_groups():
            canonical_name = str(item["canonical_name"])
            aliases = item.get("aliases", []) or []
            for alias in aliases:
                alias_text = normalize_match_text(str(alias))
                if not alias_text:
                    continue
                pattern = rf"(^|\s){re.escape(alias_text)}($|\s)"
                if re.search(pattern, normalized_text) and canonical_name not in entities:
                    entities.append(canonical_name)
                    break
        return entities
