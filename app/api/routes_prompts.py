from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.dependencies import get_db_service
from app.schemas import PromptTemplateUpdateRequest


router = APIRouter()


@router.get("/prompts")
def list_prompts(db_service=Depends(get_db_service)) -> dict[str, object]:
    return {"items": db_service.list_prompt_templates()}


@router.get("/prompts/{key}")
def get_prompt(key: str, db_service=Depends(get_db_service)) -> dict[str, object]:
    item = db_service.get_prompt_template(key)
    if not item:
        raise HTTPException(status_code=404, detail="Prompt template not found")
    return item


@router.put("/prompts/{key}")
def update_prompt(
    key: str,
    request: PromptTemplateUpdateRequest,
    db_service=Depends(get_db_service),
) -> dict[str, object]:
    if request.reset_to_default:
        item = db_service.reset_prompt_template(key)
    else:
        item = db_service.update_prompt_template(key, request.content, request.is_enabled)
    if not item:
        raise HTTPException(status_code=404, detail="Prompt template not found")
    return item
