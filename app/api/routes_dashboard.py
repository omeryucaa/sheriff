from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.dependencies import get_db_service


router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/dashboard/summary")
def get_dashboard_summary(db_service=Depends(get_db_service)) -> dict[str, object]:
    return db_service.get_dashboard_summary()


@router.get("/accounts")
def list_accounts(
    search: str | None = None,
    orgut: str | None = None,
    threat: str | None = None,
    flagged_only: bool = False,
    db_service=Depends(get_db_service),
) -> dict[str, object]:
    return {"items": db_service.list_accounts(search=search, orgut=orgut, threat=threat, flagged_only=flagged_only)}


@router.get("/accounts/{account_id}")
def get_account(account_id: int, db_service=Depends(get_db_service)) -> dict[str, object]:
    item = db_service.get_account_detail(account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Account not found")
    return item


@router.get("/accounts/{account_id}/posts")
def get_account_posts(account_id: int, db_service=Depends(get_db_service)) -> dict[str, object]:
    return {"items": db_service.list_account_posts(account_id)}


@router.get("/accounts/{account_id}/comments")
def get_account_comments(
    account_id: int,
    verdict: str | None = None,
    flagged_only: bool = False,
    db_service=Depends(get_db_service),
) -> dict[str, object]:
    return {"items": db_service.list_account_comments(account_id, verdict=verdict, flagged_only=flagged_only)}


@router.get("/accounts/{account_id}/graph")
def get_account_graph(account_id: int, db_service=Depends(get_db_service)) -> dict[str, object]:
    return db_service.get_account_graph(account_id)


@router.get("/review-queue")
def get_review_queue(search: str | None = None, db_service=Depends(get_db_service)) -> dict[str, object]:
    return {"items": db_service.list_review_queue(search=search)}
