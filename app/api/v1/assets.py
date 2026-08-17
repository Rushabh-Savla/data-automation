"""Asset browsing, streaming, and deletion."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import require_api_key
from app.db.repositories.asset_repository import AssetRepository
from app.domain.enums import AssetStatus, SourceType
from app.schemas.asset import AssetResponse

router = APIRouter(tags=["assets"], dependencies=[Depends(require_api_key)])


@router.get("/jobs/{job_id}/assets", response_model=list[AssetResponse])
def list_assets(
    job_id: uuid.UUID,
    status: AssetStatus | None = None,
    source_type: SourceType | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    return AssetRepository(db).list_for_job(
        job_id, status=status, source_type=source_type, limit=limit, offset=offset
    )


@router.get("/assets/{asset_id}", response_model=AssetResponse)
def get_asset(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    asset = AssetRepository(db).get(asset_id)
    if asset is None:
        raise HTTPException(404, f"Asset {asset_id} not found")
    return asset


@router.get("/assets/{asset_id}/file")
def get_asset_file(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    asset = AssetRepository(db).get(asset_id)
    if asset is None or not asset.nas_path:
        raise HTTPException(404, f"Asset {asset_id} not found or has no stored file")
    return FileResponse(asset.nas_path)


@router.delete("/assets/{asset_id}", status_code=204)
def delete_asset(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    repo = AssetRepository(db)
    if repo.get(asset_id) is None:
        raise HTTPException(404, f"Asset {asset_id} not found")
    repo.delete(asset_id)
    db.commit()
