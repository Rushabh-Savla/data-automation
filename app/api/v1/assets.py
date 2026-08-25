"""Asset browsing, streaming, and deletion."""

import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import require_api_key
from app.db.repositories.asset_repository import AssetRepository
from app.domain.enums import AssetStatus, SourceType
from app.schemas.asset import AssetResponse
from app.schemas.common import PageResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["assets"], dependencies=[Depends(require_api_key)])

_THUMBNAIL_MAX_DIMENSION = 320
_THUMBNAIL_JPEG_QUALITY = 82


def _thumbnail_path_for(nas_path: Path) -> Path:
    """Derives a sibling cache path for an asset's thumbnail — no DB column
    or migration needed, since it's always re-derivable from nas_path
    (already stored). e.g. .../coco_val2017_143961.jpg -> .../coco_val2017_143961.thumb.jpg"""
    return nas_path.with_name(f"{nas_path.stem}.thumb.jpg")


def _generate_thumbnail(src: Path, dest: Path) -> None:
    """Resizes src to fit within _THUMBNAIL_MAX_DIMENSION (aspect preserved)
    and writes it to dest as a JPEG. A no-op on any failure (corrupt file,
    unreadable format) — the route falls back to serving the original file,
    so a thumbnail-generation hiccup never breaks the grid."""
    try:
        from PIL import Image

        with Image.open(src) as img:
            img = img.convert("RGB")
            img.thumbnail((_THUMBNAIL_MAX_DIMENSION, _THUMBNAIL_MAX_DIMENSION))
            img.save(dest, format="JPEG", quality=_THUMBNAIL_JPEG_QUALITY)
    except Exception as e:
        logger.warning(f"Thumbnail generation failed for {src}: {e}")


@router.get("/jobs/{job_id}/assets", response_model=PageResponse[AssetResponse])
def list_assets(
    job_id: uuid.UUID,
    status: AssetStatus | None = None,
    source_type: SourceType | None = None,
    search: str | None = Query(default=None, description="Matches filename, dataset, or source item id"),
    limit: int = Query(default=100, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    repo = AssetRepository(db)
    items = repo.list_for_job(job_id, status=status, source_type=source_type, search=search, limit=limit, offset=offset)
    total = repo.count_for_job(job_id, status=status, source_type=source_type, search=search)
    return PageResponse(items=items, total=total, limit=limit, offset=offset)


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


@router.get("/assets/{asset_id}/thumbnail")
def get_asset_thumbnail(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    """A small resized JPEG for grid display — the Assets grid used to
    fetch the full original file (same route as the Download button) for
    every card just to show it in a small square, which was especially
    slow for the platform's larger sources (KITTI/SUN RGB-D). Generated
    once on first request and cached to disk (see _thumbnail_path_for) —
    every request after that is a plain file read, no repeated resize
    cost."""
    asset = AssetRepository(db).get(asset_id)
    if asset is None or not asset.nas_path:
        raise HTTPException(404, f"Asset {asset_id} not found or has no stored file")

    nas_path = Path(asset.nas_path)
    thumb_path = _thumbnail_path_for(nas_path)
    if not thumb_path.exists():
        _generate_thumbnail(nas_path, thumb_path)

    serve_path = thumb_path if thumb_path.exists() else nas_path
    return FileResponse(serve_path, media_type="image/jpeg")


@router.delete("/assets/{asset_id}", status_code=204)
def delete_asset(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    repo = AssetRepository(db)
    if repo.get(asset_id) is None:
        raise HTTPException(404, f"Asset {asset_id} not found")
    repo.delete(asset_id)
    db.commit()
