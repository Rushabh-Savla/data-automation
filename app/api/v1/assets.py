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
from app.domain.enums import AssetStatus, SourceType, RELEVANCE_ACCEPTED_STATUSES
from app.schemas.asset import AssetResponse
from app.schemas.common import PageResponse
from app.services.standardization_service import delete_asset_files
from app.storage import asset_files

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


def _remote_file_response(asset_id: uuid.UUID, nas_path: str, *, thumbnail: bool) -> FileResponse:
    """Serve an image that lives on the UI-connected NAS: fetched into the local
    cache on first use (thumbnails are cached separately and far smaller). A NAS
    that cannot be reached is a 503 the UI can explain, not a 500."""
    try:
        if thumbnail:
            thumb_path = asset_files.thumbnail_cache_file(nas_path)
            if not thumb_path.exists():
                src = asset_files.materialize(nas_path)
                thumb_path.parent.mkdir(parents=True, exist_ok=True)
                _generate_thumbnail(src, thumb_path)
            if thumb_path.exists():
                return FileResponse(thumb_path, media_type="image/jpeg")
        return FileResponse(asset_files.materialize(nas_path))
    except FileNotFoundError:
        raise HTTPException(404, f"Asset {asset_id}'s file no longer exists on the NAS")
    except asset_files.NasUnreachable as e:
        raise HTTPException(503, str(e))


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
    query_status = status
    # When filtering by "relevance_passed", return all assets that were accepted
    # (they advance through ready_for_upload -> uploading -> nas_verified -> completed)
    if status == AssetStatus.RELEVANCE_PASSED:
        query_status = RELEVANCE_ACCEPTED_STATUSES

    items = repo.list_for_job(job_id, status=query_status, source_type=source_type, search=search, limit=limit, offset=offset)
    total = repo.count_for_job(job_id, status=query_status, source_type=source_type, search=search)
    return PageResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/assets/{asset_id}", response_model=AssetResponse)
def get_asset(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    asset = AssetRepository(db).get(asset_id)
    if asset is None:
        raise HTTPException(404, f"Asset {asset_id} not found")
    return asset


@router.get("/assets/{asset_id}/file")
def get_asset_file(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    repo = AssetRepository(db)
    asset = repo.get(asset_id)
    if asset is None:
        raise HTTPException(404, f"Asset {asset_id} not found")

    target_asset = asset
    # If file was removed because asset is a duplicate, fall back to the canonical parent
    if (not target_asset.nas_path or not Path(target_asset.nas_path).exists()) and target_asset.is_duplicate_of:
        canonical = repo.get(target_asset.is_duplicate_of)
        if canonical and canonical.nas_path:
            target_asset = canonical

    if not target_asset.nas_path:
        raise HTTPException(404, f"Asset {asset_id} has no stored file")
    if asset_files.is_remote(target_asset.nas_path):
        return _remote_file_response(target_asset.id, target_asset.nas_path, thumbnail=False)
    if not Path(target_asset.nas_path).exists():
        raise HTTPException(404, f"Asset {asset_id}'s file no longer exists on disk")
    return FileResponse(target_asset.nas_path)


@router.get("/assets/{asset_id}/thumbnail")
def get_asset_thumbnail(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    """A small resized JPEG for grid display — cached to disk on first request.
    If the asset is a duplicate whose disk file was pruned, falls back to
    the canonical asset's thumbnail."""
    repo = AssetRepository(db)
    asset = repo.get(asset_id)
    if asset is None:
        raise HTTPException(404, f"Asset {asset_id} not found")

    target_asset = asset
    if (not target_asset.nas_path or not Path(target_asset.nas_path).exists()) and target_asset.is_duplicate_of:
        canonical = repo.get(target_asset.is_duplicate_of)
        if canonical and canonical.nas_path:
            target_asset = canonical

    if not target_asset.nas_path:
        raise HTTPException(404, f"Asset {asset_id} has no stored file")
    if asset_files.is_remote(target_asset.nas_path):
        return _remote_file_response(target_asset.id, target_asset.nas_path, thumbnail=True)

    nas_path = Path(target_asset.nas_path)
    thumb_path = _thumbnail_path_for(nas_path)
    if not thumb_path.exists():
        _generate_thumbnail(nas_path, thumb_path)

    if thumb_path.exists():
        return FileResponse(thumb_path, media_type="image/jpeg")
    if nas_path.exists():
        return FileResponse(nas_path, media_type="image/jpeg")
    raise HTTPException(404, f"Asset {asset_id}'s file no longer exists on disk")


@router.delete("/assets/{asset_id}", status_code=204)
def delete_asset(asset_id: uuid.UUID, db: Session = Depends(get_db)):
    repo = AssetRepository(db)
    asset = repo.get(asset_id)
    if asset is None:
        raise HTTPException(404, f"Asset {asset_id} not found")
    delete_asset_files(asset)
    repo.delete(asset_id)
    db.commit()
