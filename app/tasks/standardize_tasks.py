"""standardize_and_promote — validate, checksum, write to NAS, upsert Asset.

Exposed both as a plain function (called inline by ingest tasks, since a
single dataset can yield thousands of drafts and dispatching a separate
Celery task per image would be excessive scheduling overhead for what is a
fast local file operation) and as a Celery task (for connectors/paths that
do want per-item dispatch, and for standalone testing).
"""

import logging
import shutil
import uuid

from app.connectors.base import RawAssetDraft
from app.core.config import get_settings
from app.db.repositories.asset_repository import AssetRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus, SourceType
from app.services.standardization_service import AssetRejected, StandardizationService
from app.storage.local_filesystem import LocalFilesystemStorage
from app.storage.ram_buffer import SpooledCandidateBuffer
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


def standardize_and_promote_sync(
    job_id: uuid.UUID,
    job_source_task_id: uuid.UUID,
    source_type: SourceType,
    draft: RawAssetDraft,
    min_resolution: tuple[int, int] = (64, 64),
    relevance_score: float | None = None,
) -> tuple[AssetStatus, bool]:
    """Runs standardization inline within the caller's DB session/transaction."""
    settings = get_settings()
    storage = LocalFilesystemStorage(settings.nas_root)

    # Ensure candidate passes through MemoryManager and SpooledCandidateBuffer
    created_buffer = False
    if getattr(draft, "buffer", None) is None and draft.local_tmp_path and draft.local_tmp_path.exists():
        try:
            file_size = draft.local_tmp_path.stat().st_size
            buf = SpooledCandidateBuffer(initial_reservation=file_size)
            with open(draft.local_tmp_path, "rb") as f_in:
                shutil.copyfileobj(f_in, buf._file)
            buf.total_bytes = file_size
            buf.seek(0)
            draft.buffer = buf
            created_buffer = True
        except Exception as e:
            logger.debug(f"Could not buffer draft into memory ({e}), using path directly")

    try:
        with session_scope() as db:
            asset_repo = AssetRepository(db)
            service = StandardizationService(asset_repo, storage, min_resolution)
            try:
                asset, created = service.standardize_and_promote(
                    job_id, job_source_task_id, source_type, draft, relevance_score=relevance_score
                )
                return asset.status, created
            except AssetRejected as e:
                logger.info(f"Rejected asset {draft.source_item_id}: {e}")
                raise
    finally:
        if created_buffer and draft.buffer:
            draft.buffer.close()
            draft.buffer = None


@celery_app.task(name="app.tasks.standardize_tasks.standardize_and_promote", bind=True)
def standardize_and_promote_task(
    self, job_id: str, job_source_task_id: str, source_type: str, draft_dict: dict
):
    from pathlib import Path

    draft = RawAssetDraft(
        source_item_id=draft_dict["source_item_id"],
        source_url=draft_dict.get("source_url"),
        dataset_identifier=draft_dict.get("dataset_identifier"),
        license=draft_dict.get("license"),
        local_tmp_path=Path(draft_dict["local_tmp_path"]),
        original_name=draft_dict.get("original_name"),
        annotation_tmp_path=(
            Path(draft_dict["annotation_tmp_path"]) if draft_dict.get("annotation_tmp_path") else None
        ),
        annotation_format=draft_dict.get("annotation_format"),
        width=draft_dict.get("width"),
        height=draft_dict.get("height"),
        classes=draft_dict.get("classes", []),
        metadata=draft_dict.get("metadata", {}),
    )
    return standardize_and_promote_sync(
        uuid.UUID(job_id), uuid.UUID(job_source_task_id), SourceType(source_type), draft
    )
