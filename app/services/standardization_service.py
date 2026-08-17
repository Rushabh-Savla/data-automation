"""StandardizationService — turns a connector's RawAssetDraft into a validated,
checksummed, NAS-written, DB-persisted Asset. This is the one place "promote
to NAS + record provenance" happens, shared by every connector.
"""

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image

from app.connectors.base import RawAssetDraft
from app.db.models.asset import Asset
from app.db.repositories.asset_repository import AssetRepository
from app.domain.enums import AnnotationFormat, AssetStatus, SourceType
from app.storage.base import StorageBackend

Image.MAX_IMAGE_PIXELS = None  # suppress DecompressionBomb warnings on large datasets


class AssetRejected(Exception):
    """Raised when a draft fails validation (corrupt, too small, etc.) —
    caller should count it toward JobSourceTask.rejected_count, not error_count."""


class StandardizationService:
    def __init__(
        self,
        asset_repo: AssetRepository,
        storage: StorageBackend,
        min_resolution: tuple[int, int] = (64, 64),
    ):
        self.asset_repo = asset_repo
        self.storage = storage
        self.min_w, self.min_h = min_resolution

    def standardize_and_promote(
        self,
        job_id: uuid.UUID,
        job_source_task_id: uuid.UUID,
        source_type: SourceType,
        draft: RawAssetDraft,
    ) -> tuple[Asset, bool]:
        width, height = self._validate_image(draft.local_tmp_path)

        checksum = self._checksum(draft.local_tmp_path)

        rel_dest = self._relative_path(job_id, source_type, draft)
        nas_path = self.storage.write(draft.local_tmp_path, rel_dest)

        annotation_nas_path = None
        annotation_format = AnnotationFormat.NONE
        if draft.annotation_tmp_path is not None and draft.annotation_tmp_path.exists():
            ann_rel_dest = self._relative_annotation_path(job_id, source_type, draft)
            annotation_nas_path = self.storage.write(draft.annotation_tmp_path, ann_rel_dest)
            annotation_format = AnnotationFormat(draft.annotation_format or AnnotationFormat.NONE)

        # Computed inline (not deferred to the async dedup task) since it's a
        # cheap, already-open-the-file operation — avoids a second I/O pass
        # over every image later just to hash it.
        phash = self._compute_phash(draft.local_tmp_path)

        asset = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=job_source_task_id,
            source_type=source_type,
            source_item_id=draft.source_item_id,
            source_url=draft.source_url,
            dataset_identifier=draft.dataset_identifier,
            license=draft.license,
            original_name=draft.original_name,
            width=width,
            height=height,
            file_format=draft.local_tmp_path.suffix.lstrip(".").lower(),
            checksum_sha256=checksum,
            phash=phash,
            nas_path=nas_path,
            annotation_format=annotation_format,
            annotation_nas_path=annotation_nas_path,
            classes=draft.classes,
            status=AssetStatus.PROMOTED,
            asset_metadata=draft.metadata,
            promoted_at=datetime.now(timezone.utc),
        )
        return self.asset_repo.upsert(asset)

    def _validate_image(self, path: Path) -> tuple[int, int]:
        try:
            with Image.open(path) as img:
                img.verify()
            with Image.open(path) as img:
                width, height = img.size
        except Exception as e:
            raise AssetRejected(f"Corrupt or unreadable image: {e}") from e

        if width < self.min_w or height < self.min_h:
            raise AssetRejected(
                f"Image {width}x{height} below minimum {self.min_w}x{self.min_h}"
            )
        return width, height

    @staticmethod
    def _compute_phash(path: Path) -> str | None:
        try:
            import imagehash
            from PIL import Image

            with Image.open(path) as img:
                return str(imagehash.phash(img))
        except Exception:
            return None

    @staticmethod
    def _checksum(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _relative_path(job_id: uuid.UUID, source_type: SourceType, draft: RawAssetDraft) -> str:
        ext = draft.local_tmp_path.suffix.lower() or ".jpg"
        return f"{job_id}/{source_type.value}/images/{draft.source_item_id}{ext}"

    @staticmethod
    def _relative_annotation_path(
        job_id: uuid.UUID, source_type: SourceType, draft: RawAssetDraft
    ) -> str:
        ext = draft.annotation_tmp_path.suffix.lower() if draft.annotation_tmp_path else ".txt"
        return f"{job_id}/{source_type.value}/annotations/{draft.source_item_id}{ext}"
