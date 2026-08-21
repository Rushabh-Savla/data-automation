"""StandardizationService — turns a connector's RawAssetDraft into a validated,
checksummed, NAS-written, DB-persisted Asset. This is the one place "promote
to NAS + record provenance" happens, shared by every connector.
"""

import hashlib
import uuid
from datetime import datetime, timezone
from functools import lru_cache
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


@lru_cache(maxsize=1)
def _load_clip_model(name: str = "ViT-B/32"):
    """Lazily-loaded, process-wide CLIP model for standalone image-embedding
    extraction — decoupled from YOLO-World's own internal CLIP text-encoder
    (app/filtering/yolo_world_filter.py), which never exposes its embeddings
    for application-level use."""
    import clip
    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, preprocess = clip.load(name, device=device)
    model.eval()
    return model, preprocess, device


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
        embedding = self._compute_embedding(draft.local_tmp_path)

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
            embedding=embedding,
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
    def _compute_embedding(path: Path) -> list[float] | None:
        """CLIP image embedding — the additive "vector matching" signal for
        hybrid duplicate detection (app/filtering/dedup.py). Swallowed to
        None on any failure (model load, corrupt image, OOM), exactly like
        _compute_phash — a CLIP failure degrades that one asset to
        phash-only dedup rather than failing standardization."""
        try:
            import torch
            from PIL import Image

            model, preprocess, device = _load_clip_model()
            with Image.open(path) as img:
                image_input = preprocess(img.convert("RGB")).unsqueeze(0).to(device)
            with torch.no_grad():
                features = model.encode_image(image_input)
                # Pre-normalize so dedup's "cosine similarity" comparison is
                # a plain dot product instead of re-normalizing at compare
                # time on every pair.
                features = features / features.norm(dim=-1, keepdim=True)
            return features.squeeze(0).cpu().tolist()
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
