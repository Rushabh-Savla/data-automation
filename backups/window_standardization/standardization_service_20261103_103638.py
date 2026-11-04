"""StandardizationService — turns a connector's RawAssetDraft into a validated,
checksummed, NAS-written, DB-persisted Asset. This is the one place "promote
to NAS + record provenance" happens, shared by every connector.
"""

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from PIL import Image
from sqlalchemy.orm import object_session

from app.connectors.base import RawAssetDraft
from app.db.models.asset import Asset
from app.db.repositories.asset_repository import AssetRepository
from app.domain.enums import AnnotationFormat, AssetStatus, SourceType
from app.storage import asset_files
from app.storage.base import StorageBackend

Image.MAX_IMAGE_PIXELS = None  # suppress DecompressionBomb warnings on large datasets

logger = logging.getLogger(__name__)


class AssetRejected(Exception):
    """Raised when a draft fails validation (corrupt, too small, etc.) —
    caller should count it toward JobSourceTask.rejected_count, not error_count."""


def _refresh_paths(asset: Asset) -> None:
    """Re-read the asset's file paths from the database. Callers (trim, dedup, the
    relevance filter) load the asset earlier in their transaction; if the image was
    moved to the NAS in between, their copy still holds the old local path and the
    NAS copy would be left behind. Fail-open: if the asset is not attached to a
    session, the paths it already has are used."""
    try:
        session = object_session(asset)
        if session is not None:
            session.refresh(asset, attribute_names=["nas_path", "annotation_nas_path"])
    except Exception as e:
        logger.debug(f"Could not refresh file paths for asset {getattr(asset, 'id', '?')}: {e}")


def delete_asset_files(asset: Asset) -> None:
    """Removes an asset's promoted files from NAS — the symmetric operation
    to standardize_and_promote's write. Called once an asset stops being
    part of the kept set (relevance-rejected, deduped, trimmed for
    exceeding target_count, or explicitly deleted via the API) so NAS only
    ever holds the final filtered set, not every raw download. nas_path/
    annotation_nas_path are already absolute (see standardize_and_promote),
    so this uses them directly rather than going through StorageBackend
    (whose delete() expects a path relative to its own root) — the same
    convention get_asset_file already follows for reads. Per-file
    try/except: a deletion failure is logged and never raised, matching
    the fail-open pattern used throughout the filtering pipeline. A path may
    also be "nas://..." (image already moved to the UI-connected NAS);
    asset_files.delete removes it there — the local branch is unchanged."""
    _refresh_paths(asset)
    for path_str in (asset.nas_path, asset.annotation_nas_path):
        if not path_str:
            continue
        try:
            asset_files.delete(path_str)
        except Exception as e:
            logger.warning(f"Failed to delete asset file {path_str} for asset {asset.id}: {e}")


@lru_cache(maxsize=1)
def _load_clip_model(name: str = "ViT-B/32"):
    """Lazily-loaded, process-wide CLIP model for image-embedding extraction.

    Used only for deduplication (app/filtering/dedup.py). It is deliberately
    NOT used for relevance any more: a CLIP embedding encodes scene gist, and
    measured on real job frames it separated actual weapons from briefcase and
    wagon-wheel frames by +0.007 -- see app/filtering/vlm_filter.py for the
    measurements and what replaced it. Near-duplicate detection is a different
    question, and one these vectors genuinely do answer well."""
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
        if getattr(draft, "buffer", None) is not None:
            buf = draft.buffer
            try:
                width, height = buf.validate_image(self.min_w, self.min_h)
            except Exception as e:
                raise AssetRejected(f"Corrupt or unreadable image stream: {e}") from e
            checksum = buf.compute_sha256()
            phash = buf.compute_phash()
            embedding = buf.compute_clip_embedding()

            rel_dest = self._relative_path(job_id, source_type, draft)
            if draft.local_tmp_path and draft.local_tmp_path.exists():
                nas_path = self.storage.write(draft.local_tmp_path, rel_dest)
            else:
                # Durably stage directly from memory buffer
                dest_path = self.storage.resolve(rel_dest) if hasattr(self.storage, "resolve") else Path(rel_dest)
                buf.stage_to_file(dest_path)
                nas_path = str(dest_path)
        else:
            width, height = self._validate_image(draft.local_tmp_path)
            checksum = self._checksum(draft.local_tmp_path)
            rel_dest = self._relative_path(job_id, source_type, draft)
            nas_path = self.storage.write(draft.local_tmp_path, rel_dest)
            phash = self._compute_phash(draft.local_tmp_path)
            embedding = self._compute_embedding(draft.local_tmp_path)

        annotation_nas_path = None
        annotation_format = AnnotationFormat.NONE
        if draft.annotation_tmp_path is not None and draft.annotation_tmp_path.exists():
            ann_rel_dest = self._relative_annotation_path(job_id, source_type, draft)
            annotation_nas_path = self.storage.write(draft.annotation_tmp_path, ann_rel_dest)
            annotation_format = AnnotationFormat(draft.annotation_format or AnnotationFormat.NONE)

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
