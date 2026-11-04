"""StandardizationService — turns a connector's RawAssetDraft into a validated,
checksummed, NAS-written, DB-persisted Asset. This is the one place "promote
to NAS + record provenance" happens, shared by every connector.
"""

import hashlib
import logging
import shutil
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
        """Validate and persist an asset.

        Window candidates are atomically staged on local SSD before their
        Asset and CandidateReservation records are committed together.
        Legacy non-window ingestion retains the configured storage backend.
        """
        buf = getattr(draft, "buffer", None)

        if buf is not None:
            try:
                width, height = buf.validate_image(self.min_w, self.min_h)
            except Exception as exc:
                raise AssetRejected(
                    f"Corrupt or unreadable image stream: {exc}"
                ) from exc
            checksum = buf.compute_sha256()
            phash = buf.compute_phash()
            embedding = buf.compute_clip_embedding()
        else:
            width, height = self._validate_image(draft.local_tmp_path)
            checksum = self._checksum(draft.local_tmp_path)
            phash = self._compute_phash(draft.local_tmp_path)
            embedding = self._compute_embedding(draft.local_tmp_path)

        metadata = draft.metadata or {}
        reservation_id_raw = metadata.get("candidate_reservation_id")

        batch_id = None
        window_id = None
        reservation = None

        if reservation_id_raw:
            from sqlalchemy import select
            from app.db.models.candidate_reservation import CandidateReservation
            from app.db.models.candidate_window import CandidateWindow
            from app.db.models.image_batch import ImageBatch
            from app.storage.window_staging import WindowStagingManager

            db = self.asset_repo.db
            reservation_uuid = uuid.UUID(str(reservation_id_raw))

            reservation = db.execute(
                select(CandidateReservation)
                .where(CandidateReservation.id == reservation_uuid)
                .with_for_update()
            ).scalar_one_or_none()

            if reservation is None:
                raise RuntimeError(
                    f"Candidate reservation {reservation_uuid} not found"
                )

            if (
                reservation.job_id != job_id
                or reservation.job_source_task_id != job_source_task_id
                or str(reservation.source_type) != str(source_type.value)
                or reservation.source_item_id != draft.source_item_id
            ):
                raise RuntimeError(
                    f"Candidate reservation ownership mismatch: {reservation_uuid}"
                )

            if reservation.status not in ("RESERVED", "STAGED"):
                raise RuntimeError(
                    f"Reservation {reservation_uuid} has invalid state "
                    f"{reservation.status}"
                )

            window = db.get(CandidateWindow, reservation.window_id)
            if window is None:
                raise RuntimeError(
                    f"Candidate window {reservation.window_id} not found"
                )

            if metadata.get("candidate_window_id") != str(window.id):
                raise RuntimeError("Draft candidate window does not match reservation")

            batch = db.get(ImageBatch, window.batch_id)
            if batch is None or batch.job_id != job_id:
                raise RuntimeError("Candidate batch is missing or belongs to another job")

            batch_id = batch.id
            window_id = window.id

            staging = WindowStagingManager()
            asset_id = reservation.asset_id or reservation.id

            image_source = (
                draft.local_tmp_path
                if draft.local_tmp_path and draft.local_tmp_path.exists()
                else buf
            )
            if image_source is None:
                raise RuntimeError(
                    f"No local image or buffer available for {draft.source_item_id}"
                )

            file_ext = (
                draft.local_tmp_path.suffix.lstrip(".").lower()
                if draft.local_tmp_path
                else Path(draft.original_name or "image.jpg").suffix.lstrip(".").lower()
            ) or "jpg"

            staged_image = staging.stage_accepted_image(
                job_id,
                batch.batch_index,
                window.window_index,
                asset_id,
                file_ext,
                image_source,
            )

            if self._checksum(staged_image) != checksum:
                raise IOError(
                    f"Staged image checksum mismatch for {draft.source_item_id}"
                )

            nas_path = str(staged_image)
            annotation_nas_path = None
            annotation_format = AnnotationFormat.NONE

            if draft.annotation_tmp_path and draft.annotation_tmp_path.exists():
                annotation_format = AnnotationFormat(
                    draft.annotation_format or AnnotationFormat.NONE
                )

                annotation_dir = (
                    staging.get_window_staging_dir(
                        job_id, batch.batch_index, window.window_index
                    ) / "annotations"
                )
                annotation_dir.mkdir(parents=True, exist_ok=True)

                annotation_ext = draft.annotation_tmp_path.suffix.lower() or ".txt"
                annotation_dest = annotation_dir / f"{asset_id}{annotation_ext}"
                annotation_tmp = annotation_dir / (
                    f".{asset_id}.{uuid.uuid4().hex}.tmp"
                )

                try:
                    shutil.copy2(draft.annotation_tmp_path, annotation_tmp)
                    annotation_tmp.replace(annotation_dest)
                except Exception:
                    annotation_tmp.unlink(missing_ok=True)
                    raise

                annotation_nas_path = str(annotation_dest)

        else:
            rel_dest = self._relative_path(job_id, source_type, draft)

            if buf is not None and (
                not draft.local_tmp_path or not draft.local_tmp_path.exists()
            ):
                dest_path = (
                    self.storage.resolve(rel_dest)
                    if hasattr(self.storage, "resolve")
                    else Path(rel_dest)
                )
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                buf.stage_to_file(dest_path)
                nas_path = str(dest_path)
            else:
                nas_path = self.storage.write(draft.local_tmp_path, rel_dest)

            annotation_nas_path = None
            annotation_format = AnnotationFormat.NONE

            if draft.annotation_tmp_path and draft.annotation_tmp_path.exists():
                ann_rel_dest = self._relative_annotation_path(
                    job_id, source_type, draft
                )
                annotation_nas_path = self.storage.write(
                    draft.annotation_tmp_path, ann_rel_dest
                )
                annotation_format = AnnotationFormat(
                    draft.annotation_format or AnnotationFormat.NONE
                )

        file_format = (
            draft.local_tmp_path.suffix.lstrip(".").lower()
            if draft.local_tmp_path
            else Path(draft.original_name or "image.jpg").suffix.lstrip(".").lower()
        )

        asset = Asset(
            id=reservation.asset_id if reservation and reservation.asset_id else (
                reservation.id if reservation else uuid.uuid4()
            ),
            job_id=job_id,
            job_source_task_id=job_source_task_id,
            batch_id=batch_id,
            window_id=window_id,
            source_type=source_type,
            source_item_id=draft.source_item_id,
            source_url=draft.source_url,
            dataset_identifier=draft.dataset_identifier,
            license=draft.license,
            original_name=draft.original_name,
            width=width,
            height=height,
            file_format=file_format or "jpg",
            checksum_sha256=checksum,
            phash=phash,
            embedding=embedding,
            nas_path=nas_path,
            annotation_format=annotation_format,
            annotation_nas_path=annotation_nas_path,
            classes=draft.classes,
            status=AssetStatus.PROMOTED,
            asset_metadata=metadata,
            promoted_at=datetime.now(timezone.utc),
        )

        stored_asset, created = self.asset_repo.upsert(asset)

        if reservation is not None:
            reservation.status = "STAGED"
            reservation.asset_id = stored_asset.id
            reservation.lease_expires_at = None
            reservation.updated_at = datetime.now(timezone.utc)

        return stored_asset, created

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
