"""Repository for ImageBatch and NasTransferBatch database access."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.candidate_window import CandidateWindow
from app.domain.batch_state_machine import BatchStateMachine, IllegalBatchTransitionError
from app.domain.enums import ASSET_ACCEPTED_STATUSES, AssetStatus, BatchStatus, TransferStatus

logger = logging.getLogger(__name__)


class ImageBatchRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, batch: ImageBatch) -> ImageBatch:
        self.db.add(batch)
        self.db.flush()
        return batch

    def get(self, batch_id: uuid.UUID) -> ImageBatch | None:
        return self.db.execute(select(ImageBatch).where(ImageBatch.id == batch_id)).scalar_one_or_none()

    def get_by_index(self, job_id: uuid.UUID, batch_index: int) -> ImageBatch | None:
        return self.db.execute(
            select(ImageBatch).where(ImageBatch.job_id == job_id, ImageBatch.batch_index == batch_index)
        ).scalar_one_or_none()

    def list_for_job(self, job_id: uuid.UUID) -> Sequence[ImageBatch]:
        return self.db.execute(
            select(ImageBatch).where(ImageBatch.job_id == job_id).order_by(ImageBatch.batch_index.asc())
        ).scalars().all()

    def get_active_batch(self, job_id: uuid.UUID) -> ImageBatch | None:
        """Returns the current incomplete/processing batch for the job, if any."""
        terminal_statuses = (BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED)
        return self.db.execute(
            select(ImageBatch)
            .where(ImageBatch.job_id == job_id, ~ImageBatch.status.in_(terminal_statuses))
            .order_by(ImageBatch.batch_index.asc())
            .limit(1)
        ).scalar_one_or_none()

    def set_status(
        self,
        batch_id: uuid.UUID,
        status: BatchStatus,
        *,
        manifest_path: str | None = None,
        local_staging_path: str | None = None,
        reason: str = "",
        force: bool = False,
    ) -> None:
        batch = self.db.get(ImageBatch, batch_id)
        if batch is not None and not force:
            BatchStateMachine.transition(batch.status, status, reason=reason)

        values: dict = {"status": status, "updated_at": datetime.now(timezone.utc)}
        if manifest_path is not None:
            values["manifest_path"] = manifest_path
        if local_staging_path is not None:
            values["local_staging_path"] = local_staging_path
        if status in (BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED):
            values["completed_at"] = datetime.now(timezone.utc)
        self.db.execute(update(ImageBatch).where(ImageBatch.id == batch_id).values(**values))
        self.db.flush()

    def reconcile_batch_counters(self, batch_id: uuid.UUID) -> dict[str, int]:
        """Recomputes batch counters strictly from the Asset records."""
        from app.db.models.asset import Asset
        stmt = select(Asset).where(Asset.batch_id == batch_id)
        assets = self.db.execute(stmt).scalars().all()
        accepted = sum(1 for a in assets if a.status in ASSET_ACCEPTED_STATUSES)
        rejected = sum(1 for a in assets if a.status == AssetStatus.RELEVANCE_REJECTED)
        duplicates = sum(1 for a in assets if a.status == AssetStatus.DUPLICATE)
        uncertain = sum(1 for a in assets if a.status == AssetStatus.REVIEW_PENDING)
        errors = sum(1 for a in assets if a.status in (AssetStatus.RETRY_PENDING, AssetStatus.FAILED))

        self.update_counts(
            batch_id,
            accepted=accepted,
            rejected=rejected,
            duplicate=duplicates,
            uncertain=uncertain,
            error=errors,
        )
        self.db.flush()
        return {
            "accepted": accepted,
            "rejected": rejected,
            "duplicate": duplicates,
            "uncertain": uncertain,
            "error": errors,
        }

    def update_counts(
        self,
        batch_id: uuid.UUID,
        *,
        accepted: int | None = None,
        rejected: int | None = None,
        duplicate: int | None = None,
        uncertain: int | None = None,
        error: int | None = None,
    ) -> None:
        values: dict = {"updated_at": datetime.now(timezone.utc)}
        if accepted is not None:
            values["accepted_image_count"] = accepted
        if rejected is not None:
            values["rejected_image_count"] = rejected
        if duplicate is not None:
            values["duplicate_image_count"] = duplicate
        if uncertain is not None:
            values["uncertain_image_count"] = uncertain
        if error is not None:
            values["error_image_count"] = error
        self.db.execute(update(ImageBatch).where(ImageBatch.id == batch_id).values(**values))
        self.db.flush()


class NasTransferBatchRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, transfer: NasTransferBatch) -> NasTransferBatch:
        self.db.add(transfer)
        self.db.flush()
        return transfer

    def get(self, transfer_id: uuid.UUID) -> NasTransferBatch | None:
        return self.db.execute(select(NasTransferBatch).where(NasTransferBatch.id == transfer_id)).scalar_one_or_none()

    def get_by_batch_id(self, batch_id: uuid.UUID) -> NasTransferBatch | None:
        return self.db.execute(select(NasTransferBatch).where(NasTransferBatch.batch_id == batch_id)).scalar_one_or_none()

    def get_by_window_id(self, window_id: uuid.UUID) -> NasTransferBatch | None:
        """Return the NAS transfer associated with one candidate window."""
        return self.db.execute(
            select(NasTransferBatch).where(
                NasTransferBatch.window_id == window_id
            )
        ).scalar_one_or_none()

    def get_or_create_for_window(
        self,
        window_id: uuid.UUID,
        batch_id: uuid.UUID,
        job_id: uuid.UUID,
        remote_path: str,
        manifest_path: str,
        total_files: int,
        asset_ids: list[str] | None = None,
        window_index: int | None = None,
    ) -> NasTransferBatch:
        """
        Create exactly one durable NAS transfer per candidate window.

        Locking the CandidateWindow row serializes concurrent enqueue attempts.
        Existing transfer metadata is retained on retries.
        """
        window = self.db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.id == window_id)
            .with_for_update()
        ).scalar_one_or_none()

        if window is None:
            raise ValueError(f"Candidate window {window_id} does not exist.")

        if window.batch_id != batch_id or window.job_id != job_id:
            raise ValueError(
                "Candidate window does not belong to the supplied batch and job."
            )

        existing = self.get_by_window_id(window_id)
        if existing is not None:
            metadata = dict(existing.transfer_metadata or {})
            existing_manifest = metadata.get("manifest_path")

            if existing_manifest and existing_manifest != manifest_path:
                raise ValueError(
                    "A transfer already exists for this window with a different manifest."
                )

            return existing

        metadata = {
            "asset_ids": asset_ids or [],
            "manifest_path": manifest_path,
            "window_index": window_index or window.window_index,
        }

        transfer = NasTransferBatch(
            batch_id=batch_id,
            job_id=job_id,
            window_id=window_id,
            remote_path=remote_path,
            transfer_status=TransferStatus.PENDING,
            total_files=total_files,
            pending_files=total_files,
            uploaded_files=0,
            failed_files=0,
            bytes_transferred=0,
            transfer_metadata=metadata,
        )

        return self.create(transfer)

    def get_or_create_for_batch(
        self,
        batch_id: uuid.UUID,
        job_id: uuid.UUID,
        remote_path: str,
        total_files: int = 0,
        asset_ids: list[str] | None = None,
    ) -> NasTransferBatch:
        existing = self.get_by_batch_id(batch_id)
        if existing is not None:
            return existing

        transfer = NasTransferBatch(
            batch_id=batch_id,
            job_id=job_id,
            remote_path=remote_path,
            transfer_status=TransferStatus.PENDING,
            total_files=total_files,
            pending_files=total_files,
            uploaded_files=0,
            failed_files=0,
            bytes_transferred=0,
            transfer_metadata={"asset_ids": asset_ids or []},
        )
        return self.create(transfer)

    def list_pending(self, limit: int = 50) -> Sequence[NasTransferBatch]:
        """Lists transfers ready to be processed by the centralized NAS writer."""
        return self.db.execute(
            select(NasTransferBatch)
            .where(NasTransferBatch.transfer_status.in_([TransferStatus.PENDING, TransferStatus.UPLOADING]))
            .order_by(NasTransferBatch.created_at.asc())
            .limit(limit)
        ).scalars().all()

    def list_actionable(
        self,
        stale_threshold_seconds: int = 600,
        max_retries: int = 5,
        retry_delay_seconds: int = 30,
        limit: int = 50,
    ) -> Sequence[NasTransferBatch]:
        """Finds transfers actionable by the central NAS writer from PostgreSQL (source of truth):
        1. Status == PENDING.
        2. Status == VERIFIED (worker crashed after verification commit; requires completion & staging cleanup).
        3. Status == UPLOADING but stale (updated_at < now - stale_threshold_seconds),
           indicating worker crash during transfer.
        4. Status == FAILED but eligible for retry (attempt_count < max_retries and
           updated_at < now - retry_delay_seconds).
        Transfers with attempt_count >= max_retries are exhausted and will not be selected.
        """
        from datetime import timedelta
        from sqlalchemy import or_, and_
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(seconds=stale_threshold_seconds)
        retry_cutoff = now - timedelta(seconds=retry_delay_seconds)
        return self.db.execute(
            select(NasTransferBatch)
            .where(
                or_(
                    NasTransferBatch.transfer_status == TransferStatus.PENDING,
                    NasTransferBatch.transfer_status == TransferStatus.VERIFIED,
                    and_(
                        NasTransferBatch.transfer_status == TransferStatus.UPLOADING,
                        NasTransferBatch.updated_at < stale_cutoff,
                    ),
                    and_(
                        NasTransferBatch.transfer_status == TransferStatus.FAILED,
                        NasTransferBatch.attempt_count < max_retries,
                        NasTransferBatch.updated_at < retry_cutoff,
                    ),
                )
            )
            .order_by(NasTransferBatch.created_at.asc())
            .limit(limit)
        ).scalars().all()

    def mark_uploading(self, transfer_id: uuid.UUID, owner_token: str | None = None) -> None:
        now = datetime.now(timezone.utc)
        record = self.get(transfer_id)
        meta = dict(record.transfer_metadata or {}) if record else {}
        if owner_token:
            meta["owner_token"] = owner_token
        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(
                transfer_status=TransferStatus.UPLOADING,
                attempt_count=NasTransferBatch.attempt_count + 1,
                started_at=now,
                updated_at=now,
                transfer_metadata=meta,
            )
        )
        self.db.flush()

    def update_progress(
        self,
        transfer_id: uuid.UUID,
        uploaded_files: int,
        pending_files: int,
        bytes_transferred: int,
        failed_files: int = 0,
        metadata_update: dict | None = None,
    ) -> None:
        values = {
            "uploaded_files": uploaded_files,
            "pending_files": pending_files,
            "bytes_transferred": bytes_transferred,
            "failed_files": failed_files,
            "updated_at": datetime.now(timezone.utc),
        }
        if metadata_update:
            transfer = self.get(transfer_id)
            if transfer:
                merged = dict(transfer.transfer_metadata or {})
                merged.update(metadata_update)
                values["transfer_metadata"] = merged

        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(**values)
        )
        self.db.flush()

    def mark_verified(
        self,
        transfer_id: uuid.UUID,
        checksum: str | None = None,
        owner_token: str | None = None,
    ) -> None:
        """Marks transfer as VERIFIED conditionally if owner_token matches current claimant."""
        if owner_token:
            record = self.get(transfer_id)
            current_owner = (record.transfer_metadata or {}).get("owner_token") if record else None
            if current_owner and current_owner != owner_token:
                logger.error(f"[NAS REPO] Worker {owner_token} cannot mark verified; superseded by {current_owner}.")
                from app.storage.nas_writer import LockLostError
                raise LockLostError(
                    f"Conditional DB update failed: transfer was claimed by another worker ({current_owner})."
                )

        now = datetime.now(timezone.utc)
        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(
                transfer_status=TransferStatus.VERIFIED,
                verification_status="passed",
                checksum=checksum,
                verified_at=now,
                updated_at=now,
            )
        )
        self.db.flush()

    def mark_completed_staging_pending(
        self,
        transfer_id: uuid.UUID,
        owner_token: str | None = None,
    ) -> None:
        """Marks transfer COMPLETED conditionally if owner_token matches current claimant."""
        if owner_token:
            record = self.get(transfer_id)
            current_owner = (record.transfer_metadata or {}).get("owner_token") if record else None
            if current_owner and current_owner != owner_token:
                logger.error(f"[NAS REPO] Worker {owner_token} cannot mark completed; superseded by {current_owner}.")
                from app.storage.nas_writer import LockLostError
                raise LockLostError(
                    f"Conditional DB update failed: transfer was claimed by another worker ({current_owner})."
                )

        now = datetime.now(timezone.utc)
        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(
                transfer_status=TransferStatus.COMPLETED,
                staging_cleanup_status="pending",
                completed_at=now,
                updated_at=now,
            )
        )
        self.db.flush()

    def mark_staging_cleaned(self, transfer_id: uuid.UUID) -> None:
        """Records local staging cleanup as completed."""
        now = datetime.now(timezone.utc)
        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(
                staging_cleanup_status="completed",
                staging_cleaned_at=now,
                updated_at=now,
            )
        )
        self.db.flush()

    def list_pending_cleanups(self, limit: int = 50) -> list[NasTransferBatch]:
        """Finds completed transfers that still have local staging cleanup pending."""
        stmt = (
            select(NasTransferBatch)
            .where(
                NasTransferBatch.transfer_status == TransferStatus.COMPLETED,
                NasTransferBatch.staging_cleanup_status == "pending",
            )
            .order_by(NasTransferBatch.updated_at.asc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def mark_completed(self, transfer_id: uuid.UUID) -> None:
        """Marks transfer as COMPLETED: assets repointed to nas://, and local staging safely purged."""
        now = datetime.now(timezone.utc)
        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(
                transfer_status=TransferStatus.COMPLETED,
                staging_cleanup_status="completed",
                staging_cleaned_at=now,
                completed_at=now,
                updated_at=now,
            )
        )
        self.db.flush()

    def mark_failed(self, transfer_id: uuid.UUID, error: str) -> None:
        self.db.execute(
            update(NasTransferBatch)
            .where(NasTransferBatch.id == transfer_id)
            .values(
                transfer_status=TransferStatus.FAILED,
                verification_status="failed",
                last_error=error,
                updated_at=datetime.now(timezone.utc),
            )
        )
        self.db.flush()

