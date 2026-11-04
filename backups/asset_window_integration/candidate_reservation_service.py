"""Atomic PostgreSQL-backed candidate admission for each image batch."""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.core.config import get_settings
from app.db.models.candidate_reservation import CandidateReservation
from app.db.models.image_batch import ImageBatch
from app.db.models.job_source_task import JobSourceTask
from app.db.session import session_scope
from app.domain.enums import BatchStatus

logger = logging.getLogger(__name__)

MAX_CANDIDATES_PER_WINDOW = 600
RESERVATION_LEASE_MINUTES = 120

ACTIVE_STATUSES = (
    BatchStatus.DOWNLOADING,
    BatchStatus.REPLENISHING,
)


def _bounded_source_item_id(value: str) -> str:
    value = str(value)

    if len(value) <= 512:
        return value

    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
    return f"{value[:487]}_{digest}"


def _configured_limit() -> int:
    configured = int(get_settings().download_candidate_window)
    return max(1, min(MAX_CANDIDATES_PER_WINDOW, configured))


def reserve_candidate(
    job_id: uuid.UUID | str,
    job_source_task_id: uuid.UUID | str,
    source_type: str,
    source_item_id: str,
) -> uuid.UUID | None:
    """Reserve capacity before fetching an individual image.

    Returns an existing reservation for repeated candidate identities.
    Returns None when no active acquisition batch exists or capacity is full.

    Failed and rejected candidates continue consuming capacity. Reservations
    are not automatically released because a worker might have crashed after
    downloading a file but before recording its completion.
    """

    job_uuid = uuid.UUID(str(job_id))
    task_uuid = uuid.UUID(str(job_source_task_id))

    source_value = str(getattr(source_type, "value", source_type))
    item_key = _bounded_source_item_id(source_item_id)

    if not source_value or len(source_value) > 32:
        raise ValueError("source_type must be 1-32 characters")

    if not item_key:
        raise ValueError("source_item_id must not be empty")

    with session_scope() as db:
        task = db.get(JobSourceTask, task_uuid)

        if task is None or task.job_id != job_uuid:
            logger.warning("Candidate reservation rejected: invalid source task")
            return None

        # Lock the parent batch row. All workers must acquire this same lock
        # before counting and inserting reservations.
        batch = db.execute(
            select(ImageBatch)
            .where(
                ImageBatch.job_id == job_uuid,
                ImageBatch.status.in_(ACTIVE_STATUSES),
            )
            .order_by(ImageBatch.batch_index.asc())
            .limit(1)
            .with_for_update()
        ).scalar_one_or_none()

        if batch is None:
            return None

        existing = db.execute(
            select(CandidateReservation).where(
                CandidateReservation.batch_id == batch.id,
                CandidateReservation.source_type == source_value,
                CandidateReservation.source_item_id == item_key,
            )
        ).scalar_one_or_none()

        if existing is not None:
            return existing.id

        metadata = dict(batch.batch_metadata or {})
        persisted_limit = metadata.get("candidate_limit")

        if persisted_limit is None:
            candidate_limit = _configured_limit()
            metadata["candidate_limit"] = candidate_limit
            batch.batch_metadata = metadata
            db.flush()
        else:
            candidate_limit = max(
                1,
                min(MAX_CANDIDATES_PER_WINDOW, int(persisted_limit)),
            )

        used = db.execute(
            select(func.count(CandidateReservation.id)).where(
                CandidateReservation.batch_id == batch.id
            )
        ).scalar_one()

        if used >= candidate_limit:
            return None

        now = datetime.now(timezone.utc)

        reservation = CandidateReservation(
            id=uuid.uuid4(),
            batch_id=batch.id,
            job_id=job_uuid,
            job_source_task_id=task_uuid,
            source_type=source_value,
            source_item_id=item_key,
            status="RESERVED",
            lease_expires_at=now + timedelta(
                minutes=RESERVATION_LEASE_MINUTES
            ),
        )

        db.add(reservation)
        db.flush()

        reservation_id = reservation.id
        db.commit()

        return reservation_id


def mark_candidate_staged(
    reservation_id: uuid.UUID | str,
    asset_id: uuid.UUID | str | None = None,
) -> bool:
    """Mark a successfully standardized candidate as locally staged."""

    reservation_uuid = uuid.UUID(str(reservation_id))
    asset_uuid = uuid.UUID(str(asset_id)) if asset_id else None

    with session_scope() as db:
        reservation = db.execute(
            select(CandidateReservation)
            .where(CandidateReservation.id == reservation_uuid)
            .with_for_update()
        ).scalar_one_or_none()

        if reservation is None:
            return False

        if reservation.status not in ("RESERVED", "STAGED"):
            return reservation.status == "COMMITTED"

        reservation.status = "STAGED"

        if asset_uuid is not None:
            reservation.asset_id = asset_uuid

        reservation.lease_expires_at = None
        reservation.updated_at = datetime.now(timezone.utc)

        db.commit()
        return True


def mark_candidate_failed(
    reservation_id: uuid.UUID | str,
) -> bool:
    """Record a failed candidate without releasing its capacity slot."""

    reservation_uuid = uuid.UUID(str(reservation_id))

    with session_scope() as db:
        reservation = db.execute(
            select(CandidateReservation)
            .where(CandidateReservation.id == reservation_uuid)
            .with_for_update()
        ).scalar_one_or_none()

        if reservation is None:
            return False

        if reservation.status in ("STAGED", "COMMITTED"):
            return False

        reservation.status = "FAILED"
        reservation.lease_expires_at = None
        reservation.updated_at = datetime.now(timezone.utc)

        db.commit()
        return True


def get_candidate_window_stats(
    batch_id: uuid.UUID | str,
) -> dict[str, int]:
    """Return candidate reservation usage and remaining capacity."""

    batch_uuid = uuid.UUID(str(batch_id))

    with session_scope() as db:
        batch = db.get(ImageBatch, batch_uuid)

        if batch is None:
            raise ValueError(f"Image batch {batch_uuid} does not exist")

        used = db.execute(
            select(func.count(CandidateReservation.id)).where(
                CandidateReservation.batch_id == batch_uuid
            )
        ).scalar_one()

        configured = (
            batch.batch_metadata or {}
        ).get("candidate_limit", _configured_limit())

        limit = max(
            1,
            min(MAX_CANDIDATES_PER_WINDOW, int(configured)),
        )

        return {
            "reserved": int(used),
            "limit": limit,
            "remaining": max(0, limit - int(used)),
        }
