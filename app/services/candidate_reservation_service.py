"""Atomic, window-scoped candidate admission using PostgreSQL row locks."""

from __future__ import annotations

import hashlib
import logging
import math
import random
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone

from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError

from app.core.config import get_settings
from app.db.models.candidate_reservation import CandidateReservation
from app.db.models.candidate_window import CandidateWindow
from app.db.models.image_batch import ImageBatch
from app.db.models.job_source_task import JobSourceTask
from app.db.session import session_scope
from app.domain.enums import BatchStatus, JOB_SOURCE_TASK_TERMINAL_STATUSES, JobSourceTaskStatus, SourceType

logger = logging.getLogger(__name__)

MAX_CANDIDATES_PER_WINDOW = 5000

TERMINAL_WINDOW_STATES = {"COMPLETED", "LOCAL_FALLBACK"}


class CandidateAdmissionStatus(str, Enum):
    ADMITTED = "ADMITTED"
    DUPLICATE = "DUPLICATE"
    WINDOW_FULL = "WINDOW_FULL"
    INACTIVE = "INACTIVE"
    INVALID_OWNER = "INVALID_OWNER"


@dataclass(frozen=True)
class CandidateAdmission:
    status: CandidateAdmissionStatus
    reservation_id: uuid.UUID | None = None
    window_id: uuid.UUID | None = None
    window_index: int | None = None

    @property
    def admitted(self) -> bool:
        return self.status == CandidateAdmissionStatus.ADMITTED

ALLOWED_WINDOW_STATES = {
    "OPEN",
    "ACQUIRING",
    "FULL",
    "PROCESSING",
    "FILTERING",
    "UPLOADING",
    "NAS_PENDING",
    "VERIFYING",
    "PAUSED",
    "FAILED",
    "COMPLETED",
    "LOCAL_FALLBACK",
}


def configured_limit() -> int:
    value = int(get_settings().download_candidate_window)
    return max(1, min(MAX_CANDIDATES_PER_WINDOW, value))


def bounded_source_item_id(value: str) -> str:
    value = str(value)
    if not value:
        raise ValueError("source_item_id cannot be empty")
    if len(value) <= 512:
        return value

    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
    return f"{value[:487]}_{digest}"


def ensure_candidate_window(
    db,
    batch: ImageBatch,
    candidate_limit: int | None = None,
    force_new: bool = False,
) -> uuid.UUID:
    """Get or create the current window while the caller holds the batch lock."""
    latest = db.execute(
        select(CandidateWindow)
        .where(CandidateWindow.batch_id == batch.id)
        .order_by(CandidateWindow.window_index.desc())
        .limit(1)
        .with_for_update()
    ).scalar_one_or_none()

    if (
        not force_new
        and latest is not None
        and latest.status not in TERMINAL_WINDOW_STATES
        and latest.status != "FULL"
    ):
        return latest.id

    configured = configured_limit()
    if candidate_limit is not None:
        effective_limit = max(1, min(MAX_CANDIDATES_PER_WINDOW, int(candidate_limit)))
    else:
        # Dynamic calculation: if batch target > 600, use 600-candidate window.
        # Otherwise, scale window to remaining shortfall with overfetch buffer (e.g. 100 -> 150).
        batch_target = batch.target_image_count or configured
        accepted = batch.accepted_image_count or 0
        shortfall = max(1, batch_target - accepted)
        from app.core.config import calculate_initial_candidates
        from app.db.models.job import Job
        job_for_buf = db.get(Job, batch.job_id)
        effective_limit = min(MAX_CANDIDATES_PER_WINDOW, max(1, calculate_initial_candidates(shortfall, job_config=job_for_buf.config if job_for_buf else None)))

    if latest is not None and latest.status not in TERMINAL_WINDOW_STATES:
        latest.status = "FULL"

    next_index = latest.window_index + 1 if latest is not None else 1

    try:
        with db.begin_nested():
            window = CandidateWindow(
                id=uuid.uuid4(),
                batch_id=batch.id,
                job_id=batch.job_id,
                window_index=next_index,
                candidate_limit=effective_limit,
                status="OPEN",
            )
            db.add(window)
            db.flush()
            return window.id
    except IntegrityError:
        # Another concurrent worker created this window in the same millisecond.
        # The savepoint (begin_nested) rolled back safely without corrupting the session.
        existing = db.execute(
            select(CandidateWindow)
            .where(
                CandidateWindow.batch_id == batch.id,
                CandidateWindow.window_index == next_index,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing.id
        latest_after = db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.batch_id == batch.id)
            .order_by(CandidateWindow.window_index.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest_after is not None:
            return latest_after.id
        raise


def open_candidate_window(batch_id: uuid.UUID | str) -> uuid.UUID:
    """Get the current nonterminal window or explicitly create the next one."""
    batch_uuid = uuid.UUID(str(batch_id))

    with session_scope() as db:
        batch = db.execute(
            select(ImageBatch)
            .where(ImageBatch.id == batch_uuid)
            .with_for_update()
        ).scalar_one_or_none()

        if batch is None:
            raise ValueError(f"ImageBatch {batch_uuid} does not exist")

        window_id = ensure_candidate_window(db, batch)
        db.commit()
        return window_id


def admit_candidate_for_task(
    job_source_task_id: uuid.UUID | str,
    source_type: str,
    source_item_id: str,
) -> CandidateAdmission:
    """Reserve a candidate against the job's active batch/window.

    The batch row is locked before the candidate-window row. All source
    workers therefore serialize capacity checks against the same window.
    This function must be called before downloading or submitting a
    download future for the candidate.
    """
    task_uuid = uuid.UUID(str(job_source_task_id))
    source_value = str(getattr(source_type, "value", source_type))
    item_key = bounded_source_item_id(source_item_id)

    if not source_value or len(source_value) > 32:
        raise ValueError("source_type must contain 1-32 characters")

    terminal_batches = (
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.NAS_UPLOADING,
        BatchStatus.NAS_PENDING,
        BatchStatus.VERIFYING,
        BatchStatus.NAS_VERIFIED,
        BatchStatus.COMPLETED,
        BatchStatus.PARTIAL,
        BatchStatus.FAILED,
    )

    max_retries = 5
    for attempt in range(max_retries):
        try:
            with session_scope() as db:
                task = db.get(JobSourceTask, task_uuid)
                if task is None:
                    return CandidateAdmission(CandidateAdmissionStatus.INVALID_OWNER)

                task_source = str(getattr(task.source_type, "value", task.source_type))
                if task_source != source_value:
                    return CandidateAdmission(CandidateAdmissionStatus.INVALID_OWNER)

                # Acquire KEY SHARE locks in strict universal order: Job -> JobSourceTask.
                # This prevents lock ordering inversion with build_on_progress / orchestrator
                # and satisfies foreign key constraints before acquiring batch and window locks.
                from app.db.models.job import Job
                db.execute(
                    select(Job.id)
                    .where(Job.id == task.job_id)
                    .with_for_update(read=True, key_share=True)
                )
                db.execute(
                    select(JobSourceTask.id)
                    .where(JobSourceTask.id == task_uuid)
                    .with_for_update(read=True, key_share=True)
                )

                # Enforce global target count stop: reject new admissions if buffered target already reached
                job = db.get(Job, task.job_id)
                if job and job.target_count is not None:
                    from app.core.config import resolve_overfetch_buffer
                    buffer = resolve_overfetch_buffer(job.config)
                    buffered_max = math.ceil(job.target_count * buffer)
                    if (job.accepted_count or 0) >= buffered_max:
                        try:
                            from app.services.cancellation_service import CancellationService
                            from app.core.redis_client import get_redis
                            CancellationService(get_redis()).mark_target_reached(str(job.id))
                        except Exception:
                            pass
                        return CandidateAdmission(CandidateAdmissionStatus.WINDOW_FULL)

                batch = db.execute(
                    select(ImageBatch)
                    .where(
                        ImageBatch.job_id == task.job_id,
                        ~ImageBatch.status.in_(terminal_batches),
                    )
                    .order_by(ImageBatch.batch_index.asc())
                    .limit(1)
                    .with_for_update()
                ).scalar_one_or_none()

                if batch is None:
                    from app.tasks.batch_orchestration import BatchOrchestrator
                    orchestrator = BatchOrchestrator(db, task.job_id)
                    batch = orchestrator.get_or_create_active_batch()

                if batch is None or batch.status == BatchStatus.PAUSED:
                    return CandidateAdmission(CandidateAdmissionStatus.INACTIVE)

                window = db.execute(
                    select(CandidateWindow)
                    .where(CandidateWindow.batch_id == batch.id)
                    .order_by(CandidateWindow.window_index.desc())
                    .limit(1)
                    .with_for_update()
                ).scalar_one_or_none()

                if window is None:
                    ensure_candidate_window(db, batch)
                    window = db.execute(
                        select(CandidateWindow)
                        .where(CandidateWindow.batch_id == batch.id)
                        .order_by(CandidateWindow.window_index.desc())
                        .limit(1)
                        .with_for_update()
                    ).scalar_one_or_none()

                if window is None:
                    return CandidateAdmission(CandidateAdmissionStatus.INACTIVE)

                context = {
                    "window_id": window.id,
                    "window_index": int(window.window_index),
                }

                if window.status not in ("OPEN", "ACQUIRING", "FULL"):
                    return CandidateAdmission(
                        CandidateAdmissionStatus.INACTIVE, **context
                    )

                # Skip assets already ingested by the legacy or current pipeline.
                from app.db.models.asset import Asset

                existing_asset = db.execute(
                    select(Asset.id).where(
                        Asset.job_id == window.job_id,
                        Asset.source_type == source_value,
                        Asset.source_item_id == item_key,
                    ).limit(1)
                ).scalar_one_or_none()

                if existing_asset is not None:
                    return CandidateAdmission(
                        CandidateAdmissionStatus.DUPLICATE, **context
                    )

                # Also skip candidates reserved earlier in this batch.
                existing = db.execute(
                    select(CandidateReservation.id).where(
                        CandidateReservation.batch_id == window.batch_id,
                        CandidateReservation.source_type == source_value,
                        CandidateReservation.source_item_id == item_key,
                    )
                ).scalar_one_or_none()

                if existing is not None:
                    return CandidateAdmission(
                        CandidateAdmissionStatus.DUPLICATE, **context
                    )

                if window.status == "FULL":
                    return CandidateAdmission(
                        CandidateAdmissionStatus.WINDOW_FULL, **context
                    )

                used = db.execute(
                    select(func.count(CandidateReservation.id)).where(
                        CandidateReservation.window_id == window.id
                    )
                ).scalar_one()

                limit = max(
                    1, min(MAX_CANDIDATES_PER_WINDOW, int(window.candidate_limit))
                )

                if used >= limit:
                    window.status = "FULL"
                    db.commit()
                    return CandidateAdmission(
                        CandidateAdmissionStatus.WINDOW_FULL, **context
                    )

                # Multi-source candidate window fairness:
                # If multiple non-NAS sources are configured, prevent any single fast source
                # (e.g. pre-cached Roboflow datasets or COCO) from monopolizing 100% of the candidate
                # window and starving slower network sources (e.g. Web, Visual Genome, SUN RGB-D).
                job_sources = [
                    str(getattr(s, "value", s))
                    for s in (job.sources or [])
                    if str(getattr(s, "value", s)) != SourceType.NAS_SHARE.value
                ]
                if len(job_sources) > 1:
                    fair_share = math.ceil(limit / len(job_sources))
                    ratio = 0.70 if len(job_sources) == 2 else 0.25
                    source_limit = max(fair_share, math.ceil(limit * ratio))
                    source_used = db.execute(
                        select(func.count(CandidateReservation.id)).where(
                            CandidateReservation.window_id == window.id,
                            CandidateReservation.source_type == source_value,
                        )
                    ).scalar_one()
                    if source_used >= source_limit:
                        other_active = db.execute(
                            select(func.count(JobSourceTask.id)).where(
                                JobSourceTask.job_id == job.id,
                                JobSourceTask.source_type != source_value,
                                JobSourceTask.status.in_([
                                    JobSourceTaskStatus.DOWNLOADING.value,
                                    JobSourceTaskStatus.DISCOVERING.value,
                                ]),
                            )
                        ).scalar_one()
                        if other_active > 0:
                            return CandidateAdmission(
                                CandidateAdmissionStatus.WINDOW_FULL, **context
                            )

                try:
                    with db.begin_nested():
                        reservation = CandidateReservation(
                            id=uuid.uuid4(),
                            window_id=window.id,
                            batch_id=window.batch_id,
                            job_id=window.job_id,
                            job_source_task_id=task_uuid,
                            source_type=source_value,
                            source_item_id=item_key,
                            status="RESERVED",
                        )
                        db.add(reservation)

                        # The reservation that consumes the final slot closes admission.
                        window.status = "FULL" if used + 1 >= limit else "ACQUIRING"

                        db.flush()
                        reservation_id = reservation.id
                except IntegrityError:
                    # Concurrently reserved by another worker in this batch
                    return CandidateAdmission(
                        CandidateAdmissionStatus.DUPLICATE, **context
                    )

                db.commit()

                return CandidateAdmission(
                    CandidateAdmissionStatus.ADMITTED,
                    reservation_id=reservation_id,
                    **context,
                )
        except (OperationalError, IntegrityError, DBAPIError) as exc:
            is_deadlock = (
                "deadlock" in str(exc).lower()
                or "duplicate key" in str(exc).lower()
                or "unique constraint" in str(exc).lower()
                or "rolled back due to a previous exception" in str(exc).lower()
                or getattr(getattr(exc, "orig", None), "sqlstate", None) in ("40P01", "40001", "55P03", "23505")
            )
            if is_deadlock and attempt < max_retries - 1:
                sleep_s = (0.05 * (2 ** attempt)) + random.uniform(0.01, 0.05)
                logger.warning(
                    f"Concurrency collision in admit_candidate_for_task (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {sleep_s:.3f}s: {exc}"
                )
                time.sleep(sleep_s)
                continue
            raise


def reserve_candidate(
    window_id: uuid.UUID | str,
    job_source_task_id: uuid.UUID | str,
    source_type: str,
    source_item_id: str,
    db: Session | None = None,
) -> uuid.UUID | None:
    """Atomically reserve one candidate before its download begins.

    Returns a reservation UUID when admitted.
    Returns None for duplicates, inactive windows, or exhausted capacity.

    All reservations, including failed candidates, consume window capacity.
    """

    window_uuid = uuid.UUID(str(window_id))
    task_uuid = uuid.UUID(str(job_source_task_id))
    source_value = str(getattr(source_type, "value", source_type))
    item_key = bounded_source_item_id(source_item_id)

    if not source_value or len(source_value) > 32:
        raise ValueError("source_type must contain 1-32 characters")

    def _execute(session: Session) -> uuid.UUID | None:
        window = session.execute(
            select(CandidateWindow)
            .where(CandidateWindow.id == window_uuid)
            .with_for_update()
        ).scalar_one_or_none()

        if window is None or window.status not in ("OPEN", "ACQUIRING"):
            return None

        task = session.get(JobSourceTask, task_uuid)
        if task is None or task.job_id != window.job_id:
            return None

        task_source = str(getattr(task.source_type, "value", task.source_type))
        if task_source != source_value:
            return None

        # Protect against the same candidate appearing in another window.
        existing = session.execute(
            select(CandidateReservation.id).where(
                CandidateReservation.batch_id == window.batch_id,
                CandidateReservation.source_type == source_value,
                CandidateReservation.source_item_id == item_key,
            )
        ).scalar_one_or_none()

        if existing is not None:
            return None

        used = session.execute(
            select(func.count(CandidateReservation.id)).where(
                CandidateReservation.window_id == window_uuid
            )
        ).scalar_one()

        effective_limit = max(
            1, min(MAX_CANDIDATES_PER_WINDOW, int(window.candidate_limit))
        )

        if used >= effective_limit:
            window.status = "FULL"
            return None

        reservation = CandidateReservation(
            id=uuid.uuid4(),
            window_id=window_uuid,
            batch_id=window.batch_id,
            job_id=window.job_id,
            job_source_task_id=task_uuid,
            source_type=source_value,
            source_item_id=item_key,
            status="RESERVED",
        )

        session.add(reservation)

        if used + 1 >= effective_limit:
            window.status = "FULL"
        else:
            window.status = "ACQUIRING"

        session.flush()
        return reservation.id

    if db is not None:
        return _execute(db)
    with session_scope() as session:
        res_id = _execute(session)
        session.commit()
        return res_id


def mark_candidate_staged(
    reservation_id: uuid.UUID | str,
    asset_id: uuid.UUID | str,
) -> bool:
    """Link a durably staged asset to its candidate reservation."""

    reservation_uuid = uuid.UUID(str(reservation_id))
    asset_uuid = uuid.UUID(str(asset_id))

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
        reservation.asset_id = asset_uuid
        reservation.lease_expires_at = None
        reservation.updated_at = datetime.now(timezone.utc)

        db.commit()
        return True


def mark_candidate_failed(
    reservation_id: uuid.UUID | str,
    db: Session | None = None,
) -> bool:
    """Record failure without releasing the candidate's reserved capacity."""

    reservation_uuid = uuid.UUID(str(reservation_id))

    def _execute(session: Session) -> bool:
        reservation = session.execute(
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
        return True

    if db is not None:
        return _execute(db)
    with session_scope() as session:
        result = _execute(session)
        session.commit()
        return result


def set_candidate_window_status(
    window_id: uuid.UUID | str,
    status: str,
) -> bool:
    """Explicitly transition a window; does not automatically open another."""

    window_uuid = uuid.UUID(str(window_id))
    new_status = str(status).upper()

    if new_status not in ALLOWED_WINDOW_STATES:
        raise ValueError(f"Unsupported candidate window status: {new_status}")

    if new_status in TERMINAL_WINDOW_STATES:
        raise ValueError(
            "Terminal states require verified NAS or durable local-fallback "
            "completion; use an evidence-backed completion function."
        )

    with session_scope() as db:
        window = db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.id == window_uuid)
            .with_for_update()
        ).scalar_one_or_none()

        if window is None:
            return False

        if window.status in TERMINAL_WINDOW_STATES:
            return window.status == new_status

        if new_status in TERMINAL_WINDOW_STATES:
            window.completed_at = datetime.now(timezone.utc)

        window.status = new_status
        window.updated_at = datetime.now(timezone.utc)
        db.commit()
        return True


def get_candidate_window_stats(
    window_id: uuid.UUID | str,
) -> dict[str, int | str]:
    """Return reservation usage for one specific candidate window."""

    window_uuid = uuid.UUID(str(window_id))

    with session_scope() as db:
        window = db.get(CandidateWindow, window_uuid)

        if window is None:
            raise ValueError(f"CandidateWindow {window_uuid} does not exist")

        rows = db.execute(
            select(
                CandidateReservation.status,
                func.count(CandidateReservation.id),
            )
            .where(CandidateReservation.window_id == window_uuid)
            .group_by(CandidateReservation.status)
        ).all()

        counts = {str(status): int(count) for status, count in rows}
        used = sum(counts.values())
        limit = max(1, min(MAX_CANDIDATES_PER_WINDOW, int(window.candidate_limit)))

        return {
            "window_id": str(window_uuid),
            "window_index": int(window.window_index),
            "status": str(window.status),
            "reserved": used,
            "limit": limit,
            "remaining": max(0, limit - used),
            "staged": counts.get("STAGED", 0),
            "failed": counts.get("FAILED", 0),
        }


def cleanup_stale_reservations(db, window_id: uuid.UUID, stale_seconds: int = 60) -> int:
    """Marks abandoned reservations as FAILED so they do not block batch progression."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_seconds)
    stmt = (
        update(CandidateReservation)
        .where(
            CandidateReservation.window_id == window_id,
            CandidateReservation.status == "RESERVED",
            CandidateReservation.updated_at < cutoff,
        )
        .values(status="FAILED", lease_expires_at=None, updated_at=datetime.now(timezone.utc))
    )
    result = db.execute(stmt)
    return result.rowcount

