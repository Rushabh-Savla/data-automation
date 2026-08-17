"""Shared helpers used by every task module: building the should_stop/on_progress
callables injected into connectors, and error classification for retry policy.
"""

import logging
import uuid
from collections.abc import Callable

from sqlalchemy.orm import Session

from app.core.logging import bind_job_context, clear_job_context
from app.core.redis_client import get_redis
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.services.cancellation_service import CancellationService
from app.services.progress_service import ProgressPublisher

logger = logging.getLogger(__name__)


def build_should_stop(job_id: uuid.UUID) -> Callable[[], bool]:
    """Returns True if the job has been paused, cancelled, or has already
    reached its target_count (see build_on_progress's fast-path check)."""
    cancellation = CancellationService(get_redis())

    def should_stop() -> bool:
        return cancellation.should_stop(str(job_id))

    return should_stop


def build_on_progress(
    db: Session, job_id: uuid.UUID, job_source_task_id: uuid.UUID
) -> Callable[[dict], None]:
    task_repo = JobSourceTaskRepository(db)
    job_repo = JobRepository(db)
    publisher = ProgressPublisher(get_redis())
    cancellation = CancellationService(get_redis())

    def on_progress(delta: dict) -> None:
        checkpoint_delta = delta.get("checkpoint")
        if checkpoint_delta is not None:
            # Shallow-merge into the existing checkpoint dict rather than
            # overwrite it wholesale — a JobSourceTask can have several items
            # (e.g. Kaggle datasets) in flight concurrently within the same
            # chord, each namespaced under its own top-level key, and a plain
            # overwrite would let them clobber each other's progress.
            task = task_repo.get(job_source_task_id)
            merged = dict(task.checkpoint or {}) if task else {}
            merged.update(checkpoint_delta)
            task_repo.heartbeat(job_source_task_id, checkpoint=merged)
        else:
            task_repo.heartbeat(job_source_task_id)
        task_repo.increment_counts(
            job_source_task_id,
            discovered=delta.get("discovered", 0),
            downloaded=delta.get("downloaded", 0),
            accepted=delta.get("accepted", 0),
            rejected=delta.get("rejected", 0),
            errors=delta.get("errors", 0),
        )
        job_repo.increment_counts(
            job_id,
            discovered=delta.get("discovered", 0),
            downloaded=delta.get("downloaded", 0),
            accepted=delta.get("accepted", 0),
            rejected=delta.get("rejected", 0),
            duplicate=delta.get("duplicate", 0),
        )
        # Commit (not just flush) on every progress event, not only when the
        # enclosing task returns. A single ingest task's session_scope() can
        # stay open for a long, uninterrupted run (thousands of images); if
        # counters/checkpoint/heartbeat only became durable at task-return
        # time, (a) REST poll / DB-reading consumers would see stale counts
        # for the whole run instead of live progress, and (b) a mid-task
        # crash would roll back *all* of that run's progress instead of just
        # the item in flight when it died. Each Asset row is already
        # committed per-item in its own session (see
        # standardize_and_promote_sync) — this brings the JobSourceTask/Job
        # rollups and checkpoint to the same durability granularity.
        db.commit()

        # Fast-path completion check: as soon as a count-based job's target is
        # hit, flag it so should_stop() (checked by every connector loop)
        # stops issuing new work immediately, without waiting for every
        # source's discovery/download to fully exhaust itself.
        if delta.get("accepted"):
            job = job_repo.get(job_id)
            if job is not None and job.target_count is not None and job.accepted_count >= job.target_count:
                cancellation.mark_target_reached(str(job_id))

        publisher.publish(
            str(job_id),
            {
                "type": "progress_delta",
                "job_source_task_id": str(job_source_task_id),
                **{k: v for k, v in delta.items() if k != "checkpoint"},
            },
        )

    return on_progress


def bind_context(job_id: uuid.UUID, job_source_task_id: uuid.UUID | None = None) -> None:
    bind_job_context(str(job_id), str(job_source_task_id) if job_source_task_id else None)


def unbind_context() -> None:
    clear_job_context()
