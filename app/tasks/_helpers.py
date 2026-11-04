"""Shared helpers used by every task module: building the should_stop/on_progress
callables injected into connectors, and error classification for retry policy.
"""

import contextlib
import logging
import math
import threading
import uuid
from collections.abc import Callable

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import bind_job_context, clear_job_context
from app.core.redis_client import get_redis
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.services.cancellation_service import CancellationService
from app.services.progress_service import ProgressPublisher

logger = logging.getLogger(__name__)

_HEARTBEAT_PING_INTERVAL_S = 60

# Fair-share cap on how many images a single source can contribute before
# the job's target_count is otherwise reached (see build_on_progress's
# per-source check below). MIN keeps small-target jobs from starving a
# source down to near-zero; BUFFER gives headroom so sources that legitimately
# find fewer matches don't stop others from covering the shortfall.
_MIN_PER_SOURCE_CAP = 10
_PER_SOURCE_FAIRNESS_BUFFER = 2.0


def per_source_cap(target_count: int, num_sources: int) -> int:
    """How many accepted images a single source may contribute before it
    stops itself in favor of the other selected sources. Pulled out as a
    pure function so the fairness math is unit-testable without a DB/Redis
    fixture — see build_on_progress for where it's actually used."""
    num_sources = max(1, num_sources)
    return max(_MIN_PER_SOURCE_CAP, math.ceil(target_count / num_sources * _PER_SOURCE_FAIRNESS_BUFFER))


def buffered_target(target_count: int, buffer: float) -> int:
    """How many PROMOTED assets to fetch before stopping, given the job's
    raw target_count — see build_on_progress for why this is bigger than
    target_count itself (relevance filtering/dedup run afterward and can
    only reduce the count). Pulled out as a pure function for the same
    testability reason as per_source_cap above."""
    return math.ceil(target_count * buffer)


@contextlib.contextmanager
def heartbeat_during(task_uuid: uuid.UUID):
    """Pings JobSourceTask.last_heartbeat_at every _HEARTBEAT_PING_INTERVAL_S
    seconds for the duration of the wrapped block, on a background thread
    with its own DB session (the caller's session may be busy inside a
    single long blocking call — e.g. Kaggle's kagglehub.dataset_download(),
    or the one-time shared-cache archive downloads several connectors
    (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do inside discover() —
    for far longer than one interval). Without this, the watchdog
    (app/core/config.py::watchdog_stall_threshold_s) marks a genuinely-
    progressing discovery as stalled and re-dispatches it from scratch,
    which on a slow connection with a large archive means it never
    finishes — every restart re-downloads from the beginning."""
    from app.db.session import session_scope

    stop = threading.Event()

    def _run() -> None:
        while not stop.wait(_HEARTBEAT_PING_INTERVAL_S):
            try:
                with session_scope() as hb_db:
                    JobSourceTaskRepository(hb_db).heartbeat(task_uuid)
                    hb_db.commit()
            except Exception:
                logger.exception("heartbeat_during: ping failed for task %s", task_uuid)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=5)


def build_should_stop(job_id: uuid.UUID, job_source_task_id: uuid.UUID | None = None) -> Callable[[], bool]:
    """Returns True if the job has been paused, cancelled, has already
    reached its target_count (see build_on_progress's fast-path check),
    this one source has hit its fair-share cap, or local staging disk usage
    exceeds the high watermark (80%) backpressure threshold."""
    from app.storage.ram_buffer import LocalStagingManager

    cancellation = CancellationService(get_redis())
    source_task_str = str(job_source_task_id) if job_source_task_id is not None else None
    staging_manager = LocalStagingManager()

    def should_stop() -> bool:
        if cancellation.should_stop(str(job_id), source_task_str):
            return True
        if staging_manager.is_staging_backpressure_active():
            logger.warning(
                f"[STAGING BACKPRESSURE] Halting download for job {job_id} — local staging disk is above high watermark (80%)"
            )
            return True
        return False

    return should_stop


def build_on_progress(
    db: Session, job_id: uuid.UUID, job_source_task_id: uuid.UUID
) -> Callable[[dict], None]:
    import time
    from sqlalchemy.exc import OperationalError

    task_repo = JobSourceTaskRepository(db)
    job_repo = JobRepository(db)
    publisher = ProgressPublisher(get_redis())
    cancellation = CancellationService(get_redis())

    update_count = 0
    last_commit_time = time.monotonic()

    def on_progress(delta: dict) -> None:
        nonlocal update_count, last_commit_time

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

        # STRICT LOCK ORDERING: ALWAYS update Job first, then JobSourceTask.
        # Consistent lock acquisition order eliminates deadlock (Postgres 40P01)
        # under concurrent worker execution.
        job_repo.increment_counts(
            job_id,
            discovered=delta.get("discovered", 0),
            downloaded=delta.get("downloaded", 0),
            accepted=delta.get("accepted", 0),
            rejected=delta.get("rejected", 0),
            duplicate=delta.get("duplicate", 0),
        )
        task_repo.increment_counts(
            job_source_task_id,
            discovered=delta.get("discovered", 0),
            downloaded=delta.get("downloaded", 0),
            accepted=delta.get("accepted", 0),
            rejected=delta.get("rejected", 0),
            errors=delta.get("errors", 0),
        )

        update_count += 1
        now = time.monotonic()
        should_commit = (
            checkpoint_delta is not None
            or delta.get("errors", 0) > 0
            or update_count >= 10
            or (now - last_commit_time) >= 2.0
        )

        if should_commit:
            update_count = 0
            last_commit_time = now
            for attempt in range(3):
                try:
                    db.commit()
                    break
                except OperationalError as exc:
                    db.rollback()
                    if attempt == 2:
                        logger.warning(f"Failed to commit progress for task {job_source_task_id}: {exc}")
                        raise
                    time.sleep(0.05 * (2 ** attempt))

        # Fast-path completion check: as soon as a count-based job's
        # (buffered) target is hit, flag it so should_stop() (checked by
        # every connector loop) stops issuing new work immediately, without
        # waiting for every source's discovery/download to fully exhaust
        # itself. The raw target_count is buffered up (not checked as-is)
        # because relevance filtering/dedup run afterward and can only
        # reduce accepted_count — fetching only exactly target_count up
        # front routinely delivers fewer once irrelevant/duplicate assets
        # are removed. Fetching target_overfetch_buffer x target_count
        # instead gives filtering enough raw material that the survivors
        # still land on target_count in the common case; trim_excess_assets
        # (app/tasks/filtering_tasks.py) trims any overshoot back down
        # afterward.
        if delta.get("accepted"):
            job = job_repo.get(job_id)
            if job is not None and job.target_count is not None:
                # If batch processing is enabled for this job (default), the candidate window
                # bounds each round to download_candidate_window (e.g. 600) rather than downloading
                # the entire job target (e.g. 5000) upfront.
                batch_mode = job.config.get("batch_mode", True)
                topup_state = (job.config or {}).get("_topup") or {}
                topup_quota = topup_state.get("quota") or {}
                if topup_quota:
                    baseline = topup_state.get("raw_accepted_at_round_start", job.accepted_count)
                    target = baseline + sum(topup_quota.values())
                elif batch_mode:
                    # Bounded candidate window: stop this round once the current batch window is reached
                    candidate_window = job.config.get("candidate_window", get_settings().download_candidate_window)
                    # Current round target is capped to candidate window or remaining job target
                    active_batch_target = min(candidate_window, job.target_count)
                    target = buffered_target(active_batch_target, get_settings().target_overfetch_buffer)
                else:
                    target = buffered_target(job.target_count, get_settings().target_overfetch_buffer)
                if job.accepted_count >= target:
                    cancellation.mark_target_reached(str(job_id))
                    # Trigger processing of the acquired candidate window immediately without waiting for slow sources
                    from app.tasks.orchestration_tasks import check_and_advance_job
                    check_and_advance_job(str(job_id))
                else:
                    # Per-source fair-share cap: without this, whichever
                    # source happens to be fastest (e.g. its data was
                    # already cached) can single-handedly reach the job's
                    # whole (buffered) target before the other selected
                    # sources get a single item accepted — should_stop() is
                    # job-wide, so that one source's speed silently starves
                    # every other source of any contribution at all. Capping
                    # each source's own accepted_count keeps room for the
                    # rest; sized off the same buffered target as the
                    # job-wide check above, not the raw target_count, so
                    # per-source ceilings scale with it consistently.
                    cap = per_source_cap(target, len(job.sources or []))
                    task = task_repo.get(job_source_task_id)
                    if task is not None and task.accepted_count >= cap:
                        cancellation.mark_source_cap_reached(str(job_id), str(job_source_task_id))

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
