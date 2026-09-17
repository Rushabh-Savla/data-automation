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
    reached its target_count (see build_on_progress's fast-path check), or —
    when job_source_task_id is given — this one source has hit its own
    fair-share cap while other selected sources are still working."""
    cancellation = CancellationService(get_redis())
    source_task_str = str(job_source_task_id) if job_source_task_id is not None else None

    def should_stop() -> bool:
        return cancellation.should_stop(str(job_id), source_task_str)

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
                # On a top-up round the planner has already measured this
                # query's real post-filter yield and sized the round from it.
                # Use that number rather than the static buffer, which is only
                # a first-round prior -- a query whose survivors are 14% of
                # what it downloads needs roughly 7x, and no fixed multiplier
                # expresses that.
                topup_state = (job.config or {}).get("_topup") or {}
                topup_quota = topup_state.get("quota") or {}
                if topup_quota:
                    # Anchored to a fixed baseline captured once when the
                    # round was dispatched (plan_topup_round), NOT the live
                    # job.accepted_count read here — that field grows with
                    # every accepted image in this same round, so comparing
                    # against "current + quota" recomputed on every call is a
                    # moving target that can never be reached (quota is
                    # always positive), and the topup round never stops
                    # itself. Falling back to the live count only covers a
                    # job.config predating this baseline field.
                    baseline = topup_state.get("raw_accepted_at_round_start", job.accepted_count)
                    target = baseline + sum(topup_quota.values())
                else:
                    target = buffered_target(job.target_count, get_settings().target_overfetch_buffer)
                if job.accepted_count >= target:
                    cancellation.mark_target_reached(str(job_id))
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
