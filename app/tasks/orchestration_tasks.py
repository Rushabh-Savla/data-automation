"""Orchestration: the chord callback that marks a source complete and, once
every source for a job is terminal, evaluates whether the whole job is done
(directly for now; Phase 4 inserts the relevance-filter/dedup chain here
before completion evaluation). Also the Beat-scheduled watchdog that resumes
stalled tasks and kicks off scheduled/deadline-based jobs.
"""

import logging
import uuid
from datetime import datetime, timezone

from app.core.config import get_settings
from app.core.logging import bind_job_context, clear_job_context
from app.core.redis_client import get_redis
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.completion import JobCompletionEvaluator, JobSnapshot
from app.domain.enums import JobSourceTaskStatus, JobStatus
from app.domain.state_machine import JobStateMachine
from app.services.cancellation_service import CancellationService
from app.services.progress_service import ProgressPublisher
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.orchestration_tasks.on_source_finished", bind=True)
def on_source_finished(self, results, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_job_context(job_id, job_source_task_id)

    cancellation = CancellationService(get_redis())

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            clear_job_context()
            return

        had_errors = any(isinstance(r, dict) and r.get("error") for r in (results or []))
        had_stopped = any(isinstance(r, dict) and r.get("stopped") for r in (results or []))

        if had_stopped and cancellation.is_paused(job_id) and not cancellation.is_cancelled(job_id):
            # Paused mid-flight, not actually done: leave the task in whatever
            # non-terminal status the download task left it in (DOWNLOADING)
            # so JobService.resume() sees it as eligible to re-dispatch, and
            # skip the completion check below entirely — nothing about the
            # job's overall completion changed, it's still just paused.
            db.flush()
            clear_job_context()
            return

        if had_stopped and cancellation.is_cancelled(job_id):
            final_status = JobSourceTaskStatus.CANCELLED
        elif had_errors:
            final_status = JobSourceTaskStatus.COMPLETED_PARTIAL
        else:
            final_status = JobSourceTaskStatus.COMPLETED

        if task.status != JobSourceTaskStatus.CANCELLED:
            task_repo.set_status(task_uuid, final_status)
        db.flush()

    check_and_advance_job(job_id)
    clear_job_context()


def check_and_advance_job(job_id: str) -> None:
    """Checks whether every JobSourceTask for the job is now terminal, and if
    so, kicks off the downstream filtering/completion pipeline.

    Called from two places: on_source_finished (the chord callback, for
    sources whose discovery found at least one item) AND directly from each
    discover_* task's zero-items early return. That second call site matters
    a lot: a source that legitimately finds nothing — no credentials
    configured, a query with no matches, a blocked/rate-limited search — never
    creates a chord, so on_source_finished would never fire for it. If that
    source happens to be the last one to go terminal, nothing would ever
    re-check "is the whole job done" again and the job would sit in `running`
    forever despite every source having already finished. This function is
    the single place that check happens, reachable from both paths.
    """
    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        all_terminal = task_repo.all_terminal_for_job(uuid.UUID(job_id))

    if not all_terminal:
        return

    ProgressPublisher(get_redis()).publish(job_id, {"type": "connector_finished"})
    # Downstream pipeline: relevance filter -> dedup -> completion evaluation.
    # run_deduplication chains to evaluate_job_completion itself once it
    # finishes (see app/tasks/filtering_tasks.py).
    from app.tasks.filtering_tasks import run_relevance_filter

    run_relevance_filter.delay(job_id)


@celery_app.task(name="app.tasks.orchestration_tasks.evaluate_job_completion", bind=True)
def evaluate_job_completion(self, job_id: str):
    job_uuid = uuid.UUID(job_id)
    bind_job_context(job_id)

    with session_scope() as db:
        job_repo = JobRepository(db)
        task_repo = JobSourceTaskRepository(db)
        job = job_repo.get(job_uuid)
        if job is None or job.status not in (JobStatus.DISCOVERING, JobStatus.RUNNING, JobStatus.CANCELLING):
            clear_job_context()
            return

        cancellation = CancellationService(get_redis())
        if cancellation.is_cancelled(job_id):
            job_repo.set_status(
                job_uuid,
                JobStateMachine.transition(job.status, "finish_cancel"),
                completion_reason=None,
            )
            from app.domain.enums import JobCompletionReason

            job_repo.set_status(job_uuid, JobStatus.CANCELLED, completion_reason=JobCompletionReason.CANCELLED)
            cancellation.clear_active_tasks(job_id)
            ProgressPublisher(get_redis()).publish(job_id, {"type": "job_status", "status": "cancelled"})
            clear_job_context()
            return

        snapshot = JobSnapshot(
            target_count=job.target_count,
            deadline_at=job.deadline_at,
            accepted_count=job.accepted_count,
            all_sources_terminal=task_repo.all_terminal_for_job(job_uuid),
        )
        reason = JobCompletionEvaluator.should_stop(snapshot)
        if reason is None:
            clear_job_context()
            return

        success = JobCompletionEvaluator.is_success(reason, job.accepted_count)
        event = "complete" if success else ("fail" if job.accepted_count == 0 else "complete_partial")
        new_status = JobStateMachine.transition(job.status, event)
        job_repo.set_status(job_uuid, new_status, completion_reason=reason)
        cancellation.clear_target_reached(job_id)
        cancellation.clear_active_tasks(job_id)
        ProgressPublisher(get_redis()).publish(
            job_id, {"type": "job_status", "status": new_status.value, "completion_reason": reason.value}
        )

    clear_job_context()


@celery_app.task(name="app.tasks.orchestration_tasks.watchdog_scan")
def watchdog_scan():
    """Beat-scheduled (every 60s) safety net: resumes stalled JobSourceTasks,
    kicks off scheduled jobs whose start time has passed, and sweeps for
    deadline-based jobs that should have completed but didn't get an event."""
    settings = get_settings()
    now = datetime.now(timezone.utc)

    with session_scope() as db:
        job_repo = JobRepository(db)
        task_repo = JobSourceTaskRepository(db)

        # -- Resume stalled tasks -----------------------------------------------
        for task in task_repo.list_stalled(settings.watchdog_stall_threshold_s, now):
            logger.warning(f"Watchdog: task {task.id} stalled (source={task.source_type}), resuming")
            task_repo.set_status(task.id, JobSourceTaskStatus.STALLED)
            task_repo.increment_counts(task.id, errors=0)  # no-op, keeps intent explicit
            _resume_task(str(task.job_id), str(task.id), task.source_type, task.checkpoint or {})

        # -- Kick off scheduled jobs whose start time has passed -------------------
        for job in job_repo.list_due_scheduled(now):
            logger.info(f"Watchdog: starting scheduled job {job.id}")
            from app.services.job_service import JobService

            JobService(db).start(job.id)

        # -- Deadline sweep for running/discovering jobs -----------------------------
        for job in job_repo.list_active():
            if job.deadline_at is not None and now >= job.deadline_at:
                evaluate_job_completion.delay(str(job.id))


def _resume_task(job_id: str, job_source_task_id: str, source_type, checkpoint: dict) -> None:
    """Re-invokes the source's discovery task, which will redispatch per-item
    download tasks that each read their own namespaced checkpoint back out of
    JobSourceTask.checkpoint and continue from there. Idempotency for the
    already-processed items is enforced by the Asset unique constraint, so a
    from-scratch discovery re-run cannot duplicate data — the only cost is a
    cheap re-run of the (already-cached-by-most-SDKs) discovery/search step
    and, as a known minor limitation, a re-increment of the task's
    discovered_count rollup (a display counter, not a correctness-critical one)."""
    from app.domain.enums import SourceType

    if source_type == SourceType.KAGGLE:
        from app.tasks.discovery_tasks import discover_kaggle

        discover_kaggle.delay(job_id, job_source_task_id)
    else:
        logger.warning(f"Watchdog: no resume handler yet for source_type={source_type}")
