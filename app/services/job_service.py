"""JobService — the API-facing orchestration surface: create/start/pause/
resume/cancel. Every status change goes through JobStateMachine.transition()
so illegal transitions are impossible regardless of which endpoint or task
calls in.
"""

import logging
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.redis_client import get_redis
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.domain.enums import JOB_SOURCE_TASK_TERMINAL_STATUSES, JobSourceTaskStatus, JobStatus, SourceSelectionMode, SourceType
from app.domain.state_machine import IllegalTransitionError, JobStateMachine
from app.services.cancellation_service import CancellationService
from app.services.progress_service import ProgressPublisher

logger = logging.getLogger(__name__)

# Discovery task registry — only sources with an implemented connector appear
# here. Phase 3/4 register roboflow/youtube/web as their connectors land;
# orchestration code never needs to change, only this mapping (Open/Closed).
_SOURCE_DISCOVERY_TASKS: dict[SourceType, str] = {}

# Per-item download task registry — a source only needs to register here if
# it wants pause/resume to redispatch exactly the items still outstanding
# instead of re-running discovery from scratch (see _resume_pending_items
# below). Sources that don't register keep today's discovery-redispatch
# behavior unchanged.
_SOURCE_DOWNLOAD_TASKS: dict[SourceType, str] = {}


def register_source_discovery_task(source_type: SourceType, task_dotted_path: str) -> None:
    _SOURCE_DISCOVERY_TASKS[source_type] = task_dotted_path


def register_source_download_task(source_type: SourceType, task_dotted_path: str) -> None:
    _SOURCE_DOWNLOAD_TASKS[source_type] = task_dotted_path


def get_implemented_sources() -> set[SourceType]:
    return set(_SOURCE_DISCOVERY_TASKS.keys())


# Sources that only run when a job names them. The NAS share is the user's own
# library: pulling it into every auto-sourced job (which also fans out to every
# public dataset) would mix private images into results nobody asked to include.
_EXPLICIT_ONLY_SOURCES: frozenset[SourceType] = frozenset({SourceType.NAS_SHARE})


def _resume_pending_items(
    source_type: SourceType, job_id: str, job_source_task_id: str, checkpoint: dict | None,
    max_batch: int = 25,
) -> bool:
    """If the task's checkpoint records exactly which items a prior
    discovery round dispatched ("_pending_items", written by e.g.
    discover_kaggle) and which of those already reached a terminal outcome
    ("_completed_items", written by on_source_finished), redispatch just the
    outstanding items' download tasks directly instead of re-running
    discovery and rebuilding the whole chord.

    This is what makes resuming a paused Kaggle job cheap: a dataset that
    already finished downloading (and had its local cache cleaned up) is
    never redispatched just to re-download the whole thing and skip through
    it via its checkpoint.

    Returns True if it dispatched a redispatch chord this way (caller
    should not also redispatch discovery), False if there was nothing
    usable to resume from (caller should fall back to redispatching
    discovery — this is also what correctly triggers a genuinely new
    discovery round, e.g. topup, once every previously-dispatched item is
    already in `_completed_items`).
    """
    pending_items: dict = (checkpoint or {}).get("_pending_items") or {}
    if not pending_items:
        return False
    completed = set((checkpoint or {}).get("_completed_items", []))
    remaining = [v for k, v in pending_items.items() if k not in completed]
    if not remaining:
        return False

    if not _SOURCE_DOWNLOAD_TASKS:
        import app.tasks.ingest_tasks  # defensive: populate _SOURCE_DOWNLOAD_TASKS

    task_path = _SOURCE_DOWNLOAD_TASKS.get(source_type)
    if task_path is None:
        return False

    from celery import chord

    from app.tasks.celery_app import celery_app
    from app.tasks.orchestration_tasks import on_source_finished

    # Dispatch in paced batches to prevent Celery queue saturation and watchdog timeouts
    batch_to_dispatch = remaining[:max_batch]

    group = [celery_app.signature(task_path, args=[job_id, job_source_task_id, item]) for item in batch_to_dispatch]
    chord(group)(on_source_finished.s(job_id, job_source_task_id))
    return True


class JobService:
    def __init__(self, db: Session):
        self.db = db
        self.job_repo = JobRepository(db)
        self.task_repo = JobSourceTaskRepository(db)
        self.cancellation = CancellationService(get_redis())
        self.publisher = ProgressPublisher(get_redis())

    def create(
        self,
        query: str,
        sources: list[SourceType] | None = None,
        source_selection_mode: SourceSelectionMode | None = None,
        name: str | None = None,
        target_count: int | None = None,
        deadline_at: datetime | None = None,
        scheduled_start_at: datetime | None = None,
        schedule_cron: str | None = None,
        schedule_timezone: str = "UTC",
        config: dict | None = None,
    ) -> Job:
        from app.services.source_dispatch_service import SourceDispatchService

        resolved_sources, effective_mode = SourceDispatchService.resolve_sources(
            requested_sources=sources,
            selection_mode=source_selection_mode,
            db=self.db,
        )
        was_auto_sourced = (effective_mode != SourceSelectionMode.MANUAL)
        sources = resolved_sources

        job_config = dict(config or {})
        job_config["source_selection_mode"] = effective_mode.value

        # Intelligent Resource Orchestration:
        # - Automatic Mode: Web Images (Tier 1) -> Videos (Tier 2) -> Dataset Resources (Tier 3)
        # - Manual Mode: Concurrently execute every user-selected source
        from app.domain.resource_orchestration import build_orchestration_tiers

        if was_auto_sourced:
            tiers = build_orchestration_tiers(sources)
            if tiers:
                dispatch_sources = tiers[0]
                pending_tiers = [[s.value for s in t] for t in tiers[1:]]
            else:
                dispatch_sources = sources
                pending_tiers = []

            job_config["_orchestration_mode"] = "auto"
            job_config["_orchestration_tier"] = 1
            job_config["_orchestration_pending_tiers"] = pending_tiers
            job_config["_fast_path_remaining_sources"] = [s.value for t in tiers[1:] for s in t] if tiers else []
        else:
            # Manual Mode: all sources selected by the user run concurrently
            dispatch_sources = sources
            job_config["_orchestration_mode"] = "manual"
            job_config["_orchestration_tier"] = 1
            job_config["_orchestration_pending_tiers"] = []

        # A recurring template also sits in SCHEDULED, and stays there: it is
        # never started itself, only swept by scan_schedules, which creates a
        # child run per firing.
        status = (
            JobStatus.SCHEDULED if (scheduled_start_at or schedule_cron) else JobStatus.DRAFT
        )
        next_run_at = None
        if schedule_cron:
            from app.services.schedule_service import next_fire_time

            next_run_at = next_fire_time(schedule_cron, schedule_timezone)

        job = Job(
            id=uuid.uuid4(),
            name=name,
            query=query,
            sources=[s.value for s in sources],
            target_count=target_count,
            deadline_at=deadline_at,
            scheduled_start_at=scheduled_start_at,
            schedule_cron=schedule_cron,
            schedule_timezone=schedule_timezone,
            next_run_at=next_run_at,
            status=status,
            config=job_config,
        )
        self.job_repo.create(job)
        for source in dispatch_sources:
            self.task_repo.create(
                JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type=source.value)
            )
        self.db.flush()
        return job

    def start(self, job_id: uuid.UUID) -> Job:
        job = self.job_repo.get_or_404(job_id)
        job.status = JobStateMachine.transition(job.status, "start")
        JobRepository.apply_status_timestamps(job, job.status)
        self.db.flush()

        # Ensure active batch and initial candidate window are ready before workers start
        try:
            from app.tasks.batch_orchestration import BatchOrchestrator
            from app.services.candidate_reservation_service import ensure_candidate_window
            orchestrator = BatchOrchestrator(self.db, job.id)
            batch = orchestrator.get_or_create_active_batch()
            if batch is not None:
                ensure_candidate_window(self.db, batch)
        except Exception as exc:
            logger.warning(f"Initial batch/window creation warning: {exc}")

        # Strict Sequential Source Processing:
        # Only the first active source in canonical priority order receives initial discovery!
        from app.domain.source_priority import evaluate_sequential_execution_plan
        plan = evaluate_sequential_execution_plan(job, self.task_repo.list_for_job(job_id))
        dispatched_any = False

        if plan.active_source is not None:
            active_src = plan.active_source
            task = self.task_repo.get_for_job_and_source(job_id, active_src)
            task_path = _SOURCE_DISCOVERY_TASKS.get(active_src)
            if task_path is None and task is not None:
                self.task_repo.set_status(
                    task.id,
                    JobSourceTaskStatus.FAILED,
                    error_message=f"No connector implemented yet for source '{active_src.value}'",
                )
            elif task_path is not None and task is not None:
                self._dispatch(task_path, str(job_id), str(task.id))
                dispatched_any = True

        if not dispatched_any:
            job.status = JobStateMachine.transition(job.status, "fail")
            JobRepository.apply_status_timestamps(job, job.status)
            job.error_message = "No source connectors available for the selected sources"
        else:
            job.status = JobStateMachine.transition(job.status, "begin_running")
            JobRepository.apply_status_timestamps(job, job.status)
        self.db.flush()
        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})
        return job

    def pause(self, job_id: uuid.UUID) -> Job:
        job = self.job_repo.get_or_404(job_id)
        job.status = JobStateMachine.transition(job.status, "pause")
        JobRepository.apply_status_timestamps(job, job.status)
        self.cancellation.mark_paused(str(job_id))
        self.cancellation.revoke_active_tasks(str(job_id))
        self.db.flush()
        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})
        return job

    def resume(self, job_id: uuid.UUID) -> Job:
        job = self.job_repo.get_or_404(job_id)
        job.status = JobStateMachine.transition(job.status, "resume")
        JobRepository.apply_status_timestamps(job, job.status)
        self.cancellation.clear_paused(str(job_id))
        self.db.flush()

        from app.domain.source_priority import evaluate_sequential_execution_plan
        plan = evaluate_sequential_execution_plan(job, self.task_repo.list_for_job(job_id))
        if plan.active_source is not None:
            active_src = plan.active_source
            task = self.task_repo.get_for_job_and_source(job_id, active_src)
            if task is not None and task.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                if not _resume_pending_items(active_src, str(job_id), str(task.id), task.checkpoint):
                    task_path = _SOURCE_DISCOVERY_TASKS.get(active_src)
                    if task_path is not None:
                        self._dispatch(task_path, str(job_id), str(task.id))

        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})
        return job

    def reopen(self, job_id: uuid.UUID) -> Job:
        """Sends a `completed_partial` job back for another discovery pass,
        so results append to this same job_id/job_source_task_id instead of
        living under a disconnected new job.

        Unlike resume() (which only redrives non-terminal tasks left over
        from a pause), reopen() targets tasks that are *terminal* — that is
        exactly the state a source is in after topup gave up on it. discover_
        youtube et al overwrite the task's status themselves on entry
        regardless of what it was, but it's set to PENDING here first for UI/
        heartbeat consistency with how plan_topup_round redrives a source.

        job.config["_topup"] is cleared entirely, not just its round number:
        TopupPlanner.plan() stops immediately once round_number >= max_rounds
        (domain/topup.py), and discovery_tasks.py's scale_for_topup/
        topup_limit and _helpers.py's mark_target_reached all read the same
        dict — leaving a stale round/quota from the finished run would make
        the very next evaluation stop the reopened pass immediately, skew its
        discovery limit, or fire mark_target_reached prematurely off a
        now-meaningless baseline.
        """
        job = self.job_repo.get_or_404(job_id)
        job.status = JobStateMachine.transition(job.status, "reopen")
        job.completion_reason = None
        job.completed_at = None
        JobRepository.apply_status_timestamps(job, job.status)
        job.config = {k: v for k, v in (job.config or {}).items() if k != "_topup"}
        self.db.flush()

        allowed_sources = set(job.sources or [])
        for task in self.task_repo.list_for_job(job_id):
            if task.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                continue  # still actively working (e.g. a slower sibling source) -- leave it alone
            if task.source_type not in allowed_sources:
                continue
            source_type = SourceType(task.source_type)
            task_path = _SOURCE_DISCOVERY_TASKS.get(source_type)
            if task_path is None:
                continue
            self.task_repo.set_status(task.id, JobSourceTaskStatus.PENDING)
            self.task_repo.heartbeat(task.id)
            self._dispatch(task_path, str(job_id), str(task.id))

        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})
        return job

    def cancel(self, job_id: uuid.UUID) -> Job:
        job = self.job_repo.get_or_404(job_id)
        job.status = JobStateMachine.transition(job.status, "cancel")
        JobRepository.apply_status_timestamps(job, job.status)
        self.cancellation.mark_cancelled(str(job_id))
        self.db.flush()

        self._revoke_active_tasks(str(job_id))

        from app.tasks.cleanup_tasks import cleanup_local

        for task in self.task_repo.list_for_job(job_id):
            if task.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                self.task_repo.set_status(task.id, JobSourceTaskStatus.CANCELLED)
            cleanup_local.delay(str(task.id))

        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})

        # evaluate_job_completion is deliberately NOT enqueued here: this
        # transaction has only been flush()ed, not committed, and a Celery
        # worker picking the task up in its own session could read the
        # pre-cancel status before this commits (the API layer commits
        # after action() returns — see _apply_control in app/api/v1/jobs.py,
        # which enqueues it once CANCELLING is durably persisted).
        return job

    def _dispatch(self_or_task: Any, task_dotted_path: str = None, job_id: str = None, job_source_task_id: str = None) -> None:
        from app.tasks.celery_app import celery_app
        from sqlalchemy import event

        if isinstance(self_or_task, JobService):
            self = self_or_task
            path = task_dotted_path
            jid = job_id
            tid = job_source_task_id
        else:
            self = None
            path = self_or_task
            jid = task_dotted_path
            tid = job_id

        def send():
            celery_app.send_task(path, args=[jid, tid])

        session = getattr(self, "db", None) if self else None
        in_tx = getattr(session, "in_transaction", lambda: False)() if (session and hasattr(session, "in_transaction")) else False
        if session and in_tx and hasattr(session, "info"):
            if "pending_dispatches" not in session.info:
                session.info["pending_dispatches"] = []

                @event.listens_for(session, "after_commit", once=True)
                def _fire(s):
                    tasks = s.info.pop("pending_dispatches", [])
                    for t in tasks:
                        try:
                            t()
                        except Exception as e:
                            logger.error(f"Error dispatching task after commit: {e}")

                @event.listens_for(session, "after_rollback", once=True)
                def _clear(s):
                    s.info.pop("pending_dispatches", None)

            session.info["pending_dispatches"].append(send)
        else:
            send()

    def _revoke_active_tasks(self, job_id: str) -> None:
        self.cancellation.revoke_active_tasks(job_id)
