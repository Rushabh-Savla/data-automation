"""JobService — the API-facing orchestration surface: create/start/pause/
resume/cancel. Every status change goes through JobStateMachine.transition()
so illegal transitions are impossible regardless of which endpoint or task
calls in.
"""

import logging
import uuid
from datetime import datetime

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.redis_client import get_redis
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.domain.enums import JOB_SOURCE_TASK_TERMINAL_STATUSES, JobSourceTaskStatus, JobStatus, SourceType
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


def _resume_pending_items(
    source_type: SourceType, job_id: str, job_source_task_id: str, checkpoint: dict | None
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
    remaining = {k: v for k, v in pending_items.items() if k not in completed}
    if not remaining:
        return False

    task_path = _SOURCE_DOWNLOAD_TASKS.get(source_type)
    if task_path is None:
        return False

    from celery import chord

    from app.tasks.celery_app import celery_app
    from app.tasks.orchestration_tasks import on_source_finished

    group = [celery_app.signature(task_path, args=[job_id, job_source_task_id, item]) for item in remaining.values()]
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
        name: str | None = None,
        target_count: int | None = None,
        deadline_at: datetime | None = None,
        scheduled_start_at: datetime | None = None,
        schedule_cron: str | None = None,
        schedule_timezone: str = "UTC",
        config: dict | None = None,
    ) -> Job:
        # No sources picked -> fan out across every connector the platform
        # currently has implemented. This is what makes "just give a query"
        # actually run on every source automatically, rather than requiring
        # the caller to know and list connector names up front.
        was_auto_sourced = not sources
        if not sources:
            sources = sorted(get_implemented_sources(), key=lambda s: s.value)
            if not sources:
                raise ValueError("No source connectors are implemented yet")

        job_config = dict(config or {})

        # Fast path: for a small auto-sourced target count, dispatching every
        # connector at once wastes time — Kaggle/Roboflow pull whole datasets
        # and YouTube downloads+extracts video, all much slower than web
        # scraping for a handful of images. Try web alone first; the
        # remaining sources only get created/dispatched later if web didn't
        # reach the target (see orchestration_tasks.check_and_advance_job's
        # escalation branch). An explicit `sources` list is a deliberate
        # user choice and is never deferred like this.
        settings = get_settings()
        deferred_sources: list[SourceType] = []
        if (
            was_auto_sourced
            and settings.fast_path_enabled
            and target_count is not None
            and target_count <= settings.fast_path_max_target_count
            and SourceType.WEB in sources
        ):
            deferred_sources = [s for s in sources if s != SourceType.WEB]
            dispatch_sources = [SourceType.WEB]
            job_config["_fast_path_remaining_sources"] = [s.value for s in deferred_sources]
        else:
            dispatch_sources = sources

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

        dispatched_any = False
        for task in self.task_repo.list_for_job(job_id):
            task_path = _SOURCE_DISCOVERY_TASKS.get(SourceType(task.source_type))
            if task_path is None:
                self.task_repo.set_status(
                    task.id,
                    JobSourceTaskStatus.FAILED,
                    error_message=f"No connector implemented yet for source '{task.source_type}'",
                )
                continue
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
        self.db.flush()
        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})
        return job

    def resume(self, job_id: uuid.UUID) -> Job:
        job = self.job_repo.get_or_404(job_id)
        job.status = JobStateMachine.transition(job.status, "resume")
        JobRepository.apply_status_timestamps(job, job.status)
        self.cancellation.clear_paused(str(job_id))
        self.db.flush()

        for task in self.task_repo.list_for_job(job_id):
            if task.status in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                continue
            source_type = SourceType(task.source_type)
            if _resume_pending_items(source_type, str(job_id), str(task.id), task.checkpoint):
                continue
            task_path = _SOURCE_DISCOVERY_TASKS.get(source_type)
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

        for task in self.task_repo.list_for_job(job_id):
            if task.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                continue  # still actively working (e.g. a slower sibling source) -- leave it alone
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

        for task in self.task_repo.list_for_job(job_id):
            if task.status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
                self.task_repo.set_status(task.id, JobSourceTaskStatus.CANCELLED)

        self.publisher.publish(str(job_id), {"type": "job_status", "status": job.status.value})

        # evaluate_job_completion is deliberately NOT enqueued here: this
        # transaction has only been flush()ed, not committed, and a Celery
        # worker picking the task up in its own session could read the
        # pre-cancel status before this commits (the API layer commits
        # after action() returns — see _apply_control in app/api/v1/jobs.py,
        # which enqueues it once CANCELLING is durably persisted).
        return job

    @staticmethod
    def _dispatch(task_dotted_path: str, job_id: str, job_source_task_id: str) -> None:
        from app.tasks.celery_app import celery_app

        celery_app.send_task(task_dotted_path, args=[job_id, job_source_task_id])

    def _revoke_active_tasks(self, job_id: str) -> None:
        from app.tasks.celery_app import celery_app

        task_ids = self.cancellation.list_active_tasks(job_id)
        for task_id in task_ids:
            try:
                celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            except Exception as e:
                logger.warning(f"Failed to revoke task {task_id}: {e}")
