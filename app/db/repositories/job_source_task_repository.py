import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import (
    JOB_SOURCE_TASK_TERMINAL_STATUSES,
    JOB_TERMINAL_STATUSES,
    JobSourceTaskStatus,
    SourceType,
)
from app.domain.recovery import is_orphaned


class JobSourceTaskRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, task: JobSourceTask) -> JobSourceTask:
        self.db.add(task)
        self.db.flush()
        return task

    def get(self, task_id: uuid.UUID) -> JobSourceTask | None:
        return self.db.get(JobSourceTask, task_id)

    def get_for_job_and_source(
        self, job_id: uuid.UUID, source_type: SourceType
    ) -> JobSourceTask | None:
        stmt = select(JobSourceTask).where(
            JobSourceTask.job_id == job_id, JobSourceTask.source_type == source_type
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def list_for_job(self, job_id: uuid.UUID) -> list[JobSourceTask]:
        stmt = select(JobSourceTask).where(JobSourceTask.job_id == job_id)
        return list(self.db.execute(stmt).scalars().all())

    def all_terminal_for_job(self, job_id: uuid.UUID) -> bool:
        tasks = self.list_for_job(job_id)
        if not tasks:
            return False
        return all(t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES for t in tasks)

    def list_stalled(
        self, stall_threshold_s: int, now: datetime | None = None, *, orphan_cutoff: datetime | None = None
    ) -> list[JobSourceTask]:
        # Excludes tasks whose parent Job already reached a terminal status
        # (cancelled/completed/failed) — otherwise a task that stalled right
        # before its job was cancelled sits in a non-terminal task status
        # forever and the watchdog resurrects it (re-dispatches discovery)
        # on every scan, indefinitely, wasting worker capacity on dead jobs.
        now = now or datetime.now(timezone.utc)
        cutoff = now.timestamp() - stall_threshold_s
        stmt = (
            select(JobSourceTask)
            .join(Job, JobSourceTask.job_id == Job.id)
            .where(
                JobSourceTask.status.in_(
                    [
                        JobSourceTaskStatus.DISCOVERING,
                        JobSourceTaskStatus.DOWNLOADING,
                        JobSourceTaskStatus.STANDARDIZING,
                    ]
                ),
                Job.status.notin_(JOB_TERMINAL_STATUSES),
            )
        )
        stalled = []
        for t in self.db.execute(stmt).scalars().all():
            silent = t.last_heartbeat_at is None or t.last_heartbeat_at.timestamp() < cutoff
            # Also stalled if it last heartbeat before the latest worker boot: the
            # process that owned it died in the restart (see app/domain/recovery.py).
            if silent or is_orphaned(t.last_heartbeat_at, orphan_cutoff):
                stalled.append(t)
        return stalled

    def set_status(
        self,
        task_id: uuid.UUID,
        status: JobSourceTaskStatus,
        error_message: str | None = None,
    ) -> None:
        values: dict = {"status": status}
        if error_message is not None:
            values["error_message"] = error_message
        if status == JobSourceTaskStatus.DISCOVERING:
            values.setdefault("started_at", datetime.now(timezone.utc))
        if status in JOB_SOURCE_TASK_TERMINAL_STATUSES:
            values["completed_at"] = datetime.now(timezone.utc)
        self.db.execute(update(JobSourceTask).where(JobSourceTask.id == task_id).values(**values))

    def heartbeat(self, task_id: uuid.UUID, checkpoint: dict | None = None) -> None:
        values: dict = {"last_heartbeat_at": datetime.now(timezone.utc)}
        if checkpoint is not None:
            values["checkpoint"] = checkpoint
        self.db.execute(update(JobSourceTask).where(JobSourceTask.id == task_id).values(**values))

    def increment_counts(
        self,
        task_id: uuid.UUID,
        discovered: int = 0,
        downloaded: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        errors: int = 0,
    ) -> None:
        values = {}
        if discovered:
            values["discovered_count"] = JobSourceTask.discovered_count + discovered
        if downloaded:
            values["downloaded_count"] = JobSourceTask.downloaded_count + downloaded
        if accepted:
            values["accepted_count"] = JobSourceTask.accepted_count + accepted
        if rejected:
            values["rejected_count"] = JobSourceTask.rejected_count + rejected
        if errors:
            values["error_count"] = JobSourceTask.error_count + errors
        if not values:
            return
        self.db.execute(update(JobSourceTask).where(JobSourceTask.id == task_id).values(**values))
