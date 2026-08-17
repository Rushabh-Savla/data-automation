"""Repository isolating all SQLAlchemy access to the Job table.

Services and Celery tasks depend on this class, never on raw Session.query()
calls — this keeps ORM specifics out of business logic (Dependency Inversion)
and is the single place atomic rollup increments and status transitions happen.
"""

# Postponed annotation evaluation: this class defines a method literally named
# `list`, which — without this import — rebinds the name `list` inside the
# class namespace as soon as that def executes, breaking every `list[...]`
# type annotation on methods defined afterward in the same class body.
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models.job import Job
from app.domain.enums import JobCompletionReason, JobStatus


class JobRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, job: Job) -> Job:
        self.db.add(job)
        self.db.flush()
        return job

    def get(self, job_id: uuid.UUID) -> Job | None:
        return self.db.get(Job, job_id)

    def get_or_404(self, job_id: uuid.UUID) -> Job:
        job = self.get(job_id)
        if job is None:
            raise LookupError(f"Job {job_id} not found")
        return job

    def list(
        self,
        status: JobStatus | None = None,
        source: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        stmt = select(Job).order_by(Job.created_at.desc()).limit(limit).offset(offset)
        if status is not None:
            stmt = stmt.where(Job.status == status)
        if source is not None:
            stmt = stmt.where(Job.sources.contains([source]))
        return list(self.db.execute(stmt).scalars().all())

    def list_due_scheduled(self, now: datetime | None = None) -> list[Job]:
        """Jobs whose scheduled_start_at has passed but haven't started yet —
        used by the watchdog as a safety net for lost Celery ETA tasks."""
        now = now or datetime.now(timezone.utc)
        stmt = select(Job).where(
            Job.status == JobStatus.SCHEDULED,
            Job.scheduled_start_at.is_not(None),
            Job.scheduled_start_at <= now,
        )
        return list(self.db.execute(stmt).scalars().all())

    def list_active(self) -> list[Job]:
        from app.domain.enums import JOB_ACTIVE_STATUSES

        stmt = select(Job).where(Job.status.in_(JOB_ACTIVE_STATUSES))
        return list(self.db.execute(stmt).scalars().all())

    def set_status(
        self,
        job_id: uuid.UUID,
        status: JobStatus,
        completion_reason: JobCompletionReason | None = None,
        error_message: str | None = None,
    ) -> None:
        values: dict = {"status": status}
        if completion_reason is not None:
            values["completion_reason"] = completion_reason
        if error_message is not None:
            values["error_message"] = error_message
        if status == JobStatus.RUNNING:
            values.setdefault("started_at", datetime.now(timezone.utc))
        from app.domain.enums import JOB_TERMINAL_STATUSES

        if status in JOB_TERMINAL_STATUSES:
            values["completed_at"] = datetime.now(timezone.utc)
        self.db.execute(update(Job).where(Job.id == job_id).values(**values))

    def increment_counts(
        self,
        job_id: uuid.UUID,
        discovered: int = 0,
        downloaded: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        duplicate: int = 0,
    ) -> None:
        """Atomic `x = x + delta` update — safe under concurrent writers from
        many Celery tasks touching the same job row."""
        values = {}
        if discovered:
            values["discovered_count"] = Job.discovered_count + discovered
        if downloaded:
            values["downloaded_count"] = Job.downloaded_count + downloaded
        if accepted:
            values["accepted_count"] = Job.accepted_count + accepted
        if rejected:
            values["rejected_count"] = Job.rejected_count + rejected
        if duplicate:
            values["duplicate_count"] = Job.duplicate_count + duplicate
        if not values:
            return
        self.db.execute(update(Job).where(Job.id == job_id).values(**values))
