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

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

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

    @staticmethod
    def _filters(status: JobStatus | None, source: str | None, search: str | None) -> list[ColumnElement]:
        conditions: list[ColumnElement] = []
        if status is not None:
            conditions.append(Job.status == status)
        if source is not None:
            conditions.append(Job.sources.contains([source]))
        if search:
            pattern = f"%{search}%"
            # Matches either the free-text query or the optional job name —
            # those are the only two user-authored, human-readable fields on
            # a job, so together they cover "search for a job" intuitively.
            conditions.append(or_(Job.query.ilike(pattern), Job.name.ilike(pattern)))
        return conditions

    def list(
        self,
        status: JobStatus | None = None,
        source: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        stmt = select(Job).order_by(Job.created_at.desc()).limit(limit).offset(offset)
        for condition in self._filters(status, source, search):
            stmt = stmt.where(condition)
        return list(self.db.execute(stmt).scalars().all())

    def count(
        self,
        status: JobStatus | None = None,
        source: str | None = None,
        search: str | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(Job)
        for condition in self._filters(status, source, search):
            stmt = stmt.where(condition)
        return self.db.execute(stmt).scalar_one()

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

    def list_due_schedules(self, now: datetime | None = None) -> list[Job]:
        """Recurring templates whose next_run_at has arrived.

        Distinct from list_due_scheduled above: that one handles the one-shot
        scheduled_start_at and starts the job itself, whereas a template never
        runs -- it spawns a child run each time it fires. A template with a
        NULL next_run_at is included so a newly created one fires on its first
        sweep rather than waiting for a value nothing has written yet.
        """
        now = now or datetime.now(timezone.utc)
        stmt = select(Job).where(
            Job.schedule_cron.is_not(None),
            Job.status == JobStatus.SCHEDULED,
            or_(Job.next_run_at.is_(None), Job.next_run_at <= now),
        )
        return list(self.db.execute(stmt).scalars().all())

    def list_runs_for_template(self, template_id: uuid.UUID, limit: int = 50) -> list[Job]:
        stmt = (
            select(Job)
            .where(Job.parent_job_id == template_id)
            .order_by(Job.created_at.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def list_active(self) -> list[Job]:
        from app.domain.enums import JOB_ACTIVE_STATUSES

        stmt = select(Job).where(Job.status.in_(JOB_ACTIVE_STATUSES))
        return list(self.db.execute(stmt).scalars().all())

    @staticmethod
    def apply_status_timestamps(job: Job, status: JobStatus) -> None:
        """Stamps started_at / completed_at for a status the caller is applying
        directly to an ORM object.

        This exists because there are two ways a job's status changes in this
        codebase: set_status() below (a Core UPDATE, used by the Celery tasks)
        and direct assignment on a loaded Job (used by JobService, which needs
        the in-memory object for the rest of its work). Only the first one
        stamped the timestamps, so every job started through the API had
        started_at NULL forever and the UI showed "Started —" even while the
        job was running. Both paths now call this, so the rule lives once.
        """
        from app.domain.enums import JOB_TERMINAL_STATUSES

        if status == JobStatus.RUNNING and job.started_at is None:
            job.started_at = datetime.now(timezone.utc)
        if status in JOB_TERMINAL_STATUSES and job.completed_at is None:
            job.completed_at = datetime.now(timezone.utc)

    def set_status(
        self,
        job_id: uuid.UUID,
        status: JobStatus,
        completion_reason: JobCompletionReason | None = None,
        error_message: str | None = None,
    ) -> None:
        values: dict = {"status": status}
        if error_message is not None:
            values["error_message"] = error_message
        if status == JobStatus.RUNNING:
            values.setdefault("started_at", datetime.now(timezone.utc))
        from app.domain.enums import JOB_TERMINAL_STATUSES

        if status in JOB_TERMINAL_STATUSES:
            if completion_reason is not None:
                values["completion_reason"] = completion_reason
            values["completed_at"] = datetime.now(timezone.utc)
            # Drop the top-up bookkeeping now that no further round will
            # ever run: JobResponse surfaces config["_topup"]["round"] as
            # topup_round unconditionally, and left in place it makes a
            # finished job's UI banner claim it's still "collecting more
            # data" forever. The JSONB `-` operator removes the key
            # server-side (no-op if absent) without a read-modify-write.
            values["config"] = Job.config.op("-")("_topup")
        else:
            # Active/running jobs must never retain a completion reason or completed_at
            values["completion_reason"] = None
            values["completed_at"] = None
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
        many Celery tasks touching the same job row.

        Every column is floored at 0 via GREATEST. These counters take
        negative deltas as well as positive ones (filtering corrects
        accepted_count downward as assets are rejected, deduplicated or
        trimmed), so a stage that runs twice — a retried task, or a
        watchdog re-drive of a pass that partly completed — could otherwise
        drive a count below zero and surface as a nonsensical negative in
        the UI. Clamping here keeps the invariant in the one place every
        writer already funnels through, rather than at each call site."""
        values = {}
        if discovered:
            values["discovered_count"] = func.greatest(0, Job.discovered_count + discovered)
        if downloaded:
            values["downloaded_count"] = func.greatest(0, Job.downloaded_count + downloaded)
        if accepted:
            values["accepted_count"] = func.greatest(0, Job.accepted_count + accepted)
        if rejected:
            values["rejected_count"] = func.greatest(0, Job.rejected_count + rejected)
        if duplicate:
            values["duplicate_count"] = func.greatest(0, Job.duplicate_count + duplicate)
        if not values:
            return
        self.db.execute(update(Job).where(Job.id == job_id).values(**values))

    def set_config(self, job_id: uuid.UUID, config: dict) -> None:
        self.db.execute(update(Job).where(Job.id == job_id).values(config=config))
