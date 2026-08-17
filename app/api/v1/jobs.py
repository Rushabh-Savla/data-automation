"""Job CRUD + control (start/pause/resume/cancel) + progress endpoints."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_db, get_redis
from app.core.security import require_api_key
from app.db.models.job import Job
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.domain.enums import JOB_TERMINAL_STATUSES, JobStatus
from app.domain.state_machine import IllegalTransitionError
from app.schemas.job import (
    JobCreateRequest,
    JobDetailResponse,
    JobProgressResponse,
    JobResponse,
    JobUpdateRequest,
)
from app.services.job_service import JobService
from app.services.progress_service import ProgressPublisher

router = APIRouter(prefix="/jobs", tags=["jobs"], dependencies=[Depends(require_api_key)])


@router.post("", response_model=JobDetailResponse, status_code=201)
def create_job(payload: JobCreateRequest, db: Session = Depends(get_db)):
    service = JobService(db)
    job = service.create(
        query=payload.query,
        sources=payload.sources,
        name=payload.name,
        target_count=payload.target_count,
        deadline_at=payload.deadline_at,
        scheduled_start_at=payload.scheduled_start_at,
        config=payload.config,
    )
    db.commit()
    db.refresh(job)
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job.id))


@router.get("", response_model=list[JobResponse])
def list_jobs(
    status: JobStatus | None = None,
    source: str | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    return JobRepository(db).list(status=status, source=source, limit=limit, offset=offset)


@router.get("/{job_id}", response_model=JobDetailResponse)
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id))


@router.patch("/{job_id}", response_model=JobDetailResponse)
def update_job(job_id: uuid.UUID, payload: JobUpdateRequest, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    if job.status not in (JobStatus.DRAFT, JobStatus.SCHEDULED):
        raise HTTPException(409, "Only draft/scheduled jobs can be updated")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(job, field, value)
    db.commit()
    db.refresh(job)
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id))


@router.delete("/{job_id}", status_code=204)
def delete_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    if job.status not in JOB_TERMINAL_STATUSES and job.status != JobStatus.DRAFT:
        raise HTTPException(409, "Only terminal or draft jobs can be deleted; cancel it first")
    db.delete(job)
    db.commit()


@router.post("/{job_id}/start", response_model=JobDetailResponse)
def start_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.start(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id))


@router.post("/{job_id}/pause", response_model=JobDetailResponse)
def pause_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.pause(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id))


@router.post("/{job_id}/resume", response_model=JobDetailResponse)
def resume_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.resume(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id))


@router.post("/{job_id}/cancel", response_model=JobDetailResponse)
def cancel_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.cancel(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id))


@router.get("/{job_id}/progress", response_model=JobProgressResponse)
def get_job_progress(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    tasks = JobSourceTaskRepository(db).list_for_job(job_id)
    return JobProgressResponse(
        job_id=job.id,
        status=job.status,
        target_count=job.target_count,
        accepted_count=job.accepted_count,
        rejected_count=job.rejected_count,
        duplicate_count=job.duplicate_count,
        discovered_count=job.discovered_count,
        downloaded_count=job.downloaded_count,
        source_tasks=tasks,
    )


def _apply_control(db: Session, job_id: uuid.UUID, action) -> Job:
    _get_job_or_404(db, job_id)
    service = JobService(db)
    try:
        job = action(service)
    except IllegalTransitionError as e:
        raise HTTPException(409, str(e)) from e
    db.commit()
    db.refresh(job)
    return job


def _get_job_or_404(db: Session, job_id: uuid.UUID) -> Job:
    job = JobRepository(db).get(job_id)
    if job is None:
        raise HTTPException(404, f"Job {job_id} not found")
    return job


def _to_detail(job: Job, tasks) -> JobDetailResponse:
    data = JobResponse.model_validate(job).model_dump()
    data["source_tasks"] = tasks
    return JobDetailResponse(**data)
