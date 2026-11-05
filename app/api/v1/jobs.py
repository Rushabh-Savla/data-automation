"""Job CRUD + control (start/pause/resume/cancel) + progress endpoints."""

import shutil
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_db, get_redis
from app.core.config import get_settings
from app.core.security import require_api_key
from app.db.models.job import Job
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.domain.enums import JOB_TERMINAL_STATUSES, AssetStatus, JobStatus
from app.domain.state_machine import IllegalTransitionError
from app.schemas.common import PageResponse
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
        source_selection_mode=payload.source_selection_mode,
        name=payload.name,
        target_count=payload.target_count,
        deadline_at=payload.deadline_at,
        scheduled_start_at=payload.scheduled_start_at,
        schedule_cron=payload.schedule_cron,
        schedule_timezone=payload.schedule_timezone,
        config=payload.config,
    )
    db.commit()
    db.refresh(job)
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job.id), db=db)


@router.get("", response_model=PageResponse[JobResponse])
def list_jobs(
    status: JobStatus | None = None,
    source: str | None = None,
    search: str | None = Query(default=None, description="Matches job name or query text"),
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    repo = JobRepository(db)
    items = repo.list(status=status, source=source, search=search, limit=limit, offset=offset)
    total = repo.count(status=status, source=source, search=search)
    return PageResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/{job_id}", response_model=JobDetailResponse)
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.patch("/{job_id}", response_model=JobDetailResponse)
def update_job(job_id: uuid.UUID, payload: JobUpdateRequest, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    if job.status not in (JobStatus.DRAFT, JobStatus.SCHEDULED):
        raise HTTPException(409, "Only draft/scheduled jobs can be updated")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(job, field, value)
    db.commit()
    db.refresh(job)
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.delete("/{job_id}", status_code=204)
def delete_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    # SCHEDULED is deletable as well as terminal/draft. A recurring template
    # stays SCHEDULED for its entire life by design -- it never runs itself --
    # so restricting deletion to terminal states would make a template
    # impossible to ever remove, and it would keep spawning runs forever.
    # Deleting it does not touch the runs it already produced: parent_job_id
    # is ON DELETE SET NULL, so those become ordinary standalone jobs and the
    # data they collected is preserved.
    deletable = set(JOB_TERMINAL_STATUSES) | {JobStatus.DRAFT, JobStatus.SCHEDULED}
    if job.status not in deletable:
        raise HTTPException(409, "Only scheduled, terminal or draft jobs can be deleted; cancel it first")

    # Collect task IDs and asset files before DB cascade removes them
    task_repo = JobSourceTaskRepository(db)
    task_ids = [str(t.id) for t in task_repo.list_for_job(job_id)]

    from app.db.models.asset import Asset
    assets = db.execute(select(Asset).where(Asset.job_id == job_id)).scalars().all()
    asset_file_paths = []
    for a in assets:
        if a.nas_path:
            asset_file_paths.append(a.nas_path)
        if a.annotation_nas_path:
            asset_file_paths.append(a.annotation_nas_path)

    # 1. Clean local task scratch workdirs and staging folders immediately
    settings = get_settings()
    for tid in task_ids:
        workdir = settings.work_root / tid
        if workdir.exists():
            shutil.rmtree(workdir, ignore_errors=True)

    staging_dir = settings.work_root / "staging" / f"job_{job_id}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir, ignore_errors=True)

    # 2. Clean remote/promoted files from NAS or local storage
    if asset_file_paths:
        from app.tasks.cleanup_tasks import cleanup_job_storage
        try:
            cleanup_job_storage.delay(str(job_id), asset_file_paths)
        except Exception:
            from app.storage import asset_files
            for p in asset_file_paths:
                try:
                    asset_files.delete(p)
                except Exception:
                    pass

    db.delete(job)
    db.commit()


@router.post("/{job_id}/start", response_model=JobDetailResponse)
def start_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.start(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.post("/{job_id}/pause", response_model=JobDetailResponse)
def pause_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.pause(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.post("/{job_id}/resume", response_model=JobDetailResponse)
def resume_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.resume(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.post("/{job_id}/cancel", response_model=JobDetailResponse)
def cancel_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _apply_control(db, job_id, lambda svc: svc.cancel(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.post("/{job_id}/reopen", response_model=JobDetailResponse)
def reopen_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    """Sends a completed_partial job back for another discovery pass toward
    its original target_count, appending to the same job_id rather than
    requiring a disconnected new job."""
    job = _apply_control(db, job_id, lambda svc: svc.reopen(job_id))
    return _to_detail(job, JobSourceTaskRepository(db).list_for_job(job_id), db=db)


@router.get("/{job_id}/progress", response_model=JobProgressResponse)
def get_job_progress(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id)
    tasks = JobSourceTaskRepository(db).list_for_job(job_id)
    counts = AssetRepository(db).counts_by_status_for_job(job_id)
    return JobProgressResponse(
        job_id=job.id,
        status=job.status,
        target_count=job.target_count,
        accepted_count=job.accepted_count,
        relevance_accepted_count=counts.get(AssetStatus.RELEVANCE_PASSED.value, 0),
        rejected_count=job.rejected_count,
        duplicate_count=job.duplicate_count,
        discovered_count=job.discovered_count,
        downloaded_count=job.downloaded_count,
        review_pending_count=counts.get(AssetStatus.REVIEW_PENDING.value, 0),
        retry_pending_count=counts.get(AssetStatus.RETRY_PENDING.value, 0),
        upload_pending_count=counts.get(AssetStatus.READY_FOR_UPLOAD.value, 0),
        uploading_count=counts.get(AssetStatus.UPLOADING.value, 0),
        nas_verified_count=counts.get(AssetStatus.NAS_VERIFIED.value, 0),
        completed_count=counts.get(AssetStatus.COMPLETED.value, 0),
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
    if job.status == JobStatus.CANCELLING:
        # Only enqueued now that CANCELLING is durably committed — enqueuing
        # inside JobService.cancel() let a Celery worker's own session race
        # ahead of this commit and read the pre-cancel status, which crashed
        # the state-machine transition and stranded the job in CANCELLING
        # forever (still counted as "active"). See JobService.cancel().
        from app.tasks.orchestration_tasks import evaluate_job_completion

        evaluate_job_completion.delay(str(job.id))
    return job


def _get_job_or_404(db: Session, job_id: uuid.UUID) -> Job:
    job = JobRepository(db).get(job_id)
    if job is None:
        raise HTTPException(404, f"Job {job_id} not found")
    return job


def _to_detail(job: Job, tasks, db: Session | None = None) -> JobDetailResponse:
    data = JobResponse.model_validate(job).model_dump()
    data["source_tasks"] = tasks
    if db is not None:
        counts = AssetRepository(db).counts_by_status_for_job(job.id)
        data["relevance_accepted_count"] = counts.get(AssetStatus.RELEVANCE_PASSED.value, 0)
        data["review_pending_count"] = counts.get(AssetStatus.REVIEW_PENDING.value, 0)
        data["retry_pending_count"] = counts.get(AssetStatus.RETRY_PENDING.value, 0)
        data["upload_pending_count"] = counts.get(AssetStatus.READY_FOR_UPLOAD.value, 0)
        data["uploading_count"] = counts.get(AssetStatus.UPLOADING.value, 0)
        data["nas_verified_count"] = counts.get(AssetStatus.NAS_VERIFIED.value, 0)
        data["completed_count"] = counts.get(AssetStatus.COMPLETED.value, 0)
    return JobDetailResponse(**data)
