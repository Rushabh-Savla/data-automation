"""Export trigger + status/download. Packaging runs as a background Celery
task (see app/tasks/export_tasks.py) — this endpoint returns 202 immediately."""

import uuid

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.security import require_api_key
from app.db.models.export_job import ExportJob
from app.db.repositories.export_repository import ExportRepository
from app.db.repositories.job_repository import JobRepository
from app.schemas.export import ExportCreateRequest, ExportResponse

router = APIRouter(tags=["exports"], dependencies=[Depends(require_api_key)])


@router.post("/jobs/{job_id}/export", response_model=ExportResponse, status_code=202)
def create_export(job_id: uuid.UUID, payload: ExportCreateRequest, db: Session = Depends(get_db)):
    if JobRepository(db).get(job_id) is None:
        raise HTTPException(404, f"Job {job_id} not found")

    export_job = ExportJob(
        id=uuid.uuid4(), job_id=job_id, format=payload.format.value, filter=payload.filter
    )
    ExportRepository(db).create(export_job)
    db.commit()
    db.refresh(export_job)

    from app.tasks.export_tasks import build_export

    build_export.delay(str(export_job.id))

    return export_job


@router.get("/exports/{export_id}", response_model=ExportResponse)
def get_export(export_id: uuid.UUID, db: Session = Depends(get_db)):
    export_job = ExportRepository(db).get(export_id)
    if export_job is None:
        raise HTTPException(404, f"Export {export_id} not found")
    return export_job


@router.get("/exports/{export_id}/download")
def download_export(export_id: uuid.UUID, db: Session = Depends(get_db)):
    export_job = ExportRepository(db).get(export_id)
    if export_job is None or not export_job.file_path:
        raise HTTPException(404, f"Export {export_id} not found or not ready")
    return FileResponse(
        export_job.file_path,
        media_type="application/zip",
        filename=f"export_{export_job.job_id}_{export_job.format}.zip",
    )
