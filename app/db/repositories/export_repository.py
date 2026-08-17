import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models.export_job import ExportJob
from app.domain.enums import ExportJobStatus


class ExportRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, export_job: ExportJob) -> ExportJob:
        self.db.add(export_job)
        self.db.flush()
        return export_job

    def get(self, export_id: uuid.UUID) -> ExportJob | None:
        return self.db.get(ExportJob, export_id)

    def list_for_job(self, job_id: uuid.UUID) -> list[ExportJob]:
        stmt = select(ExportJob).where(ExportJob.job_id == job_id).order_by(ExportJob.requested_at.desc())
        return list(self.db.execute(stmt).scalars().all())

    def set_status(
        self,
        export_id: uuid.UUID,
        status: ExportJobStatus,
        error_message: str | None = None,
        **extra,
    ) -> None:
        values: dict = {"status": status, **extra}
        if error_message is not None:
            values["error_message"] = error_message
        if status in (ExportJobStatus.COMPLETED, ExportJobStatus.FAILED):
            values["completed_at"] = datetime.now(timezone.utc)
        self.db.execute(update(ExportJob).where(ExportJob.id == export_id).values(**values))

    def set_progress(self, export_id: uuid.UUID, processed_count: int) -> None:
        self.db.execute(
            update(ExportJob).where(ExportJob.id == export_id).values(processed_count=processed_count)
        )
