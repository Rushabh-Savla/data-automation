"""build_export — packages a job's accepted assets into a downloadable zip
as a background task, so the triggering API request never blocks on
packaging (POST /jobs/{id}/export returns 202 immediately)."""

import logging
import uuid
from pathlib import Path

from app.core.config import get_settings
from app.core.redis_client import get_redis
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.export_repository import ExportRepository
from app.db.session import session_scope
from app.domain.enums import ExportFormat, ExportJobStatus
from app.services.export_service import ExportAssetView, ExportService
from app.services.progress_service import ProgressPublisher
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.export_tasks.build_export", bind=True)
def build_export(self, export_job_id: str):
    export_uuid = uuid.UUID(export_job_id)
    settings = get_settings()
    publisher = ProgressPublisher(get_redis())

    with session_scope() as db:
        export_repo = ExportRepository(db)
        asset_repo = AssetRepository(db)
        export_job = export_repo.get(export_uuid)
        if export_job is None:
            return

        job_id = export_job.job_id
        export_repo.set_status(export_uuid, ExportJobStatus.RUNNING)
        db.flush()

        min_score = (export_job.filter or {}).get("min_relevance_score")
        db_assets = asset_repo.list_for_export(job_id, min_relevance_score=min_score)
        # Snapshot into plain, session-independent views before the session
        # closes below — the export build can take a while over many assets
        # and must not touch a detached SQLAlchemy instance mid-build.
        assets = [
            ExportAssetView(
                id=str(a.id),
                source_type=a.source_type,
                source_url=a.source_url,
                dataset_identifier=a.dataset_identifier,
                license=a.license,
                nas_path=a.nas_path,
                annotation_format=a.annotation_format,
                annotation_nas_path=a.annotation_nas_path,
                width=a.width,
                height=a.height,
                classes=a.classes,
                asset_metadata=a.asset_metadata,
            )
            for a in db_assets
        ]
        export_format = ExportFormat(export_job.format)
        export_repo.set_progress(export_uuid, 0)

    def on_progress(n: int) -> None:
        with session_scope() as db2:
            ExportRepository(db2).set_progress(export_uuid, n)
        publisher.publish(str(job_id), {"type": "export_progress", "export_id": export_job_id, "processed": n})

    staging_dir = settings.work_root / "exports" / export_job_id
    zip_path = settings.nas_root / "exports" / f"{export_job_id}.zip"

    try:
        service = ExportService()
        count = service.build(assets, export_format, staging_dir, zip_path, on_progress)
        size_bytes = zip_path.stat().st_size if zip_path.exists() else 0

        with session_scope() as db:
            ExportRepository(db).set_status(
                export_uuid,
                ExportJobStatus.COMPLETED,
                file_path=str(zip_path),
                file_size_bytes=size_bytes,
                asset_count=count,
            )
        publisher.publish(str(job_id), {"type": "export_complete", "export_id": export_job_id, "asset_count": count})
    except Exception as e:
        logger.exception(f"Export {export_job_id} failed")
        with session_scope() as db:
            ExportRepository(db).set_status(export_uuid, ExportJobStatus.FAILED, error_message=str(e))
        publisher.publish(str(job_id), {"type": "export_failed", "export_id": export_job_id, "error": str(e)})
