"""cleanup_local — deletes intermediate local files once a JobSourceTask's
items have been successfully promoted to NAS + recorded in the DB: raw video
files, extracted zip contents, kagglehub cache blobs, and the per-task workdir.
"""

import logging
import shutil
import uuid
from pathlib import Path

from app.core.config import get_settings
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.cleanup_tasks.cleanup_local")
def cleanup_local(job_source_task_id: str) -> dict:
    settings = get_settings()
    workdir = settings.work_root / job_source_task_id
    removed = False
    if workdir.exists():
        shutil.rmtree(workdir, ignore_errors=True)
        removed = True
        logger.info(f"Cleaned up workdir: {workdir}")
    return {"job_source_task_id": job_source_task_id, "workdir_removed": removed}


def cleanup_kaggle_cache(slug: str) -> None:
    """Best-effort removal of kagglehub's cached dataset blob once every image
    from it has been promoted — today the cache is kept forever."""
    try:
        import kagglehub

        cached_path = Path(kagglehub.dataset_download(slug))
        # dataset_download() is itself idempotent/cached, so calling it here just
        # resolves the existing cache path without re-downloading.
        if cached_path.exists():
            shutil.rmtree(cached_path, ignore_errors=True)
            logger.info(f"Cleaned up kagglehub cache for {slug}: {cached_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up kagglehub cache for {slug}: {e}")
