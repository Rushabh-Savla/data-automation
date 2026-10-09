"""Reclaiming the scratch space the pipeline leaves behind.

cleanup_local deletes the intermediate local files for a JobSourceTask once
its items have been promoted to NAS and recorded in the DB: raw video files,
extracted zip contents, kagglehub cache blobs and the per-task workdir.

It was written correctly and then never called. With no call site anywhere --
not in the task chain, not in the Beat schedule -- every job since deployment
left its scratch directory behind: 3.3 GB across 32,985 files on this host,
against 307 MB of actual delivered assets. purge_orphan_work_dirs below is
the safety net for the directories already stranded that way, since no
JobSourceTask is going to come back and clean them up now.
"""

import logging
import shutil
import time
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


# A workdir younger than this is assumed to belong to a task that is still
# running. Mtime is the only evidence available here: a directory whose task
# row says "terminal" can still be mid-write if the status was committed
# before the last file was flushed, and deleting underneath a live download
# destroys the checkpoint that makes resume possible.
_ORPHAN_MIN_AGE_S = 6 * 3600


@celery_app.task(name="app.tasks.cleanup_tasks.purge_orphan_work_dirs")
def purge_orphan_work_dirs() -> dict:
    """Beat-scheduled sweep for scratch directories nothing owns any more.

    Deletes a directory under WORK_ROOT only when all three hold:
      * its name is a UUID that maps to a JobSourceTask which is terminal, or
        to no JobSourceTask at all (the job was deleted);
      * it has not been modified for _ORPHAN_MIN_AGE_S;
      * it is a directory directly under WORK_ROOT.

    The shared cache directory is skipped by name -- it is deliberately
    long-lived and shared across jobs, so age tells us nothing about whether
    it is still wanted.
    """
    import uuid as _uuid

    from sqlalchemy import select

    from app.db.models.asset import Asset
    from app.db.models.job_source_task import JobSourceTask
    from app.db.session import session_scope
    from app.domain.enums import JOB_SOURCE_TASK_TERMINAL_STATUSES, AssetStatus
    from app.storage import asset_files

    settings = get_settings()
    work_root = settings.work_root
    if not work_root.exists():
        return {"scanned": 0, "removed": 0, "reclaimed_bytes": 0}

    now = time.time()
    candidates: list[Path] = []
    for entry in work_root.iterdir():
        if not entry.is_dir() or entry.name.startswith("_"):
            continue
        try:
            if now - entry.stat().st_mtime < _ORPHAN_MIN_AGE_S:
                continue
        except OSError:
            continue
        candidates.append(entry)

    if not candidates:
        return {"scanned": 0, "removed": 0, "reclaimed_bytes": 0}

    task_ids: dict[Path, _uuid.UUID] = {}
    for entry in candidates:
        try:
            task_ids[entry] = _uuid.UUID(entry.name)
        except ValueError:
            # Not a task workdir at all; leave it alone rather than guess.
            continue

    with session_scope() as db:
        rows = db.execute(
            select(JobSourceTask.id, JobSourceTask.status).where(
                JobSourceTask.id.in_(list(task_ids.values()))
            )
        ).all()
        status_by_id = {row[0]: row[1] for row in rows}

        # Assets that are still local, waiting for the NAS offload — a long
        # NAS outage can leave these sitting past _ORPHAN_MIN_AGE_S. Deleting
        # the workdir out from under them would silently and permanently lose
        # the image (offload_asset_batch would find the local file gone and
        # have nothing left to retry).
        pending_offload_task_ids = {
            row[0]
            for row in db.execute(
                select(Asset.job_source_task_id)
                .where(
                    Asset.job_source_task_id.in_(list(task_ids.values())),
                    Asset.status == AssetStatus.RELEVANCE_PASSED,
                    Asset.nas_path.is_not(None),
                    ~Asset.nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                )
                .distinct()
            ).scalars()
        }

    removed = 0
    skipped_pending_offload = 0
    reclaimed = 0
    for entry, task_id in task_ids.items():
        status = status_by_id.get(task_id)
        # Absent => the task (or its whole job) was deleted; terminal => it is
        # finished and will not resume. Anything else is still live.
        if status is not None and status not in JOB_SOURCE_TASK_TERMINAL_STATUSES:
            continue
        if task_id in pending_offload_task_ids:
            skipped_pending_offload += 1
            logger.warning(
                f"purge_orphan_work_dirs: skipping {entry} — still has image(s) "
                "awaiting NAS offload; the offload sweeper will retry them"
            )
            continue
        try:
            size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
        except OSError:
            size = 0
        shutil.rmtree(entry, ignore_errors=True)
        removed += 1
        reclaimed += size

    if removed:
        logger.info(
            f"purge_orphan_work_dirs: removed {removed} orphaned workdir(s), "
            f"reclaimed {reclaimed / 1e9:.2f} GB"
        )
    return {
        "scanned": len(candidates),
        "removed": removed,
        "skipped_pending_offload": skipped_pending_offload,
        "reclaimed_bytes": reclaimed,
    }
