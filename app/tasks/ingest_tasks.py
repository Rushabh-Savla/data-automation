"""Per-source download tasks. Each task owns one unit of source-specific
download work (one Kaggle dataset, one Roboflow project, one YouTube video,
one web-scrape shard).

FatalConnectorError fails the task immediately (no retry) but the chord it
belongs to still completes — this is the mechanism for isolating one
dataset's failure from the rest of its source's work.

The per-source tasks below are thin wrappers around _run_item_download,
which owns the shared plumbing (DB session, should_stop/on_progress wiring,
standardization, exception handling) so adding a new source's download task
doesn't mean re-implementing that machinery (DRY).
"""

import hashlib
import logging
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

from app.connectors.base import RawAssetDraft, SourceItem
from app.connectors.kaggle_connector import KaggleConnector
from app.connectors.roboflow_connector import RoboflowConnector
from app.connectors.web_connector import WebScrapeConnector
from app.connectors.youtube_connector import YouTubeConnector
from app.core.config import get_settings
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.db.repositories.credential_repository import CredentialRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.enums import JobSourceTaskStatus, SourceType
from app.services.credential_service import CredentialService
from app.services.standardization_service import AssetRejected
from app.tasks._helpers import bind_context, build_on_progress, build_should_stop, unbind_context
from app.tasks.celery_app import celery_app
from app.tasks.cleanup_tasks import cleanup_kaggle_cache
from app.tasks.standardize_tasks import standardize_and_promote_sync

logger = logging.getLogger(__name__)

FetchFn = Callable[..., Iterator[RawAssetDraft]]


def _run_item_download(
    job_id: str,
    job_source_task_id: str,
    source_type: SourceType,
    checkpoint_key: str,
    fetch: Callable[[dict, Callable, Callable], Iterator[RawAssetDraft]],
    on_done: Callable[[], None] | None = None,
) -> dict:
    """Shared driver: resolve the task's namespaced checkpoint, wire up
    should_stop/on_progress, run the connector's fetch() generator through
    standardization, and translate JobStopped/FatalConnectorError into the
    right task-level outcome. `fetch(checkpoint, should_stop, on_progress)`
    is a closure the caller builds around its specific connector + item.
    """
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return {"error": "job_source_task not found"}

        checkpoint = (task.checkpoint or {}).get(checkpoint_key, {})
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DOWNLOADING)

        should_stop = build_should_stop(job_uuid)
        base_on_progress = build_on_progress(db, job_uuid, task_uuid)

        def on_progress(delta: dict) -> None:
            if "checkpoint" in delta:
                delta = {**delta, "checkpoint": {checkpoint_key: delta["checkpoint"]}}
            base_on_progress(delta)

        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task.job else (64, 64)

        accepted = rejected = 0
        try:
            for draft in fetch(checkpoint, should_stop, on_progress):
                try:
                    standardize_and_promote_sync(job_uuid, task_uuid, source_type, draft, min_res)
                    accepted += 1
                    on_progress({"accepted": 1})
                except AssetRejected:
                    rejected += 1
                    on_progress({"rejected": 1})
        except JobStopped:
            logger.info(f"Job {job_id} stopped mid-item ({checkpoint_key})")
            unbind_context()
            return {"item": checkpoint_key, "accepted": accepted, "rejected": rejected, "stopped": True}
        except FatalConnectorError as e:
            task_repo.increment_counts(task_uuid, errors=1)
            task_repo.set_status(task_uuid, JobSourceTaskStatus.FAILED, error_message=str(e))
            unbind_context()
            logger.error(f"Fatal error downloading {checkpoint_key}: {e}")
            return {"item": checkpoint_key, "error": str(e)}

        if on_done is not None:
            on_done()

    unbind_context()
    return {"item": checkpoint_key, "accepted": accepted, "rejected": rejected}


@celery_app.task(
    name="app.tasks.ingest_tasks.download_kaggle_dataset",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_kaggle_dataset(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.KAGGLE,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    slug = item.external_ref["slug"]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        credential_service = CredentialService(CredentialRepository(db))
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = KaggleConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        with credential_service.scoped_env(SourceType.KAGGLE):
            yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(
        job_id, job_source_task_id, SourceType.KAGGLE, slug, fetch,
        on_done=lambda: cleanup_kaggle_cache(slug),
    )


@celery_app.task(
    name="app.tasks.ingest_tasks.download_roboflow_item",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_roboflow_item(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.ROBOFLOW,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    checkpoint_key = f"{item.external_ref['workspace']}/{item.external_ref['project']}"
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        credential_service = CredentialService(CredentialRepository(db))
        credentials = credential_service.resolve(SourceType.ROBOFLOW)
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)
        export_format = task.job.config.get("roboflow", {}).get("export_format", "yolov8") if task and task.job else "yolov8"

    api_key = credentials.get("ROBOFLOW_API_KEY", "")
    connector = RoboflowConnector(api_key=api_key, export_format=export_format, min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.ROBOFLOW, checkpoint_key, fetch)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_youtube_video",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_youtube_video(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.YOUTUBE,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    checkpoint_key = item.external_ref.get("video_id", item.external_ref["url"])
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        yt_config = task.job.config.get("youtube", {}) if task and task.job else {}
        min_res = tuple(task.job.config.get("min_resolution", (320, 240))) if task and task.job else (320, 240)

    connector = YouTubeConnector(
        frame_mode=yt_config.get("frame_mode", "scene"),
        frames_per_video=yt_config.get("frames_per_video", 100),
        min_resolution=min_res,
        cc_only=yt_config.get("cc_only", True),
    )

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.YOUTUBE, checkpoint_key, fetch)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_web_batch",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_web_batch(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.WEB,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(",".join(item.external_ref["urls"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = WebScrapeConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.WEB, shard_id, fetch)
