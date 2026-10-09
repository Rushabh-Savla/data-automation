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
from app.connectors.coco_connector import CocoConnector
from app.connectors.davis_connector import DavisConnector
from app.connectors.kaggle_connector import KaggleConnector
from app.connectors.kitti_connector import KittiConnector
from app.connectors.nuimages_connector import NuImagesConnector
from app.connectors.open_images_connector import OpenImagesConnector
from app.connectors.pascal_voc_connector import PascalVocConnector
from app.connectors.roboflow_connector import RoboflowConnector
from app.connectors.sun_rgbd_connector import SunRgbdConnector
from app.connectors.visual_genome_connector import VisualGenomeConnector
from app.connectors.web_connector import WebScrapeConnector
from app.connectors.youtube_connector import YouTubeConnector
from app.core.config import get_settings
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.db.repositories.credential_repository import CredentialRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.enums import JobSourceTaskStatus, SourceType
from app.services.credential_service import CredentialService
from app.services.job_service import register_source_download_task
from app.services.standardization_service import AssetRejected
from app.tasks._helpers import bind_context, build_on_progress, build_should_stop, heartbeat_during, unbind_context
from app.tasks.celery_app import celery_app
from app.tasks.cleanup_tasks import cleanup_kaggle_cache
from app.tasks.standardize_tasks import standardize_and_promote_sync

logger = logging.getLogger(__name__)

FetchFn = Callable[..., Iterator[RawAssetDraft]]

# Separate, higher ceiling for errors that carry a real retry_after_s
# (e.g. a rate-limit cooldown) — each retry waits minutes, not seconds,
# so more attempts are affordable than a task's normal max_retries=5.
_RATE_LIMIT_RETRY_CEILING = 10


def _run_item_download(
    job_id: str,
    job_source_task_id: str,
    source_type: SourceType,
    checkpoint_key: str,
    fetch: Callable[[dict, Callable, Callable], Iterator[RawAssetDraft]],
    on_done: Callable[[], None] | None = None,
    bound_task=None,
) -> dict:
    """Shared driver: resolve the task's namespaced checkpoint, wire up
    should_stop/on_progress, run the connector's fetch() generator through
    standardization, and translate JobStopped/FatalConnectorError/exhausted-
    retry TransientConnectorError into the right task-level outcome.
    `fetch(checkpoint, should_stop, on_progress)` is a closure the caller
    builds around its specific connector + item. `bound_task` is the
    calling Celery task's `self` (all 12 callers are `bind=True`) — needed
    to tell an exhausted retry apart from one with attempts left, below.

    FatalConnectorError handling (Kaggle fix):
    A FatalConnectorError on one dataset (e.g. a restricted/firearm-class
    dataset returning 403) increments the error counter and returns a clean
    result dict — it does NOT set the whole source task to FAILED. Setting
    FAILED here would mark the entire Kaggle source as failed even when 4
    out of 5 datasets downloaded successfully, which is wrong. The chord
    callback (on_source_finished) decides the final task status based on
    the aggregate of all per-item results. Per-item fatal errors are errors,
    not source-level failures.

    Exhausted-retry TransientConnectorError handling (YouTube fix):
    autoretry_for=(TransientConnectorError, ...) on each download task
    retries automatically, but once every retry is exhausted Celery
    re-raises the original exception unhandled — which, for a task that's
    part of a chord (every source here dispatches one), raises ChordError
    into on_source_finished instead of running it. That leaves the
    JobSourceTask stuck in DOWNLOADING forever, since nothing else ever
    calls check_and_advance_job for it again. This isn't a rare edge case:
    a systemic block (e.g. YouTube's CDN 403ing every video for a given
    host — see youtube_connector.py's _classify_error) makes every item's
    retries exhaust the same way, guaranteeing the source gets stuck. So
    once bound_task confirms this was the last permitted attempt, degrade
    exactly like FatalConnectorError above (record it, keep the chord
    intact) instead of letting it propagate.
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
        # Reset the stall clock to task-start rather than inheriting a
        # null/stale heartbeat from an earlier phase. Several connectors
        # (Kaggle's kagglehub.dataset_download(), and every connector with a
        # large one-time shared-cache download — Pascal VOC/DAVIS/SUN RGB-D/
        # KITTI/nuImages) spend minutes inside a single blocking call before
        # their fetch() loop ever yields its first item and triggers a
        # progress-based heartbeat. Without this, the watchdog (see
        # app/core/config.py::watchdog_stall_threshold_s) can mark a
        # genuinely-progressing task "stalled" and re-dispatch it from
        # scratch, causing duplicate downloads of the same content.
        task_repo.heartbeat(task_uuid)
        db.commit()

        should_stop = build_should_stop(job_uuid, task_uuid)
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
            # Increment the error counter on the source task but do NOT set
            # the task status to FAILED — one inaccessible dataset (e.g. a
            # restricted firearm/NSFW dataset returning 403) must not mark
            # the entire Kaggle source as failed when other datasets in the
            # same chord succeeded. The chord callback (on_source_finished)
            # will set COMPLETED_PARTIAL if any errors were recorded.
            with session_scope() as err_db:
                err_repo = JobSourceTaskRepository(err_db)
                err_repo.increment_counts(task_uuid, errors=1)
                # Surfaces the specific reason (e.g. Kaggle's 403/401 dataset
                # gating) via the existing error_message field instead of it
                # only ever reaching the worker log — status is left
                # unchanged (still DOWNLOADING), this is not a task failure.
                err_repo.set_status(task_uuid, JobSourceTaskStatus.DOWNLOADING, error_message=str(e))
                err_db.commit()
            unbind_context()
            logger.error(f"Fatal error downloading {checkpoint_key} (recorded as error, not task failure): {e}")
            return {"item": checkpoint_key, "accepted": accepted, "rejected": rejected, "error": str(e)}
        except (TransientConnectorError, ConnectionError, TimeoutError) as e:
            retry_after_s = getattr(e, "retry_after_s", None)
            retries_so_far = getattr(getattr(bound_task, "request", None), "retries", None) or 0

            # Rate-limit style errors carry a real wait time — retry with
            # that instead of the decorator's fast exponential backoff
            # (which exhausts in seconds, not the minutes a cooldown needs).
            if retry_after_s and bound_task is not None and retries_so_far < _RATE_LIMIT_RETRY_CEILING:
                unbind_context()
                raise bound_task.retry(countdown=retry_after_s, max_retries=_RATE_LIMIT_RETRY_CEILING, exc=e)

            max_retries = getattr(bound_task, "max_retries", None)
            if bound_task is not None and not retry_after_s and max_retries is not None and retries_so_far < max_retries:
                unbind_context()
                raise  # attempts remain — let autoretry_for retry this task again

            if retry_after_s:
                # Rate-limiting, not a real error — don't count it against the
                # source; just skip this item, topup will pick up the slack.
                unbind_context()
                logger.warning(f"Skipping {checkpoint_key} after repeated rate-limiting: {e}")
                return {"item": checkpoint_key, "accepted": accepted, "rejected": rejected}

            with session_scope() as err_db:
                err_repo = JobSourceTaskRepository(err_db)
                err_repo.increment_counts(task_uuid, errors=1)
                err_repo.set_status(task_uuid, JobSourceTaskStatus.DOWNLOADING, error_message=str(e))
                err_db.commit()
            unbind_context()
            logger.error(f"Exhausted retries downloading {checkpoint_key} (recorded as error, not task failure): {e}")
            return {"item": checkpoint_key, "accepted": accepted, "rejected": rejected, "error": str(e)}
        except Exception as e:
            # Anything not covered above (a DB constraint violation, an
            # unclassified third-party SDK error, ...) must not crash this
            # task outright: an unhandled exception in one chord member
            # breaks the WHOLE chord — Celery surfaces it as ChordError to
            # every other member and on_source_finished never runs, which
            # permanently strands the JobSourceTask in DOWNLOADING even
            # though the rest of the chord's items finished fine. That's
            # exactly what the FatalConnectorError/TransientConnectorError
            # handling above exists to prevent for known error types; any
            # other exception gets the same degrade-and-continue treatment
            # instead of being left to propagate and poison the chord.
            with session_scope() as err_db:
                err_repo = JobSourceTaskRepository(err_db)
                err_repo.increment_counts(task_uuid, errors=1)
                err_repo.set_status(task_uuid, JobSourceTaskStatus.DOWNLOADING, error_message=str(e))
                err_db.commit()
            unbind_context()
            logger.exception(f"Unexpected error downloading {checkpoint_key} (recorded as error, not task failure)")
            return {"item": checkpoint_key, "accepted": accepted, "rejected": rejected, "error": str(e)}

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
        # kagglehub.dataset_download() is one blocking call with no progress
        # callback of its own — for a large dataset on a slow connection it
        # can run well past the watchdog's stall threshold before the walk
        # loop below ever yields its first item and starts its own
        # per-image heartbeats. Without this, the watchdog kills and
        # restarts the task from scratch, which for a persistently slow/huge
        # dataset means it never actually finishes (killed → re-download
        # from zero → killed again).
        with credential_service.scoped_env(SourceType.KAGGLE), heartbeat_during(uuid.UUID(job_source_task_id)):
            yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(
        job_id, job_source_task_id, SourceType.KAGGLE, slug, fetch,
        on_done=lambda: cleanup_kaggle_cache(slug), bound_task=self,
    )


register_source_download_task(SourceType.KAGGLE, "app.tasks.ingest_tasks.download_kaggle_dataset")


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
        api_keys = credential_service.resolve_pool(SourceType.ROBOFLOW, "ROBOFLOW_API_KEY")
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)
        export_format = task.job.config.get("roboflow", {}).get("export_format", "yolov8") if task and task.job else "yolov8"

    connector = RoboflowConnector(api_keys=api_keys, export_format=export_format, min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.ROBOFLOW, checkpoint_key, fetch, bound_task=self)



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

    # Frames extracted per second of video, user-configurable per job; 1.0
    # (one frame per second) when unset or not a usable positive number.
    extraction_fps = yt_config.get("fps", 1.0)
    if not extraction_fps or extraction_fps <= 0:
        extraction_fps = 1.0

    connector = YouTubeConnector(
        # No default ceiling: an unset "frames_per_video" means every frame
        # the chosen fps/interval produces is kept, for both auto-discovered
        # and explicit-URL videos. Operators who want a per-video ceiling
        # (e.g. to keep one video from dominating a large auto-discovery job)
        # set it explicitly via job config.
        frames_per_video=yt_config.get("frames_per_video"),
        # "fps" extracts exactly one frame per second — deterministic and
        # matches the documented behaviour. "scene" was the old default but
        # produced unpredictable frame counts and often zero results.
        frame_mode=yt_config.get("frame_mode", "fps"),
        extraction_fps=extraction_fps,
        min_resolution=min_res,
        # False = do not filter by CC license at download time. yt-dlp's
        # flat search metadata omits the license field so cc_only=True
        # silently drops every video. Operators can override via job config.
        cc_only=yt_config.get("cc_only", False),
    )

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.YOUTUBE, checkpoint_key, fetch, bound_task=self)


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

    return _run_item_download(job_id, job_source_task_id, SourceType.WEB, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_coco_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_coco_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.COCO,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["image_ids"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = CocoConnector(min_resolution=min_res, split=item.external_ref.get("split", "val2017"))

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.COCO, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_open_images_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_open_images_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.OPEN_IMAGES,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["image_ids"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = OpenImagesConnector(min_resolution=min_res, split=item.external_ref.get("split", "validation"))

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.OPEN_IMAGES, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_pascal_voc_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_pascal_voc_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.PASCAL_VOC,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["image_ids"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = PascalVocConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.PASCAL_VOC, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_visual_genome_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_visual_genome_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.VISUAL_GENOME,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["image_ids"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = VisualGenomeConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.VISUAL_GENOME, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_davis_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_davis_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.DAVIS,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["frames"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = DavisConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.DAVIS, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_sun_rgbd_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_sun_rgbd_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.SUN_RGBD,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["record_indices"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = SunRgbdConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        # The 6.9GB SUNRGBD.zip is fetched lazily, on the first shard's first
        # image (discovery only needs the 4MB metadata file), so it is one
        # long blocking call with no per-image heartbeat yet -- and the other
        # shards sit waiting on the cache lock meanwhile. On a slow link that
        # outlasts the watchdog's stall threshold, which then marks the source
        # stalled and re-runs its discovery, queueing a second set of shards
        # that block on the same lock.
        with heartbeat_during(uuid.UUID(job_source_task_id)):
            yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.SUN_RGBD, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_nuscenes_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_nuscenes_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.NUSCENES,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["sample_data_tokens"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = NuImagesConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.NUSCENES, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_kitti_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_kitti_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.KITTI,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["image_ids"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    connector = KittiConnector(min_resolution=min_res)

    def fetch(checkpoint, should_stop, on_progress):
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.KITTI, shard_id, fetch, bound_task=self)


@celery_app.task(
    name="app.tasks.ingest_tasks.download_nas_share_shard",
    bind=True,
    autoretry_for=(TransientConnectorError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def download_nas_share_shard(self, job_id: str, job_source_task_id: str, item_dict: dict):
    from app.connectors.nas_share_connector import NasShareConnector
    from app.services.nas_config_service import get_saved_params

    settings = get_settings()
    item = SourceItem(
        source_type=SourceType.NAS_SHARE,
        external_ref=item_dict["external_ref"],
        license_hint=item_dict.get("license_hint"),
        dataset_identifier=item_dict.get("dataset_identifier"),
    )
    shard_id = hashlib.md5(str(item.external_ref["paths"]).encode()).hexdigest()[:12]
    workdir = settings.work_root / job_source_task_id
    workdir.mkdir(parents=True, exist_ok=True)

    with session_scope() as db:
        task = JobSourceTaskRepository(db).get(uuid.UUID(job_source_task_id))
        min_res = tuple(task.job.config.get("min_resolution", (64, 64))) if task and task.job else (64, 64)

    # Credentials are resolved here, inside the worker, from the encrypted config —
    # never carried in item_dict (which is stored in Celery messages/checkpoints).
    params = get_saved_params()
    connector = NasShareConnector(params, min_resolution=min_res) if params is not None else None

    def fetch(checkpoint, should_stop, on_progress):
        if connector is None:
            # Raised inside fetch (not before the driver) so it is recorded as an item
            # error instead of breaking the chord for every other shard.
            raise FatalConnectorError("NAS is not configured")
        yield from connector.fetch(item, workdir, checkpoint, should_stop, on_progress)

    return _run_item_download(job_id, job_source_task_id, SourceType.NAS_SHARE, shard_id, fetch, bound_task=self)
