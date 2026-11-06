"""Discovery tasks: enumerate candidate work for a source, then dispatch a
Celery chord of per-item download tasks with on_source_finished as the
callback. Each source gets its own chord, so one source's failures never
block another source's chord from completing — the concrete mechanism for
"failure in one source must not interrupt the remaining jobs."
"""

import hashlib
import logging
import uuid
from urllib.parse import parse_qs, urlparse

from celery import chord

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
from app.core.redis_client import get_redis
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.credential_repository import CredentialRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.enums import JobSourceTaskStatus, SourceType
from app.services.cancellation_service import CancellationService
from app.services.credential_service import CredentialService
from app.services.job_service import register_source_discovery_task
from app.tasks._helpers import bind_context, heartbeat_during, unbind_context
from app.tasks.celery_app import celery_app
from app.tasks.ingest_tasks import (
    download_coco_shard,
    download_davis_shard,
    download_kaggle_dataset,
    download_kitti_shard,
    download_nas_share_shard,
    download_nuscenes_shard,
    download_open_images_shard,
    download_pascal_voc_shard,
    download_roboflow_item,
    download_sun_rgbd_shard,
    download_visual_genome_shard,
    download_web_batch,
    download_youtube_video,
)
from app.tasks.orchestration_tasks import check_and_advance_job, on_source_finished

logger = logging.getLogger(__name__)


def _should_skip_discovery(task_repo: JobSourceTaskRepository, job_uuid: uuid.UUID, task_uuid: uuid.UUID, source_type: str) -> bool:
    cancellation = CancellationService(get_redis())
    if cancellation.should_stop(str(job_uuid), str(task_uuid)):
        logger.info(f"Job {job_uuid} should_stop active; skipping discovery for {source_type}")
        task_repo.set_status(
            task_uuid,
            JobSourceTaskStatus.COMPLETED,
        )
        return True
    return False


def _default_container_limit(target_count: int | None, *, min_n: int = 5, max_n: int = 30, divisor: int = 30) -> int:
    """How many datasets/projects/videos to consider by default, scaled with
    the job's target image count. Each one can yield anywhere from a
    handful to thousands of images downstream, so a flat default (e.g. 5)
    starves a large-target job of enough raw material — see
    YouTubeConnector.discover()'s query-variant loop and this same fix
    applied to discover_youtube below, generalized to every other
    "search for N containers, then download everything inside them" source
    (Kaggle datasets, Roboflow projects)."""
    return min(max_n, max(min_n, (target_count or 100) // divisor))


def _default_image_limit(target_count: int | None, *, min_n: int = 20, max_n: int = 5000, multiplier: int = 3) -> int:
    """How many candidate images to pull by default from a locally-cached,
    fixed-vocabulary dataset (COCO/Open Images/Pascal VOC/Visual Genome/
    DAVIS/SUN RGB-D/nuImages/KITTI) or YouTube, scaled with the job's
    target count. `multiplier` alone already gives relevance filtering
    (which routinely rejects a majority of candidates) real headroom
    proportional to what was actually asked for -- `min_n` here is only a
    floor against degenerate near-zero targets (target_count 1-6ish),
    not a substitute for that scaling.

    Previously min_n=200 unconditionally, regardless of target_count: a
    target of 11-20 still got at least 200 candidates discovered and
    dispatched for download, most of them guaranteed to be thrown away
    (observed directly: a target_count=20 YouTube job discovering/
    dispatching 400 downloads on top of a live queue already contended by
    other jobs). Worse than wasted local compute for COCO/etc. -- every
    dispatched item is a real Celery task competing for the same limited
    worker pool every other job's downloads share, so one oversized small
    job measurably slows down everything else running at the time."""
    return min(max_n, max(min_n, (target_count or 200) * multiplier))


def topup_round_number(job) -> int:
    """Which top-up round this job is on (1 = the first, original pass).

    Discovery limits are scaled by this so a later round reaches further into
    the result set than the one before it. Without that, re-running discovery
    returns the same top-N items, every asset is skipped as a duplicate by the
    Asset unique constraint, and the round delivers nothing -- the loop would
    spin without converging.
    """
    return int(((job.config or {}).get("_topup") or {}).get("round", 1))


def scale_for_topup(limit: int, job, *, hard_max: int) -> int:
    """Widens a discovery limit for later top-up rounds.

    Round 1 is unchanged, so first-pass behaviour is exactly what it was.
    Each subsequent round multiplies the limit, bounded by hard_max so a
    runaway round cannot ask a connector for an unbounded result set.
    """
    return min(hard_max, limit * max(1, topup_round_number(job)))



def topup_limit(limit: int, job, source_type: SourceType, *, already_seen: int, hard_max: int) -> int:
    """A discovery limit that leaves room for the quota the planner set.

    scale_for_topup only multiplies by the round number, which is a guess. The
    planner has already worked out how many new assets this source should
    fetch this round, and if discovery offers fewer new candidates than that
    the round cannot deliver it. `already_seen` is what the job has fetched
    before (every one of those is skipped as a duplicate), so the limit is that
    plus the quota. It is an upper bound only: the job-wide target check still
    stops the round once enough has been fetched.
    """
    quota = (((job.config or {}).get("_topup") or {}).get("quota") or {}).get(source_type.value, 0)
    return min(hard_max, max(limit, already_seen + quota))


@celery_app.task(name="app.tasks.discovery_tasks.discover_kaggle", bind=True)
def discover_kaggle(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        explicit_slugs: list[tuple[str, str]] = job.config.get("kaggle", {}).get("slugs", [])
        discovery_error_reason: str | None = None
        if explicit_slugs:
            items = [{"slug": slug, "license": lic} for slug, lic in explicit_slugs]
        else:
            connector = KaggleConnector()
            with heartbeat_during(task_uuid):
                page = connector.discover(
                    job.query, cursor=None,
                    limit=job.config.get("kaggle", {}).get("max_datasets", scale_for_topup(_default_container_limit(job.target_count), job, hard_max=200)),
                )
            discovery_error_reason = page.error_reason
            items = [
                {"slug": si.external_ref["slug"], "license": si.license_hint or "Unknown"}
                for si in page.items
            ]

        if not items:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=discovery_error_reason or "No Kaggle datasets found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        task_repo.increment_counts(task_uuid, discovered=len(items))

        item_dicts = [
            {
                "external_ref": {"slug": it["slug"]},
                "license_hint": it["license"],
                "dataset_identifier": it["slug"],
            }
            for it in items
        ]

        # Records exactly what this round dispatched, keyed by slug (the
        # same key download_kaggle_dataset uses as its checkpoint_key). A
        # paused/resumed job reads this back (JobService.resume) to
        # redispatch only the datasets that are still outstanding instead of
        # re-running discovery and rebuilding the whole chord from scratch —
        # see on_source_finished for where "still outstanding" gets updated.
        pending = {**(task.checkpoint or {}).get("_pending_items", {})}
        pending.update({it["slug"]: item for it, item in zip(items, item_dicts)})
        task_repo.heartbeat(task_uuid, checkpoint={**(task.checkpoint or {}), "_pending_items": pending})
        db.flush()

    download_group = [download_kaggle_dataset.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.KAGGLE, "app.tasks.discovery_tasks.discover_kaggle")


@celery_app.task(name="app.tasks.discovery_tasks.discover_roboflow", bind=True)
def discover_roboflow(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        rf_config = job.config.get("roboflow", {})
        explicit_projects: list[list] = rf_config.get("projects", [])
        discovery_error_reason: str | None = None
        if explicit_projects:
            items = [
                {"workspace": ws, "project": proj, "version": ver, "license": lic}
                for ws, proj, ver, lic in explicit_projects
            ]
        else:
            api_keys = CredentialService(CredentialRepository(db)).resolve_pool(SourceType.ROBOFLOW, "ROBOFLOW_API_KEY")
            connector = RoboflowConnector(api_keys=api_keys)
            with heartbeat_during(task_uuid):
                page = connector.discover(
                    job.query, cursor=None,
                    limit=rf_config.get("max_projects", scale_for_topup(_default_container_limit(job.target_count), job, hard_max=200)),
                )
            discovery_error_reason = page.error_reason
            items = [
                {
                    "workspace": si.external_ref["workspace"],
                    "project": si.external_ref["project"],
                    "version": si.external_ref.get("version", 1),
                    "license": si.license_hint or "Unknown",
                }
                for si in page.items
            ]

        if not items:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=discovery_error_reason or "No Roboflow projects found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        task_repo.increment_counts(task_uuid, discovered=len(items))

        item_dicts = [
            {
                "external_ref": {"workspace": it["workspace"], "project": it["project"], "version": it["version"]},
                "license_hint": it["license"],
                "dataset_identifier": f"{it['workspace']}/{it['project']}",
            }
            for it in items
        ]

        pending = {**(task.checkpoint or {}).get("_pending_items", {})}
        pending.update({f"{it['workspace']}/{it['project']}": item for it, item in zip(items, item_dicts)})
        task_repo.heartbeat(task_uuid, checkpoint={**(task.checkpoint or {}), "_pending_items": pending})
        db.flush()

    download_group = [download_roboflow_item.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.ROBOFLOW, "app.tasks.discovery_tasks.discover_roboflow")


@celery_app.task(name="app.tasks.discovery_tasks.discover_youtube", bind=True)
def discover_youtube(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        yt_config = job.config.get("youtube", {})
        explicit_urls: list[str] = yt_config.get("urls", [])
        discovery_error_reason: str | None = None
        discovery_retry_after_s: float | None = None
        if explicit_urls:
            items = []
            for u in explicit_urls:
                video_id = _video_id_from_url(u)
                if video_id is None:
                    logger.warning(f"Skipping non-single-video YouTube URL for job {job_id}: {u}")
                    continue
                items.append({"url": u, "video_id": video_id, "license": "unknown"})
        else:
            credentials = CredentialService(CredentialRepository(db)).resolve(SourceType.YOUTUBE)
            connector = YouTubeConnector(cc_only=yt_config.get("cc_only", False))
            with heartbeat_during(task_uuid):
                page = connector.discover(
                    job.query, cursor=None,
                    # Unlike Kaggle/Roboflow "containers" (a handful yield everything
                    # needed), one video caps out at frames_per_video frames — reaching
                    # a large target needs many more videos, so this scales like the
                    # image-scan sources below instead of the tiny container default.
                    limit=yt_config.get("max_videos", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                    api_key=credentials.get("YOUTUBE_API_KEY", ""),
                )
            discovery_error_reason = page.error_reason
            discovery_retry_after_s = page.retry_after_s
            items = [
                {
                    "url": si.external_ref["url"],
                    "video_id": si.external_ref["video_id"],
                    "license": si.license_hint or "unknown",
                }
                for si in page.items
            ]

        if not items:
            if discovery_retry_after_s:
                # Temporary (e.g. rate-limit cooldown), not exhausted — reschedule
                # instead of marking the source done. Status stays DISCOVERING.
                logger.info(
                    f"YouTube discovery for job {job_id} deferred {discovery_retry_after_s:.0f}s: "
                    f"{discovery_error_reason}"
                )
                task_repo.heartbeat(task_uuid)
                db.commit()
                unbind_context()
                raise self.retry(countdown=discovery_retry_after_s, max_retries=10)
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=discovery_error_reason or "No YouTube videos found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        task_repo.increment_counts(task_uuid, discovered=len(items))

        item_dicts = [
            {
                "external_ref": {"url": it["url"], "video_id": it["video_id"]},
                "license_hint": it["license"],
                "dataset_identifier": it["video_id"],
            }
            for it in items
        ]

        pending = {**(task.checkpoint or {}).get("_pending_items", {})}
        pending.update({it["video_id"]: item for it, item in zip(items, item_dicts)})
        task_repo.heartbeat(task_uuid, checkpoint={**(task.checkpoint or {}), "_pending_items": pending})
        db.flush()

    download_group = [download_youtube_video.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


def _video_id_from_url(url: str) -> str | None:
    """Extract a single video id from a YouTube URL, or None if the URL
    does not point at a single video (e.g. a playlist or channel link)."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if path == "/watch":
        values = parse_qs(parsed.query).get("v")
        return values[0] if values else None

    if "/playlist" in path or "/channel/" in path or "/c/" in path or path.startswith("/@"):
        return None

    for prefix in ("/shorts/", "/embed/", "/v/"):
        if path.startswith(prefix):
            return path[len(prefix):].split("/")[0] or None

    if parsed.netloc.endswith("youtu.be"):
        return path.lstrip("/").split("/")[0] or None

    return None


register_source_discovery_task(SourceType.YOUTUBE, "app.tasks.discovery_tasks.discover_youtube")


@celery_app.task(name="app.tasks.discovery_tasks.discover_web", bind=True)
def discover_web(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        web_config = job.config.get("web", {})
        connector = WebScrapeConnector(safesearch=web_config.get("safesearch", "moderate"))
        # Default max_results (200) only applies when the job has no
        # target_count (e.g. deadline-only jobs) — otherwise a job asking
        # for e.g. 500 images used to silently cap at 200 raw search
        # results (and far fewer once validated) regardless of what was
        # actually requested.
        desired = scale_for_topup(
            job.target_count or web_config.get("max_results", 200), job, hard_max=50_000
        )
        with heartbeat_during(task_uuid):
            page = connector.discover(job.query, cursor=None, limit=desired)
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No web images found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_urls = sum(len(s.external_ref["urls"]) for s in shards)

        item_dicts = [
            {
                "external_ref": {"urls": s.external_ref["urls"]},
                "license_hint": None,
                "dataset_identifier": s.dataset_identifier,
            }
            for s in shards
        ]

        # Persist pending shards in checkpoint so batch windows and resumes can
        # redispatch them directly without re-running discovery and re-incrementing counters.
        existing_pending = (task.checkpoint or {}).get("_pending_items", {})
        pending = dict(existing_pending)
        new_urls = 0
        for item in item_dicts:
            shard_id = hashlib.md5(",".join(item["external_ref"]["urls"]).encode()).hexdigest()[:12]
            if shard_id not in pending:
                pending[shard_id] = item
                new_urls += len(item["external_ref"]["urls"])

        if new_urls > 0:
            task_repo.increment_counts(task_uuid, discovered=new_urls)
        elif not existing_pending and total_urls > 0:
            task_repo.increment_counts(task_uuid, discovered=total_urls)

        task_repo.heartbeat(task_uuid, checkpoint={**(task.checkpoint or {}), "_pending_items": pending})
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DOWNLOADING)
        db.commit()

    download_group = [download_web_batch.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.WEB, "app.tasks.discovery_tasks.discover_web")


@celery_app.task(name="app.tasks.discovery_tasks.discover_coco", bind=True)
def discover_coco(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        coco_config = job.config.get("coco", {})
        # train2017 is on by default: val2017 alone is 5,000 images (~535 with a car),
        # a ceiling no real target fits under. Its annotations are read from a
        # one-time SQLite index, not re-parsed per call, so the old cost objection
        # is gone. `{"coco": {"allow_train2017": false}}` keeps a job to val2017.
        connector = CocoConnector(
            split=coco_config.get("split", "val2017"),
            allow_train2017=coco_config.get("allow_train2017", True),
        )
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=coco_config.get(
                    "max_images",
                    topup_limit(
                        scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000),
                        job,
                        SourceType.COCO,
                        already_seen=AssetRepository(db).count_for_job(job_uuid),
                        hard_max=50_000,
                    ),
                ),
                explicit_categories=coco_config.get("categories"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No COCO images found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["image_ids"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": None,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_coco_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.COCO, "app.tasks.discovery_tasks.discover_coco")


@celery_app.task(name="app.tasks.discovery_tasks.discover_open_images", bind=True)
def discover_open_images(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        oi_config = job.config.get("open_images", {})
        connector = OpenImagesConnector(split=oi_config.get("split", "validation"))
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=oi_config.get("max_samples", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                explicit_classes=oi_config.get("classes"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No Open Images samples found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["image_ids"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": s.external_ref,
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_open_images_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.OPEN_IMAGES, "app.tasks.discovery_tasks.discover_open_images")


@celery_app.task(name="app.tasks.discovery_tasks.discover_pascal_voc", bind=True)
def discover_pascal_voc(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        voc_config = job.config.get("pascal_voc", {})
        connector = PascalVocConnector()
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=voc_config.get("max_images", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                explicit_categories=voc_config.get("categories"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No Pascal VOC images found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["image_ids"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_pascal_voc_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.PASCAL_VOC, "app.tasks.discovery_tasks.discover_pascal_voc")


@celery_app.task(name="app.tasks.discovery_tasks.discover_visual_genome", bind=True)
def discover_visual_genome(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        vg_config = job.config.get("visual_genome", {})
        connector = VisualGenomeConnector()
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=vg_config.get("max_images", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                explicit_categories=vg_config.get("categories"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No Visual Genome images found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["image_ids"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": s.external_ref,
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_visual_genome_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.VISUAL_GENOME, "app.tasks.discovery_tasks.discover_visual_genome")


@celery_app.task(name="app.tasks.discovery_tasks.discover_davis", bind=True)
def discover_davis(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        davis_config = job.config.get("davis", {})
        connector = DavisConnector()
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=davis_config.get("max_images", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                explicit_categories=davis_config.get("sequences"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No DAVIS frames found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_frames = sum(len(s.external_ref["frames"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_frames)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_davis_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.DAVIS, "app.tasks.discovery_tasks.discover_davis")


@celery_app.task(name="app.tasks.discovery_tasks.discover_sun_rgbd", bind=True)
def discover_sun_rgbd(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        sun_config = job.config.get("sun_rgbd", {})
        connector = SunRgbdConnector()
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=sun_config.get(
                    "max_images",
                    topup_limit(
                        scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000),
                        job,
                        SourceType.SUN_RGBD,
                        already_seen=AssetRepository(db).count_for_job(job_uuid),
                        hard_max=50_000,
                    ),
                ),
                explicit_categories=sun_config.get("categories"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No SUN RGB-D images found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["record_indices"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_sun_rgbd_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.SUN_RGBD, "app.tasks.discovery_tasks.discover_sun_rgbd")


@celery_app.task(name="app.tasks.discovery_tasks.discover_nuscenes", bind=True)
def discover_nuscenes(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        nu_config = job.config.get("nuscenes", {})
        connector = NuImagesConnector()
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=nu_config.get("max_samples", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                explicit_categories=nu_config.get("categories"),
                dataset=nu_config.get("dataset"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No nuImages samples found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_samples = sum(len(s.external_ref["sample_data_tokens"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_samples)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_nuscenes_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.NUSCENES, "app.tasks.discovery_tasks.discover_nuscenes")


@celery_app.task(name="app.tasks.discovery_tasks.discover_kitti", bind=True)
def discover_kitti(self, job_id: str, job_source_task_id: str):
    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        # Reset the stall clock to discovery-start — several sources
        # (Pascal VOC/DAVIS/SUN RGB-D/KITTI/nuImages) do a long blocking
        # shared-cache download inside discover() itself, before returning
        # anything; without a fresh heartbeat here the watchdog could mark
        # a genuinely-progressing discovery as stalled (see
        # app/tasks/ingest_tasks.py::_run_item_download for the same fix).
        task_repo.heartbeat(task_uuid)
        # commit (not flush): this discover() call can block for a long time
        # (large one-time shared-cache downloads) inside this same open
        # transaction — a flush alone stays invisible to the watchdog's
        # separate session until the whole call finishes, which would
        # defeat the point of writing an early heartbeat.
        db.commit()

        kitti_config = job.config.get("kitti", {})
        connector = KittiConnector()
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=kitti_config.get("max_images", scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000)),
                explicit_categories=kitti_config.get("categories"),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No KITTI images found for query",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["image_ids"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_kitti_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.KITTI, "app.tasks.discovery_tasks.discover_kitti")


@celery_app.task(name="app.tasks.discovery_tasks.discover_nas_share", bind=True)
def discover_nas_share(self, job_id: str, job_source_task_id: str):
    from app.connectors.nas_share_connector import NasShareConnector
    from app.services.nas_config_service import get_saved_params

    job_uuid = uuid.UUID(job_id)
    task_uuid = uuid.UUID(job_source_task_id)
    bind_context(job_uuid, task_uuid)

    with session_scope() as db:
        task_repo = JobSourceTaskRepository(db)
        task = task_repo.get(task_uuid)
        if task is None:
            unbind_context()
            return

        if _should_skip_discovery(task_repo, job_uuid, task_uuid, str(task.source_type)):
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        job = task.job
        task_repo.set_status(task_uuid, JobSourceTaskStatus.DISCOVERING)
        task_repo.heartbeat(task_uuid)
        # commit (not flush): walking a large share can take a while inside this
        # open transaction; see discover_coco for why the heartbeat must be visible.
        db.commit()

        try:
            params = get_saved_params()
        except Exception as e:  # e.g. encryption key missing/rotated
            params, config_error = None, f"NAS credentials could not be read: {e}"
        else:
            config_error = "NAS is not configured — add it under NAS in the sidebar"

        if params is None:
            task_repo.set_status(task_uuid, JobSourceTaskStatus.COMPLETED_PARTIAL, error_message=config_error)
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        nas_config = job.config.get("nas_share", {})
        connector = NasShareConnector(params, min_resolution=tuple(job.config.get("min_resolution", (64, 64))))
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=nas_config.get(
                    "max_images",
                    topup_limit(
                        scale_for_topup(_default_image_limit(job.target_count), job, hard_max=50_000),
                        job,
                        SourceType.NAS_SHARE,
                        already_seen=AssetRepository(db).count_for_job(job_uuid),
                        hard_max=50_000,
                    ),
                ),
            )
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid,
                JobSourceTaskStatus.COMPLETED_PARTIAL,
                error_message=page.error_reason or "No images found on the NAS",
            )
            db.commit()
            check_and_advance_job(job_id)
            unbind_context()
            return

        total_images = sum(len(s.external_ref["paths"]) for s in shards)
        task_repo.increment_counts(task_uuid, discovered=total_images)
        db.flush()

    item_dicts = [
        {
            "external_ref": {**s.external_ref},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_nas_share_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.NAS_SHARE, "app.tasks.discovery_tasks.discover_nas_share")
