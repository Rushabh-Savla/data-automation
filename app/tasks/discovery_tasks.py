"""Discovery tasks: enumerate candidate work for a source, then dispatch a
Celery chord of per-item download tasks with on_source_finished as the
callback. Each source gets its own chord, so one source's failures never
block another source's chord from completing — the concrete mechanism for
"failure in one source must not interrupt the remaining jobs."
"""

import logging
import uuid

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
from app.db.repositories.credential_repository import CredentialRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.enums import JobSourceTaskStatus, SourceType
from app.services.credential_service import CredentialService
from app.services.job_service import register_source_discovery_task
from app.tasks._helpers import bind_context, heartbeat_during, unbind_context
from app.tasks.celery_app import celery_app
from app.tasks.ingest_tasks import (
    download_coco_shard,
    download_davis_shard,
    download_kaggle_dataset,
    download_kitti_shard,
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
                    job.query, cursor=None, limit=job.config.get("kaggle", {}).get("max_datasets", 5)
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
        db.flush()

    item_dicts = [
        {
            "external_ref": {"slug": it["slug"]},
            "license_hint": it["license"],
            "dataset_identifier": it["slug"],
        }
        for it in items
    ]

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
            credentials = CredentialService(CredentialRepository(db)).resolve(SourceType.ROBOFLOW)
            connector = RoboflowConnector(api_key=credentials.get("ROBOFLOW_API_KEY", ""))
            with heartbeat_during(task_uuid):
                page = connector.discover(job.query, cursor=None, limit=rf_config.get("max_projects", 5))
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
        db.flush()

    item_dicts = [
        {
            "external_ref": {"workspace": it["workspace"], "project": it["project"], "version": it["version"]},
            "license_hint": it["license"],
            "dataset_identifier": f"{it['workspace']}/{it['project']}",
        }
        for it in items
    ]

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
        if explicit_urls:
            items = [{"url": u, "video_id": _video_id_from_url(u), "license": "unknown"} for u in explicit_urls]
        else:
            credentials = CredentialService(CredentialRepository(db)).resolve(SourceType.YOUTUBE)
            connector = YouTubeConnector(cc_only=yt_config.get("cc_only", True))
            with heartbeat_during(task_uuid):
                page = connector.discover(
                    job.query, cursor=None, limit=yt_config.get("max_videos", 5),
                    api_key=credentials.get("YOUTUBE_API_KEY", ""),
                )
            discovery_error_reason = page.error_reason
            items = [
                {
                    "url": si.external_ref["url"],
                    "video_id": si.external_ref["video_id"],
                    "license": si.license_hint or "unknown",
                }
                for si in page.items
            ]

        if not items:
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
        db.flush()

    item_dicts = [
        {
            "external_ref": {"url": it["url"], "video_id": it["video_id"]},
            "license_hint": it["license"],
            "dataset_identifier": it["video_id"],
        }
        for it in items
    ]

    download_group = [download_youtube_video.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


def _video_id_from_url(url: str) -> str:
    if "watch?v=" in url:
        return url.split("watch?v=")[1].split("&")[0]
    return url.rstrip("/").rsplit("/", 1)[-1]


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
        desired = job.target_count or web_config.get("max_results", 200)
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
        task_repo.increment_counts(task_uuid, discovered=total_urls)
        db.flush()

    item_dicts = [
        {
            "external_ref": {"urls": s.external_ref["urls"]},
            "license_hint": None,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

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
        connector = CocoConnector(
            split=coco_config.get("split", "val2017"),
            allow_train2017=coco_config.get("allow_train2017", False),
        )
        with heartbeat_during(task_uuid):
            page = connector.discover(
                job.query,
                cursor=None,
                limit=coco_config.get("max_images", 200),
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
            "external_ref": {"image_ids": s.external_ref["image_ids"], "split": s.external_ref["split"]},
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
                limit=oi_config.get("max_samples", 200),
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
                limit=voc_config.get("max_images", 200),
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
            "external_ref": {"image_ids": s.external_ref["image_ids"]},
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
                limit=vg_config.get("max_images", 200),
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
                limit=davis_config.get("max_images", 200),
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
            "external_ref": {"frames": s.external_ref["frames"]},
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
                limit=sun_config.get("max_images", 200),
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
            "external_ref": {"record_indices": s.external_ref["record_indices"]},
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
                limit=nu_config.get("max_samples", 200),
                explicit_categories=nu_config.get("categories"),
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
            "external_ref": {"sample_data_tokens": s.external_ref["sample_data_tokens"]},
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
                limit=kitti_config.get("max_images", 200),
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
            "external_ref": {"image_ids": s.external_ref["image_ids"]},
            "license_hint": s.license_hint,
            "dataset_identifier": s.dataset_identifier,
        }
        for s in shards
    ]

    download_group = [download_kitti_shard.s(job_id, job_source_task_id, item) for item in item_dicts]
    chord(download_group)(on_source_finished.s(job_id, job_source_task_id))
    unbind_context()


register_source_discovery_task(SourceType.KITTI, "app.tasks.discovery_tasks.discover_kitti")
