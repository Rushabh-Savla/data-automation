"""Discovery tasks: enumerate candidate work for a source, then dispatch a
Celery chord of per-item download tasks with on_source_finished as the
callback. Each source gets its own chord, so one source's failures never
block another source's chord from completing — the concrete mechanism for
"failure in one source must not interrupt the remaining jobs."
"""

import logging
import uuid

from celery import chord

from app.connectors.kaggle_connector import KaggleConnector
from app.connectors.roboflow_connector import RoboflowConnector
from app.connectors.web_connector import WebScrapeConnector
from app.connectors.youtube_connector import YouTubeConnector
from app.db.repositories.credential_repository import CredentialRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.db.session import session_scope
from app.domain.enums import JobSourceTaskStatus, SourceType
from app.services.credential_service import CredentialService
from app.services.job_service import register_source_discovery_task
from app.tasks._helpers import bind_context, unbind_context
from app.tasks.celery_app import celery_app
from app.tasks.ingest_tasks import (
    download_kaggle_dataset,
    download_roboflow_item,
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

        explicit_slugs: list[tuple[str, str]] = job.config.get("kaggle", {}).get("slugs", [])
        if explicit_slugs:
            items = [{"slug": slug, "license": lic} for slug, lic in explicit_slugs]
        else:
            connector = KaggleConnector()
            page = connector.discover(job.query, cursor=None, limit=job.config.get("kaggle", {}).get("max_datasets", 5))
            items = [
                {"slug": si.external_ref["slug"], "license": si.license_hint or "Unknown"}
                for si in page.items
            ]

        if not items:
            task_repo.set_status(
                task_uuid, JobSourceTaskStatus.COMPLETED_PARTIAL, error_message="No Kaggle datasets found for query"
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

        rf_config = job.config.get("roboflow", {})
        explicit_projects: list[list] = rf_config.get("projects", [])
        if explicit_projects:
            items = [
                {"workspace": ws, "project": proj, "version": ver, "license": lic}
                for ws, proj, ver, lic in explicit_projects
            ]
        else:
            credentials = CredentialService(CredentialRepository(db)).resolve(SourceType.ROBOFLOW)
            connector = RoboflowConnector(api_key=credentials.get("ROBOFLOW_API_KEY", ""))
            page = connector.discover(job.query, cursor=None, limit=rf_config.get("max_projects", 5))
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
                task_uuid, JobSourceTaskStatus.COMPLETED_PARTIAL, error_message="No Roboflow projects found for query"
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

        yt_config = job.config.get("youtube", {})
        explicit_urls: list[str] = yt_config.get("urls", [])
        if explicit_urls:
            items = [{"url": u, "video_id": _video_id_from_url(u), "license": "unknown"} for u in explicit_urls]
        else:
            credentials = CredentialService(CredentialRepository(db)).resolve(SourceType.YOUTUBE)
            connector = YouTubeConnector(cc_only=yt_config.get("cc_only", True))
            page = connector.discover(
                job.query, cursor=None, limit=yt_config.get("max_videos", 5),
                api_key=credentials.get("YOUTUBE_API_KEY", ""),
            )
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
                task_uuid, JobSourceTaskStatus.COMPLETED_PARTIAL, error_message="No YouTube videos found for query"
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

        web_config = job.config.get("web", {})
        connector = WebScrapeConnector(safesearch=web_config.get("safesearch", "moderate"))
        page = connector.discover(job.query, cursor=None, limit=web_config.get("max_results", 200))
        shards = page.items

        if not shards:
            task_repo.set_status(
                task_uuid, JobSourceTaskStatus.COMPLETED_PARTIAL, error_message="No web images found for query"
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
