"""Celery application: queues, routing, retry defaults, and the Beat schedule
for the watchdog (stalled-task resume, scheduled-job kickoff, deadline sweep).
"""

from celery import Celery
from celery.signals import task_postrun, task_prerun

from app.core.config import get_settings
from app.core.logging import configure_logging

settings = get_settings()
configure_logging()

celery_app = Celery(
    "data_automation",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "app.tasks.discovery_tasks",
        "app.tasks.ingest_tasks",
        "app.tasks.standardize_tasks",
        "app.tasks.cleanup_tasks",
        "app.tasks.orchestration_tasks",
        "app.tasks.filtering_tasks",
        "app.tasks.export_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=settings.celery_task_soft_time_limit_s,
    task_time_limit=settings.celery_task_time_limit_s,
    task_routes={
        "app.tasks.discovery_tasks.*": {"queue": "discovery"},
        "app.tasks.ingest_tasks.*": {"queue": "download"},
        "app.tasks.standardize_tasks.*": {"queue": "standardize"},
        "app.tasks.cleanup_tasks.*": {"queue": "cleanup"},
        "app.tasks.orchestration_tasks.*": {"queue": "orchestration"},
        "app.tasks.filtering_tasks.*": {"queue": "filtering"},
        "app.tasks.export_tasks.*": {"queue": "export"},
    },
    beat_schedule={
        "watchdog-scan": {
            "task": "app.tasks.orchestration_tasks.watchdog_scan",
            "schedule": 60.0,
        },
    },
)


@task_prerun.connect
def _track_active_task(task_id=None, task=None, args=None, kwargs=None, **_):
    """Records this task's id under the job's active-task set so cancel() can
    revoke(terminate=True) it if it's already executing, not just queued."""
    job_id = (kwargs or {}).get("job_id") or (args[0] if args else None)
    if not job_id:
        return
    from app.core.redis_client import get_redis
    from app.services.cancellation_service import CancellationService

    CancellationService(get_redis()).register_active_task(str(job_id), task_id)


@task_postrun.connect
def _untrack_active_task(task_id=None, task=None, args=None, kwargs=None, **_):
    job_id = (kwargs or {}).get("job_id") or (args[0] if args else None)
    if not job_id:
        return
    from app.core.redis_client import get_redis
    from app.services.cancellation_service import CancellationService

    CancellationService(get_redis()).unregister_active_task(str(job_id), task_id)
