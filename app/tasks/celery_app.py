"""Celery application: queues, routing, retry defaults, and the Beat schedule
for the watchdog (stalled-task resume, scheduled-job kickoff, deadline sweep).
"""

import logging

from celery import Celery
from celery.signals import task_postrun, task_prerun, worker_process_init, worker_ready

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
        "app.tasks.nas_tasks",
        "app.tasks.batch_orchestration",
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
        "app.tasks.batch_orchestration.*": {"queue": "orchestration"},
        # Reuses the "standardize" queue (already consumed by every IO worker
        # deployment, no server-side queue-list change needed). A dedicated
        # "nas_offload" queue removed the risk of 8 IO-worker processes
        # busy-polling for the NAS's 1-2 connection slots at once, but every
        # deployment needs its own worker command updated to consume it — not
        # worth that operational cost here. The connection-slot wait itself is
        # now bounded (SLOT_WAIT_TIMEOUT_S in app/connectors/nas/base.py)
        # instead of hanging forever, so busy-polling fails loudly rather than
        # silently if it ever becomes a real problem again.
        "app.tasks.nas_tasks.*": {"queue": "standardize"},
    },
    beat_schedule={
        "watchdog-scan": {
            "task": "app.tasks.orchestration_tasks.watchdog_scan",
            "schedule": 60.0,
        },
        # Reclaims scratch directories whose task finished (or whose job was
        # deleted) and that nothing will come back for. cleanup_local is now
        # called on the normal path too; this is the safety net for anything
        # that path missed, including everything stranded while cleanup_local
        # had no call site at all. Six-hourly because the directories it
        # targets are already hours old by definition.
        # Recurring job templates. Separate from watchdog-scan so a slow or
        # failing sweep of one cannot delay the other.
        "scan-schedules": {
            "task": "app.tasks.orchestration_tasks.scan_schedules",
            "schedule": 60.0,
        },
        "purge-orphan-work-dirs": {
            "task": "app.tasks.cleanup_tasks.purge_orphan_work_dirs",
            "schedule": 300.0,
        },
        # Removes the NAS copy of images that stopped being kept (see nas_tasks).
        "nas-leak-sweep": {
            "task": "app.tasks.nas_tasks.sweep_leaked_nas_files",
            "schedule": 60.0,
        },
        # Centralized NAS upload queue reconciliation: sweeps actionable transfers
        # (pending, stale uploading, retryable failed) and pending staging cleanups.
        "nas-reconcile-transfers": {
            "task": "app.tasks.nas_tasks.reconcile_nas_transfers",
            "schedule": 60.0,
        },
        # End-to-end batch lifecycle reconciliation: sweeps counter drift, resumes paused batches,
        # and re-enqueues orphan ready batches.
        "batch-lifecycle-reconcile": {
            "task": "app.tasks.batch_orchestration.reconcile_batch_lifecycle",
            "schedule": 60.0,
        },
    },
)


@worker_ready.connect
def _record_boot_for_restart_recovery(**_):
    """Note when this worker came up, so the watchdog can tell work a restart
    orphaned from work that is merely slow (see app/domain/recovery.py).

    Never allowed to stop a worker starting: without the marker recovery just
    falls back to the watchdog's ordinary stall timers.
    """
    try:
        from app.core.redis_client import get_redis
        from app.services.worker_boot import record_worker_boot

        record_worker_boot(get_redis())
    except Exception:
        logging.getLogger(__name__).exception("Could not record worker boot time")


@worker_process_init.connect
def _reset_db_connections_after_fork(**_):
    """Give each prefork child its own Postgres connections.

    Without this, every child inherits the parent's pooled sockets and two
    processes end up multiplexing one connection, producing
    "another command is already in progress". See
    app/db/session.dispose_engine_for_fork for the full explanation.
    """
    from app.db.session import dispose_engine_for_fork

    dispose_engine_for_fork()


@worker_process_init.connect
def _cap_torch_threads(**_):
    """Stop each child from grabbing every core.

    torch/OpenMP default their intra-op pool to the machine's CPU count. On
    this host (64 cores) a worker at --concurrency=12 would spawn ~768
    compute threads across children, so the box spends its time context
    switching instead of doing inference -- the measured effect was
    relevance_filter_batch taking up to 3320s for 50 images. Capping per
    child keeps the total in line with the core count.
    """
    import os

    threads = os.environ.get("TORCH_NUM_THREADS")
    if not threads:
        return
    os.environ.setdefault("OMP_NUM_THREADS", threads)
    os.environ.setdefault("MKL_NUM_THREADS", threads)
    try:
        import torch

        torch.set_num_threads(int(threads))
    except Exception:  # torch absent (api image) or bad value -- non-fatal
        pass


@worker_process_init.connect
def _cap_gpu_memory(**_):
    """Bound this process's share of the GPU.

    This host also runs unrelated production containers. Without a cap, a
    filtering child can allocate the whole card (PyTorch's caching allocator
    holds freed blocks rather than returning them to the driver), and the
    next process to need VRAM -- ours or someone else's -- fails with an
    out-of-memory error. set_per_process_memory_fraction makes torch itself
    refuse to grow past the limit, so the failure is contained to this
    worker instead of taking down a neighbour.
    """
    import os

    fraction = os.environ.get("TORCH_CUDA_MEMORY_FRACTION")
    if not fraction:
        return
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.set_per_process_memory_fraction(float(fraction), 0)
    except Exception:  # no GPU in this container, or a bad value -- non-fatal
        pass


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
