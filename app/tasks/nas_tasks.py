"""NAS storage tasks — move filtered images off the local disk onto the NAS.

Pipeline position: downloads and filtering still work on local staging files (the
relevance model and CLIP need real files, and rejected images should never cost
NAS traffic). Offload is triggered once per job, from plan_topup_round once the
job's kept set is final — every top-up round has run, dedup has settled, and any
surplus past target_count has been trimmed — rather than per relevance-filter pass,
so an asset that a later round would have deduped or trimmed away never makes a
round trip to NAS first. At that point offload_asset_batch uploads each
RELEVANCE_PASSED asset to the NAS, verifies the copy, repoints Asset.nas_path at
"nas://...", and only then removes the local file. Nothing here can fail a job: if
the NAS is unreachable the image simply stays local and is retried, later, by the
sweeper.

Assets are offloaded in batches over one shared SFTP/SSH connection rather than
one task + one fresh connection per asset — a weak NAS can crash under a storm
of back-to-back connection churn even though each one individually honors the
nas_sftp_max_concurrency cap (see base.limited_concurrency)."""

import inspect
import json
import logging
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import or_, select

from app.connectors.nas.base import NasClient, join_relpath, open_client
from app.core.config import get_settings
from app.core.exceptions import ConnectorError, FatalConnectorError, TransientConnectorError
from app.core.redis_client import get_redis
from app.core.slugify import job_folder_name
from app.db.models.asset import Asset
from app.db.models.job import Job
from app.db.repositories.asset_repository import AssetRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus
from app.services.nas_config_service import get_active_storage_params, is_storage_active
from app.storage import asset_files
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

# Extended lock TTL to ensure batches up to 2000 images have ample time without lock expiration.
_LOCK_TTL_S = 3600
_SWEEP_BATCH = 2000
MAX_NAS_OFFLOAD_BATCH_SIZE = 2000

# Set for nas_offload_cooldown_s after a connect/transient failure, so a NAS
# that just crashed and came back isn't immediately hit by every worker's
# independent retry at once (see offload_asset_batch).
_COOLDOWN_KEY = "nas_offload:cooldown"


def _cooldown_active(redis) -> bool:
    return bool(redis.get(_COOLDOWN_KEY))


def _start_cooldown(redis) -> None:
    redis.set(_COOLDOWN_KEY, "1", ex=get_settings().nas_offload_cooldown_s)


def _remote_rel_for(storage_path: str, local_file: Path, job_folder: str | None = None) -> str:
    """<storage_path>/<job>/<source>/{images|annotations}/<name> — the same
    layout the local staging area uses, so a job's files stay together.

    The local layout's leading segment is always the job's UUID; on the NAS it is
    replaced with `job_folder` (a human-readable, still-unique folder name derived
    from the job's name) when the caller has one, so the job stays easy to find."""
    settings = get_settings()
    try:
        rel = local_file.resolve().relative_to(settings.nas_root.resolve()).as_posix()
    except ValueError:
        rel = "/".join(local_file.parts[-4:])
    if job_folder is not None:
        _job_id_segment, _, remainder = rel.partition("/")
        if remainder:
            rel = f"{job_folder}/{remainder}"
    return join_relpath(storage_path, rel)


def _job_folder_for_asset(db, job_id: uuid.UUID, cache: dict[uuid.UUID, str] | None = None) -> str:
    if cache is not None and job_id in cache:
        return cache[job_id]
    job_name = db.execute(select(Job.name).where(Job.id == job_id)).scalar_one_or_none()
    folder = job_folder_name(job_name, job_id)
    if cache is not None:
        cache[job_id] = folder
    return folder


def _prefetch_job_folders(db, asset_ids: list[uuid.UUID]) -> dict[uuid.UUID, str]:
    """One query for every job touched by this batch instead of one per asset
    — most of a batch comes from the same filter pass on the same job."""
    if not asset_ids:
        return {}
    job_ids = {
        row[0]
        for row in db.execute(select(Asset.job_id).where(Asset.id.in_(asset_ids)).distinct()).all()
    }
    if not job_ids:
        return {}
    names = dict(db.execute(select(Job.id, Job.name).where(Job.id.in_(job_ids))).all())
    return {job_id: job_folder_name(names.get(job_id), job_id) for job_id in job_ids}


def _prefetch_job_storage_paths(db, asset_ids: list[uuid.UUID]) -> dict[uuid.UUID, str | None]:
    if not asset_ids or not hasattr(db, "execute"):
        return {}
    job_ids = {
        row[0]
        for row in db.execute(select(Asset.job_id).where(Asset.id.in_(asset_ids)).distinct()).all()
    }
    if not job_ids:
        return {}
    rows = db.execute(select(Job.id, Job.config).where(Job.id.in_(job_ids))).all()
    res: dict[uuid.UUID, str | None] = {}
    for jid, cfg in rows:
        if isinstance(cfg, dict):
            val = (cfg.get("nas_storage_path") or cfg.get("nas_folder") or "").strip()
            res[jid] = val if val else ""
        else:
            res[jid] = ""
    return res


def _upload_verified(client: NasClient, local_file: Path, rel: str) -> None:
    client.upload(local_file, rel)
    remote_size = client.size(rel)
    if remote_size != local_file.stat().st_size:
        # Never repoint the asset at a copy we could not verify.
        raise TransientConnectorError(f"NAS copy of {local_file.name} did not verify (size mismatch)")


def _discard_remote(client: NasClient, rels: list[str]) -> None:
    for rel in rels:
        try:
            client.delete(rel)
        except Exception as exc:  # best effort: the connection may be what failed
            logger.warning(f"Could not discard partial NAS copy {rel}: {exc}")


def _still_kept(asset_uuid: uuid.UUID, remote_image: str) -> bool:
    with session_scope() as db:
        current = AssetRepository(db).get(asset_uuid)
        return (
            current is not None
            and current.status == AssetStatus.RELEVANCE_PASSED
            and current.nas_path == remote_image
        )


@celery_app.task(
    name="app.tasks.nas_tasks.offload_asset_batch",
    bind=True,
    autoretry_for=(TransientConnectorError,),
    retry_backoff=True,
    retry_backoff_max=900,
    retry_jitter=True,
    max_retries=6,
)
def offload_asset_batch(self, asset_ids: list[str]) -> dict:
    """Uploads a whole chunk of assets over one SFTP/SSH connection instead of
    reconnecting per asset. Each asset still goes through its own in-flight
    lock so the sweeper and the filter can't double-upload the same one.

    A connect/transient failure starts a cooldown (see _start_cooldown) and
    re-raises so Celery retries this whole batch with backoff; assets already
    offloaded earlier in the batch are idempotently skipped on retry (their
    nas_path is already a nas:// path by then). A fatal failure (bad login)
    is not retried — the images stay local and the sweeper picks them up once
    the configuration is fixed, same as before.
    """
    if not asset_ids:
        return {}
    redis = get_redis()
    if _cooldown_active(redis):
        return {"skipped_cooldown": len(asset_ids)}

    params = get_active_storage_params()
    if params is None:
        return {"no_nas_storage": len(asset_ids)}

    # Bulk-prefetch everything needed before opening the connection: the job
    # folder name is looked up once per *job*, not once per asset (a batch is
    # usually one filter chunk from one job), and the Celery/filter-sided
    # locks are all acquired in one Redis round-trip instead of one per asset
    # while the SFTP connection sits open and idle for each.
    with session_scope() as db:
        job_folder_cache = _prefetch_job_folders(db, [uuid.UUID(a) for a in asset_ids])
        storage_path_cache = _prefetch_job_storage_paths(db, [uuid.UUID(a) for a in asset_ids])

    pipe = redis.pipeline()
    for asset_id in asset_ids:
        pipe.set(f"nas_offload:{asset_id}", "1", nx=True, ex=_LOCK_TTL_S)
    lock_acquired = pipe.execute()

    settings = get_settings()
    results: dict[str, str] = {}
    runnable = [
        asset_id for asset_id, got in zip(asset_ids, lock_acquired, strict=True) if got
    ]
    for asset_id, got in zip(asset_ids, lock_acquired, strict=True):
        if not got:
            results[asset_id] = "in_flight"
    if not runnable:
        return results

    logger.info(f"[NAS OFFLOAD] Starting batch upload for {len(runnable)} asset(s) over single connection...")
    offload_params = inspect.signature(_offload).parameters
    try:
        with open_client(params) as client:
            for i, asset_id in enumerate(runnable):
                try:
                    kwargs = {}
                    if "storage_path_cache" in offload_params:
                        kwargs["storage_path_cache"] = storage_path_cache
                    results[asset_id] = _offload(
                        uuid.UUID(asset_id),
                        client=client,
                        params=params,
                        job_folder_cache=job_folder_cache,
                        **kwargs,
                    )
                finally:
                    redis.delete(f"nas_offload:{asset_id}")
                if settings.nas_sftp_upload_pacing_s and i < len(runnable) - 1:
                    time.sleep(settings.nas_sftp_upload_pacing_s)
        logger.info(
            f"[NAS OFFLOAD] Completed batch upload: {len(results)} asset(s) transferred over single connection."
        )
    except TransientConnectorError:
        # Keeps every not-yet-processed asset local; Celery retries the whole
        # batch with backoff, and the cooldown stops other batches piling on
        # in the meantime.
        _start_cooldown(redis)
        raise
    except FatalConnectorError as exc:
        # Bad login / missing share: retrying now cannot help. The images stay
        # local and the sweeper tries again once the user has fixed the config.
        logger.warning(f"NAS offload batch could not connect, keeping {len(asset_ids)} asset(s) local: {exc}")
        return {"failed_connect": len(asset_ids)}
    return results


def _offload(
    asset_uuid: uuid.UUID,
    client: NasClient | None = None,
    params=None,
    job_folder_cache: dict[uuid.UUID, str] | None = None,
    storage_path_cache: dict[uuid.UUID, str | None] | None = None,
    **kwargs,
) -> str:
    """Offloads one asset. Pass an already-open `client` to reuse a connection
    across a batch (see offload_asset_batch); omit it to open and close a
    connection for just this one asset (used directly by the sweeper/tests).
    `params`, `job_folder_cache` and `storage_path_cache` let a caller that
    already looked these up for the whole batch skip repeating that work per
    asset; all are re-fetched here when omitted."""
    if params is None:
        params = get_active_storage_params()
        if params is None:
            return "no_nas_storage"

    with session_scope() as db:
        asset = AssetRepository(db).get(asset_uuid)
        if (
            asset is None
            or asset.status != AssetStatus.RELEVANCE_PASSED
            or not asset.nas_path
            or asset_files.is_remote(asset.nas_path)
        ):
            return "skipped"
        image_local = Path(asset.nas_path)
        ann_local = (
            Path(asset.annotation_nas_path)
            if asset.annotation_nas_path and not asset_files.is_remote(asset.annotation_nas_path)
            else None
        )
        job_folder = _job_folder_for_asset(db, asset.job_id, cache=job_folder_cache)

        custom_storage = (storage_path_cache or {}).get(asset.job_id)
        if custom_storage is None and hasattr(db, "execute"):
            cfg = db.execute(select(Job.config).where(Job.id == asset.job_id)).scalar_one_or_none()
            if isinstance(cfg, dict):
                custom_storage = (cfg.get("nas_storage_path") or cfg.get("nas_folder") or "").strip()
            else:
                custom_storage = None

        # If custom_storage was explicitly found to be empty (""), the job has NO NAS path configured.
        # Images must not be stored to NAS; they stay local only and are exported locally.
        if custom_storage == "":
            logger.info(
                f"NAS offload skipped for asset {asset_uuid}: job {asset.job_id} has no NAS storage destination path configured. Image stays local."
            )
            return "skipped_no_nas_destination"

    if not image_local.exists():
        # Silent before: the asset is now unrecoverable (no local file, never
        # offloaded) and nothing else will ever flag it, so this must be loud.
        logger.warning(f"NAS offload of asset {asset_uuid} skipped: local file {image_local} is gone")
        return "local_missing"
    target_base = custom_storage.strip().strip("/") if custom_storage and custom_storage.strip() else params.storage_path
    image_rel = _remote_rel_for(target_base, image_local, job_folder)
    ann_rel = _remote_rel_for(target_base, ann_local, job_folder) if ann_local and ann_local.exists() else None

    attempted: list[str] = []

    def _upload_all(c: NasClient) -> None:
        try:
            attempted.append(image_rel)
            _upload_verified(c, image_local, image_rel)
            if ann_rel:
                attempted.append(ann_rel)
                _upload_verified(c, ann_local, ann_rel)
        except BaseException:
            # Never leave a half-written or unverified copy at its final path: if
            # this asset ends up rejected, nothing else knows the file exists.
            _discard_remote(c, attempted)
            raise

    try:
        if client is not None:
            _upload_all(client)
        else:
            with open_client(params) as c:
                _upload_all(c)
    except FileNotFoundError:
        # Rejected/trimmed/deleted mid-upload, and its local file went with it.
        return "local_missing"
    except TransientConnectorError:
        raise  # keeps the local file; Celery retries with backoff
    except FatalConnectorError as exc:
        # Bad login / missing share: retrying now cannot help. The image stays
        # local and the sweeper tries again once the user has fixed the config.
        logger.warning(f"NAS offload of asset {asset_uuid} failed, keeping it local: {exc}")
        return "failed"

    remote_image = asset_files.make_remote_path(image_rel)
    remote_ann = asset_files.make_remote_path(ann_rel) if ann_rel else None
    with session_scope() as db:
        # Row lock: a concurrent reject/trim must either finish before we look
        # or wait until we have committed.
        locked = db.execute(select(Asset).where(Asset.id == asset_uuid).with_for_update()).scalar_one_or_none()
        if locked is None or locked.status != AssetStatus.RELEVANCE_PASSED or locked.nas_path != str(image_local):
            # Rejected/trimmed/deleted while we were uploading: undo our copy.
            asset_files.delete(remote_image)
            if remote_ann:
                asset_files.delete(remote_ann)
            return "raced"
        locked.nas_path = remote_image
        if remote_ann:
            locked.annotation_nas_path = remote_ann

    # A trim/dedup that read this asset before we committed still holds its old local
    # path, so it deleted nothing on the NAS. Look again now that our copy is visible.
    if not _still_kept(asset_uuid, remote_image):
        asset_files.delete(remote_image)
        if remote_ann:
            asset_files.delete(remote_ann)
        for local_file in (image_local, ann_local):
            if local_file is not None:
                local_file.unlink(missing_ok=True)
        return "raced_after_commit"

    # Committed and still wanted — only now is it safe to free the local disk.
    for local_file in (image_local, ann_local):
        if local_file is not None:
            try:
                local_file.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning(f"Could not remove local copy {local_file}: {exc}")
    thumb = image_local.with_name(f"{image_local.stem}.thumb.jpg")
    thumb.unlink(missing_ok=True)
    return "offloaded"


@celery_app.task(name="app.tasks.nas_tasks.sweep_unoffloaded_assets")
def sweep_unoffloaded_assets() -> int:
    """Safety net (beat, every few minutes): offload kept images that are still
    local — because the NAS was down when they passed, or because they predate the
    NAS being connected. Bounded per run so connecting a NAS to a large local
    library migrates it gradually instead of in one burst."""
    if not is_storage_active():
        return 0
    if _cooldown_active(get_redis()):
        return 0
    with session_scope() as db:
        ids = [
            str(i)
            for i in db.execute(
                select(Asset.id)
                .where(
                    Asset.status == AssetStatus.RELEVANCE_PASSED,
                    Asset.nas_path.is_not(None),
                    ~Asset.nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                )
                .limit(_SWEEP_BATCH)
            ).scalars()
        ]
    for i in range(0, len(ids), MAX_NAS_OFFLOAD_BATCH_SIZE):
        offload_asset_batch.delay(ids[i : i + MAX_NAS_OFFLOAD_BATCH_SIZE])
    return len(ids)


_NOT_KEPT = (AssetStatus.RELEVANCE_REJECTED, AssetStatus.DUPLICATE, AssetStatus.FAILED)
_RECONCILE_BATCH = 100


@celery_app.task(name="app.tasks.nas_tasks.sweep_leaked_nas_files")
def sweep_leaked_nas_files() -> int:
    """Safety net: delete the NAS copy of any image that is no longer part of the
    kept set (rejected, duplicate, trimmed). The normal paths already do this, but
    an upload that finishes at the same moment the image is rejected can slip past
    them; this makes "the NAS holds only the kept images" true regardless of timing.
    A file that could not be removed (NAS busy/offline) is left for the next run."""
    with session_scope() as db:
        rows = db.execute(
            select(Asset.id, Asset.nas_path, Asset.annotation_nas_path)
            .where(
                Asset.status.in_(_NOT_KEPT),
                or_(
                    Asset.nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                    Asset.annotation_nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                ),
            )
            .limit(_RECONCILE_BATCH)
        ).all()
    cleaned = 0
    for asset_id, image_path, annotation_path in rows:
        try:
            for path in (image_path, annotation_path):
                if asset_files.is_remote(path):
                    asset_files.delete(path, strict=True)
        except (asset_files.NasUnreachable, ConnectorError) as exc:
            logger.warning(f"NAS cleanup of asset {asset_id} deferred: {exc}")
            continue
        with session_scope() as db:
            asset = AssetRepository(db).get(asset_id)
            # Only clear paths that are still the ones we just deleted, and only while
            # the asset is still not kept (a re-judged asset must keep its files).
            if asset is not None and asset.status in _NOT_KEPT:
                if asset.nas_path == image_path:
                    asset.nas_path = None
                if asset.annotation_nas_path == annotation_path:
                    asset.annotation_nas_path = None
        cleaned += 1
    return cleaned


def schedule_offload(asset_ids: list[str]) -> None:
    """Called by the relevance filter after PASS verdicts are committed. Never
    raises: filtering must not be affected by anything NAS-related."""
    if not asset_ids:
        return
    try:
        if not is_storage_active():
            return
        if _cooldown_active(get_redis()):
            return
        for i in range(0, len(asset_ids), MAX_NAS_OFFLOAD_BATCH_SIZE):
            offload_asset_batch.delay(asset_ids[i : i + MAX_NAS_OFFLOAD_BATCH_SIZE])
    except Exception:
        logger.exception("Could not schedule NAS offload; the sweeper will pick these up")


def schedule_offload_for_job(job_id: str) -> None:
    """Called by finalize_relevance_filter once ALL batches for a job are done.

    Drains the Redis list of accumulated RELEVANCE_PASSED asset IDs that
    relevance_filter_batch collected during the pass.

    Upload strategy requested:
    - If total relevant assets <= 2000 (e.g. 1000 assets): upload all in 1 single batch
      over 1 connection request.
    - If total relevant assets > 2000: divide into batches of 2000 images per connection
      request for smooth transaction.
    """
    try:
        if not is_storage_active():
            return

        # If the job has no NAS storage destination path configured, do not offload to NAS.
        # Images stay in local storage for local export.
        with session_scope() as db:
            if hasattr(db, "execute"):
                try:
                    job_uuid = uuid.UUID(str(job_id))
                    job = db.execute(select(Job).where(Job.id == job_uuid)).scalar_one_or_none()
                    if job is not None and isinstance(job.config, dict):
                        nas_path = (job.config.get("nas_storage_path") or job.config.get("nas_folder") or "").strip()
                        if not nas_path:
                            logger.info(
                                "Job %s has no NAS storage destination path configured; skipping NAS offload (files remain local for export).",
                                job_id,
                            )
                            redis = get_redis()
                            redis.delete(f"job:{job_id}:offload_pending")
                            return
                except (ValueError, TypeError):
                    pass
        redis = get_redis()
        raw = redis.lrange(f"job:{job_id}:offload_pending", 0, -1)
        redis.delete(f"job:{job_id}:offload_pending")
        if not raw:
            return
        # Decode bytes and deduplicate while preserving insertion order.
        seen: set[str] = set()
        asset_ids: list[str] = []
        for item in raw:
            s = item.decode() if isinstance(item, bytes) else item
            if s not in seen:
                seen.add(s)
                asset_ids.append(s)
        if not asset_ids:
            return
        if _cooldown_active(redis):
            logger.info(
                "NAS offload for job %s deferred (cooldown active); sweeper will retry %d asset(s)",
                job_id,
                len(asset_ids),
            )
            return

        total_assets = len(asset_ids)
        num_chunks = -(-total_assets // MAX_NAS_OFFLOAD_BATCH_SIZE)
        logger.info(
            f"[NAS OFFLOAD] Job {job_id}: Scheduling {total_assets} relevant image(s) for NAS offload. "
            f"Strategy: batches of up to {MAX_NAS_OFFLOAD_BATCH_SIZE} per connection request -> {num_chunks} batch(es)."
        )

        dispatched = 0
        for i in range(0, total_assets, MAX_NAS_OFFLOAD_BATCH_SIZE):
            batch = asset_ids[i : i + MAX_NAS_OFFLOAD_BATCH_SIZE]
            offload_asset_batch.delay(batch)
            dispatched += len(batch)
        logger.info(
            f"[NAS OFFLOAD] Job {job_id}: Dispatched all {dispatched} asset(s) across {num_chunks} connection request batch(es)."
        )
    except Exception:
        logger.exception(
            "Could not schedule NAS offload for job %s; sweeper will pick these up", job_id
        )

@celery_app.task(
    name="app.tasks.nas_tasks.process_nas_upload_queue",
    bind=True,
    autoretry_for=(TransientConnectorError,),
    retry_backoff=True,
    retry_backoff_max=900,
    retry_jitter=True,
    max_retries=5,
)
def process_nas_upload_queue(self, max_transfers: int = 10) -> dict:
    """Process durable NAS transfers under one global Redis lock."""

    from app.storage.nas_writer import (
        NasBatchWriter,
        NasWriterLock,
        dequeue_nas_batch,
        is_cooldown_active,
    )

    if not is_storage_active():
        return {"status": "skipped", "reason": "no_active_storage"}

    if is_cooldown_active():
        return {"status": "skipped", "reason": "cooldown_active"}

    lock = NasWriterLock(
        ttl_s=300,
        heartbeat_interval_s=60,
        allow_local_fallback=False,
    )

    if not lock.acquire(timeout_s=0):
        return {"status": "skipped", "reason": "concurrency_lock_held"}

    results = []

    try:
        params = get_active_storage_params()
        if params is None:
            return {"status": "skipped", "reason": "no_active_storage"}

        limit = max(1, min(int(max_transfers), 10))

        # One connection reused for sequential transfers.
        with open_client(params) as client:
            for _ in range(limit):

                # Redis heartbeat also verifies current lock ownership.
                if not lock.extend(300):
                    raise TransientConnectorError(
                        "NAS writer lock lease lost; stopping transfer processing."
                    )

                with session_scope() as db:
                    transfer_id = dequeue_nas_batch(db)

                    if transfer_id is None:
                        break

                    writer = NasBatchWriter(
                        db,
                        params=params,
                        lock=lock,
                    )

                    result = writer.upload_batch(
                        transfer_id,
                        client=client,
                    )

                results.append({
                    "transfer_id": str(transfer_id),
                    "result": result,
                })

        return {
            "status": "completed",
            "processed_batches": len(results),
        }

    finally:
        lock.release()


@celery_app.task(
    name="app.tasks.nas_tasks.reconcile_nas_transfers"
)
def reconcile_nas_transfers() -> dict:
    """Discover actionable NAS transfers and dispatch the central queue."""

    # Resolve dynamically so configuration changes are reflected.
    from app.services.nas_config_service import is_storage_active as storage_active
    from app.db.repositories.image_batch_repository import NasTransferBatchRepository

    if not storage_active():
        return {"status": "skipped", "reason": "no_active_storage"}

    with session_scope() as db:
        repo = NasTransferBatchRepository(db)
        actionable = repo.list_actionable(limit=50)
        pending_cleanups = repo.list_pending_cleanups(limit=50)

    if actionable:
        process_nas_upload_queue.delay()

        return {
            "status": "dispatched",
            "actionable_count": len(actionable),
            "cleaned_count": 0,
        }

    return {
        "status": "idle",
        "actionable_count": 0,
        "cleaned_count": 0,
    }


def _ensure_dir(client: NasClient, rel_dir: str) -> None:
    if hasattr(client, "_mkdirs"):
        client._mkdirs(rel_dir)
    elif hasattr(client, "_abs"):
        p = client._abs(rel_dir)
        if hasattr(p, "mkdir"):
            p.mkdir(parents=True, exist_ok=True)


def _rmdir_quiet(client: NasClient, rel_dir: str) -> None:
    try:
        if hasattr(client, "_sftp"):
            client._sftp.rmdir(client._abs(rel_dir))
        elif hasattr(client, "_client") and hasattr(client._client, "_sftp"):
            client._client._sftp.rmdir(client._client._abs(rel_dir))
        elif hasattr(client, "_abs"):
            p = client._abs(rel_dir)
            if hasattr(p, "rmdir"):
                p.rmdir()
    except Exception:
        pass


@celery_app.task(name="app.tasks.nas_tasks.consolidate_job_dataset_on_nas")
def consolidate_job_dataset_on_nas(job_id: str) -> dict:
    """Consolidates distributed batches and windows on the remote NAS into a
    unified, production-ready dataset layout:
      <storage_path>/<job_folder>/images/<file>
      <storage_path>/<job_folder>/annotations/<file>
      <storage_path>/<job_folder>/manifest.json

    Fast-renames remote files server-side without re-uploading bytes,
    cleans up empty intermediate directories, builds a master manifest,
    and updates all Asset.nas_path and Asset.annotation_nas_path records in PostgreSQL.
    """
    with session_scope() as db:
        try:
            job_uuid = uuid.UUID(str(job_id))
        except (ValueError, TypeError):
            return {"status": "skipped", "reason": "invalid_job_id"}

        job = db.get(Job, job_uuid)
        if job is None:
            return {"status": "skipped", "reason": "job_not_found"}

        cfg = job.config if isinstance(job.config, dict) else {}
        storage_path = (cfg.get("nas_storage_path") or cfg.get("nas_folder") or "").strip()
        if not storage_path:
            return {"status": "skipped", "reason": "no_nas_storage_path"}

        job_folder = _job_folder_for_asset(db, job_uuid)
        job_name = job.name

    if not is_storage_active():
        return {"status": "skipped", "reason": "storage_inactive"}

    params = get_active_storage_params()
    if params is None:
        return {"status": "skipped", "reason": "no_active_storage"}

    remote_job_dir = join_relpath(storage_path, job_folder)
    images_dir = f"{remote_job_dir}/images"
    annotations_dir = f"{remote_job_dir}/annotations"

    moved_images = 0
    moved_annotations = 0
    deleted_manifests = 0
    subdirs_to_clean = set()

    with open_client(params) as client:
        _ensure_dir(client, images_dir)

        all_entries = list(client.walk(remote_job_dir))

        for entry in all_entries:
            path = entry.path
            parts = path.split("/")
            filename = parts[-1]

            # Skip top-level manifest or already consolidated files
            if path == f"{remote_job_dir}/manifest.json":
                continue
            if path.startswith(f"{images_dir}/") or path.startswith(f"{annotations_dir}/"):
                continue

            # Remove intermediate batch or window manifest files
            if filename == "manifest.json":
                try:
                    client.delete(path)
                    deleted_manifests += 1
                except Exception as e:
                    logger.warning(f"[CONSOLIDATE] Could not delete batch manifest {path}: {e}")
                continue

            # Move image files into unified images/ directory
            if "/images/" in path or any(filename.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                target_rel = f"{images_dir}/{filename}"
                if target_rel != path:
                    try:
                        client._rename(path, target_rel)
                        moved_images += 1
                    except Exception as e:
                        logger.warning(f"[CONSOLIDATE] Failed to move image {path} -> {target_rel}: {e}")

            # Move annotation files into unified annotations/ directory
            elif "/annotations/" in path or any(filename.lower().endswith(ext) for ext in [".json", ".xml", ".txt"]):
                _ensure_dir(client, annotations_dir)
                target_rel = f"{annotations_dir}/{filename}"
                if target_rel != path:
                    try:
                        client._rename(path, target_rel)
                        moved_annotations += 1
                    except Exception as e:
                        logger.warning(f"[CONSOLIDATE] Failed to move annotation {path} -> {target_rel}: {e}")

        # Recursively remove all intermediate staging directories (e.g. batch_*, window_*)
        try:
            top_entries = client.list_dir(remote_job_dir)
            for top_entry in top_entries:
                if not top_entry.is_dir:
                    continue
                # Preserve the consolidated images directory
                if top_entry.name == "images":
                    continue
                # If annotations directory exists but has no annotations, purge it
                if top_entry.name == "annotations":
                    ann_files = list(client.walk(annotations_dir))
                    if not ann_files:
                        try:
                            client.rmtree(annotations_dir)
                        except Exception as e:
                            logger.warning(f"[CONSOLIDATE] Could not remove empty annotations dir: {e}")
                    continue
                # Remove any intermediate batch or window directory
                staging_subdir = f"{remote_job_dir}/{top_entry.name}"
                try:
                    client.rmtree(staging_subdir)
                except Exception as e:
                    logger.warning(f"[CONSOLIDATE] Could not rmtree staging subdir {staging_subdir}: {e}")
        except Exception as e:
            logger.warning(f"[CONSOLIDATE] Error while purging staging subdirs: {e}")

        # Re-walk final images and annotations
        final_images = list(client.walk(images_dir))
        final_annots = list(client.walk(annotations_dir)) if client.exists(annotations_dir) else []

        # Write master manifest
        structure_dict = {
            "images_dir": "images/",
            "manifest": "manifest.json",
        }
        if final_annots:
            structure_dict["annotations_dir"] = "annotations/"

        manifest_payload = {
            "job_id": str(job_id),
            "job_name": job_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_images": len(final_images),
            "total_annotations": len(final_annots),
            "structure": structure_dict,
            "images": [
                {
                    "filename": Path(e.path).name,
                    "rel_path": f"images/{Path(e.path).name}",
                    "size": e.size,
                }
                for e in sorted(final_images, key=lambda x: x.path)
            ],
            "annotations": [
                {
                    "filename": Path(e.path).name,
                    "rel_path": f"annotations/{Path(e.path).name}",
                    "size": e.size,
                }
                for e in sorted(final_annots, key=lambda x: x.path)
            ],
        }

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
            json.dump(manifest_payload, tf, indent=2)
            tmp_local_path = Path(tf.name)

        try:
            client.upload(tmp_local_path, f"{remote_job_dir}/manifest.json")
        finally:
            tmp_local_path.unlink(missing_ok=True)

    # Update PostgreSQL Asset records
    with session_scope() as db:
        stmt = select(Asset).where(
            Asset.job_id == job_uuid,
            Asset.status.in_([AssetStatus.COMPLETED, AssetStatus.NAS_VERIFIED, AssetStatus.RELEVANCE_PASSED]),
        )
        updated_assets = 0
        image_names_set = {Path(e.path).name for e in final_images}
        annot_names_set = {Path(e.path).name for e in final_annots}

        for asset in db.execute(stmt).scalars():
            asset_fn = f"{asset.id}.jpg"
            if asset.nas_path:
                curr_fn = Path(asset.nas_path).name
                if curr_fn in image_names_set:
                    asset_fn = curr_fn
            if asset_fn in image_names_set:
                asset.nas_path = asset_files.make_remote_path(f"{images_dir}/{asset_fn}")
                annot_fn = f"{Path(asset_fn).stem}.json"
                if annot_fn in annot_names_set:
                    asset.annotation_nas_path = asset_files.make_remote_path(f"{annotations_dir}/{annot_fn}")
                asset.status = AssetStatus.COMPLETED
                updated_assets += 1

        db.commit()

    logger.info(
        f"[CONSOLIDATE] Job {job_id} dataset consolidated on NAS at {remote_job_dir}: "
        f"{len(final_images)} images, {len(final_annots)} annotations, {updated_assets} DB assets updated."
    )
    return {
        "status": "completed",
        "remote_job_dir": remote_job_dir,
        "total_images": len(final_images),
        "total_annotations": len(final_annots),
        "moved_images": moved_images,
        "moved_annotations": moved_annotations,
        "deleted_manifests": deleted_manifests,
        "updated_assets": updated_assets,
    }
