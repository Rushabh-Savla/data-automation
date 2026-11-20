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

from sqlalchemy import or_, select, update

from app.connectors.nas.base import NasClient, NasConnectionError, join_relpath, open_client
from app.core.config import get_settings
from app.core.exceptions import ConnectorError, FatalConnectorError, TransientConnectorError
from app.core.redis_client import get_redis
from app.core.slugify import job_folder_name
from app.db.models.asset import Asset
from app.db.models.job import Job
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.job_repository import JobRepository
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
    cooldown = getattr(get_settings(), "nas_offload_cooldown_s", 60)
    try:
        redis.set(_COOLDOWN_KEY, "1", ex=cooldown)
    except Exception:
        pass


def _remote_rel_for(storage_path: str, local_file: Path, job_folder: str | None = None) -> str:
    """<storage_path>/<job>/<source>/{images|annotations}/<name> — the same
    layout the local staging area uses, so a job's files stay together.

    The local layout's leading segment is always the job's UUID; on the NAS it is
    replaced with `job_folder` (a human-readable, still-unique folder name derived
    from the job's name) when the caller has one, so the job stays easy to find."""
    settings = get_settings()
    resolved = local_file.resolve()
    rel = None
    try:
        rel = resolved.relative_to(settings.nas_root.resolve()).as_posix()
    except ValueError:
        pass
    if rel is None:
        try:
            rel = resolved.relative_to(settings.local_staging_path.resolve()).as_posix()
        except ValueError:
            pass
    if rel is None:
        parts = local_file.parts
        job_indices = [i for i, p in enumerate(parts) if p.startswith("job_")]
        if job_indices:
            rel = "/".join(parts[job_indices[-1]:])
        else:
            rel = "/".join(parts[-4:])
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
            and current.status in (
                AssetStatus.RELEVANCE_PASSED,
                AssetStatus.READY_FOR_UPLOAD,
                AssetStatus.UPLOADING,
                AssetStatus.NAS_VERIFIED,
                AssetStatus.COMPLETED,
            )
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
            or asset.status not in (AssetStatus.RELEVANCE_PASSED, AssetStatus.READY_FOR_UPLOAD)
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
        if (
            locked is None
            or locked.status not in (AssetStatus.RELEVANCE_PASSED, AssetStatus.READY_FOR_UPLOAD)
            or locked.nas_path != str(image_local)
        ):
            # Rejected/trimmed/deleted while we were uploading: undo our copy.
            asset_files.delete(remote_image)
            if remote_ann:
                asset_files.delete(remote_ann)
            return "raced"
        locked.nas_path = remote_image
        if remote_ann:
            locked.annotation_nas_path = remote_ann
        locked.status = AssetStatus.NAS_VERIFIED

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
                    Asset.status.in_([AssetStatus.RELEVANCE_PASSED, AssetStatus.READY_FOR_UPLOAD]),
                    Asset.nas_path.is_not(None),
                    ~Asset.nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                )
                .limit(_SWEEP_BATCH)
            ).scalars()
        ]
    for i in range(0, len(ids), MAX_NAS_OFFLOAD_BATCH_SIZE):
        offload_asset_batch.delay(ids[i : i + MAX_NAS_OFFLOAD_BATCH_SIZE])
    return len(ids)


_NOT_KEPT = (AssetStatus.RELEVANCE_REJECTED, AssetStatus.DUPLICATE, AssetStatus.FAILED, AssetStatus.SURPLUS)
_RECONCILE_BATCH = 100


@celery_app.task(name="app.tasks.nas_tasks.sweep_leaked_nas_files")
def sweep_leaked_nas_files() -> int:
    """Disabled: only send requests to NAS after a batch is complete."""
    return 0


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
        db_unoffloaded_ids: list[str] = []
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

                    # Query database directly for any unoffloaded accepted assets for this job.
                    # Exclude assets already associated with an ImageBatch (which are transferred
                    # by the authoritative NasTransferBatch mechanism).
                    stmt = select(Asset.id).where(
                        Asset.job_id == job_uuid,
                        Asset.status.in_([AssetStatus.RELEVANCE_PASSED, AssetStatus.READY_FOR_UPLOAD]),
                        Asset.nas_path.is_not(None),
                        ~Asset.nas_path.like(f"{asset_files.NAS_SCHEME}%"),
                        Asset.batch_id.is_(None),
                    )
                    db_unoffloaded_ids = [str(aid) for aid in db.execute(stmt).scalars().all()]
                except (ValueError, TypeError):
                    pass

        redis = get_redis()
        raw = redis.lrange(f"job:{job_id}:offload_pending", 0, -1)
        redis.delete(f"job:{job_id}:offload_pending")

        seen: set[str] = set()
        asset_ids: list[str] = []
        for item in (raw or []):
            s = item.decode() if isinstance(item, bytes) else item
            if s not in seen:
                seen.add(s)
                asset_ids.append(s)
        for aid in db_unoffloaded_ids:
            if aid not in seen:
                seen.add(aid)
                asset_ids.append(aid)

        # Exclude assets that already belong to an ImageBatch (handled by NasTransferBatch)
        if asset_ids:
            with session_scope() as db:
                if hasattr(db, "execute"):
                    try:
                        valid_uids = []
                        for aid in asset_ids:
                            try:
                                valid_uids.append(uuid.UUID(str(aid)))
                            except (ValueError, TypeError):
                                pass
                        if valid_uids:
                            batched_stmt = select(Asset.id).where(
                                Asset.id.in_(valid_uids),
                                Asset.batch_id.is_not(None),
                            )
                            batched_ids = {str(uid) for uid in db.execute(batched_stmt).scalars().all()}
                            asset_ids = [aid for aid in asset_ids if aid not in batched_ids]
                    except Exception:
                        pass

        if not asset_ids:
            return

        if _cooldown_active(redis):
            logger.info(
                "NAS offload for job %s deferred (cooldown active); %d asset(s) preserved for retry",
                job_id,
                len(asset_ids),
            )
            # Re-enqueue in redis as fallback
            redis.rpush(f"job:{job_id}:offload_pending", *asset_ids)
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
    default_retry_delay=20,
    retry_backoff_max=900,
    retry_jitter=True,
    max_retries=5,
)
def process_nas_upload_queue(self, max_transfers: int = 500) -> dict:
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

        limit = max(1, min(int(max_transfers), 500))

        # Check for pending transfers before establishing any NAS connection
        with session_scope() as db:
            first_transfer_id = dequeue_nas_batch(db)

        if first_transfer_id is None:
            return {
                "status": "completed",
                "processed_batches": 0,
            }

        # One connection reused for sequential transfers.
        try:
            with open_client(params) as client:
                current_id = first_transfer_id
                for _ in range(limit):
                    if current_id is None:
                        with session_scope() as db:
                            current_id = dequeue_nas_batch(db)
                        if current_id is None:
                            break

                    # Redis heartbeat also verifies current lock ownership.
                    if not lock.extend(300):
                        raise TransientConnectorError(
                            "NAS writer lock lease lost; stopping transfer processing."
                        )

                    with session_scope() as db:
                        writer = NasBatchWriter(
                            db,
                            params=params,
                            lock=lock,
                        )

                        result = writer.upload_batch(
                            current_id,
                            client=client,
                        )

                    results.append({
                        "transfer_id": str(current_id),
                        "result": result,
                    })
                    current_id = None
        except (NasConnectionError, TransientConnectorError) as conn_err:
            from app.storage.nas_writer import start_cooldown
            start_cooldown(30)
            logger.warning(f"[NAS QUEUE] Connection to NAS failed ({conn_err}); activated 30s cooldown.")
            raise

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
    from app.storage.nas_writer import is_cooldown_active

    if not storage_active():
        return {"status": "skipped", "reason": "no_active_storage"}

    if is_cooldown_active():
        return {"status": "skipped", "reason": "cooldown_active"}

    with session_scope() as db:
        repo = NasTransferBatchRepository(db)
        actionable = repo.list_actionable(limit=200)
        pending_cleanups = repo.list_pending_cleanups(limit=200)

    if actionable:
        process_nas_upload_queue.delay(max_transfers=max(len(actionable), 50))

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


@celery_app.task(name="app.tasks.nas_tasks.consolidate_job_dataset_on_nas", bind=True)
def consolidate_job_dataset_on_nas(self_or_job_id, job_id: str | None = None) -> dict:
    """Consolidates distributed batches and windows on the remote NAS into a
    unified, production-ready dataset layout:
      <storage_path>/<job_folder>/images/<file>
      <storage_path>/<job_folder>/annotations/<file>
      <storage_path>/<job_folder>/manifest.json

    Fast-renames remote files server-side without re-uploading bytes,
    cleans up empty intermediate directories, builds a master manifest,
    and updates all Asset.nas_path and Asset.annotation_nas_path records in PostgreSQL.
    """
    if job_id is None:
        job_id = str(self_or_job_id)
        task_self = None
    else:
        task_self = self_or_job_id

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

    # 1. Distributed Lock to prevent duplicate concurrent consolidation tasks for the same job
    redis = get_redis()
    consolidation_lock_key = f"lock:consolidate_job:{job_id}"
    consolidation_lock = redis.lock(consolidation_lock_key, timeout=600)
    acquired_job_lock = False
    try:
        acquired_job_lock = consolidation_lock.acquire(blocking=False)
    except Exception as e:
        logger.warning(f"[CONSOLIDATE] Could not acquire redis consolidation lock for job {job_id}: {e}")
        acquired_job_lock = True

    if not acquired_job_lock:
        logger.info(f"[CONSOLIDATE] Consolidation task already in progress for job {job_id}; skipping duplicate.")
        return {"status": "skipped", "reason": "already_running"}

    # 2. Check for active/pending transfers or in-flight upload batches in PostgreSQL before proceeding
    from app.db.models.image_batch import NasTransferBatch, ImageBatch
    from app.domain.enums import TransferStatus, BatchStatus
    with session_scope() as db:
        has_active_transfers = db.execute(
            select(NasTransferBatch.id).where(
                NasTransferBatch.job_id == job_uuid,
                NasTransferBatch.transfer_status.in_([
                    TransferStatus.PENDING,
                    TransferStatus.UPLOADING,
                    TransferStatus.VERIFIED,
                ]),
            ).limit(1)
        ).scalar_one_or_none() is not None

        has_in_flight = db.execute(
            select(ImageBatch.id).where(
                ImageBatch.job_id == job_uuid,
                ImageBatch.status.in_([
                    BatchStatus.READY_FOR_UPLOAD,
                    BatchStatus.UPLOADING,
                    BatchStatus.NAS_UPLOADING,
                    BatchStatus.NAS_PENDING,
                    BatchStatus.VERIFYING,
                ]),
            ).limit(1)
        ).scalar_one_or_none() is not None

    if has_active_transfers or has_in_flight:
        logger.warning(
            f"[CONSOLIDATE] Job {job_id} still has active transfers or in-flight uploads "
            f"(transfers={has_active_transfers}, in_flight={has_in_flight}). Deferring consolidation."
        )
        try:
            consolidation_lock.release()
        except Exception:
            pass
        if task_self and hasattr(task_self, "retry"):
            retries = getattr(getattr(task_self, "request", None), "retries", 0)
            if retries < 30:
                raise task_self.retry(countdown=10, max_retries=30)
        return {"status": "deferred", "reason": "active_transfers_in_flight"}

    # 3. Respect NAS Writer Lock: ensure no active batch upload is actively writing to NAS
    from app.storage.nas_writer import NasWriterLock
    writer_lock = NasWriterLock(ttl_s=300, heartbeat_interval_s=60, allow_local_fallback=False)
    if not writer_lock.acquire(timeout_s=30):
        logger.info(f"[CONSOLIDATE] NAS writer lock is held by upload queue. Deferring consolidation for job {job_id}.")
        try:
            consolidation_lock.release()
        except Exception:
            pass
        if task_self and hasattr(task_self, "retry"):
            retries = getattr(getattr(task_self, "request", None), "retries", 0)
            if retries < 30:
                raise task_self.retry(countdown=10, max_retries=30)
        return {"status": "deferred", "reason": "writer_lock_held"}

    remote_job_dir = join_relpath(storage_path, job_folder)
    images_dir = f"{remote_job_dir}/images"
    annotations_dir = f"{remote_job_dir}/annotations"

    moved_images = 0
    moved_annotations = 0
    deleted_manifests = 0
    subdirs_to_clean = set()

    with session_scope() as db:
        curr_job = JobRepository(db).get(job_uuid)
        job_target = curr_job.target_count if curr_job else None
        accepted_assets = list(
            db.execute(
                select(Asset).where(
                    Asset.job_id == job_uuid,
                    Asset.status.in_([
                        AssetStatus.COMPLETED,
                        AssetStatus.NAS_VERIFIED,
                        AssetStatus.RELEVANCE_PASSED,
                        AssetStatus.READY_FOR_UPLOAD,
                        AssetStatus.UPLOADING,
                    ]),
                )
            ).scalars().all()
        )
        accepted_ids = {str(a.id) for a in accepted_assets}
        accepted_filenames = {f"{a.id}.jpg" for a in accepted_assets}
        for a in accepted_assets:
            if a.nas_path:
                accepted_filenames.add(Path(a.nas_path).name)

    try:
        with open_client(params) as client:
            _ensure_dir(client, images_dir)
    
            all_entries = list(client.walk(remote_job_dir))
    
            for entry in all_entries:
                path = entry.path
                parts = path.split("/")
                filename = parts[-1]
                stem = Path(filename).stem
    
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
    
                # Filter out non-accepted/duplicate image files: delete them instead of moving
                if "/images/" in path or any(filename.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                    if stem not in accepted_ids and filename not in accepted_filenames:
                        try:
                            client.delete(path)
                            logger.info(f"[CONSOLIDATE] Discarded duplicate/rejected intermediate image: {path}")
                        except Exception as e:
                            logger.debug(f"[CONSOLIDATE] Could not delete duplicate intermediate image {path}: {e}")
                        continue

                    target_rel = f"{images_dir}/{filename}"
                    if target_rel != path:
                        try:
                            client._rename(path, target_rel)
                            moved_images += 1
                        except Exception as e:
                            logger.warning(f"[CONSOLIDATE] Failed to move image {path} -> {target_rel}: {e}")
    
                # Move annotation files into unified annotations/ directory
                elif "/annotations/" in path or any(filename.lower().endswith(ext) for ext in [".json", ".xml", ".txt"]):
                    if stem not in accepted_ids and filename not in accepted_filenames:
                        try:
                            client.delete(path)
                        except Exception:
                            pass
                        continue

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

            # Prune any duplicate, rejected, or unaccepted files from the final images directory
            for entry in list(client.walk(images_dir)):
                fn = Path(entry.path).name
                stem = Path(entry.path).stem
                if stem not in accepted_ids and fn not in accepted_filenames:
                    try:
                        client.delete(entry.path)
                        logger.info(f"[CONSOLIDATE] Pruned duplicate/rejected image from NAS images directory: {entry.path}")
                    except Exception as exc:
                        logger.warning(f"[CONSOLIDATE] Could not delete duplicate image {entry.path}: {exc}")

            if client.exists(annotations_dir):
                for entry in list(client.walk(annotations_dir)):
                    fn = Path(entry.path).name
                    stem = Path(entry.path).stem
                    if stem not in accepted_ids and fn not in accepted_filenames:
                        try:
                            client.delete(entry.path)
                        except Exception:
                            pass
    
            # Re-walk final images and annotations
            final_images = list(client.walk(images_dir))
            final_annots = list(client.walk(annotations_dir)) if client.exists(annotations_dir) else []

            # If NAS final image count exceeds target_count, prune surplus images safely from NAS
            with session_scope() as db:
                curr_job = JobRepository(db).get(job_uuid)
                job_target = curr_job.target_count if curr_job else None

            if job_target is not None and len(final_images) > job_target:
                from app.tasks.filtering_tasks import _asset_quality_sort_key
                from app.services.standardization_service import is_asset_file_shared
                with session_scope() as db:
                    all_accepted = list(
                        db.execute(
                            select(Asset).where(
                                Asset.job_id == job_uuid,
                                Asset.status.in_([
                                    AssetStatus.COMPLETED,
                                    AssetStatus.NAS_VERIFIED,
                                    AssetStatus.RELEVANCE_PASSED,
                                    AssetStatus.READY_FOR_UPLOAD,
                                    AssetStatus.UPLOADING,
                                ]),
                            )
                        ).scalars().all()
                    )
                    sorted_acc = sorted(all_accepted, key=_asset_quality_sort_key, reverse=True)
                    surplus_db = sorted_acc[job_target:]
                    surplus_id_set = {str(a.id) for a in surplus_db}
                    surplus_filename_set = {f"{a.id}.jpg" for a in surplus_db}
                    for a in surplus_db:
                        if a.nas_path:
                            surplus_filename_set.add(Path(a.nas_path).name)

                    for entry in list(client.walk(images_dir)):
                        fn = Path(entry.path).name
                        stem = Path(entry.path).stem
                        if fn in surplus_filename_set or stem in surplus_id_set:
                            asset_obj = next((a for a in surplus_db if str(a.id) == stem or (a.nas_path and Path(a.nas_path).name == fn)), None)
                            shared = False
                            if asset_obj:
                                shared = is_asset_file_shared(asset_obj, entry.path)
                            if not shared:
                                try:
                                    client.delete(entry.path)
                                except Exception as exc:
                                    logger.debug(f"[CONSOLIDATE] Error deleting surplus image {entry.path}: {exc}")

                    for a in surplus_db:
                        a.status = AssetStatus.SURPLUS
                        md = dict(a.asset_metadata or {})
                        md["trimmed_as_surplus"] = True
                        md["surplus_trimmed_at"] = datetime.now(timezone.utc).isoformat()
                        a.asset_metadata = md
                    db.commit()

                # Re-walk final images after surplus pruning
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
                    Asset.status.in_([
                        AssetStatus.COMPLETED,
                        AssetStatus.NAS_VERIFIED,
                        AssetStatus.RELEVANCE_PASSED,
                        AssetStatus.READY_FOR_UPLOAD,
                        AssetStatus.UPLOADING,
                    ]),
                )
                updated_assets = 0
                failed_assets = 0
                image_names_set = {Path(e.path).name for e in final_images}
                annot_names_set = {Path(e.path).name for e in final_annots}
                recovered_any = False
    
                for asset in db.execute(stmt).scalars():
                    asset_fn = f"{asset.id}.jpg"
                    if asset.nas_path:
                        curr_fn = Path(asset.nas_path).name
                        if curr_fn in image_names_set:
                            asset_fn = curr_fn
                    if asset_fn in image_names_set:
                        asset.nas_path = asset_files.make_remote_path(f"{images_dir}/{asset_fn}")
                        stem = Path(asset_fn).stem
                        found_annot = None
                        for ext in (".json", ".xml", ".txt"):
                            if f"{stem}{ext}" in annot_names_set:
                                found_annot = f"{stem}{ext}"
                                break
                        if found_annot:
                            asset.annotation_nas_path = asset_files.make_remote_path(f"{annotations_dir}/{found_annot}")
                        asset.status = AssetStatus.COMPLETED
                        updated_assets += 1
                    else:
                        # Last-resort fallback: check if local image exists and upload it right now
                        local_uploaded = False
                        if asset.nas_path and not asset_files.is_remote(asset.nas_path):
                            p = Path(asset.nas_path)
                            if p.is_file():
                                try:
                                    client.upload(p, f"{images_dir}/{asset_fn}")
                                    asset.nas_path = asset_files.make_remote_path(f"{images_dir}/{asset_fn}")
                                    if asset.annotation_nas_path and not asset_files.is_remote(asset.annotation_nas_path):
                                        ann_p = Path(asset.annotation_nas_path)
                                        if ann_p.is_file():
                                            ann_fn = f"{Path(asset_fn).stem}{ann_p.suffix}"
                                            client.upload(ann_p, f"{annotations_dir}/{ann_fn}")
                                            asset.annotation_nas_path = asset_files.make_remote_path(f"{annotations_dir}/{ann_fn}")
                                    asset.status = AssetStatus.COMPLETED
                                    updated_assets += 1
                                    local_uploaded = True
                                    recovered_any = True
                                    logger.info(f"[CONSOLIDATE] Recovered local asset {asset.id} and uploaded to NAS.")
                                except Exception as e:
                                    logger.warning(f"[CONSOLIDATE] Failed to recover local asset {asset.id}: {e}")
    
                        if not local_uploaded:
                            if asset.status in (AssetStatus.READY_FOR_UPLOAD, AssetStatus.UPLOADING, AssetStatus.RELEVANCE_PASSED):
                                logger.info(
                                    f"[CONSOLIDATE] Asset {asset.id} is in in-flight status {asset.status}; "
                                    "preserving status for upload pipeline instead of marking FAILED."
                                )
                                continue
                            logger.warning(
                                f"[CONSOLIDATE] Asset {asset.id} not found in NAS consolidated directory for job {job_id}. "
                                "Marking status as FAILED."
                            )
                            asset.status = AssetStatus.FAILED
                            failed_assets += 1
    
                if recovered_any:
                    final_images = list(client.walk(images_dir))
                    final_annots = list(client.walk(annotations_dir)) if client.exists(annotations_dir) else []
                    manifest_payload["total_images"] = len(final_images)
                    manifest_payload["total_annotations"] = len(final_annots)
                    manifest_payload["images"] = [
                        {"filename": Path(e.path).name, "rel_path": f"images/{Path(e.path).name}", "size": e.size}
                        for e in sorted(final_images, key=lambda x: x.path)
                    ]
                    manifest_payload["annotations"] = [
                        {"filename": Path(e.path).name, "rel_path": f"annotations/{Path(e.path).name}", "size": e.size}
                        for e in sorted(final_annots, key=lambda x: x.path)
                    ]
                    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
                        json.dump(manifest_payload, tf, indent=2)
                        tmp_local_path = Path(tf.name)
                    try:
                        client.upload(tmp_local_path, f"{remote_job_dir}/manifest.json")
                    finally:
                        tmp_local_path.unlink(missing_ok=True)

            db.execute(
                update(Asset)
                .where(
                    Asset.job_id == job_uuid,
                    Asset.status.in_(_NOT_KEPT),
                )
                .values(nas_path=None, annotation_nas_path=None)
            )

            from app.services.job_counters import reconcile
            reconciled = reconcile(db, job_uuid)
            db.commit()

            try:
                from app.services.progress_service import ProgressPublisher
                publisher = ProgressPublisher(get_redis())
                publisher.publish(str(job_uuid), {"type": "job_progress_updated"})
            except Exception as e:
                logger.warning(f"[CONSOLIDATE] Error publishing job progress after consolidation: {e}")

        logger.info(
            f"[CONSOLIDATE] Job {job_id} dataset consolidated on NAS at {remote_job_dir}: "
            f"{len(final_images)} images, {len(final_annots)} annotations, "
            f"{updated_assets} DB assets verified, {failed_assets} missing assets marked FAILED."
        )
        try:
            r = get_redis()
            if r:
                r.set(f"nas_consolidated:{job_uuid}", "1", ex=86400 * 7)
        except Exception:
            pass
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
    finally:
        try:
            writer_lock.release()
        except Exception:
            pass
        try:
            consolidation_lock.release()
        except Exception:
            pass
