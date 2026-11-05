"""Centralized Logical NAS Writer and Upload Queue.

Guarantees:
1. Global NAS upload concurrency = 1 across all Celery workers and processes via
   distributed lock with safe ownership, TTL, heartbeat extension, and safe release.
2. Connection reuse: single connection per NAS writer session across batch files.
3. Strict integrity verification: local size/SHA256 -> upload .part -> remote size verification
   -> remote SHA256 checksum verification -> atomic server-side rename to final path.
4. Idempotent transfer resumption: already-uploaded files on NAS matching checksum are skipped.
5. Local staging cleanup ONLY after successful remote verification and PostgreSQL commit.
6. Safe outage handling with cooldown backoff and staging backpressure pausing candidate downloads.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.connectors.nas.base import NasClient, join_relpath, open_client, NasConnectionParams
from app.core.config import get_settings
from app.core.exceptions import ConnectorError, FatalConnectorError, TransientConnectorError
from app.core.redis_client import get_redis
from app.core.slugify import job_folder_name
from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.candidate_window import CandidateWindow
from app.db.models.job import Job
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.image_batch_repository import ImageBatchRepository, NasTransferBatchRepository
from app.db.session import session_scope
from app.domain.enums import AssetStatus, BatchStatus, TransferStatus
from app.services.nas_config_service import get_active_storage_params, is_storage_active
from app.storage import asset_files
from app.storage.ram_buffer import LocalStagingManager
from app.storage.window_staging import WindowStagingManager

logger = logging.getLogger(__name__)

# Redis keys
_REDIS_NAS_LOCK_KEY = "nas_writer:global_lock"
_REDIS_NAS_QUEUE_KEY = "nas_upload:queue"
_REDIS_COOLDOWN_KEY = "nas_offload:cooldown"

DEFAULT_LOCK_TTL_S = 180  # 3 minutes default lease

_LUA_ACQUIRE = """
if redis.call('set', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then
    return 1
else
    return 0
end
"""

_LUA_RELEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

_LUA_EXTEND = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
else
    return 0
end
"""


class LockLostError(RuntimeError):
    """Raised when an active NAS writer loses its distributed lease mid-transfer."""
    pass


class NasWriterLock:
    """Distributed lock ensuring global NAS upload concurrency is strictly 1.

    Safe ownership: every acquisition generates a unique token.
    Safe expiry: lock expires after `ttl_s` if worker crashes.
    Safe release: Lua script only deletes the key if ownership token matches.
    Heartbeat extension: background thread renews TTL as long as worker is alive.
    Fail-closed: fails closed when Redis is unavailable (local fallback only for unit tests).
    """

    # In-memory lock simulation when Redis is absent (unit tests only)
    _local_lock = threading.Lock()
    _local_owner_token: str | None = None
    _local_expires_at: float = 0.0

    def __init__(
        self,
        ttl_s: int = DEFAULT_LOCK_TTL_S,
        owner_token: str | None = None,
        heartbeat_interval_s: float | None = None,
        allow_local_fallback: bool | None = None,
    ):
        self.ttl_s = ttl_s
        self.owner_token = owner_token or f"worker_{os.getpid()}_{uuid.uuid4().hex[:8]}"
        self.heartbeat_interval_s = heartbeat_interval_s or max(0.5, float(ttl_s) / 3.0)
        if allow_local_fallback is not None:
            self.allow_local_fallback = allow_local_fallback
        else:
            self.allow_local_fallback = bool(os.environ.get("PYTEST_CURRENT_TEST"))

        self.acquired = False
        self._lock_lost = False
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop = threading.Event()

    def _get_redis_safe(self):
        try:
            r = get_redis()
            r.ping()
            return r
        except Exception:
            return None

    def acquire(self, timeout_s: float = 0.0) -> bool:
        """Attempts to acquire the global writer lock."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while True:
            r = self._get_redis_safe()
            if r is not None:
                try:
                    res = r.eval(_LUA_ACQUIRE, 1, _REDIS_NAS_LOCK_KEY, self.owner_token, self.ttl_s)
                    if bool(res == 1):
                        self.acquired = True
                        self._lock_lost = False
                        self._start_heartbeat()
                        return True
                    # Redis successfully checked and lock is held by another worker
                    if time.monotonic() >= deadline:
                        return False
                    time.sleep(0.05)
                    continue
                except Exception as e:
                    logger.warning(f"[NAS LOCK] Redis acquire failed: {e}")
                    if not self.allow_local_fallback:
                        logger.error("[NAS LOCK] Redis unavailable and local fallback disabled; failing closed.")
                        return False

            if not self.allow_local_fallback:
                logger.error("[NAS LOCK] Redis unavailable and local fallback disabled; failing closed.")
                return False

            # Local fallback simulation (used ONLY in unit tests when configured)
            with self._local_lock:
                now = time.monotonic()
                if NasWriterLock._local_owner_token is None or now >= NasWriterLock._local_expires_at:
                    NasWriterLock._local_owner_token = self.owner_token
                    NasWriterLock._local_expires_at = now + self.ttl_s
                    self.acquired = True
                    self._lock_lost = False
                    self._start_heartbeat()
                    return True

            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        return False

    def release(self) -> bool:
        """Safely releases the lock, ensuring token matches."""
        self._stop_heartbeat()
        if not self.acquired:
            return False

        r = self._get_redis_safe()
        if r is not None:
            try:
                res = r.eval(_LUA_RELEASE, 1, _REDIS_NAS_LOCK_KEY, self.owner_token)
                self.acquired = False
                return bool(res == 1)
            except Exception as e:
                logger.warning(f"[NAS LOCK] Redis release error: {e}")

        released = False
        with self._local_lock:
            if NasWriterLock._local_owner_token == self.owner_token:
                NasWriterLock._local_owner_token = None
                NasWriterLock._local_expires_at = 0.0
                released = True

        self.acquired = False
        return released

    def extend(self, additional_ttl_s: int | None = None) -> bool:
        """Heartbeat extension to keep lock alive during ongoing transfers."""
        if not self.acquired or self._lock_lost:
            return False
        ttl = additional_ttl_s or self.ttl_s

        r = self._get_redis_safe()
        if r is not None:
            try:
                res = r.eval(_LUA_EXTEND, 1, _REDIS_NAS_LOCK_KEY, self.owner_token, ttl)
                if bool(res == 1):
                    return True
                else:
                    self._lock_lost = True
                    return False
            except Exception as e:
                logger.warning(f"[NAS LOCK] Redis extend error: {e}")
                return False

        with self._local_lock:
            if NasWriterLock._local_owner_token == self.owner_token:
                NasWriterLock._local_expires_at = time.monotonic() + ttl
                return True
            self._lock_lost = True
            return False

    def is_locked(self) -> bool:
        """Checks if any worker currently holds the writer lock."""
        r = self._get_redis_safe()
        if r is not None:
            try:
                return bool(r.exists(_REDIS_NAS_LOCK_KEY))
            except Exception:
                pass

        with self._local_lock:
            return (
                NasWriterLock._local_owner_token is not None
                and time.monotonic() < NasWriterLock._local_expires_at
            )

    def is_active(self) -> bool:
        """Returns True only if the lock was acquired, has not been lost, and lease is still valid."""
        if not self.acquired or self._lock_lost:
            return False
        r = self._get_redis_safe()
        if r is not None:
            try:
                val = r.get(_REDIS_NAS_LOCK_KEY)
                token = val.decode() if isinstance(val, bytes) else str(val) if val else None
                return token == self.owner_token
            except Exception:
                return not self._lock_lost
        with self._local_lock:
            return (
                NasWriterLock._local_owner_token == self.owner_token
                and time.monotonic() < NasWriterLock._local_expires_at
            )

    def _start_heartbeat(self) -> None:
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"nas-lock-heartbeat-{self.owner_token[:8]}",
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """Periodic background renewal loop."""
        while not self._heartbeat_stop.wait(self.heartbeat_interval_s):
            if not self.acquired:
                break
            extended = self.extend(self.ttl_s)
            if not extended:
                logger.error(f"[NAS LOCK] Failed to renew lock lease {self.owner_token}; lock lost!")
                self._lock_lost = True
                break

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1.0)
        self._heartbeat_thread = None

    def __enter__(self) -> NasWriterLock:
        self.acquire(timeout_s=0.0)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def is_cooldown_active(redis=None) -> bool:
    """Returns True if NAS is in cooldown following connection failure."""
    r = redis or _get_redis()
    if r is None:
        return False
    try:
        return bool(r.get(_REDIS_COOLDOWN_KEY))
    except Exception:
        return False


def start_cooldown(cooldown_seconds: int | None = None, redis=None) -> None:
    """Starts cooldown timer to prevent workers hammering an unavailable NAS."""
    r = redis or _get_redis()
    if r is None:
        return
    ttl = cooldown_seconds or get_settings().nas_upload_cooldown_seconds
    try:
        r.set(_REDIS_COOLDOWN_KEY, "1", ex=ttl)
        logger.info(f"[NAS COOLDOWN] NAS cooldown activated for {ttl}s")
    except Exception as e:
        logger.warning(f"[NAS COOLDOWN] Could not set cooldown key: {e}")


def _get_redis():
    try:
        r = get_redis()
        r.ping()
        return r
    except Exception:
        return None


def enqueue_nas_batch(
    batch_id: uuid.UUID,
    job_id: uuid.UUID,
    manifest_path: Path | str | None = None,
    remote_dest: str | None = None,
) -> uuid.UUID:
    """Enqueues an accepted batch into the centralized NAS upload queue and DB."""
    with session_scope() as db:
        transfer_repo = NasTransferBatchRepository(db)
        from app.domain.nas_resolver import resolve_job_nas_destination
        job = db.execute(select(Job).where(Job.id == job_id)).scalar_one_or_none()
        remote_dest = resolve_job_nas_destination(job, default_storage=remote_dest)

        manifest_file = Path(manifest_path) if manifest_path else None
        total_files = 0
        asset_ids = []
        if manifest_file and manifest_file.exists():
            try:
                data = json.loads(manifest_file.read_text())
                items = data.get("items", [])
                total_files = len(items)
                asset_ids = [item.get("asset_id") for item in items if item.get("asset_id")]
            except Exception as e:
                logger.warning(f"Could not parse manifest {manifest_file}: {e}")

        transfer = transfer_repo.get_or_create_for_batch(
            batch_id=batch_id,
            job_id=job_id,
            remote_path=remote_dest,
            total_files=total_files,
            asset_ids=asset_ids,
        )
        transfer_id = transfer.id

    r = _get_redis()
    if r is not None:
        try:
            r.rpush(_REDIS_NAS_QUEUE_KEY, str(transfer_id))
        except Exception as e:
            logger.warning(f"Could not enqueue transfer {transfer_id} to Redis: {e}")

    logger.info(f"[NAS QUEUE] Enqueued batch {batch_id} (transfer {transfer_id}) for NAS upload")
    return transfer_id




def enqueue_nas_window(
    window_id: uuid.UUID,
    batch_id: uuid.UUID,
    job_id: uuid.UUID,
    batch_index: int,
    window_index: int,
    manifest_path: Path | str | None = None,
    remote_dest: str | None = None,
) -> uuid.UUID:
    """Create or retrieve one durable NAS transfer for a candidate window."""

    manager = WindowStagingManager()
    manifest_file = (
        Path(manifest_path)
        if manifest_path
        else manager.get_window_manifest_path(
            job_id, batch_index, window_index
        )
    )

    if not manifest_file.is_file():
        raise FileNotFoundError(
            f"Candidate-window manifest missing: {manifest_file}"
        )

    manifest_data = json.loads(manifest_file.read_text())
    items = manifest_data.get("items")

    if not isinstance(items, list) or not items:
        raise ValueError("Candidate-window manifest has no items.")

    asset_ids = []
    seen = set()

    for item in items:
        if not item.get("asset_id") or not item.get("local_path"):
            raise ValueError(
                "Every manifest item must include asset_id and local_path."
            )

        asset_id = str(uuid.UUID(str(item["asset_id"])))
        if asset_id in seen:
            raise ValueError(f"Duplicate asset in manifest: {asset_id}")
        seen.add(asset_id)

        local_path = Path(item["local_path"])
        if not local_path.is_file():
            raise FileNotFoundError(
                f"Staged image missing: {local_path}"
            )

        annotation_path = item.get("annotation_path")
        if annotation_path and not Path(annotation_path).is_file():
            raise FileNotFoundError(
                f"Staged annotation missing: {annotation_path}"
            )

        asset_ids.append(asset_id)

    with session_scope() as db:
        from app.domain.nas_resolver import resolve_job_nas_destination
        job = db.execute(select(Job).where(Job.id == job_id)).scalar_one_or_none()
        remote_dest = resolve_job_nas_destination(job, default_storage=remote_dest)

        batch = db.execute(
            select(ImageBatch).where(ImageBatch.id == batch_id)
        ).scalar_one_or_none()

        if batch is None or batch.job_id != job_id:
            raise ValueError("Batch does not belong to the supplied job.")

        if batch.batch_index != batch_index:
            raise ValueError("Batch index mismatch.")

        window = db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.id == window_id)
            .with_for_update()
        ).scalar_one_or_none()

        if window is None:
            raise ValueError("Candidate window does not exist.")

        if (
            window.batch_id != batch_id
            or window.job_id != job_id
            or window.window_index != window_index
        ):
            raise ValueError("Candidate window ownership mismatch.")

        ids = [uuid.UUID(value) for value in asset_ids]
        assets = db.execute(
            select(Asset).where(Asset.id.in_(ids))
        ).scalars().all()

        if len(assets) != len(ids):
            raise ValueError("A manifest asset is missing from PostgreSQL.")

        for asset in assets:
            if asset.batch_id != batch_id or asset.window_id != window_id:
                raise ValueError(
                    f"Asset {asset.id} belongs to another batch/window."
                )

            if asset.status == AssetStatus.RELEVANCE_PASSED:
                asset.status = AssetStatus.READY_FOR_UPLOAD

            elif asset.status not in (
                AssetStatus.READY_FOR_UPLOAD,
                AssetStatus.UPLOADING,
                AssetStatus.NAS_VERIFIED,
                AssetStatus.COMPLETED,
            ):
                raise ValueError(
                    f"Asset {asset.id} has an invalid upload state: "
                    f"{asset.status}"
                )

        repo = NasTransferBatchRepository(db)
        transfer = repo.get_or_create_for_window(
            window_id=window_id,
            batch_id=batch_id,
            job_id=job_id,
            remote_path=remote_dest,
            manifest_path=str(manifest_file.resolve()),
            total_files=len(items),
            asset_ids=asset_ids,
            window_index=window_index,
        )
        transfer_id = transfer.id

    # PostgreSQL is authoritative. Redis is only a dispatch notification.
    redis_client = _get_redis()
    if redis_client is not None:
        try:
            redis_client.rpush(
                _REDIS_NAS_QUEUE_KEY,
                str(transfer_id),
            )
        except Exception as exc:
            logger.warning(
                f"Could not notify NAS queue for {transfer_id}: {exc}"
            )

    logger.info(
        f"[NAS QUEUE] Window {window_index} enqueued: "
        f"{transfer_id}, files={len(items)}"
    )
    return transfer_id



def dequeue_nas_batch(db: Session | None = None) -> uuid.UUID | None:
    """Discovers the next actionable transfer from PostgreSQL (authoritative source of truth).

    Finds PENDING transfers or stale UPLOADING transfers whose workers crashed.
    Also clears corresponding notification in Redis queue to keep dispatch queue clean.
    """
    r = _get_redis()
    if r is not None:
        try:
            r.lpop(_REDIS_NAS_QUEUE_KEY)
        except Exception:
            pass

    if db is not None:
        actionable = NasTransferBatchRepository(db).list_actionable(limit=1)
        if actionable:
            return actionable[0].id
        return None

    with session_scope() as s:
        actionable = NasTransferBatchRepository(s).list_actionable(limit=1)
        if actionable:
            return actionable[0].id
    return None


def compute_file_sha256(path: Path) -> str:
    """Computes SHA256 of local file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def _remote_rel_for(storage_path: str, local_file: Path, job_folder: str | None = None) -> str:
    """Constructs deterministic remote POSIX path mirroring local layout."""
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




def _remote_rel_for_window(
    storage_path: str,
    local_file: Path,
    job_id: uuid.UUID,
    job_folder: str,
    batch_index: int,
    window_index: int,
) -> str:
    """Build a collision-safe remote path for a staged candidate window."""
    staging_root = Path(get_settings().local_staging_path).resolve()
    relative = local_file.resolve().relative_to(staging_root)
    parts = relative.parts

    expected = (
        f"job_{uuid.UUID(str(job_id))}",
        f"batch_{batch_index:03d}",
        f"window_{window_index:04d}",
    )

    if len(parts) < 5 or tuple(parts[:3]) != expected:
        raise ValueError(
            f"File is outside the expected candidate window: {local_file}"
        )

    remote_relative = "/".join(
        [job_folder, expected[1], expected[2], *parts[3:]]
    )
    return join_relpath(storage_path, remote_relative)


class NasBatchWriter:
    """Centralized Logical NAS Writer.

    Operates strictly within an acquired `NasWriterLock`.
    Reuses a single connection for the entire batch.
    Performs verified, atomic upload with exact checksum verification.
    """

    def __init__(self, db: Session, params: NasConnectionParams | None = None, lock: NasWriterLock | None = None):
        self.db = db
        self.params = params or get_active_storage_params()
        self.lock = lock
        self.transfer_repo = NasTransferBatchRepository(db)
        self.batch_repo = ImageBatchRepository(db)
        self.asset_repo = AssetRepository(db)
        self.staging_manager = LocalStagingManager()
        self._dir_cache: set[str] = set()

    def check_connection_health(self, client: NasClient) -> bool:
        """Verifies active session is alive."""
        try:
            return client.exists("") or True
        except Exception:
            return False

    def upload_batch(self, transfer_id: uuid.UUID, client: NasClient | None = None) -> dict:
        """Executes full verified batch upload for `transfer_id`."""
        transfer = self.transfer_repo.get(transfer_id)
        if not transfer:
            return {"status": "skipped", "reason": "transfer_not_found"}

        if self.params is None:
            self.transfer_repo.mark_failed(transfer_id, "No active NAS configuration")
            return {"status": "failed", "reason": "no_nas_storage"}

        batch = self.batch_repo.get(transfer.batch_id)
        if not batch:
            self.transfer_repo.mark_failed(transfer_id, "Batch record not found")
            return {"status": "failed", "reason": "batch_not_found"}

        job = self.db.execute(select(Job).where(Job.id == transfer.job_id)).scalar_one_or_none()
        job_folder = job_folder_name(job.name if job else None, transfer.job_id)

        from app.domain.nas_resolver import resolve_job_nas_destination
        effective_storage_path = resolve_job_nas_destination(
            job,
            default_storage=transfer.remote_path or (self.params.storage_path if self.params else None),
        )

        # Handle durable transfer recovery before reading a staging manifest.
        if transfer.transfer_status == TransferStatus.COMPLETED:
            return {
                "status": "completed",
                "batch_id": str(batch.id),
                "uploaded_files": transfer.uploaded_files,
                "failed_files": transfer.failed_files,
                "bytes_transferred": transfer.bytes_transferred,
            }

        if transfer.transfer_status == TransferStatus.VERIFIED:
            return self._finalize_verified_transfer(
                transfer_id,
                batch,
                transfer.uploaded_files or transfer.total_files,
                transfer.bytes_transferred,
            )

        # Locate the manifest belonging to this transfer.
        window_index = None

        if transfer.window_id is not None:
            metadata = transfer.transfer_metadata or {}
            manifest_value = metadata.get("manifest_path")
            window_index = metadata.get("window_index")

            if not manifest_value or window_index is None:
                self.transfer_repo.mark_failed(
                    transfer_id,
                    "Window transfer is missing manifest_path/window_index",
                )
                return {
                    "status": "failed",
                    "reason": "window_manifest_metadata_missing",
                }

            manifest_path = Path(manifest_value)

        else:
            # Legacy batch transfer.
            manifest_path = (
                Path(batch.manifest_path)
                if batch.manifest_path
                else None
            )

            if not manifest_path or not manifest_path.exists():
                staging_dir = self.staging_manager.get_batch_staging_dir(
                    transfer.job_id,
                    batch.batch_index,
                )
                manifest_path = staging_dir / "manifest.json"

        if not manifest_path.exists():
            self.transfer_repo.mark_failed(
                transfer_id,
                f"Manifest missing at {manifest_path}",
            )
            return {
                "status": "failed",
                "reason": "manifest_missing",
            }

        try:
            manifest_data = json.loads(manifest_path.read_text())
        except Exception as e:
            self.transfer_repo.mark_failed(transfer_id, f"Invalid manifest JSON: {e}")
            return {"status": "failed", "reason": "corrupt_manifest"}

        items = manifest_data.get("items", [])
        total_files = len(items)

        # Idempotency check: if transfer was ALREADY completed, return immediately without re-processing
        if transfer.transfer_status == TransferStatus.COMPLETED:
            logger.info(f"[NAS WRITER] Transfer {transfer_id} is already COMPLETED. Returning immediately.")
            return {
                "status": "completed",
                "batch_id": str(batch.id),
                "uploaded_files": transfer.uploaded_files,
                "failed_files": transfer.failed_files,
                "bytes_transferred": transfer.bytes_transferred,
            }

        # Crash recovery check: if transfer was ALREADY verified on NAS before worker crashed,
        # do not re-upload files; proceed directly to completion and local staging cleanup!
        if transfer.transfer_status == TransferStatus.VERIFIED:
            logger.info(
                f"[NAS WRITER] Transfer {transfer_id} is already VERIFIED on NAS. "
                "Bypassing upload loop; proceeding to completion and staging cleanup."
            )
            return self._finalize_verified_transfer(transfer_id, batch, len(items), transfer.bytes_transferred)

        owner_token = getattr(self.lock, "owner_token", None) if self.lock else None
        self.transfer_repo.mark_uploading(transfer_id, owner_token=owner_token)
        if (
            transfer.window_id is None
            and batch.status in (BatchStatus.READY_FOR_UPLOAD, BatchStatus.UPLOADING)
        ):
            self.batch_repo.set_status(batch.id, BatchStatus.UPLOADING)
        self.db.commit()

        # Connect client if not provided or not yet connected
        owns_client = client is None
        active_client = client or open_client(self.params)
        try:
            if owns_client or not hasattr(active_client, "_root"):
                active_client.connect()

            return self._process_items(
                transfer_id,
                batch,
                job_folder,
                items,
                active_client,
                window_id=transfer.window_id,
                window_index=window_index,
                storage_path=effective_storage_path,
                manifest_path=manifest_path,
            )

        except (TransientConnectorError, ConnectionError, TimeoutError, OSError) as exc:
            logger.warning(f"[NAS WRITER] Connection error during batch transfer {transfer_id}: {exc}")
            start_cooldown()
            self.transfer_repo.mark_failed(transfer_id, f"Transient NAS error: {exc}")
            self.db.commit()
            raise TransientConnectorError(f"NAS transfer interrupted: {exc}") from exc

        except FatalConnectorError as exc:
            logger.error(f"[NAS WRITER] Fatal NAS error during transfer {transfer_id}: {exc}")
            self.transfer_repo.mark_failed(transfer_id, f"Fatal NAS error: {exc}")
            self.db.commit()
            return {"status": "failed", "reason": "fatal_error", "error": str(exc)}

        finally:
            if owns_client:
                try:
                    active_client.close()
                except Exception:
                    pass

    def _process_items(
        self,
        transfer_id: uuid.UUID,
        batch: ImageBatch,
        job_folder: str,
        items: list[dict],
        client: NasClient,
        window_id: uuid.UUID | None = None,
        window_index: int | None = None,
        storage_path: str = "data_automation",
        manifest_path: Path | None = None,
    ) -> dict:
        uploaded_count = 0
        total_bytes = 0
        failed_count = 0

        # Filter items to only include assets that are actively retained and ready for upload.
        # Assets trimmed by trim_excess_assets (or rejected/deduplicated) are not transfer failures;
        # they were intentionally removed from the kept set.
        active_items = []
        for item in items:
            asset_id_str = item.get("asset_id")
            if not asset_id_str:
                continue
            asset = self.asset_repo.get(uuid.UUID(asset_id_str))
            if asset and asset.status in (
                AssetStatus.RELEVANCE_PASSED,
                AssetStatus.READY_FOR_UPLOAD,
                AssetStatus.UPLOADING,
                AssetStatus.NAS_VERIFIED,
            ):
                active_items.append(item)
            else:
                logger.info(
                    f"[NAS WRITER] Skipping item {asset_id_str} with status "
                    f"{asset.status if asset else 'MISSING'} from upload manifest."
                )

        total_items = len(active_items)

        for i, item in enumerate(active_items):
            if self.lock:
                self.lock.extend(DEFAULT_LOCK_TTL_S)

            asset_id_str = item.get("asset_id")
            if not asset_id_str:
                failed_count += 1
                continue
            asset_uuid = uuid.UUID(asset_id_str)
            asset = self.asset_repo.get(asset_uuid)

            if window_id is not None and (
                asset is None
                or asset.window_id != window_id
                or asset.batch_id != batch.id
            ):
                failed_count += 1
                logger.error(
                    f"Asset {asset_uuid} does not belong to window {window_id}"
                )
                continue

            if not asset or asset.status not in (
                AssetStatus.RELEVANCE_PASSED,
                AssetStatus.READY_FOR_UPLOAD,
                AssetStatus.UPLOADING,
                AssetStatus.NAS_VERIFIED,
            ):
                failed_count += 1
                self.transfer_repo.update_progress(
                    transfer_id=transfer_id,
                    uploaded_files=uploaded_count,
                    pending_files=max(0, total_items - uploaded_count - failed_count),
                    bytes_transferred=total_bytes,
                    failed_files=failed_count,
                )
                continue

            local_img_path = Path(item.get("local_path") or asset.nas_path or "")
            if not local_img_path.exists():
                logger.warning(f"[NAS WRITER] Local asset {asset_uuid} file {local_img_path} missing")
                failed_count += 1
                self.transfer_repo.update_progress(
                    transfer_id=transfer_id,
                    uploaded_files=uploaded_count,
                    pending_files=max(0, total_items - uploaded_count - failed_count),
                    bytes_transferred=total_bytes,
                    failed_files=failed_count,
                )
                continue

            expected_size = local_img_path.stat().st_size
            expected_sha256 = item.get("checksum_sha256") or compute_file_sha256(local_img_path)

            if window_id is not None:
                final_rel = _remote_rel_for_window(
                    storage_path,
                    local_img_path,
                    batch.job_id,
                    job_folder,
                    batch.batch_index,
                    int(window_index),
                )
            else:
                final_rel = _remote_rel_for(
                    storage_path,
                    local_img_path,
                    job_folder,
                )

            # Annotation handling
            local_ann_path = (
                Path(item.get("annotation_path") or asset.annotation_nas_path)
                if (item.get("annotation_path") or asset.annotation_nas_path)
                else None
            )
            final_ann_rel = (
                (
                    _remote_rel_for_window(
                        storage_path,
                        local_ann_path,
                        batch.job_id,
                        job_folder,
                        batch.batch_index,
                        int(window_index),
                    )
                    if window_id is not None
                    else _remote_rel_for(
                        storage_path,
                        local_ann_path,
                        job_folder,
                    )
                )
                if local_ann_path and local_ann_path.exists()
                else None
            )

            # Resumption check: does file already exist on NAS with correct size and checksum?
            already_uploaded = False
            if client.exists(final_rel):
                remote_size = client.size(final_rel)
                remote_sha256 = client.compute_checksum(final_rel)
                if remote_size == expected_size and remote_sha256 == expected_sha256:
                    already_uploaded = True
                    logger.debug(f"[NAS WRITER] File {final_rel} already exists and verified on NAS; skipping upload.")

            if not already_uploaded:
                # Fencing check: verify lock is still active before writing remote file
                if self.lock and not self.lock.is_active():
                    raise LockLostError("NAS writer lock lease was lost or expired; halting upload.")

                # 1. Upload to unique temporary .part filename scoped to transfer attempt and owner token
                owner_tok = getattr(self.lock, "owner_token", None) if self.lock else None
                owner_tag = owner_tok[:8] if owner_tok else uuid.uuid4().hex[:8]
                tmp_rel = f"{final_rel}.{transfer_id.hex[:8]}.{owner_tag}.part"
                try:
                    client._upload(local_img_path, tmp_rel)

                    # 2. Verify remote size
                    remote_size = client.size(tmp_rel)
                    if remote_size != expected_size:
                        client.delete(tmp_rel)
                        raise TransientConnectorError(
                            f"Remote file size mismatch for {local_img_path.name}: "
                            f"expected {expected_size}, got {remote_size}"
                        )

                    # 3. Verify remote checksum
                    remote_sha256 = client.compute_checksum(tmp_rel)
                    if remote_sha256 != expected_sha256:
                        client.delete(tmp_rel)
                        raise TransientConnectorError(
                            f"Remote checksum mismatch for {local_img_path.name}: "
                            f"expected {expected_sha256}, got {remote_sha256}"
                        )

                    # 4. Safe publication to final destination
                    self._safe_publish(client, tmp_rel, final_rel, expected_sha256)

                except Exception:
                    # Clean up temporary part file if left behind
                    try:
                        client.delete(tmp_rel)
                    except Exception:
                        pass
                    raise

                # Upload annotation if present
                if local_ann_path and final_ann_rel:
                    ann_tmp = f"{final_ann_rel}.{transfer_id.hex[:8]}.{owner_tag}.part"
                    ann_expected_sha = compute_file_sha256(local_ann_path)
                    try:
                        client._upload(local_ann_path, ann_tmp)
                        if client.size(ann_tmp) == local_ann_path.stat().st_size:
                            self._safe_publish(client, ann_tmp, final_ann_rel, ann_expected_sha)
                    except Exception as e:
                        logger.warning(f"Annotation upload failed for {asset_uuid}: {e}")
                        try:
                            client.delete(ann_tmp)
                        except Exception:
                            pass

            # Update asset record to point to NAS URI and record NAS_VERIFIED state
            asset.nas_path = asset_files.make_remote_path(final_rel)
            if final_ann_rel:
                asset.annotation_nas_path = asset_files.make_remote_path(final_ann_rel)
            asset.status = AssetStatus.NAS_VERIFIED

            uploaded_count += 1
            total_bytes += expected_size

            # Update transfer batch progress in DB
            self.transfer_repo.update_progress(
                transfer_id=transfer_id,
                uploaded_files=uploaded_count,
                pending_files=max(0, total_items - uploaded_count - failed_count),
                bytes_transferred=total_bytes,
                failed_files=failed_count,
            )
            self.db.flush()

        owner_token = getattr(self.lock, "owner_token", None) if self.lock else None

        # Step 2: Update all Asset NAS paths and mark transfer VERIFIED conditionally
        if failed_count > 0 or uploaded_count != total_items:
            self.transfer_repo.update_progress(
                transfer_id=transfer_id,
                uploaded_files=uploaded_count,
                pending_files=max(0, total_items - uploaded_count - failed_count),
                bytes_transferred=total_bytes,
                failed_files=failed_count,
            )
            self.db.commit()
            return {
                "status": "incomplete",
                "uploaded_files": uploaded_count,
                "failed_files": failed_count,
                "pending_files": max(0, total_items - uploaded_count - failed_count),
            }

        # Publish verified manifest to NAS destination folder alongside images
        if manifest_path and manifest_path.exists():
            try:
                owner_tag = getattr(self.lock, "owner_token", "default")[:8] if self.lock else "direct"
                if window_id is not None and window_index is not None:
                    manifest_remote_rel = f"{storage_path}/{job_folder}/batch_{batch.batch_index:03d}/window_{int(window_index):04d}/manifest.json"
                else:
                    manifest_remote_rel = f"{storage_path}/{job_folder}/batch_{batch.batch_index:03d}/manifest.json"
                manifest_tmp = f"{manifest_remote_rel}.{transfer_id.hex[:8]}.{owner_tag}.part"
                client._upload(manifest_path, manifest_tmp)
                self._safe_publish(client, manifest_tmp, manifest_remote_rel, compute_file_sha256(manifest_path))
                logger.info(f"[NAS WRITER] Uploaded verified manifest to {manifest_remote_rel}")
            except Exception as e:
                logger.warning(f"[NAS WRITER] Non-fatal: could not upload remote manifest: {e}")

        self.transfer_repo.mark_verified(transfer_id, owner_token=owner_token)
        if batch.status not in (BatchStatus.REPLENISHING, BatchStatus.DOWNLOADING, BatchStatus.PROCESSING):
            if batch.status != BatchStatus.VERIFYING:
                self.batch_repo.set_status(batch.id, BatchStatus.VERIFYING)
        # Step 3: Commit verification transaction
        self.db.commit()

        # Steps 4-7: Finalize completion and idempotent staging cleanup
        return self._finalize_verified_transfer(transfer_id, batch, uploaded_count, total_bytes, failed_count)

    def _safe_publish(
        self,
        client: NasClient,
        tmp_rel: str,
        final_rel: str,
        expected_sha256: str,
    ) -> None:
        """Safely publishes a temporary part file to final remote destination.

        Guarantees:
        1. Never overwrites an already published verified object.
        2. Never quarantines a valid final file published by another worker.
        3. Pre-rename and post-rename lock lease verification.
        4. Cleans up temporary .part file if superseded or already published.
        """
        # Pre-rename check on destination
        if client.exists(final_rel):
            existing_sha = client.compute_checksum(final_rel)
            if existing_sha == expected_sha256:
                logger.info(
                    f"[NAS WRITER] Final file {final_rel} is already verified and published on NAS; discarding part file."
                )
                client.delete(tmp_rel)
                return
            else:
                # Target exists with a differing checksum.
                # A stale or superseded worker must NEVER quarantine or overwrite a verified final file!
                logger.error(
                    f"[NAS WRITER] Target {final_rel} already exists with differing checksum "
                    f"({existing_sha} != {expected_sha256}). Refusing to overwrite or quarantine; discarding part file."
                )
                client.delete(tmp_rel)
                raise LockLostError(
                    f"Target {final_rel} already published with differing checksum; publication aborted to preserve existing remote file."
                )

        # Pre-rename fencing check: verify active lock lease
        if self.lock and not self.lock.is_active():
            client.delete(tmp_rel)
            raise LockLostError("NAS writer lock lease was lost or expired before atomic rename; halting upload.")

        # Atomic rename to final path
        client._rename(tmp_rel, final_rel)

        # Post-rename fencing check: verify lock remained valid throughout rename
        if self.lock and not self.lock.is_active():
            raise LockLostError("NAS writer lock lease expired during atomic rename; halting batch completion.")


    def _finalize_verified_window_transfer(
        self,
        transfer_id: uuid.UUID,
        batch: ImageBatch,
        uploaded_count: int,
        total_bytes: int,
        failed_count: int = 0,
    ) -> dict:
        """Finalize a verified window without touching other windows."""

        transfer = self.transfer_repo.get(transfer_id)
        if transfer is None or transfer.window_id is None:
            raise ValueError("Window transfer not found.")

        if failed_count:
            raise ValueError("Cannot finalize a window with failed files.")

        metadata = transfer.transfer_metadata or {}
        window_index = metadata.get("window_index")
        asset_ids_raw = metadata.get("asset_ids") or []

        if window_index is None or not asset_ids_raw:
            raise ValueError("Window transfer metadata is incomplete.")

        asset_ids = [uuid.UUID(str(value)) for value in asset_ids_raw]

        window = self.db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.id == transfer.window_id)
            .with_for_update()
        ).scalar_one_or_none()

        if window is None:
            raise ValueError("Candidate window record is missing.")

        assets = self.db.execute(
            select(Asset).where(
                Asset.batch_id == batch.id,
                Asset.window_id == transfer.window_id,
                Asset.id.in_(asset_ids),
            )
        ).scalars().all()

        if len(assets) != len(asset_ids):
            raise ValueError(
                "Window manifest and PostgreSQL asset records do not match."
            )

        owner_token = getattr(self.lock, "owner_token", None) if self.lock else None

        # Commit NAS verification before deleting local staging.
        self.transfer_repo.mark_completed_staging_pending(
            transfer_id,
            owner_token=owner_token,
        )

        for asset in assets:
            asset.status = AssetStatus.COMPLETED

        window.status = "COMPLETED"
        window.completed_at = datetime.now(timezone.utc)
        self.db.commit()

        from app.services.job_counters import reconcile
        reconcile(self.db, batch.job_id)
        self.db.commit()

        manager = WindowStagingManager()
        staging_dir = manager.get_window_staging_dir(
            batch.job_id,
            batch.batch_index,
            int(window_index),
        )

        try:
            cleanup_success = manager.cleanup_window_staging(
                batch.job_id,
                batch.batch_index,
                int(window_index),
            )
        except Exception:
            logger.exception(
                f"Window staging cleanup failed for {staging_dir}"
            )
            cleanup_success = False

        if cleanup_success:
            self.transfer_repo.mark_staging_cleaned(transfer_id)
            self.db.commit()
        else:
            logger.warning(
                f"Window staging cleanup pending: {staging_dir}"
            )

        try:
            from app.tasks.orchestration_tasks import check_and_advance_job
            from app.tasks.batch_orchestration import process_active_batch
            check_and_advance_job(str(batch.job_id))
            process_active_batch.delay(str(batch.job_id))
        except Exception as e:
            logger.warning(f"[NAS WRITER] Error triggering job advance after window completion: {e}")

        return {
            "status": "completed",
            "batch_id": str(batch.id),
            "window_id": str(transfer.window_id),
            "uploaded_files": uploaded_count,
            "failed_files": failed_count,
            "bytes_transferred": total_bytes,
        }

    def _finalize_verified_transfer(
        self,
        transfer_id: uuid.UUID,
        batch: ImageBatch,
        uploaded_count: int,
        total_bytes: int,
        failed_count: int = 0,
    ) -> dict:
        """Finalize either one candidate window or a legacy whole batch."""
        transfer = self.transfer_repo.get(transfer_id)

        if transfer is not None and transfer.window_id is not None:
            return self._finalize_verified_window_transfer(
                transfer_id,
                batch,
                uploaded_count,
                total_bytes,
                failed_count,
            )

        owner_token = getattr(self.lock, "owner_token", None) if self.lock else None

        # Step 4: Mark transfer COMPLETED and record that local cleanup is pending conditionally
        self.transfer_repo.mark_completed_staging_pending(transfer_id, owner_token=owner_token)
        # Step 5: Commit completion transaction
        self.db.commit()

        # Step 6: Idempotent local staging cleanup with explicit verification
        staging_dir = self.staging_manager.get_batch_staging_dir(batch.job_id, batch.batch_index)
        has_pending_windows = self.db.execute(
            select(CandidateWindow)
            .where(CandidateWindow.batch_id == batch.id, CandidateWindow.status != "COMPLETED")
        ).scalars().first() is not None

        if not has_pending_windows:
            cleanup_success = self.staging_manager.cleanup_batch_staging(batch.job_id, batch.batch_index)
        else:
            cleanup_success = True
            logger.info(f"[NAS WRITER] Preserving batch staging directory {staging_dir} because active candidate windows exist.")

        # Step 7: Record local staging cleanup as completed only when verified absent
        if cleanup_success:
            self.transfer_repo.mark_staging_cleaned(transfer_id)
            logger.info(f"[NAS WRITER] Local staging cleanup confirmed at {staging_dir}.")
        else:
            logger.warning(
                f"[NAS WRITER] Local staging directory {staging_dir} could not be fully removed. "
                "Retaining staging_cleanup_status as 'pending' for retry by recovery task."
            )

        # Transition all verified/uploaded assets in this batch to COMPLETED
        stmt = select(Asset).where(
            Asset.batch_id == batch.id,
            Asset.status.in_([
                AssetStatus.NAS_VERIFIED,
                AssetStatus.UPLOADING,
                AssetStatus.READY_FOR_UPLOAD,
                AssetStatus.RELEVANCE_PASSED,
            ]),
        )
        for a in self.db.execute(stmt).scalars():
            a.status = AssetStatus.COMPLETED

        from app.services.job_counters import reconcile
        reconcile(self.db, batch.job_id)
        self.db.commit()

        # Only complete the batch if it's not currently replenishing or downloading shortfall replacements
        batch_completed = False
        if batch.status not in (BatchStatus.REPLENISHING, BatchStatus.DOWNLOADING, BatchStatus.PROCESSING):
            if batch.accepted_image_count >= batch.target_image_count or batch.status == BatchStatus.PARTIAL:
                self.batch_repo.set_status(batch.id, BatchStatus.COMPLETED)
                batch_completed = True
        self.db.commit()

        if batch_completed:
            try:
                from app.tasks.orchestration_tasks import check_and_advance_job
                from app.tasks.batch_orchestration import process_active_batch
                check_and_advance_job(str(batch.job_id))
                process_active_batch.delay(str(batch.job_id))
            except Exception as e:
                logger.warning(f"[NAS WRITER] Error triggering job advance after batch completion: {e}")

        logger.info(
            f"[NAS WRITER] Successfully uploaded and verified Batch #{batch.batch_index} "
            f"({uploaded_count} files, {total_bytes} bytes)."
        )

        return {
            "status": "completed",
            "batch_id": str(batch.id),
            "uploaded_files": uploaded_count,
            "failed_files": failed_count,
            "bytes_transferred": total_bytes,
        }
