"""ram_buffer — Bounded RAM buffering, memory pool coordination, and local SSD staging.

Provides:
  - MemoryManager: Global, byte-based RAM accounting coordinated across Celery
    worker processes via Redis (with lease TTLs so killed/crashed workers cannot
    leak allocations permanently). Falls back to thread-safe local accounting
    when Redis is unavailable (e.g. offline unit testing).
  - SpooledCandidateBuffer: In-memory/spooled candidate container backed by
    tempfile.SpooledTemporaryFile. Small files remain in RAM; large files spill
    to disk. Provides zero-disk-copy SHA256, pHash, CLIP embedding, and PIL
    validation. Releases its RAM lease on close.
  - LocalStagingManager: Manages durable on-disk staging for accepted batch images
    prior to NAS offload, tracking local SSD capacity and enforcing high-water (80%)
    and resume (65%) backpressure.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import shutil
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator

from PIL import Image

from app.core.config import Settings, get_settings
from app.core.redis_client import get_redis

logger = logging.getLogger(__name__)

# Redis keys for distributed RAM accounting
_REDIS_RAM_ALLOCATED_KEY = "storage:ram:allocated_bytes"
_REDIS_RAM_LEASE_PREFIX = "storage:ram:lease:"
_REDIS_RAM_ACTIVE_LEASES_SET = "storage:ram:active_leases"
_REDIS_STAGING_BACKPRESSURE_KEY = "storage:staging:backpressure"

# Default spool threshold: images <= 5MB stay in memory; larger spill to disk
DEFAULT_MAX_SPOOL_BYTES = 5 * 1024 * 1024
DEFAULT_LEASE_TTL_S = 120


class RAMBudgetExceededError(RuntimeError):
    """Raised when the global RAM buffer quota is exhausted and cannot be allocated within timeout."""


class StagingBackpressureError(RuntimeError):
    """Raised or used to signal that local SSD staging is above high watermark (80%)."""


# Lua script for atomic acquisition across distributed worker processes
_LUA_ACQUIRE = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local requested = tonumber(ARGV[1])
local max_limit = tonumber(ARGV[2])
local lease_id = ARGV[3]
local ttl = tonumber(ARGV[4])
local owner = ARGV[5]

if (current + requested) <= max_limit then
    redis.call('INCRBY', KEYS[1], requested)
    -- Store format: size:owner
    redis.call('SET', KEYS[2], tostring(requested) .. ':' .. tostring(owner), 'EX', ttl)
    redis.call('SADD', KEYS[3], lease_id)
    return 1
else
    return 0
end
"""

# Lua script for atomic release with ownership validation
_LUA_RELEASE = """
local lease_val = redis.call('GET', KEYS[2])
if lease_val then
    local separator = string.find(lease_val, ":")
    local size = tonumber(string.sub(lease_val, 1, separator - 1))
    local owner = string.sub(lease_val, separator + 1)

    local caller_owner = ARGV[2]
    if caller_owner and caller_owner ~= "" and owner ~= caller_owner then
        -- Attempt to release another worker's allocation: prohibited
        return -1
    end

    redis.call('DEL', KEYS[2])
    redis.call('SREM', KEYS[3], ARGV[1])
    local current = tonumber(redis.call('GET', KEYS[1]) or '0')
    local new_val = current - size
    if new_val < 0 then new_val = 0 end
    redis.call('SET', KEYS[1], new_val)
    return size
else
    redis.call('SREM', KEYS[3], ARGV[1])
    return 0
end
"""

# Lua script for extending lease TTL
_LUA_RENEW = """
local lease_val = redis.call('GET', KEYS[1])
if lease_val then
    local separator = string.find(lease_val, ":")
    local owner = string.sub(lease_val, separator + 1)
    local caller_owner = ARGV[2]
    if caller_owner and caller_owner ~= "" and owner ~= caller_owner then
        return -1
    end
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
    return 1
else
    return 0
end
"""


@dataclass
class MemoryLease:
    lease_id: str
    size_bytes: int
    created_at: float
    manager: MemoryManager
    owner_id: str = ""
    released: bool = False

    def release(self) -> int:
        if not self.released:
            self.released = True
            return self.manager.release(self)
        return 0

    def renew(self, ttl_s: int | None = None) -> bool:
        """Extends lease TTL to prevent premature expiration during long tasks."""
        if not self.released:
            return self.manager.renew(self, ttl_s)
        return False

    def __enter__(self) -> MemoryLease:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class MemoryManager:
    """Byte-based RAM buffer budget manager.

    Coordinates globally across Celery worker processes via Redis atomic Lua scripts
    and per-lease TTL keys. If a worker process abruptly dies or is killed,
    orphaned lease keys expire automatically in Redis and are reconciled,
    preventing permanent budget leakage.
    """

    def __init__(
        self,
        max_bytes: int | None = None,
        lease_ttl_s: int = DEFAULT_LEASE_TTL_S,
        allow_local_fallback: bool | None = None,
    ):
        self.settings: Settings = get_settings()
        self.max_bytes: int = (
            max_bytes if max_bytes is not None else self.settings.ram_buffer_max_bytes
        )
        self.lease_ttl_s: int = lease_ttl_s
        self.owner_id: str = f"{os.getpid()}_{uuid.uuid4().hex[:6]}"
        # If not explicitly specified, allow local fallback during unit tests (PYTEST_CURRENT_TEST),
        # but in production fail closed if Redis is down to enforce global limits.
        if allow_local_fallback is not None:
            self.allow_local_fallback = allow_local_fallback
        else:
            self.allow_local_fallback = bool(os.environ.get("PYTEST_CURRENT_TEST"))

        self._local_lock = threading.Lock()
        self._local_allocated: int = 0
        self._local_leases: dict[str, tuple[int, str]] = {}

    def _get_redis_safe(self):
        try:
            client = get_redis()
            client.ping()
            return client
        except Exception:
            return None

    def acquire(self, size_bytes: int, timeout_s: float = 30.0) -> MemoryLease:
        """Acquires a RAM lease for `size_bytes`.

        Blocks/waits up to `timeout_s` if current memory usage would exceed `max_bytes`.
        Raises `RAMBudgetExceededError` if quota cannot be fulfilled within `timeout_s`
        or if Redis is unavailable in production (fail-closed).
        """
        if size_bytes <= 0:
            return MemoryLease(
                lease_id=str(uuid.uuid4()),
                size_bytes=0,
                created_at=time.time(),
                manager=self,
                owner_id=self.owner_id,
            )

        if size_bytes > self.max_bytes:
            raise RAMBudgetExceededError(
                f"Requested allocation {size_bytes} bytes exceeds total global capacity {self.max_bytes} bytes"
            )

        deadline = time.monotonic() + timeout_s
        lease_id = str(uuid.uuid4())

        while True:
            r = self._get_redis_safe()
            if r is not None:
                allocated = self._acquire_redis(r, lease_id, size_bytes)
                if allocated:
                    return MemoryLease(
                        lease_id=lease_id,
                        size_bytes=size_bytes,
                        created_at=time.time(),
                        manager=self,
                        owner_id=self.owner_id,
                    )
            elif self.allow_local_fallback:
                # In-memory fallback (allowed in testing/offline dev)
                allocated = self._acquire_local(lease_id, size_bytes)
                if allocated:
                    return MemoryLease(
                        lease_id=lease_id,
                        size_bytes=size_bytes,
                        created_at=time.time(),
                        manager=self,
                        owner_id=self.owner_id,
                    )
            else:
                raise RAMBudgetExceededError(
                    "Redis coordination is unavailable and local fallback is disabled in production. "
                    "Failing closed to prevent unbounded RAM usage across worker processes."
                )

            if time.monotonic() >= deadline:
                current_used = self.get_allocated_bytes()
                raise RAMBudgetExceededError(
                    f"RAM buffer budget full: cannot allocate {size_bytes} bytes within {timeout_s}s. "
                    f"Current usage: {current_used}/{self.max_bytes} bytes"
                )

            # Backpressure sleep before retrying
            time.sleep(0.05)

    def _acquire_redis(self, r, lease_id: str, size_bytes: int) -> bool:
        lease_key = f"{_REDIS_RAM_LEASE_PREFIX}{lease_id}"
        keys = [_REDIS_RAM_ALLOCATED_KEY, lease_key, _REDIS_RAM_ACTIVE_LEASES_SET]
        args = [size_bytes, self.max_bytes, lease_id, self.lease_ttl_s, self.owner_id]
        try:
            res = r.eval(_LUA_ACQUIRE, len(keys), *keys, *args)
            return bool(res == 1)
        except Exception as e:
            if self.allow_local_fallback:
                logger.warning(f"Redis memory allocation error: {e}, falling back to local lock")
                return self._acquire_local(lease_id, size_bytes)
            logger.error(f"Redis memory allocation failed: {e}")
            return False

    def _acquire_local(self, lease_id: str, size_bytes: int) -> bool:
        with self._local_lock:
            if self._local_allocated + size_bytes <= self.max_bytes:
                self._local_allocated += size_bytes
                self._local_leases[lease_id] = (size_bytes, self.owner_id)
                return True
            return False

    def release(self, lease: MemoryLease) -> int:
        """Releases the memory reserved by `lease` after validating ownership."""
        if lease.size_bytes <= 0:
            return 0

        r = self._get_redis_safe()
        if r is not None:
            lease_key = f"{_REDIS_RAM_LEASE_PREFIX}{lease.lease_id}"
            keys = [_REDIS_RAM_ALLOCATED_KEY, lease_key, _REDIS_RAM_ACTIVE_LEASES_SET]
            args = [lease.lease_id, lease.owner_id]
            try:
                freed = r.eval(_LUA_RELEASE, len(keys), *keys, *args)
                if freed == -1:
                    raise PermissionError(
                        f"Cannot release lease {lease.lease_id}: ownership mismatch (owned by another worker)"
                    )
                return int(freed or 0)
            except PermissionError:
                raise
            except Exception as e:
                logger.warning(f"Redis memory release error: {e}, releasing locally")

        with self._local_lock:
            stored = self._local_leases.get(lease.lease_id)
            if stored is not None:
                size, owner = stored
                if lease.owner_id and owner and lease.owner_id != owner:
                    raise PermissionError(
                        f"Cannot release lease {lease.lease_id}: ownership mismatch (local)"
                    )
                self._local_leases.pop(lease.lease_id)
                self._local_allocated = max(0, self._local_allocated - size)
                return size
            return 0

    def renew(self, lease: MemoryLease, ttl_s: int | None = None) -> bool:
        """Renews the lease TTL in Redis to support long tasks."""
        if lease.size_bytes <= 0:
            return True
        ttl = ttl_s or self.lease_ttl_s
        r = self._get_redis_safe()
        if r is not None:
            lease_key = f"{_REDIS_RAM_LEASE_PREFIX}{lease.lease_id}"
            keys = [lease_key]
            args = [ttl, lease.owner_id]
            try:
                res = r.eval(_LUA_RENEW, len(keys), *keys, *args)
                return bool(res == 1)
            except Exception as e:
                logger.warning(f"Failed to renew lease {lease.lease_id}: {e}")
                return False
        return True

    def get_allocated_bytes(self) -> int:
        """Returns the current number of allocated RAM bytes."""
        r = self._get_redis_safe()
        if r is not None:
            try:
                val = r.get(_REDIS_RAM_ALLOCATED_KEY)
                return int(val) if val is not None else 0
            except Exception:
                pass
        with self._local_lock:
            return self._local_allocated

    def reconcile_leases(self) -> int:
        """Reconciles Redis memory accounting by sweeping active leases whose TTL has expired.

        Protects against permanent memory leakage if a worker process was killed mid-task.
        """
        r = self._get_redis_safe()
        if r is None:
            return self.get_allocated_bytes()

        try:
            active_ids = r.smembers(_REDIS_RAM_ACTIVE_LEASES_SET) or set()
            total_active = 0
            expired_ids = []

            for raw_id in active_ids:
                lid = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
                lease_key = f"{_REDIS_RAM_LEASE_PREFIX}{lid}"
                val = r.get(lease_key)
                if val is not None:
                    sval = val.decode() if isinstance(val, bytes) else str(val)
                    size_str = sval.split(":", 1)[0]
                    total_active += int(size_str)
                else:
                    expired_ids.append(lid)

            if expired_ids:
                r.srem(_REDIS_RAM_ACTIVE_LEASES_SET, *expired_ids)
                logger.info(f"[RAM RECONCILIATION] Cleaned up {len(expired_ids)} expired lease(s)")

            r.set(_REDIS_RAM_ALLOCATED_KEY, total_active)
            return total_active
        except Exception as e:
            logger.warning(f"Error during RAM lease reconciliation: {e}")
            return self.get_allocated_bytes()

    @contextlib.contextmanager
    def reserve(self, size_bytes: int, timeout_s: float = 30.0) -> Iterator[MemoryLease]:
        """Context manager for acquiring and safely releasing a RAM lease."""
        lease = self.acquire(size_bytes, timeout_s=timeout_s)
        try:
            yield lease
        finally:
            lease.release()


class SpooledCandidateBuffer:
    """Temporary storage for a single downloaded candidate image.

    Backed by tempfile.SpooledTemporaryFile:
      - Resides purely in RAM if size <= max_spool_bytes (default 5MB).
      - Automatically rolls over to temporary SSD disk if size > max_spool_bytes.
      - Provides in-memory image validation, checksumming, pHash, and CLIP
        embeddings without intermediate disk writes.
      - Tied to a MemoryLease; upon close(), buffer resources and RAM allocations
        are cleanly freed.
    """

    def __init__(
        self,
        memory_manager: MemoryManager | None = None,
        max_spool_bytes: int = DEFAULT_MAX_SPOOL_BYTES,
        initial_reservation: int = 1024 * 1024,  # Default 1MB initial estimate
    ):
        self.memory_manager = memory_manager or MemoryManager()
        self.max_spool_bytes = max_spool_bytes
        self._file = tempfile.SpooledTemporaryFile(max_size=max_spool_bytes, mode="w+b")
        self.lease: MemoryLease = self.memory_manager.acquire(initial_reservation)
        self.total_bytes: int = 0
        self._closed: bool = False

    @property
    def is_rolled_to_disk(self) -> bool:
        """True if the SpooledTemporaryFile exceeded max_spool_bytes and rolled to disk."""
        return getattr(self._file, "_rolled", False)

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed SpooledCandidateBuffer")
        n = self._file.write(data)
        self.total_bytes += n

        # If actual bytes written exceed our initial lease reservation, adjust lease
        if self.total_bytes > self.lease.size_bytes:
            extra = self.total_bytes - self.lease.size_bytes
            try:
                extra_lease = self.memory_manager.acquire(extra, timeout_s=10.0)
                # Combine size into existing lease and release the auxiliary lease
                self.lease.size_bytes += extra_lease.size_bytes
                extra_lease.size_bytes = 0
                extra_lease.release()
            except RAMBudgetExceededError:
                # If we cannot expand RAM allocation, buffer will automatically spill to disk
                pass
        return n

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._file.seek(offset, whence)

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def get_bytes(self) -> bytes:
        """Reads all bytes without changing the caller's logical position."""
        pos = self._file.tell()
        self._file.seek(0)
        content = self._file.read()
        self._file.seek(pos)
        return content

    def to_pil_image(self) -> Image.Image:
        """Opens and returns a PIL Image directly from the in-memory stream."""
        self._file.seek(0)
        return Image.open(self._file)

    def validate_image(self, min_w: int = 64, min_h: int = 64) -> tuple[int, int]:
        """Validates image integrity and resolution directly from buffer without disk writes."""
        self._file.seek(0)
        try:
            with Image.open(self._file) as img:
                img.verify()
        except Exception as e:
            raise ValueError(f"Corrupt or unreadable image stream: {e}") from e

        self._file.seek(0)
        with Image.open(self._file) as img:
            width, height = img.size

        if width < min_w or height < min_h:
            raise ValueError(f"Image {width}x{height} below minimum resolution {min_w}x{min_h}")
        return width, height

    def compute_sha256(self) -> str:
        """Computes SHA256 checksum streaming from the buffer without intermediate disk saving."""
        pos = self._file.tell()
        self._file.seek(0)
        h = hashlib.sha256()
        while chunk := self._file.read(65536):
            h.update(chunk)
        self._file.seek(pos)
        return h.hexdigest()

    def compute_phash(self) -> str | None:
        """Computes perceptual hash (pHash) directly from in-memory PIL image."""
        try:
            import imagehash

            self._file.seek(0)
            with Image.open(self._file) as img:
                return str(imagehash.phash(img))
        except Exception as e:
            logger.debug(f"pHash calculation failed on buffer: {e}")
            return None

    def compute_clip_embedding(self) -> list[float] | None:
        """Computes normalized CLIP embedding directly from in-memory PIL image."""
        try:
            import torch
            from app.services.standardization_service import _load_clip_model

            model, preprocess, device = _load_clip_model()
            self._file.seek(0)
            with Image.open(self._file) as img:
                image_input = preprocess(img.convert("RGB")).unsqueeze(0).to(device)
            with torch.no_grad():
                features = model.encode_image(image_input)
                features = features / features.norm(dim=-1, keepdim=True)
            return features.squeeze(0).cpu().tolist()
        except Exception as e:
            logger.debug(f"CLIP embedding failed on buffer: {e}")
            return None

    def stage_to_file(self, dest_path: Path) -> Path:
        """Atomically writes buffer contents to destination path on SSD."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest_path.with_suffix(f"{dest_path.suffix}.tmp_{uuid.uuid4().hex[:8]}")
        self._file.seek(0)
        with open(tmp_path, "wb") as f_out:
            shutil.copyfileobj(self._file, f_out)
        tmp_path.replace(dest_path)
        return dest_path

    def close(self):
        """Closes the spooled file and releases the global RAM lease."""
        if not self._closed:
            self._closed = True
            try:
                self._file.close()
            finally:
                self.lease.release()

    def __enter__(self) -> SpooledCandidateBuffer:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class LocalStagingManager:
    """Manages local SSD staging directory and backpressure watermarks.

    Directory structure:
      {local_staging_path}/job_{job_id}/batch_{batch_index}/images/{asset_id}.ext
      {local_staging_path}/job_{job_id}/batch_{batch_index}/manifest.json

    Backpressure:
      - High watermark (80%): pauses candidate downloads across all jobs.
      - Resume watermark (65%): resumes downloads once staged batches are uploaded and cleaned.
      - Between 65% and 80% (e.g. at 75%), state MUST remain paused if already paused.
      - Fails safe by activating backpressure if disk metrics are missing or unavailable.
    """

    _shared_backpressure: bool = False
    _lock = threading.Lock()

    def __init__(self, staging_path: Path | None = None, max_bytes: int | None = None):
        self.settings = get_settings()
        self.staging_path = Path(staging_path or self.settings.local_staging_path).resolve()
        self.max_bytes = max_bytes if max_bytes is not None else self.settings.local_staging_max_bytes
        self.high_watermark = self.settings.local_staging_high_watermark
        self.resume_watermark = self.settings.local_staging_resume_watermark
        self.staging_path.mkdir(parents=True, exist_ok=True)

    def _get_redis_safe(self):
        try:
            client = get_redis()
            client.ping()
            return client
        except Exception:
            return None

    def get_staging_disk_usage(self) -> tuple[int, float]:
        """Calculates total bytes stored in staging directory and usage fraction against max_bytes.

        Also safeguards against physical disk exhaustion if filesystem free space drops below 2GB.
        """
        staging_size = 0
        if not self.staging_path.exists():
            raise FileNotFoundError(f"Staging path {self.staging_path} does not exist")

        for root, _, files in os.walk(self.staging_path):
            for f in files:
                try:
                    staging_size += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
        if self.max_bytes <= 0:
            return staging_size, 1.0
        quota_fraction = staging_size / self.max_bytes

        # Physical safety check: if disk free space is below 2GB, enforce emergency backpressure
        try:
            total_fs, used_fs, free_fs = shutil.disk_usage(self.staging_path)
            if free_fs < 2 * 1024 * 1024 * 1024:  # Under 2GB physical free disk
                return staging_size, 0.99
        except OSError:
            pass

        return staging_size, quota_fraction

    def is_staging_backpressure_active(self) -> bool:
        """Evaluates staging disk backpressure with hysteresis:

        Triggers at >= high_watermark (0.80), clears only when <= resume_watermark (0.65).
        Between resume and high watermark, strictly preserves the previous active/paused state.
        If staging metrics are missing, invalid, or an exception occurs, fails safely by activating backpressure (returns True).
        """
        try:
            _, fraction = self.get_staging_disk_usage()
            if fraction is None or not (0.0 <= fraction <= 10.0):
                logger.error("[STAGING BACKPRESSURE] Invalid disk usage fraction. Failing safely with backpressure active.")
                return True
        except Exception as e:
            logger.error(f"[STAGING BACKPRESSURE] Failed to get staging disk usage: {e}. Failing safely with backpressure active.")
            return True

        marker_file = self.staging_path / ".staging_backpressure_active"
        r = self._get_redis_safe()
        with LocalStagingManager._lock:
            currently_active = LocalStagingManager._shared_backpressure
        if r is not None:
            try:
                currently_active = bool(r.exists(_REDIS_STAGING_BACKPRESSURE_KEY))
                with LocalStagingManager._lock:
                    LocalStagingManager._shared_backpressure = currently_active
            except Exception:
                pass
        elif marker_file.exists():
            currently_active = True
            with LocalStagingManager._lock:
                LocalStagingManager._shared_backpressure = True

        if fraction >= self.high_watermark:
            with LocalStagingManager._lock:
                LocalStagingManager._shared_backpressure = True
            try:
                marker_file.touch(exist_ok=True)
            except Exception:
                pass
            if not currently_active:
                logger.warning(
                    f"[STAGING BACKPRESSURE] Staging disk usage ({fraction * 100:.1f}%) "
                    f"exceeded high watermark ({self.high_watermark * 100:.0f}%). Pausing downloads."
                )
            if r is not None:
                try:
                    r.set(_REDIS_STAGING_BACKPRESSURE_KEY, "1")
                except Exception:
                    pass
            return True

        if fraction <= self.resume_watermark:
            with LocalStagingManager._lock:
                LocalStagingManager._shared_backpressure = False
            try:
                if marker_file.exists():
                    marker_file.unlink()
            except Exception:
                pass
            if currently_active:
                logger.info(
                    f"[STAGING BACKPRESSURE] Staging disk usage dropped to ({fraction * 100:.1f}%) "
                    f"<= resume watermark ({self.resume_watermark * 100:.0f}%). Resuming downloads."
                )
            if r is not None:
                try:
                    r.delete(_REDIS_STAGING_BACKPRESSURE_KEY)
                except Exception:
                    pass
            return False

        # Between resume and high watermark (e.g., 75%): maintain previous state
        return currently_active

    def get_batch_staging_dir(self, job_id: uuid.UUID, batch_index: int) -> Path:
        """Returns the isolated local staging folder for a specific batch."""
        return self.staging_path / f"job_{job_id}" / f"batch_{batch_index:03d}"

    def stage_accepted_image(
        self,
        job_id: uuid.UUID,
        batch_index: int,
        asset_id: uuid.UUID,
        file_ext: str,
        source: SpooledCandidateBuffer | bytes | Path,
    ) -> Path:
        """Durably stages an accepted asset into local SSD staging:

        {staging_path}/job_{job_id}/batch_{batch_index}/images/{asset_id}.ext
        """
        ext = f".{file_ext.lstrip('.')}" if file_ext else ".jpg"
        batch_dir = self.get_batch_staging_dir(job_id, batch_index)
        images_dir = batch_dir / "images"
        images_dir.mkdir(parents=True, exist_ok=True)

        dest_path = images_dir / f"{asset_id}{ext}"

        if isinstance(source, SpooledCandidateBuffer):
            source.stage_to_file(dest_path)
        elif isinstance(source, Path):
            tmp_path = dest_path.with_suffix(f"{dest_path.suffix}.tmp_{uuid.uuid4().hex[:8]}")
            shutil.copyfile(source, tmp_path)
            tmp_path.replace(dest_path)
        elif isinstance(source, (bytes, bytearray)):
            tmp_path = dest_path.with_suffix(f"{dest_path.suffix}.tmp_{uuid.uuid4().hex[:8]}")
            tmp_path.write_bytes(source)
            tmp_path.replace(dest_path)
        else:
            raise TypeError(f"Unsupported source type for staging: {type(source)}")

        return dest_path

    def cleanup_candidate(self, source: SpooledCandidateBuffer | Path | None) -> None:
        """Immediately cleans up temporary buffers or files for rejected or duplicate images."""
        if source is None:
            return
        if isinstance(source, SpooledCandidateBuffer):
            source.close()
        elif isinstance(source, Path) and source.exists():
            try:
                source.unlink()
            except OSError as e:
                logger.warning(f"Could not delete temporary candidate file {source}: {e}")

    def cleanup_batch_staging(self, job_id: uuid.UUID, batch_index: int) -> bool:
        """Deletes local staging directory for a batch after its images are safely confirmed on NAS.

        Performs explicit post-cleanup verification to guarantee the directory is gone.
        Returns True if the directory is confirmed absent; returns False if any files/directory remain.
        """
        batch_dir = self.get_batch_staging_dir(job_id, batch_index)
        if not batch_dir.exists():
            return True

        try:
            shutil.rmtree(batch_dir)
        except OSError as e:
            logger.error(f"[STAGING CLEANUP] Failed to remove batch staging folder {batch_dir}: {e}")
            return False

        # Explicit post-deletion existence verification
        if batch_dir.exists():
            logger.error(f"[STAGING CLEANUP] Staging directory still exists after deletion attempt: {batch_dir}")
            return False

        logger.info(f"[STAGING CLEANUP] Confirmed removal of local staging for batch at {batch_dir}")
        return True
