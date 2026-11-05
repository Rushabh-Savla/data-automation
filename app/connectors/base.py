"""Shared connector abstractions.

Orchestration code (app/tasks/discovery_tasks.py, app/tasks/ingest_tasks.py)
resolves connectors through CONNECTOR_REGISTRY / get_connector() and never
imports a concrete connector class directly — adding a new source means
writing one class and registering it, with zero changes to Celery tasks,
API routes, or the Job state machine (Open/Closed principle).
"""

import fcntl
import logging
import random
import shutil
import tarfile
import tempfile
import time
import zipfile
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import requests

from app.connectors.archive_handler import discover_valid_images, is_archive, safe_extract_archive
from app.core.exceptions import FatalConnectorError, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

# Shared request headers for connectors' direct `requests.get()`/`.head()`
# calls to public dataset archives (COCO/Open Images/Pascal VOC/Visual
# Genome/DAVIS/SUN RGB-D/KITTI/nuImages). Plain `requests` sends
# `User-Agent: python-requests/x.y.z` by default — an obvious bot signature
# some hosts (particularly the smaller academic servers this platform pulls
# from: rgbd.cs.princeton.edu, data.vision.ee.ethz.ch, host.robots.ox.ac.uk)
# rate-limit or block more readily than a normal browser UA. Also used by
# RoboflowConnector's Universe search scrape (universe.roboflow.com/search
# is a Cloudflare-fronted page, not a documented API, and 403'd every query
# without this). Kaggle and Roboflow's actual REST API calls (kagglehub,
# api.roboflow.com) go through official SDKs/clients and don't need this.
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def download_with_retry(
    url: str,
    tmp_path: Path,
    *,
    timeout: int = 300,
    max_attempts: int = 5,
    headers: dict | None = None,
) -> None:
    """Streams `url` into `tmp_path`, retrying with HTTP Range resume on a
    dropped connection instead of failing outright on the first transient
    blip — the one-time shared-cache archive downloads (KITTI ~12GB, DAVIS
    ~830MB, Pascal VOC ~2GB, SUN RGB-D ~6.9GB, Visual Genome/nuImages
    metadata) routinely hit a mid-stream connection drop on a real network
    (observed in production: `IncompleteRead`/`ConnectionError` after
    hundreds of MB), and without retry a single drop failed the whole
    discovery task — the caller's own outer try/except just reports it as
    a permanent failure, never retries, and (without resume) even a manual
    re-run would have re-downloaded from byte zero.

    Raises `requests.HTTPError` immediately without retrying — a 403/404
    won't fix itself. Any other exception (dropped connection, timeout,
    chunked-encoding error) is retried up to `max_attempts` times with
    capped exponential backoff before being re-raised, so a genuinely
    unreachable host still surfaces as an error rather than retrying
    forever.

    Callers keep doing their own `tmp_path.replace(dest)` once this
    returns, exactly as before — this only replaces the inner write loop,
    not the surrounding cache/atomic-rename structure every connector
    already has."""
    req_headers = dict(headers or {})
    last_error: Exception | None = None

    for attempt in range(max_attempts):
        resume_from = tmp_path.stat().st_size if tmp_path.exists() else 0
        attempt_headers = dict(req_headers)
        mode = "wb"
        if resume_from:
            attempt_headers["Range"] = f"bytes={resume_from}-"
            mode = "ab"

        try:
            with requests.get(url, stream=True, timeout=timeout, headers=attempt_headers) as r:
                if resume_from and r.status_code == 200:
                    # Server ignored the Range request (doesn't support
                    # resume for this URL) — the response body is the whole
                    # file again, so start over rather than append a full
                    # copy after the partial bytes already on disk.
                    mode = "wb"
                r.raise_for_status()
                with open(tmp_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            return
        except requests.HTTPError:
            raise
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                wait = min(2**attempt, 30)
                logger.warning(
                    f"Download of {url} interrupted ({e}); retrying in {wait}s "
                    f"(attempt {attempt + 2}/{max_attempts})"
                )
                time.sleep(wait)

    raise last_error


@contextmanager
def exclusive_cache_lock(lock_path: Path) -> Iterator[None]:
    """Cross-process lock around a shared-cache download/extract.

    A job's download shards run as a Celery chord on a multi-process worker,
    and every shard calls its connector's `_ensure_*_cached()` before touching
    an image. Without this, N shards all see "archive missing" at once and all
    append to the same `<archive>.tmp` (download_with_retry resumes from the
    tmp's size), which produced a 15GB "SUNRGBD.zip" for a 6.9GB archive and a
    permanent `Corrupt zip64 end of central directory record`.

    Callers take the lock, re-check whether another worker finished while they
    waited, and only then download. `flock` is per open file description, so it
    also serialises threads within one process.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


@contextmanager
def limited_concurrency(
    lock_dir: Path, name: str, slots: int, *, poll_s: float = 0.05, max_wait_s: float | None = None
) -> Iterator[None]:
    """At most `slots` holders at once, across every process and thread on this host.

    A counting semaphore built from `slots` lock files. The workers all mount
    the same work directory, so this bounds requests to one small server no
    matter how many shards, threads or worker containers are running.

    Needed because per-shard limits multiply: 8 worker slots x 4 fetch threads
    was ~32 simultaneous fresh connections to the SUN RGB-D host, which started
    dropping them (RemoteDisconnected on ~110 requests in 15 minutes) and then
    refused this machine altogether for a while. Hold it only for the duration
    of one request.

    `max_wait_s` bounds the wait itself (default: wait forever, the original
    behavior) — without it, a slot leaked by a wedged holder (a connection
    stuck mid-handshake, say) can block every future caller with no visible
    error at all. Raises `TimeoutError` if no slot opens up in time.
    """
    lock_dir.mkdir(parents=True, exist_ok=True)
    handle = None
    started = time.monotonic()
    while handle is None:
        for slot in random.sample(range(slots), slots):  # random order spreads contention
            candidate = open(lock_dir / f".{name}.slot{slot}", "a")  # noqa: SIM115 - held for the slot's lifetime
            try:
                fcntl.flock(candidate, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                candidate.close()
                continue
            handle = candidate
            break
        else:
            if max_wait_s is not None and time.monotonic() - started >= max_wait_s:
                raise TimeoutError(f"Timed out after {max_wait_s:.0f}s waiting for a free '{name}' connection slot")
            time.sleep(poll_s * (1 + random.random()))
    try:
        yield
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


def ensure_archive(
    url: str,
    archive_path: Path,
    *,
    label: str,
    timeout: int,
    validate: Callable[[Path], bool] | None = None,
    required_free_bytes: int | None = None,
) -> None:
    """Makes sure `archive_path` holds a complete, valid download of `url`.

    Must be called with the cache lock held (see exclusive_cache_lock).

    A cached archive that fails `validate` is discarded and fetched again, so a
    bad file left by an earlier run heals itself instead of failing every retry
    -- the old `if not archive.exists()` guard trusted whatever was on disk. A
    fresh download is validated before it is renamed into place; if it is bad
    the tmp is dropped and a TransientConnectorError is raised so the task's
    own retry starts from byte zero rather than resuming a corrupt tmp.

    `required_free_bytes` (download plus extraction) is checked before a fresh
    download starts. The archives here run to tens of GB and the shared cache
    lives on the same disk as the database, so running out mid-download is not
    a harmless failure; it is refused up front with the numbers instead.
    """
    if archive_path.exists() and validate is not None and not validate(archive_path):
        logger.warning(f"Cached {label} archive {archive_path} is corrupt; discarding it to re-download")
        archive_path.unlink()
    if archive_path.exists():
        return

    tmp_path = archive_path.with_name(archive_path.name + ".tmp")
    if required_free_bytes is not None:
        # A partial tmp from an earlier attempt is resumed, so it is already paid for.
        already_downloaded = tmp_path.stat().st_size if tmp_path.exists() else 0
        free = shutil.disk_usage(archive_path.parent).free + already_downloaded
        if free < required_free_bytes:
            raise FatalConnectorError(
                f"Not enough free disk space to fetch {label}: needs about "
                f"{required_free_bytes / 1e9:.1f} GB (download + extraction), "
                f"{free / 1e9:.1f} GB free in {archive_path.parent}"
            )

    logger.info(f"Downloading {label} ({url})")
    try:
        download_with_retry(url, tmp_path, timeout=timeout, headers=HTTP_HEADERS)
    except requests.HTTPError as e:
        raise FatalConnectorError(f"{label} archive not accessible: {e}") from e

    if validate is not None and not validate(tmp_path):
        tmp_path.unlink(missing_ok=True)
        raise TransientConnectorError(f"Downloaded {label} archive failed validation; will re-download")
    tmp_path.replace(archive_path)


def extract_into_cache(archive_path: Path, cache_dir: Path, sentinel: str, remove_archive: bool = False) -> None:
    """Extracts a .zip/.tgz into `cache_dir` so `cache_dir/sentinel` only ever
    exists once the whole archive is in place.

    Extracting straight into the cache dir meant an interrupted run left a
    half-populated top-level directory, which the connectors' `exists()` check
    then treated as a finished cache forever. Here the archive is unpacked into
    a staging dir on the same filesystem, its top-level entries are moved into
    place, and the sentinel entry is moved last.

    Must be called with the cache lock held. A corrupt archive is deleted and
    reported as transient so the retry downloads a fresh copy. `remove_archive`
    deletes the archive once it has been extracted, for archives too big to keep
    both copies of.
    """
    for stale in cache_dir.glob(".extract-*"):
        shutil.rmtree(stale, ignore_errors=True)

    staging = Path(tempfile.mkdtemp(prefix=".extract-", dir=cache_dir))
    try:
        try:
            if archive_path.suffix == ".zip":
                with zipfile.ZipFile(archive_path) as zf:
                    zf.extractall(staging)
            else:
                with tarfile.open(archive_path) as tf:
                    tf.extractall(staging, filter="data")
        except (zipfile.BadZipFile, tarfile.TarError, EOFError, zlib.error) as e:
            archive_path.unlink(missing_ok=True)
            raise TransientConnectorError(
                f"{archive_path.name} is corrupt ({e}); discarded, will re-download"
            ) from e

        for entry in sorted(staging.iterdir(), key=lambda p: p.name == sentinel):
            target = cache_dir / entry.name
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            entry.replace(target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    if not (cache_dir / sentinel).exists():
        raise FatalConnectorError(f"{archive_path.name} did not contain the expected '{sentinel}' entry")
    if remove_archive:
        archive_path.unlink(missing_ok=True)


@dataclass
class SourceItem:
    """One unit of discovered work (a dataset/project/video/image-batch reference)."""

    source_type: SourceType
    external_ref: dict
    license_hint: str | None = None
    dataset_identifier: str | None = None


@dataclass
class DiscoveryPage:
    items: list[SourceItem]
    next_cursor: str | None
    exhausted: bool
    # Set by a connector when it has zero items to report *and* knows why
    # (e.g. the search endpoint was blocked/unreachable rather than genuinely
    # returning no matches) — lets task-level error_message text distinguish
    # "we searched and there's nothing" from "the search itself failed",
    # which otherwise look identical to an operator.
    error_reason: str | None = None
    # Set with a zero-item page when it's a temporary condition (e.g. a
    # rate-limit cooldown), not real exhaustion — tells the caller to retry
    # in ~N seconds instead of marking the source done. None (default, and
    # every existing connector) keeps the normal exhausted-page handling.
    retry_after_s: float | None = None


@dataclass
class RawAssetDraft:
    """Connector output before standardization: local temp files + metadata,
    not yet validated/checksummed/written to NAS/recorded in the DB."""

    source_item_id: str
    source_url: str | None
    dataset_identifier: str | None
    license: str | None
    local_tmp_path: Path
    original_name: str | None = None
    annotation_tmp_path: Path | None = None
    annotation_format: str | None = None
    width: int | None = None
    height: int | None = None
    classes: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    buffer: Any = None


# Injected by the task wrapper: returns True if the job has been paused/cancelled.
ShouldStopFn = Callable[[], bool]
# Injected by the task wrapper: called with a progress delta dict
# (e.g. {"discovered": 1} or {"checkpoint": {...}}) after each unit of work.
OnProgressFn = Callable[[dict], None]


class DiscoveryConnector(Protocol):
    def discover(self, query: str, cursor: str | None, limit: int) -> DiscoveryPage: ...


class DownloadConnector(Protocol):
    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]: ...


CONNECTOR_REGISTRY: dict[SourceType, type] = {}


def register_connector(source_type: SourceType):
    def decorator(cls):
        CONNECTOR_REGISTRY[source_type] = cls
        return cls

    return decorator


def get_connector(source_type: SourceType, **kwargs):
    cls = CONNECTOR_REGISTRY.get(source_type)
    if cls is None:
        raise ValueError(f"No connector registered for source type: {source_type}")
    return cls(**kwargs)
