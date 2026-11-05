"""asset_files — the one place that knows an Asset's file may live on a remote NAS.

Asset.nas_path is either an absolute local path (today's behaviour, and still the
default) or "nas://<path>" once the image has been moved to a UI-connected NAS.
Everything that needs the bytes — the file/thumbnail endpoints, export, the
relevance filter, deletion — goes through here, so:

  * with no NAS in play every function is a thin pass-through to the local path,
    exactly what those callers did before; and
  * remote handling (reach the NAS, cache locally, clean up) exists once.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from app.connectors.nas.base import (
    NasAuthError,
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    normalize_relpath,
    open_client,
)
from app.core.config import get_settings
from app.core.exceptions import ConnectorError
from app.services.nas_config_service import get_saved_params

logger = logging.getLogger(__name__)

NAS_SCHEME = "nas://"

# A cached file younger than this is never evicted: a reader that was just handed
# the path must be able to open it even if another request trips the size limit.
_EVICTION_GRACE_S = 300
_EVICT_TO_FRACTION = 0.8


class NasUnreachable(Exception):
    """The image lives on the NAS but the NAS cannot be reached right now (not
    configured, offline, login rejected). Mapped to HTTP 503 by the API."""


def is_remote(path: str | None) -> bool:
    return bool(path) and str(path).startswith(NAS_SCHEME)


def make_remote_path(rel: str) -> str:
    return NAS_SCHEME + normalize_relpath(rel)


def remote_rel(path: str) -> str:
    return normalize_relpath(str(path)[len(NAS_SCHEME):])


def _cache_root() -> Path:
    return get_settings().work_root / "nas_cache"


def _cache_file(path: str) -> Path:
    digest = hashlib.sha1(path.encode()).hexdigest()
    return _cache_root() / digest[:2] / f"{digest}{Path(path).suffix.lower()}"


def thumbnail_cache_file(path: str) -> Path:
    """Where the thumbnail of a remote image is cached. Kept apart from the
    image cache so evicting a full-size image does not throw away its (tiny)
    thumbnail and force another download just to redraw a grid card."""
    digest = hashlib.sha1(path.encode()).hexdigest()
    return _cache_root() / "thumbs" / digest[:2] / f"{digest}.thumb.jpg"


@contextmanager
def _session() -> Iterator[NasClient]:
    """A connected client. Anything that goes wrong *connecting* — including
    "share not found" — means the NAS is unavailable (NasUnreachable), which is
    different from a single file being gone (FileNotFoundError, raised by the
    operations inside the block and translated by _translate)."""
    params = get_saved_params()
    if params is None:
        raise NasUnreachable("This image is stored on the NAS, but no NAS connection is configured.")
    client = open_client(params)
    try:
        client.connect()
    except ConnectorError as exc:
        raise NasUnreachable(f"The NAS could not be reached: {exc}") from exc
    try:
        yield client
    finally:
        client.close()


def _translate(exc: ConnectorError) -> Exception:
    if isinstance(exc, NasNotFoundError):
        return FileNotFoundError(str(exc))
    if isinstance(exc, (NasAuthError, NasConnectionError)):
        return NasUnreachable(f"The NAS could not be reached: {exc}")
    return NasUnreachable(str(exc))


def materialize(path: str) -> Path:
    """A local file path holding the asset's bytes. Local paths are returned
    untouched; remote ones are fetched into the bounded cache (and re-used on the
    next call). Raises FileNotFoundError if the file is gone, NasUnreachable if the
    NAS cannot be reached."""
    if not is_remote(path):
        return Path(path)
    cached = _cache_file(path)
    if cached.exists():
        os.utime(cached)  # recency marker for eviction
        return cached
    try:
        with _session() as client:
            client.download(remote_rel(path), cached)
    except ConnectorError as exc:
        raise _translate(exc) from exc
    _evict_if_needed()
    return cached


def local_path_for_read(path: str) -> str:
    """str-in/str-out variant for callers that pass paths around as strings."""
    return str(materialize(path))


def exists(path: str) -> bool:
    if not is_remote(path):
        return Path(path).exists()
    try:
        with _session() as client:
            return client.exists(remote_rel(path))
    except ConnectorError as exc:
        raise _translate(exc) from exc


def delete(path: str, strict: bool = False) -> None:
    """Remove a stored file. The local branch is the exact operation callers used
    before (unlink, missing is fine). Remote deletes are best-effort like every
    other cleanup in the pipeline: a NAS hiccup is logged, never raised, so it can
    never break filtering or trimming. `strict=True` (used by the reconciler, which
    must know whether the file is really gone) raises NasUnreachable instead."""
    if not is_remote(path):
        Path(path).unlink(missing_ok=True)
        return
    try:
        with _session() as client:
            client.delete(remote_rel(path))
        _cache_file(path).unlink(missing_ok=True)
        thumbnail_cache_file(path).unlink(missing_ok=True)
    except (ConnectorError, NasUnreachable) as exc:
        if strict:
            if isinstance(exc, NasUnreachable):
                raise
            raise _translate(exc) from exc
        logger.warning(f"Could not delete {path} from the NAS: {exc}")


def delete_batch(paths: list[str], strict: bool = False) -> int:
    """Remove a collection of stored files.

    Local files are unlinked directly. Remote files reuse a single NAS session
    rather than connecting per file, and if the NAS is unreachable, remote deletions
    are aborted gracefully after logging once, rather than stalling on thousands of
    failed connection timeouts.
    """
    local_paths: list[str] = []
    remote_paths: list[str] = []

    for p in paths:
        if not p:
            continue
        if is_remote(p):
            remote_paths.append(p)
        else:
            local_paths.append(p)

    deleted = 0
    for lp in local_paths:
        try:
            Path(lp).unlink(missing_ok=True)
            deleted += 1
        except OSError:
            pass

    if remote_paths:
        # Always clean local cache / thumbnail files
        for rp in remote_paths:
            _cache_file(rp).unlink(missing_ok=True)
            thumbnail_cache_file(rp).unlink(missing_ok=True)

        try:
            with _session() as client:
                for rp in remote_paths:
                    try:
                        client.delete(remote_rel(rp))
                        deleted += 1
                    except Exception as exc:
                        logger.warning(f"Could not delete {rp} from NAS: {exc}")
        except (ConnectorError, NasUnreachable) as exc:
            logger.warning(
                f"NAS unreachable during batch deletion of {len(remote_paths)} files: {exc}. Skipping remaining remote deletions."
            )
            if strict:
                raise
        except Exception as exc:
            logger.warning(
                f"Error during remote batch deletion for {len(remote_paths)} files: {exc}"
            )
            if strict:
                raise

    return deleted


def _evict_if_needed() -> None:
    limit = int(get_settings().nas_cache_max_gb * 1024**3)
    root = _cache_root()
    if limit <= 0 or not root.exists():
        return
    files = [(f, f.stat()) for f in root.rglob("*") if f.is_file() and ".thumb." not in f.name]
    total = sum(st.st_size for _, st in files)
    if total <= limit:
        return
    now = time.time()
    for f, st in sorted(files, key=lambda item: item[1].st_mtime):
        if total <= limit * _EVICT_TO_FRACTION:
            break
        if now - st.st_mtime < _EVICTION_GRACE_S:
            continue
        try:
            f.unlink()
            total -= st.st_size
        except OSError:
            pass
