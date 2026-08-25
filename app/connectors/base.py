"""Shared connector abstractions.

Orchestration code (app/tasks/discovery_tasks.py, app/tasks/ingest_tasks.py)
resolves connectors through CONNECTOR_REGISTRY / get_connector() and never
imports a concrete connector class directly — adding a new source means
writing one class and registering it, with zero changes to Celery tasks,
API routes, or the Job state machine (Open/Closed principle).
"""

import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import requests

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
