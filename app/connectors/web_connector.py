"""WebScrapeConnector — net-new connector replacing ddg_downloader.py.

ddg_downloader.py's YOLOv8-COCO-80-class + K-means dominant-color
verification block (including the confirmed `target_xclasses` NameError bug
at its line 185) is NOT ported here. It's superseded by the shared
YOLO-World relevance-filter stage (app/filtering/yolo_world_filter.py),
which is open-vocabulary (not limited to 80 COCO classes) and runs once,
shared across every noisy source, rather than being reimplemented per
connector. This also finally gives the web source real provenance tracking —
the legacy script wrote no manifest at all.

Discovery uses `ddgs` (same library the legacy script used) to find candidate
image URLs, but only to build a *validated* URL list — each candidate must
pass, before it's queued for download:
  - a robots.txt check against the source host
  - an AI opt-out check (X-Robots-Tag: noai / noimageai response header)
  - a Content-Type/size check (must look like a real image, not an HTML
    error page, placeholder, or zero-byte response)

Download is delegated to `img2dataset` for high-throughput, retrying,
concurrency-controlled fetching — replacing the legacy script's ad-hoc
per-image `requests.get()` loop.
"""

import csv
import hashlib
import json
import logging
import urllib.robotparser
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import requests

from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

_AI_OPTOUT_MARKERS = ("noai", "noimageai")
_MIN_BYTES = 2_000  # rejects near-empty/placeholder responses
_MAX_BYTES = 25_000_000
_ROBOTS_CACHE: dict[str, urllib.robotparser.RobotFileParser] = {}


def _robots_allowed(url: str) -> bool:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    rp = _ROBOTS_CACHE.get(origin)
    if rp is None:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(origin + "/robots.txt")
        try:
            rp.read()
        except Exception:
            # If robots.txt can't be fetched, fail open (most sites have none) —
            # this matches standard crawler behavior.
            rp = None
        _ROBOTS_CACHE[origin] = rp
    if rp is None:
        return True
    try:
        return rp.can_fetch("*", url)
    except Exception:
        return True


def _validate_candidate(url: str, timeout: float = 8.0) -> bool:
    """robots.txt + AI opt-out + Content-Type/size pre-check via HEAD, so
    invalid URLs never reach the img2dataset download step."""
    if not _robots_allowed(url):
        return False
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return False

    robots_tag = resp.headers.get("X-Robots-Tag", "").lower()
    if any(marker in robots_tag for marker in _AI_OPTOUT_MARKERS):
        return False

    content_type = resp.headers.get("Content-Type", "")
    if "image" not in content_type:
        return False

    content_length = resp.headers.get("Content-Length")
    if content_length is not None:
        try:
            size = int(content_length)
            if size < _MIN_BYTES or size > _MAX_BYTES:
                return False
        except ValueError:
            pass

    return resp.status_code == 200


@register_connector(SourceType.WEB)
class WebScrapeConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64), safesearch: str = "moderate"):
        self.min_w, self.min_h = min_resolution
        self.safesearch = safesearch

    # -- Discovery: gather + validate candidate URLs into shards -----------------

    def discover(self, query: str, cursor: str | None, limit: int = 200) -> DiscoveryPage:
        from ddgs import DDGS

        try:
            with DDGS() as ddgs:
                results = list(ddgs.images(query, safesearch=self.safesearch, max_results=limit))
        except Exception as e:
            logger.error(f"DuckDuckGo image search failed: {e}")
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        validated: list[SourceItem] = []
        for r in results:
            url = r.get("image")
            if not url:
                continue
            if not _validate_candidate(url):
                continue
            validated.append(
                SourceItem(
                    source_type=SourceType.WEB,
                    external_ref={"url": url, "title": r.get("title", "")},
                    license_hint=None,
                    dataset_identifier=query,
                )
            )

        # Batch validated URLs into shards of ~25 — a shard is one Celery
        # download task's unit of work, and img2dataset's own thread pool
        # parallelizes within it. Keeping shards small bounds how long a
        # pause/cancel request can take to actually stop.
        shard_size = 25
        shards = [validated[i : i + shard_size] for i in range(0, len(validated), shard_size)]
        items = [
            SourceItem(
                source_type=SourceType.WEB,
                external_ref={"urls": [si.external_ref["url"] for si in shard]},
                license_hint=None,
                dataset_identifier=query,
            )
            for shard in shards
        ]
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    # -- Download: run img2dataset over one shard's URL list -----------------

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        from img2dataset import download as img2dataset_download

        if should_stop():
            raise JobStopped("should_stop() signalled before web shard download started")

        urls = item.external_ref["urls"]
        shard_id = hashlib.md5(",".join(urls).encode()).hexdigest()[:12]
        shard_workdir = workdir / shard_id
        shard_workdir.mkdir(parents=True, exist_ok=True)

        url_list_path = shard_workdir / "urls.csv"
        with open(url_list_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["url"])
            for u in urls:
                writer.writerow([u])

        output_dir = shard_workdir / "output"
        try:
            img2dataset_download(
                url_list=str(url_list_path),
                output_folder=str(output_dir),
                input_format="csv",
                url_col="url",
                output_format="files",
                thread_count=8,
                resize_mode="no",
                encode_format="jpg",
            )
        except Exception as e:
            raise TransientConnectorError(f"img2dataset failed for shard {shard_id}: {e}")

        if should_stop():
            on_progress({"checkpoint": {}})
            raise JobStopped("should_stop() signalled after web shard download")

        yield from self._walk_output(output_dir, item.dataset_identifier or "", on_progress)

    def _walk_output(self, output_dir: Path, query: str, on_progress: OnProgressFn) -> Iterator[RawAssetDraft]:
        from PIL import Image

        if not output_dir.exists():
            return

        for json_path in sorted(output_dir.rglob("*.json")):
            img_path = json_path.with_suffix(".jpg")
            if not img_path.exists():
                continue
            try:
                with open(json_path) as f:
                    meta = json.load(f)
            except Exception:
                meta = {}

            if meta.get("status") != "success":
                on_progress({"rejected": 1})
                continue

            try:
                with Image.open(img_path) as im:
                    im.verify()
                with Image.open(img_path) as im:
                    w, h = im.size
                if w < self.min_w or h < self.min_h:
                    on_progress({"rejected": 1})
                    continue
            except Exception:
                on_progress({"rejected": 1})
                continue

            original_url = meta.get("url", "")
            source_item_id = hashlib.md5(original_url.encode()).hexdigest()[:16]
            draft = RawAssetDraft(
                source_item_id=source_item_id,
                source_url=original_url,
                dataset_identifier=query,
                license=None,
                local_tmp_path=img_path,
                original_name=img_path.name,
                width=w,
                height=h,
                metadata={"search_query": query},
            )
            on_progress({"downloaded": 1})
            yield draft
