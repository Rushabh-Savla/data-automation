"""WebScrapeConnector — net-new connector replacing ddg_downloader.py.

ddg_downloader.py's YOLOv8-COCO-80-class + K-means dominant-color
verification block (including the confirmed `target_xclasses` NameError bug
at its line 185) is NOT ported here. It's superseded by the shared
relevance-filter stage (app/filtering/vlm_filter.py),
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
import subprocess
import sys
import time
import urllib.robotparser
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests

from app.connectors.archive_handler import is_archive, safe_extract_archive, SUPPORTED_IMAGE_EXTENSIONS
from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.core.exceptions import CandidateWindowFull, FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

_AI_OPTOUT_MARKERS = ("noai", "noimageai")
_MIN_BYTES = 2_000  # rejects near-empty/placeholder responses
_MAX_BYTES = 25_000_000
_ROBOTS_CACHE: dict[str, urllib.robotparser.RobotFileParser] = {}
_IMG2DATASET_TIMEOUT_S = 600
_VALIDATE_TIMEOUT_S = 3.0  # HEAD requests should be fast; 8s serial was the main bottleneck
_VALIDATE_MAX_WORKERS = 16
_DISCOVER_PAGE_SIZE = 100  # ddgs image engines cap out well under this per call regardless
_DISCOVER_PAGES_PER_VARIANT = 6  # pages to walk for one query phrasing before moving to the next
_DISCOVER_PAGE_RETRIES = 2  # per-page retries before treating that page as exhausted
_DISCOVER_RETRY_DELAY_S = 1.0
_DISCOVER_MIN_ATTEMPTS = 20
_DISCOVER_MAX_ATTEMPTS_CAP = 200  # hard ceiling on (variant, page) fetches regardless of target size
_DISCOVER_ATTEMPTS_PER_TARGET = 10  # scales attempt budget with how many validated URLs are wanted

# Generic photography-style qualifiers, not object-specific — appended to the
# job's own query to get DuckDuckGo's differently-indexed results for a
# slightly different phrasing of the same subject. A single literal query
# genuinely has a finite pool of unique image results (~35-350 for a typical
# two-word phrase, even after full page pagination) — nowhere near enough for
# a job asking for hundreds or thousands of images. Downstream relevance
# filtering (app/filtering/vlm_filter.py, see filtering_tasks.py) is what keeps this
# safe: broadening recall here can only add candidates for filtering to
# reject, it can't let anything irrelevant through on its own.
_QUERY_VARIANT_SUFFIXES = (
    "",
    " photo",
    " picture",
    " image",
    " stock photo",
    " close up",
    " isolated on white background",
    " product photo",
)


def _robots_allowed(url: str) -> bool:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    rp = _ROBOTS_CACHE.get(origin)
    if rp is None:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(origin + "/robots.txt")
        try:
            # NOT rp.read() — that calls urllib.request.urlopen() with no
            # timeout, which blocks on Python's global socket default
            # (unset = block forever) if the host accepts the connection
            # but never responds. One such host anywhere in a discovery
            # batch hangs the whole ThreadPoolExecutor batch (it waits for
            # every submitted future), which hangs the whole discover()
            # call — this is what silently stuck jobs at 0/target for
            # 20+ minutes rather than failing fast. requests.get() with an
            # explicit timeout can't hang like this.
            resp = requests.get(
                origin + "/robots.txt", timeout=_VALIDATE_TIMEOUT_S, headers={"User-Agent": "Mozilla/5.0"}
            )
            if resp.status_code >= 400:
                rp = None  # no robots.txt (404 etc.) — fail open, same as before
            else:
                rp.parse(resp.text.splitlines())
        except Exception:
            # Robots.txt fetch failed/timed out — fail open (most sites have
            # none) — this matches standard crawler behavior.
            rp = None
        _ROBOTS_CACHE[origin] = rp
    if rp is None:
        return True
    try:
        return rp.can_fetch("*", url)
    except Exception:
        return True


def _validate_candidate(url: str, timeout: float = _VALIDATE_TIMEOUT_S) -> bool:
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
        """`limit` is the number of *validated* URLs wanted, not a raw
        ddgs page size — a single ddgs call at `limit` routinely validates
        down to a small fraction of that (dead links, hotlink-blocked CDNs,
        non-image redirects), so a job asking for e.g. 500 images used to
        get stuck around ~16. Note ddgs's `max_results` is capped per call
        by the underlying engines (~35 results for images regardless of how
        high it's set) — raising it alone just re-fetches the same first
        page, so real pagination requires ddgs's `page` kwarg instead.

        Even with full page pagination, one literal query phrase has a
        finite pool of unique results (often only a few hundred), which
        can't satisfy a job asking for e.g. 2000 images. So this also walks
        _QUERY_VARIANT_SUFFIXES — generic rephrasings of the same query —
        each with its own page walk, until enough candidates validate, the
        (query, page) budget (scaled by `limit`, capped at
        _DISCOVER_MAX_ATTEMPTS_CAP) is exhausted, or every variant runs dry.
        A query with genuinely few real results still terminates promptly
        (each variant/page stops as soon as it stops returning anything
        new) rather than spinning through the whole budget.
        """
        from ddgs import DDGS

        seen_urls: set[str] = set()
        validated: list[SourceItem] = []
        max_attempts = min(
            _DISCOVER_MAX_ATTEMPTS_CAP, max(_DISCOVER_MIN_ATTEMPTS, limit // _DISCOVER_ATTEMPTS_PER_TARGET)
        )
        attempts = 0

        for suffix in _QUERY_VARIANT_SUFFIXES:
            if len(validated) >= limit or attempts >= max_attempts:
                break
            variant_query = f"{query}{suffix}".strip()

            for page in range(1, _DISCOVER_PAGES_PER_VARIANT + 1):
                if len(validated) >= limit or attempts >= max_attempts:
                    break
                attempts += 1

                results = None
                # A single page fetch failing (e.g. a transient 403 from
                # DuckDuckGo's anti-bot throttling — more likely now that a
                # large job walks many pages/variants in one discover()
                # call) shouldn't truncate an otherwise-healthy discovery;
                # only give up on this variant after every retry fails.
                for retry in range(_DISCOVER_PAGE_RETRIES):
                    try:
                        with DDGS() as ddgs:
                            results = list(
                                ddgs.images(
                                    variant_query,
                                    safesearch=self.safesearch,
                                    max_results=_DISCOVER_PAGE_SIZE,
                                    page=page,
                                    backend="bing,duckduckgo",
                                )
                            )
                        break
                    except Exception as e:
                        logger.warning(
                            f"DuckDuckGo image search failed ({variant_query!r}, page {page}, retry {retry}): {e}"
                        )
                        time.sleep(_DISCOVER_RETRY_DELAY_S)
                if results is None:
                    logger.error(f"DuckDuckGo image search gave up on {variant_query!r} page {page}")
                    break  # try the next variant instead of the whole query

                new_candidates = []
                for r in results:
                    url = r.get("image")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    new_candidates.append(r)

                if not new_candidates:
                    break  # this variant is exhausted; move on to the next one

                with ThreadPoolExecutor(max_workers=_VALIDATE_MAX_WORKERS) as pool:
                    future_to_r = {pool.submit(_validate_candidate, r["image"]): r for r in new_candidates}
                    for future in as_completed(future_to_r):
                        if not future.result():
                            continue
                        r = future_to_r[future]
                        validated.append(
                            SourceItem(
                                source_type=SourceType.WEB,
                                external_ref={"url": r["image"], "title": r.get("title", "")},
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
        admit_candidate=None,
    ) -> Iterator[RawAssetDraft]:
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
        self._run_img2dataset(url_list_path, output_dir, shard_id)

        if should_stop():
            on_progress({"checkpoint": {}})
            raise JobStopped("should_stop() signalled after web shard download")

        yield from self._walk_output(output_dir, item.dataset_identifier or "", on_progress, admit_candidate=admit_candidate)

    @staticmethod
    def _run_img2dataset(url_list_path: Path, output_dir: Path, shard_id: str) -> None:
        """Runs img2dataset as a subprocess rather than calling `download()`
        in-process. img2dataset always spawns a `multiprocessing.Pool`
        internally (even with processes_count=1 — see
        img2dataset.distributor.multiprocessing_distributor), and Celery's
        prefork pool workers are themselves daemonic child processes, which
        Python's multiprocessing refuses to let spawn further children
        ("daemonic processes are not allowed to have children"). Running
        img2dataset in its own OS process via subprocess sidesteps that
        restriction entirely, since it's no longer a multiprocessing.Process
        child of the (daemonic) Celery worker.
        """
        cmd = [
            sys.executable,
            "-m",
            "img2dataset.main",
            f"--url_list={url_list_path}",
            f"--output_folder={output_dir}",
            "--input_format=csv",
            "--url_col=url",
            "--output_format=files",
            "--thread_count=8",
            "--resize_mode=no",
            "--encode_format=jpg",
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=_IMG2DATASET_TIMEOUT_S, check=False
            )
        except subprocess.TimeoutExpired as e:
            raise TransientConnectorError(
                f"img2dataset timed out after {_IMG2DATASET_TIMEOUT_S}s for shard {shard_id}"
            ) from e
        if result.returncode != 0:
            tail = "\n".join(result.stderr.strip().splitlines()[-20:])
            raise TransientConnectorError(f"img2dataset failed for shard {shard_id} (exit {result.returncode}): {tail}")

    def _walk_output(self, output_dir: Path, query: str, on_progress: OnProgressFn, admit_candidate=None) -> Iterator[RawAssetDraft]:
        from PIL import Image

        if not output_dir.exists():
            return

        # Extract any downloaded archives discovered in output
        for p in list(output_dir.rglob("*")):
            if p.is_file() and is_archive(p):
                try:
                    safe_extract_archive(p, remove_archive=True)
                except Exception as e:
                    logger.warning(f"Could not extract web archive {p.name}: {e}")

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

            original_url = meta.get("url", "")
            source_item_id = hashlib.md5(original_url.encode()).hexdigest()[:16]

            admission = None
            if admit_candidate is not None:
                admission = admit_candidate(source_item_id)
                raw_status = getattr(admission, "status", "")
                status = str(getattr(raw_status, "value", raw_status)).upper()
                if status == "DUPLICATE":
                    continue
                if status == "WINDOW_FULL":
                    raise CandidateWindowFull(
                        getattr(admission, "window_id", None),
                        getattr(admission, "window_index", None),
                        0,
                    )
                if status != "ADMITTED":
                    raise FatalConnectorError(
                        f"Candidate admission failed for {source_item_id}: {status}"
                    )

            try:
                with Image.open(img_path) as im:
                    im.verify()
                with Image.open(img_path) as im:
                    w, h = im.size
                if w < self.min_w or h < self.min_h:
                    if admission is not None and getattr(admission, "reservation_id", None):
                        from app.services.candidate_reservation_service import mark_candidate_failed
                        mark_candidate_failed(admission.reservation_id)
                    on_progress({"rejected": 1})
                    continue
            except Exception:
                if admission is not None and getattr(admission, "reservation_id", None):
                    from app.services.candidate_reservation_service import mark_candidate_failed
                    mark_candidate_failed(admission.reservation_id)
                on_progress({"rejected": 1})
                continue

            metadata = {"search_query": query}
            if admission is not None:
                metadata["candidate_reservation_id"] = str(admission.reservation_id)
                metadata["candidate_window_id"] = str(admission.window_id)
                metadata["candidate_window_index"] = int(admission.window_index)

            draft = RawAssetDraft(
                source_item_id=source_item_id,
                source_url=original_url,
                dataset_identifier=query,
                license=None,
                local_tmp_path=img_path,
                original_name=img_path.name,
                width=w,
                height=h,
                metadata=metadata,
            )
            on_progress({"downloaded": 1})
            yield draft
