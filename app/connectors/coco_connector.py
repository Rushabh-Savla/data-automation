"""CocoConnector — targeted, category-filtered downloads from the official
COCO 2017 dataset (https://cocodataset.org/), never the full 1GB/18GB
image archives.

Ported from the user-supplied reference script (coco_and_open_images_scrap.py
::COCOScraper): download+cache the annotations zip once, substring-match the
query against the 80 category names, then fetch only the specific images
that contain a matching category via each image's own `coco_url`. Adapted to
this platform's discover()/fetch() Protocol — discover() never raises
(degrades to error_reason, see app/connectors/kaggle_connector.py's own
discipline around this), checkpointed+sharded fetch instead of the reference
script's single synchronous scrape() call.

Deliberately preserves the native COCO_JSON annotation format (a small
per-shard annotation subset + asset_metadata["coco_image_id"]) instead of
converting to YOLO at ingest time — app/services/export_service.py already
knows how to convert COCO_JSON assets to yolo/coco output formats on demand
via app/services/format_conversion.py::parse_coco_boxes_for_image().

Two splits, searched in order: val2017 (5,000 images; only ~535 contain a car)
and then train2017 (118,000; ~12,000 with a car). val2017 alone caps a job well
short of any real target -- a 1000-image "cars" job could only ever reach the
~359 of those 535 that pass the relevance filter -- so discovery continues into
train2017 once val2017's matches run out.

The annotation JSON is far too large to re-parse per call (train2017's is
448MB, several GB of RAM, and every discovery and every download shard used to
do it), so each split is loaded ONCE into a SQLite index and everything else
reads only the rows it needs.
"""

import hashlib
import json
import logging
import os
import random
import sqlite3
import threading
import time
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import requests

from app.connectors.base import (
    HTTP_HEADERS,
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    exclusive_cache_lock,
    register_connector,
)
from app.connectors.query_terms import match_labels
from app.core.config import get_settings
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

_ANNOTATIONS_ZIP_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
_SHARD_SIZE = 20


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "coco"


def _ensure_annotations_cached(split: str) -> Path:
    """Downloads+caches the COCO annotations zip once (~241MB), extracting
    only the requested split's instances_*.json member. Two jobs racing to
    populate this cache concurrently is possible but low-probability/low-
    impact — the final on-disk state is correct either way, just possibly
    redundant work — PROVIDED each writer uses its own tmp file. A shared,
    fixed tmp filename doesn't hold that: whichever writer finishes first
    calls Path.replace(), which removes the tmp file, so a second writer
    still mid-download raises FileNotFoundError on its own later replace()
    (observed in production: "annotations_trainval2017.zip.tmp -> ...zip").
    Suffixing the tmp filename with a per-call unique token avoids that
    while keeping the same fast-path/redundant-work tradeoff."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    json_path = cache_dir / f"instances_{split}.json"
    if json_path.exists():
        return json_path

    unique = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"

    zip_path = cache_dir / "annotations_trainval2017.zip"
    if not zip_path.exists():
        logger.info(f"Downloading COCO annotations ({_ANNOTATIONS_ZIP_URL})")
        tmp_path = cache_dir / f"annotations_trainval2017.zip.{unique}.tmp"
        try:
            with requests.get(_ANNOTATIONS_ZIP_URL, stream=True, timeout=120, headers=HTTP_HEADERS) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            if not zip_path.exists():
                tmp_path.replace(zip_path)
        finally:
            tmp_path.unlink(missing_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        member = f"annotations/instances_{split}.json"
        if member not in zf.namelist():
            raise FatalConnectorError(f"COCO split '{split}' not found in annotations archive")
        tmp_json = cache_dir / f"instances_{split}.json.{unique}.tmp"
        try:
            with zf.open(member) as src, open(tmp_json, "wb") as dst:
                dst.write(src.read())
            if not json_path.exists():
                tmp_json.replace(json_path)
        finally:
            tmp_json.unlink(missing_ok=True)

    return json_path


# Each image is a separate HTTP request to images.cocodataset.org, measured at ~4s for
# 130KB (mostly connection setup and latency, not bandwidth), and a shard used to
# fetch its 20 images one after another. Several at once overlaps that waiting.
# It is an S3-backed host built for this, unlike the small academic servers other
# connectors talk to.
_FETCH_CONCURRENCY = 6
_DOWNLOAD_ATTEMPTS = 3
_MAX_BACKOFF_S = 8

_local = threading.local()


def _session() -> requests.Session:
    """One keep-alive session per thread, so a shard's images share connections
    instead of each paying a new TCP handshake."""
    session = getattr(_local, "session", None)
    if session is None:
        session = _local.session = requests.Session()
    return session


def _drop_session() -> None:
    session = getattr(_local, "session", None)
    _local.session = None
    if session is not None:
        session.close()


def _download_image(url: str, dest: Path) -> None:
    """Streams one image to `dest`, retrying transient failures with jittered backoff.

    A client error (a 404 for an image that no longer exists) will not fix itself
    and is raised at once; connection errors and 5xx are retried. Before this, one
    dropped connection threw the candidate away.
    """
    last_error: Exception | None = None
    for attempt in range(_DOWNLOAD_ATTEMPTS):
        try:
            with _session().get(url, stream=True, timeout=30, headers=HTTP_HEADERS) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
            return
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code < 500:
                raise
            last_error = e
        except (requests.RequestException, OSError) as e:
            last_error = e
        _drop_session()  # the connection may be the problem; do not reuse it
        if attempt < _DOWNLOAD_ATTEMPTS - 1:
            time.sleep(min(2**attempt, _MAX_BACKOFF_S) * (0.5 + random.random()))
    raise last_error


def _build_index(json_path: Path, index_path: Path) -> None:
    """Loads one split's annotation JSON into a SQLite file.

    meta(key, value)        categories and licenses, as JSON
    images(id, info, anns)  each image's own record and ALL of its annotations, as JSON
    image_cats(image_id, category_id)  which categories each image contains

    Whole records are stored as JSON rather than broken into columns, so a shard
    can rebuild exactly the per-shard COCO subset it always wrote. Written to a
    temp file and renamed, so a reader never sees a half-built index.
    """
    tmp_path = index_path.with_name(index_path.name + ".tmp")
    tmp_path.unlink(missing_ok=True)
    data = json.loads(json_path.read_text())
    conn = sqlite3.connect(tmp_path)
    try:
        # The file only becomes the index when it is renamed after a complete build, so a
        # crash mid-build just discards it: syncing to disk on every statement buys
        # nothing, and on a busy disk it made the build (and its tests) crawl.
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("CREATE TABLE images (id INTEGER PRIMARY KEY, info TEXT NOT NULL, anns TEXT NOT NULL)")
        conn.execute("CREATE TABLE image_cats (image_id INTEGER NOT NULL, category_id INTEGER NOT NULL)")
        conn.executemany(
            "INSERT INTO meta VALUES (?, ?)",
            [
                ("categories", json.dumps(data.get("categories", []))),
                ("licenses", json.dumps(data.get("licenses", []))),
            ],
        )

        anns_by_image: dict[int, list[dict]] = defaultdict(list)
        for ann in data.get("annotations", []):
            anns_by_image[ann["image_id"]].append(ann)

        images, image_cats = [], []
        for image in data.get("images", []):
            anns = anns_by_image.get(image["id"], [])
            images.append((image["id"], json.dumps(image), json.dumps(anns)))
            image_cats.extend((image["id"], cid) for cid in {a["category_id"] for a in anns if "category_id" in a})
            if len(images) >= 5000:
                conn.executemany("INSERT INTO images VALUES (?, ?, ?)", images)
                conn.executemany("INSERT INTO image_cats VALUES (?, ?)", image_cats)
                images.clear()
                image_cats.clear()
        conn.executemany("INSERT INTO images VALUES (?, ?, ?)", images)
        conn.executemany("INSERT INTO image_cats VALUES (?, ?)", image_cats)
        conn.execute("CREATE INDEX idx_image_cats_category ON image_cats (category_id)")
        conn.commit()
    except BaseException:
        conn.close()
        tmp_path.unlink(missing_ok=True)
        raise
    conn.close()
    tmp_path.replace(index_path)


def _ensure_index(split: str) -> Path:
    """The SQLite index for one split, built once from its annotation JSON.

    The build parses the whole JSON (train2017: ~448MB), so it runs under a lock:
    concurrent shards wait for it and read the result instead of each parsing it.
    """
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    index_path = cache_dir / f"index_{split}.sqlite3"
    if index_path.exists():
        return index_path
    with exclusive_cache_lock(cache_dir / f".index_{split}.lock"):
        if not index_path.exists():
            _build_index(_ensure_annotations_cached(split), index_path)
    return index_path


def _read_meta(conn: sqlite3.Connection, key: str) -> list[dict]:
    return json.loads(conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()[0])


@register_connector(SourceType.COCO)
class CocoConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64), split: str = "val2017", allow_train2017: bool = False):
        self.min_w, self.min_h = min_resolution
        self.split = split
        self._train2017_allowed = allow_train2017

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_categories: list[str] | None = None
    ) -> DiscoveryPage:
        # discover() must never raise (app/connectors/kaggle_connector.py's
        # own discipline around this) — discovery_tasks.py's discover_coco
        # doesn't wrap this call in try/except, so raising here would fail
        # the Celery task outright and leave the job's completion evaluation
        # never triggered for this source.
        if self.split == "train2017" and not self._train2017_allowed:
            return DiscoveryPage(
                items=[],
                next_cursor=None,
                exhausted=True,
                error_reason=(
                    "COCO train2017 annotations are ~470MB and not enabled by default — "
                    "set config.coco.allow_train2017=true if this is intended"
                ),
            )

        # val2017 first, then train2017 -- and train2017 only if val2017 cannot fill
        # the limit, so a small job never builds or reads the big index.
        splits = [self.split]
        if self.split != "train2017" and self._train2017_allowed:
            splits.append("train2017")

        per_split: list[tuple[str, list[int], bool]] = []  # (split, image ids, unlabelled)
        remaining = limit
        matched_ids: set[int] = set()
        try:
            # Labelled images first: those the dataset says contain what was asked for.
            for position, split in enumerate(splits):
                if remaining <= 0:
                    break
                with closing(sqlite3.connect(_ensure_index(split))) as conn:
                    if position == 0:
                        categories = _read_meta(conn, "categories")
                        if explicit_categories:
                            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
                            matched_ids = {cat["id"] for cat in categories if cat["name"].lower() in wanted}
                        else:
                            names = {cat["name"]: cat["id"] for cat in categories}
                            matched_ids = {names[n] for n in match_labels(query, names)}
                        if not matched_ids and explicit_categories:
                            return DiscoveryPage(
                                items=[],
                                next_cursor=None,
                                exhausted=True,
                                error_reason=(
                                    f"No COCO categories matched {explicit_categories!r} — COCO has 80 classes"
                                ),
                            )
                    if not matched_ids:
                        break  # the classes are the same in every split: nothing labelled to find
                    # Ordered by id: a larger `limit` in a later top-up round is then a
                    # superset of this one, so the earlier images are skipped as
                    # duplicates rather than replaced.
                    placeholders = ",".join("?" * len(matched_ids))
                    image_ids = [
                        row[0]
                        for row in conn.execute(
                            f"SELECT DISTINCT image_id FROM image_cats WHERE category_id IN ({placeholders}) "
                            "ORDER BY image_id LIMIT ?",
                            (*sorted(matched_ids), remaining),
                        )
                    ]
                per_split.append((split, image_ids, False))
                remaining -= len(image_ids)

            # The labels could not fill the limit -- or named nothing for this query at all
            # ("guitar", "kitchen", "golden retriever"): fill the rest from the whole dataset
            # and let the relevance filter, which looks at the image, decide (open_vocab.py).
            # Never for explicit category overrides, which are an exact request.
            if remaining > 0 and not explicit_categories:
                exclude_sql = ""
                exclude_args: tuple = ()
                if matched_ids:
                    placeholders = ",".join("?" * len(matched_ids))
                    exclude_sql = (
                        f"WHERE id NOT IN (SELECT image_id FROM image_cats WHERE category_id IN ({placeholders})) "
                    )
                    exclude_args = tuple(sorted(matched_ids))
                for split in splits:
                    if remaining <= 0:
                        break
                    with closing(sqlite3.connect(_ensure_index(split))) as conn:
                        image_ids = [
                            row[0]
                            for row in conn.execute(
                                f"SELECT id FROM images {exclude_sql}"
                                "ORDER BY ((id * 2654435761) & 4294967295), id LIMIT ?",
                                (*exclude_args, remaining),
                            )
                        ]
                    per_split.append((split, image_ids, True))
                    remaining -= len(image_ids)
        except Exception as e:
            logger.warning(f"COCO annotations unavailable: {e}")
            return DiscoveryPage(
                items=[],
                next_cursor=None,
                exhausted=True,
                error_reason=f"Failed to download/parse COCO annotations: {e}",
            )

        items = [
            SourceItem(
                source_type=SourceType.COCO,
                external_ref={
                    "image_ids": image_ids[i : i + _SHARD_SIZE],
                    "split": split,
                    "unlabelled": unlabelled,
                },
                license_hint=None,
                dataset_identifier=f"coco-{split}",
            )
            for split, image_ids, unlabelled in per_split
            for i in range(0, len(image_ids), _SHARD_SIZE)
        ]
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    @staticmethod
    def _match_categories(query: str, categories: list[dict]) -> set[int]:
        """Ids of the COCO classes the query is asking for (see query_terms.match_labels):
        per term, with synonyms and plurals, so "puppy" finds dog and "cats" finds cat.
        Empty when the query names none of the 80 classes."""
        names = {cat["name"]: cat["id"] for cat in categories}
        return {names[name] for name in match_labels(query, names)}

    # -- Download ----------------------------------------------------------------

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
        admit_candidate=None,
    ) -> Iterator[RawAssetDraft]:
        from PIL import Image
        from app.core.exceptions import CandidateWindowFull

        split = item.external_ref.get("split", self.split)
        image_ids = item.external_ref["image_ids"]

        try:
            with closing(sqlite3.connect(_ensure_index(split))) as conn:
                placeholders = ",".join("?" * len(image_ids))
                rows = conn.execute(
                    f"SELECT id, info, anns FROM images WHERE id IN ({placeholders})", image_ids
                ).fetchall()
                categories = _read_meta(conn, "categories")
                licenses = {lic["id"]: lic["name"] for lic in _read_meta(conn, "licenses")}
        except FatalConnectorError:
            raise
        except Exception as e:
            raise TransientConnectorError(f"COCO annotations unavailable: {e}") from e

        images_by_id = {row[0]: json.loads(row[1]) for row in rows}
        annotations_by_image = {row[0]: json.loads(row[2]) for row in rows}

        # Small per-shard subset (not the whole cached file) written once,
        # shared as annotation_tmp_path by every draft in this shard — cheap
        # since a shard is capped at _SHARD_SIZE images.
        subset = {
            "images": [images_by_id[iid] for iid in image_ids if iid in images_by_id],
            "annotations": [ann for iid in image_ids for ann in annotations_by_image.get(iid, [])],
            "categories": categories,
        }
        shard_hash = hashlib.md5(str(image_ids).encode()).hexdigest()[:12]
        subset_path = workdir / f"coco_subset_{shard_hash}.json"
        subset_path.write_text(json.dumps(subset))

        # Candidate admission happens BEFORE a download future is submitted.
        # Only candidates admitted by PostgreSQL enter the thread pool.
        start = max(0, int(checkpoint.get("index", 0)))
        pool = ThreadPoolExecutor(max_workers=_FETCH_CONCURRENCY)
        downloads = {}
        admissions = {}
        duplicate_ids = set()
        stop_at = None
        stop_signal = None

        def mark_failed_reservation(admission):
            reservation_id = getattr(admission, "reservation_id", None)
            if reservation_id is None:
                return
            try:
                from app.services.candidate_reservation_service import mark_candidate_failed
                mark_candidate_failed(reservation_id)
            except Exception:
                logger.exception(
                    "Could not mark failed COCO reservation %s", reservation_id
                )

        try:
            # Reserve first, then submit. If the window fills, no later
            # candidate is submitted, even if this shard has more images.
            for i, image_id in enumerate(image_ids):
                if i < start:
                    continue

                if should_stop():
                    stop_at = i
                    stop_signal = JobStopped(
                        "Job stop requested during COCO candidate admission"
                    )
                    break

                image_info = images_by_id.get(image_id)
                if image_info is None:
                    continue

                source_item_id = f"coco_{split}_{image_id}"
                admission = None

                if admit_candidate is not None:
                    admission = admit_candidate(source_item_id)
                    raw_status = getattr(admission, "status", "")
                    status = str(getattr(raw_status, "value", raw_status)).upper()

                    if status == "DUPLICATE":
                        duplicate_ids.add(image_id)
                        continue

                    if status == "WINDOW_FULL":
                        stop_at = i
                        stop_signal = CandidateWindowFull(
                            getattr(admission, "window_id", None),
                            getattr(admission, "window_index", None),
                            i,
                        )
                        break

                    if status != "ADMITTED":
                        stop_at = i
                        stop_signal = FatalConnectorError(
                            f"Candidate admission failed for {source_item_id}: "
                            f"{status or 'unknown admission status'}"
                        )
                        break

                    if (
                        getattr(admission, "reservation_id", None) is None
                        or getattr(admission, "window_id", None) is None
                    ):
                        stop_at = i
                        stop_signal = FatalConnectorError(
                            f"Incomplete reservation metadata for {source_item_id}"
                        )
                        break

                    admissions[image_id] = admission

                dest = workdir / image_info["file_name"]
                downloads[image_id] = pool.submit(
                    _download_image,
                    image_info.get("coco_url"),
                    dest,
                )

            # Finish all already-admitted candidates before signalling the
            # window boundary. Never submit candidates beyond stop_at.
            process_end = stop_at if stop_at is not None else len(image_ids)

            for i in range(start, process_end):
                image_id = image_ids[i]
                image_info = images_by_id.get(image_id)

                if image_info is None:
                    on_progress({"rejected": 1})
                    on_progress({"checkpoint": {"index": i + 1}})
                    continue

                if image_id in duplicate_ids:
                    on_progress({"checkpoint": {"index": i + 1}})
                    continue

                future = downloads.get(image_id)
                if future is None:
                    continue

                url = image_info.get("coco_url")
                dest = workdir / image_info["file_name"]

                try:
                    future.result()
                    with Image.open(dest) as img:
                        img.verify()
                    with Image.open(dest) as img:
                        w, h = img.size

                    if w < self.min_w or h < self.min_h:
                        mark_failed_reservation(admissions.get(image_id))
                        on_progress({"rejected": 1})
                        on_progress({"checkpoint": {"index": i + 1}})
                        continue

                except Exception as exc:
                    mark_failed_reservation(admissions.get(image_id))
                    logger.warning(
                        f"COCO image {image_id} download/validation failed: {exc}"
                    )
                    on_progress({"rejected": 1})
                    on_progress({"checkpoint": {"index": i + 1}})
                    continue

                admission = admissions.get(image_id)
                reservation_metadata = {}
                if admission is not None:
                    reservation_metadata = {
                        "candidate_reservation_id": str(admission.reservation_id),
                        "candidate_window_id": str(admission.window_id),
                        "candidate_window_index": int(admission.window_index),
                    }

                draft = RawAssetDraft(
                    source_item_id=f"coco_{split}_{image_id}",
                    source_url=url,
                    dataset_identifier=item.dataset_identifier,
                    license=licenses.get(image_info.get("license")),
                    local_tmp_path=dest,
                    original_name=image_info["file_name"],
                    annotation_tmp_path=subset_path,
                    annotation_format="coco_json",
                    width=w,
                    height=h,
                    metadata={
                        "coco_image_id": image_id,
                        "split": split,
                        "matched_by": "vlm" if item.external_ref.get("unlabelled") else "label",
                        **reservation_metadata,
                    },
                )
                on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
                yield draft

            if stop_signal is not None:
                # Resume at the first candidate that was not admitted.
                on_progress({"checkpoint": {"index": stop_at}})
                raise stop_signal

        finally:
            # Wait for already-submitted downloads. No unadmitted download
            # should remain running after the source reports its boundary.
            pool.shutdown(wait=True, cancel_futures=True)
