"""SunRgbdConnector — targeted downloads from SUN RGB-D
(rgbd.cs.princeton.edu), restricted to its 2D bounding-box annotation layer
(SUNRGBDMeta2DBB_v2.mat) — the depth/3D channels have no representation in
this platform's AnnotationFormat.

SUN RGB-D's own object vocabulary is open, crowd-sourced free text (1000+
unique strings, e.g. "chair", "airconditioner", "art_board" — plus a small
amount of genuine data-entry noise from the original toolbox), so matching
here is direct lexical substring search over the class index (same as every
other connector added this session — no CLIP/semantic layer).

Discovery only ever needs the small (~4.3MB) metadata .mat file. The images
live in one 6.9GB archive (SUNRGBD.zip) with no per-image URLs, but the
archive is a zip on a server that honours range requests, so fetch() reads its
member index once (66MB, cached as a 1.4MB JSON) and then pulls just the images
a job wants, ~50KB each — see app/connectors/remote_zip.py. Downloading and
unpacking the whole thing took ~45 minutes and 14GB of disk for what is
usually a few dozen images.
"""

import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from app.connectors import remote_zip
from app.connectors.base import (
    HTTP_HEADERS,
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    download_with_retry,
    exclusive_cache_lock,
    limited_concurrency,
    register_connector,
)
from app.connectors.query_terms import match_labels
from app.core.config import get_settings
from app.core.exceptions import JobStopped, TransientConnectorError
from app.domain.enums import SourceType
from app.services.format_conversion import BBox, ClassRegistry, boxes_to_yolo_lines

logger = logging.getLogger(__name__)

_META_URL = "https://rgbd.cs.princeton.edu/data/SUNRGBDMeta2DBB_v2.mat"
_IMAGES_URL = "https://rgbd.cs.princeton.edu/data/SUNRGBD.zip"
_SHARD_SIZE = 20
# Each image is one ~1.6s round trip for ~50KB, so a shard fetches a few at a
# time. Threads within a shard are bounded by _MAX_CONCURRENT_REQUESTS below,
# which is the limit that actually protects the server.
_FETCH_CONCURRENCY = 4
# Simultaneous requests to rgbd.cs.princeton.edu across ALL shards, processes and
# worker containers. It is a small academic server: ~32 concurrent connections
# (8 worker slots x 4 threads) made it drop requests and then refuse this host.
_MAX_CONCURRENT_REQUESTS = 4
_LICENSE_NOTE = (
    "SUN RGB-D (research use — sourced from NYUv2/B3DO/SUN3D, each with its "
    "own terms; verify per-subset terms before commercial use)"
)


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "sun_rgbd"


def _ensure_records_cached() -> list[dict]:
    """Downloads the small 2D-bbox metadata .mat once and converts it to a
    cached JSON of plain records: [{"sequence", "rgbname", "boxes": [
    {"class", "bbox": [x,y,w,h]}, ...]}, ...] — avoids re-parsing the
    MATLAB struct array (via scipy) on every discover() call."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    records_path = cache_dir / "records.json"
    if records_path.exists():
        return json.loads(records_path.read_text())

    mat_path = cache_dir / "SUNRGBDMeta2DBB_v2.mat"
    if not mat_path.exists():
        logger.info(f"Downloading SUN RGB-D 2D bbox metadata ({_META_URL})")
        tmp_path = mat_path.with_suffix(".mat.tmp")
        download_with_retry(_META_URL, tmp_path, timeout=60, headers=HTTP_HEADERS)
        tmp_path.replace(mat_path)

    import scipy.io as sio

    mat = sio.loadmat(mat_path)
    arr = mat["SUNRGBDMeta2DBB"]
    records = []
    for i in range(arr.shape[1]):
        rec = arr[0, i]
        sequence = str(rec["sequenceName"][0])
        rgbname = str(rec["rgbname"][0])
        gt = rec["groundtruth2DBB"]
        boxes = []
        for j in range(gt.shape[1]):
            obj = gt[0, j]
            class_name = str(obj["classname"][0]).strip()
            # The original toolbox has a small amount of data-entry noise
            # (e.g. a filesystem path leaking in as a "class name") —
            # filter anything that isn't a short label-like token.
            if not class_name or len(class_name) > 40 or "/" in class_name:
                continue
            x, y, w, h = (float(v) for v in obj["gtBb2D"][0])
            boxes.append({"class": class_name, "bbox": [x, y, w, h]})
        records.append({"sequence": sequence, "rgbname": rgbname, "boxes": boxes})

    records_path.write_text(json.dumps(records))
    return records


def _is_rgb_image(member_name: str) -> bool:
    return "/image/" in member_name and member_name.endswith((".jpg", ".png"))


def _read_image(entry: list[int]) -> bytes:
    """One image out of the remote archive, holding one of the host-wide request slots."""
    with limited_concurrency(_cache_dir(), "sun_rgbd_http", _MAX_CONCURRENT_REQUESTS):
        return remote_zip.read_member(_IMAGES_URL, tuple(entry), headers=HTTP_HEADERS)


def _member_name(rec: dict) -> str:
    return f"{rec['sequence']}/image/{rec['rgbname']}"


def _ensure_image_index() -> dict[str, list[int]]:
    """member name -> [header_offset, compress_size, file_size, compress_type, crc]
    for every RGB image in SUNRGBD.zip, built once from the archive's central
    directory (66MB) — never the 6.9GB of data.

    Every shard of a job calls this concurrently, so the one-time build runs
    under an exclusive lock; the others wait for it and read the result."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    index_path = cache_dir / "image_index.json"
    if index_path.exists():
        return json.loads(index_path.read_text())

    with exclusive_cache_lock(cache_dir / ".images.lock"):
        if index_path.exists():
            return json.loads(index_path.read_text())

        logger.info(f"Indexing SUN RGB-D image archive ({_IMAGES_URL}, central directory only)")
        index = remote_zip.build_member_index(_IMAGES_URL, _is_rgb_image, headers=HTTP_HEADERS)
        tmp_path = index_path.with_name(index_path.name + ".tmp")
        tmp_path.write_text(json.dumps(index))
        tmp_path.replace(index_path)
        return json.loads(index_path.read_text())


def _unlabelled_candidates(total_records: int, labelled: set[int]) -> list[int]:
    """Every record not already matched by label, in a fixed pseudo-random order.

    SUN RGB-D labels only the objects an annotator marked, so an image can show
    a person that no `person` box records. The order is a hash of the index, not
    the index itself: the dataset is grouped by scene type, so the first N by
    index would be N images from one room type. It is fixed, so a larger limit
    in a later top-up round is a superset of this one and the earlier images are
    skipped as duplicates rather than replaced.
    """
    return sorted(
        (i for i in range(total_records) if i not in labelled),
        key=lambda i: hashlib.md5(str(i).encode()).hexdigest(),
    )


def _class_index(records: list[dict]) -> dict[str, list[int]]:
    index: dict[str, list[int]] = defaultdict(list)
    for idx, rec in enumerate(records):
        for name in {b["class"].lower() for b in rec["boxes"]}:
            index[name].append(idx)
    return dict(index)


@register_connector(SourceType.SUN_RGBD)
class SunRgbdConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_categories: list[str] | None = None
    ) -> DiscoveryPage:
        try:
            records = _ensure_records_cached()
        except Exception as e:
            logger.warning(f"SUN RGB-D metadata unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/parse SUN RGB-D metadata: {e}",
            )

        index = _class_index(records)
        if explicit_categories:
            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
            matched_classes = {c for c in index if c in wanted}
            if not matched_classes:
                return DiscoveryPage(
                    items=[], next_cursor=None, exhausted=True,
                    error_reason=f"No SUN RGB-D categories matched {explicit_categories!r}",
                )
        else:
            matched_classes = match_labels(query, index)

        labelled = sorted({idx for cls in matched_classes for idx in index[cls]})
        record_indices = labelled[:limit]

        # The labels could not fill the limit -- or none names this query at all: offer the
        # unlabelled images too and let the relevance filter decide. "Exactly n images" cannot
        # be met from labels alone (for "car and person" the dataset labels 93 people and no
        # cars). Never for explicit category overrides, which are an exact request.
        scan_indices: list[int] = []
        if not explicit_categories and len(labelled) < limit:
            scan_indices = _unlabelled_candidates(len(records), set(labelled))[: limit - len(labelled)]

        if not record_indices and not scan_indices:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        def shards_of(indices: list[int], unlabelled: bool) -> list[SourceItem]:
            return [
                SourceItem(
                    source_type=SourceType.SUN_RGBD,
                    external_ref={"record_indices": indices[i : i + _SHARD_SIZE], "unlabelled": unlabelled},
                    license_hint=_LICENSE_NOTE,
                    dataset_identifier="sun-rgbd",
                )
                for i in range(0, len(indices), _SHARD_SIZE)
            ]

        items = shards_of(record_indices, unlabelled=False) + shards_of(scan_indices, unlabelled=True)
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    # -- Download ----------------------------------------------------------------

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        import shutil

        from PIL import Image

        if should_stop():
            raise JobStopped("should_stop() signalled before SUN RGB-D shard fetch began")

        try:
            records = _ensure_records_cached()
            image_index = _ensure_image_index()
        except Exception as e:
            raise TransientConnectorError(f"SUN RGB-D dataset unavailable: {e}") from e

        cache_dir = _cache_dir()
        record_indices = item.external_ref["record_indices"]

        # A copy already unpacked on disk (an earlier full download) wins;
        # otherwise the rest of this shard's images are fetched from the archive
        # a few at a time, ahead of the walk below.
        def local_path(record_idx: int) -> Path:
            return cache_dir / _member_name(records[record_idx])

        to_fetch = {
            record_idx: image_index[_member_name(records[record_idx])]
            for i, record_idx in enumerate(record_indices)
            if i >= checkpoint.get("index", 0)
            and not local_path(record_idx).exists()
            and _member_name(records[record_idx]) in image_index
        }

        pool = ThreadPoolExecutor(max_workers=_FETCH_CONCURRENCY)
        try:
            downloads = {
                record_idx: pool.submit(_read_image, entry) for record_idx, entry in to_fetch.items()
            }
            for i, record_idx in enumerate(record_indices):
                if i < checkpoint.get("index", 0):
                    continue
                if should_stop():
                    on_progress({"checkpoint": {"index": i}})
                    raise JobStopped("should_stop() signalled during SUN RGB-D shard walk")

                rec = records[record_idx]
                src_img = local_path(record_idx)
                if not src_img.exists() and record_idx not in downloads:
                    on_progress({"rejected": 1})
                    continue

                dest = workdir / f"sunrgbd_{record_idx}{src_img.suffix}"
                try:
                    if src_img.exists():
                        shutil.copy2(src_img, dest)
                    else:
                        dest.write_bytes(downloads[record_idx].result())
                    with Image.open(dest) as img:
                        img.verify()
                    with Image.open(dest) as img:
                        w, h = img.size
                    if w < self.min_w or h < self.min_h:
                        on_progress({"rejected": 1})
                        continue
                except (requests.RequestException, remote_zip.RangeNotSupportedError) as e:
                    # The network, not this image: keep our place and let the shard retry.
                    on_progress({"checkpoint": {"index": i}})
                    raise TransientConnectorError(f"SUN RGB-D image fetch failed: {e}") from e
                except Exception as e:
                    logger.warning(f"SUN RGB-D record {record_idx} copy/validate failed: {e}")
                    on_progress({"rejected": 1})
                    continue

                registry = ClassRegistry()
                boxes = [
                    BBox(
                        class_name=b["class"],
                        xmin=b["bbox"][0], ymin=b["bbox"][1],
                        xmax=b["bbox"][0] + b["bbox"][2], ymax=b["bbox"][1] + b["bbox"][3],
                    )
                    for b in rec["boxes"]
                ]
                ann_path = None
                annotation_format = None
                if boxes:
                    lines = boxes_to_yolo_lines(boxes, w, h, registry)
                    ann_path = workdir / f"sunrgbd_{record_idx}.txt"
                    ann_path.write_text("\n".join(lines))
                    annotation_format = "yolo_txt"

                draft = RawAssetDraft(
                    source_item_id=f"sunrgbd_{record_idx}",
                    source_url=None,
                    dataset_identifier=item.dataset_identifier,
                    license=item.license_hint,
                    local_tmp_path=dest,
                    original_name=src_img.name,
                    annotation_tmp_path=ann_path,
                    annotation_format=annotation_format,
                    width=w,
                    height=h,
                    classes=registry.names,
                    # "vlm": found by the relevance model among images the dataset does not label
                    # for this query, so the boxes here are SUN's other objects, not the queried one.
                    metadata={
                        "sun_rgbd_sequence": rec["sequence"],
                        "matched_by": "vlm" if item.external_ref.get("unlabelled") else "label",
                    },
                )
                on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
                yield draft
        finally:
            # A stop or a failure part-way must not wait for the rest of the queue.
            pool.shutdown(wait=False, cancel_futures=True)
