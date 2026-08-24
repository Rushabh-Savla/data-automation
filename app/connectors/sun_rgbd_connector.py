"""SunRgbdConnector — targeted downloads from SUN RGB-D
(rgbd.cs.princeton.edu), restricted to its 2D bounding-box annotation layer
(SUNRGBDMeta2DBB_v2.mat) — the depth/3D channels have no representation in
this platform's AnnotationFormat.

SUN RGB-D's own object vocabulary is open, crowd-sourced free text (1000+
unique strings, e.g. "chair", "airconditioner", "art_board" — plus a small
amount of genuine data-entry noise from the original toolbox), so matching
here is direct lexical substring search over the class index (same as every
other connector added this session — no CLIP/semantic layer).

Discovery only ever needs the small (~4.3MB) metadata .mat file — the
~6.9GB bundled image archive (SUNRGBD.zip, no per-image direct-URL fetch
possible, unlike COCO/Open Images/Visual Genome) is only downloaded lazily
the first time fetch() actually needs an image, not on every search.
"""

import json
import logging
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

import requests

from app.connectors.base import (
    HTTP_HEADERS,
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.core.config import get_settings
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType
from app.services.format_conversion import BBox, ClassRegistry, boxes_to_yolo_lines

logger = logging.getLogger(__name__)

_META_URL = "https://rgbd.cs.princeton.edu/data/SUNRGBDMeta2DBB_v2.mat"
_IMAGES_URL = "https://rgbd.cs.princeton.edu/data/SUNRGBD.zip"
_SHARD_SIZE = 20
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
        with requests.get(_META_URL, stream=True, timeout=60, headers=HTTP_HEADERS) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
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


def _ensure_images_cached() -> Path:
    """Downloads+extracts the full SUNRGBD.zip once (~6.9GB) — only called
    from fetch(), never discover()."""
    cache_dir = _cache_dir()
    images_root = cache_dir / "SUNRGBD"
    if images_root.exists():
        return cache_dir

    zip_path = cache_dir / "SUNRGBD.zip"
    if not zip_path.exists():
        logger.info(f"Downloading SUN RGB-D images ({_IMAGES_URL})")
        tmp_path = zip_path.with_suffix(".zip.tmp")
        try:
            with requests.get(_IMAGES_URL, stream=True, timeout=300, headers=HTTP_HEADERS) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
        except requests.HTTPError as e:
            raise FatalConnectorError(f"SUN RGB-D image archive not accessible: {e}") from e
        tmp_path.replace(zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(cache_dir)

    return cache_dir


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
        else:
            keyword_lower = query.strip().lower()
            matched_classes = {c for c in index if keyword_lower in c or c in keyword_lower}
        if not matched_classes:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No SUN RGB-D categories matched query {query!r} — use explicit "
                    "category overrides"
                ),
            )

        record_indices = sorted({idx for cls in matched_classes for idx in index[cls]})[:limit]
        if not record_indices:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [record_indices[i : i + _SHARD_SIZE] for i in range(0, len(record_indices), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.SUN_RGBD,
                external_ref={"record_indices": shard},
                license_hint=_LICENSE_NOTE,
                dataset_identifier="sun-rgbd",
            )
            for shard in shards
        ]
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

        try:
            records = _ensure_records_cached()
            cache_dir = _ensure_images_cached()
        except Exception as e:
            raise TransientConnectorError(f"SUN RGB-D dataset unavailable: {e}") from e

        record_indices = item.external_ref["record_indices"]

        for i, record_idx in enumerate(record_indices):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during SUN RGB-D shard walk")

            rec = records[record_idx]
            src_img = cache_dir / rec["sequence"] / "image" / rec["rgbname"]
            if not src_img.exists():
                on_progress({"rejected": 1})
                continue

            dest = workdir / f"sunrgbd_{record_idx}{src_img.suffix}"
            try:
                shutil.copy2(src_img, dest)
                with Image.open(dest) as img:
                    img.verify()
                with Image.open(dest) as img:
                    w, h = img.size
                if w < self.min_w or h < self.min_h:
                    on_progress({"rejected": 1})
                    continue
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
                metadata={"sun_rgbd_sequence": rec["sequence"]},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
