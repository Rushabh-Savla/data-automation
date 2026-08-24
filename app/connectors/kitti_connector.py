"""KittiConnector — targeted, class-filtered downloads from the KITTI Vision
Benchmark Suite's 2D object detection subset (cvlibs.net/datasets/kitti),
fetched as plain public HTTPS objects from KITTI's own S3 bucket (no login,
no AWS SDK/signing needed — the bucket's objects are public-read, so an
ordinary `requests.get` on the known URL works exactly like any other
connector's downloads).

Only the `training/` split is used — KITTI's `testing/` split ships images
with no labels at all, useless for a labeled-detection dataset. Restricted
to KITTI's native 2D bounding-box annotation layer (bbox_left/top/right/
bottom fields); the 3D box fields (h/w/l/x/y/z/ry) are ignored — this
platform has no 3D annotation representation.
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

_IMAGES_URL = "https://s3.eu-central-1.amazonaws.com/avg-kitti/data_object_image_2.zip"
_LABELS_URL = "https://s3.eu-central-1.amazonaws.com/avg-kitti/data_object_label_2.zip"
_SHARD_SIZE = 20
_LICENSE_NOTE = "KITTI Vision Benchmark Suite (CC BY-NC-SA 3.0 — academic/non-commercial use only)"
# KITTI's fixed object-type vocabulary; "DontCare" is a non-object filler
# annotation, deliberately excluded.
_KITTI_CLASSES = ["Car", "Van", "Truck", "Pedestrian", "Person_sitting", "Cyclist", "Tram", "Misc"]


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "kitti"


def _download_and_extract(url: str, cache_dir: Path, marker_dir: Path) -> None:
    if marker_dir.exists():
        return
    zip_path = cache_dir / (url.rsplit("/", 1)[-1])
    if not zip_path.exists():
        logger.info(f"Downloading KITTI archive ({url})")
        tmp_path = zip_path.with_suffix(".zip.tmp")
        try:
            with requests.get(url, stream=True, timeout=300, headers=HTTP_HEADERS) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
        except requests.HTTPError as e:
            raise FatalConnectorError(f"KITTI archive not accessible: {e}") from e
        tmp_path.replace(zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(cache_dir)


def _ensure_dataset_cached() -> Path:
    """Downloads+extracts KITTI's 2D object-detection images (~12GB) and
    labels (~5MB) archives once, returning the cache root (containing
    training/image_2/ and training/label_2/)."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    _download_and_extract(_IMAGES_URL, cache_dir, cache_dir / "training" / "image_2")
    _download_and_extract(_LABELS_URL, cache_dir, cache_dir / "training" / "label_2")
    return cache_dir


def _parse_kitti_label(label_path: Path) -> list[BBox]:
    """KITTI label lines: `type truncated occluded alpha left top right
    bottom h w l x y z ry [score]` — only type + 2D bbox (fields 4-7) are
    used here."""
    boxes = []
    for line in label_path.read_text().strip().splitlines():
        parts = line.split()
        if len(parts) < 8:
            continue
        obj_type = parts[0]
        if obj_type == "DontCare":
            continue
        left, top, right, bottom = (float(p) for p in parts[4:8])
        boxes.append(BBox(class_name=obj_type, xmin=left, ymin=top, xmax=right, ymax=bottom))
    return boxes


def _ensure_class_index_cached(cache_dir: Path) -> dict[str, list[str]]:
    index_path = cache_dir / "class_index.json"
    if index_path.exists():
        return json.loads(index_path.read_text())

    index: dict[str, list[str]] = defaultdict(list)
    label_dir = cache_dir / "training" / "label_2"
    for label_path in label_dir.glob("*.txt"):
        try:
            boxes = _parse_kitti_label(label_path)
        except Exception as e:
            logger.warning(f"Skipping unreadable KITTI label {label_path.name}: {e}")
            continue
        image_id = label_path.stem
        for name in {b.class_name for b in boxes}:
            index[name].append(image_id)

    index_path.write_text(json.dumps(index))
    return dict(index)


@register_connector(SourceType.KITTI)
class KittiConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_categories: list[str] | None = None
    ) -> DiscoveryPage:
        try:
            cache_dir = _ensure_dataset_cached()
            class_index = _ensure_class_index_cached(cache_dir)
        except FatalConnectorError as e:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True, error_reason=str(e))
        except Exception as e:
            logger.warning(f"KITTI dataset unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/index KITTI: {e}",
            )

        if explicit_categories:
            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
            matched = {c for c in _KITTI_CLASSES if c.lower() in wanted}
        else:
            keyword_lower = query.strip().lower()
            matched = {c for c in _KITTI_CLASSES if keyword_lower in c.lower() or c.lower() in keyword_lower}
        if not matched:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No KITTI object types matched query {query!r} — KITTI's vocabulary is "
                    "just 8 driving-scene classes; use explicit category overrides"
                ),
            )

        image_ids = sorted({iid for cls in matched for iid in class_index.get(cls, [])})[:limit]
        if not image_ids:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [image_ids[i : i + _SHARD_SIZE] for i in range(0, len(image_ids), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.KITTI,
                external_ref={"image_ids": shard},
                license_hint=_LICENSE_NOTE,
                dataset_identifier="kitti-object-2d",
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
            cache_dir = _ensure_dataset_cached()
        except Exception as e:
            raise TransientConnectorError(f"KITTI dataset unavailable: {e}") from e

        image_ids = item.external_ref["image_ids"]

        for i, image_id in enumerate(image_ids):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during KITTI shard walk")

            src_img = cache_dir / "training" / "image_2" / f"{image_id}.png"
            src_label = cache_dir / "training" / "label_2" / f"{image_id}.txt"
            if not src_img.exists() or not src_label.exists():
                on_progress({"rejected": 1})
                continue

            dest = workdir / src_img.name
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
                logger.warning(f"KITTI image {image_id} copy/validate failed: {e}")
                on_progress({"rejected": 1})
                continue

            registry = ClassRegistry()
            boxes = _parse_kitti_label(src_label)
            ann_path = None
            annotation_format = None
            if boxes:
                lines = boxes_to_yolo_lines(boxes, w, h, registry)
                ann_path = workdir / f"{image_id}.txt"
                ann_path.write_text("\n".join(lines))
                annotation_format = "yolo_txt"

            draft = RawAssetDraft(
                source_item_id=f"kitti_{image_id}",
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
                metadata={"kitti_image_id": image_id},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
