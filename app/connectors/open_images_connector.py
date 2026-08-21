"""OpenImagesConnector — targeted, class-filtered downloads from Google's
Open Images dataset (https://storage.googleapis.com/openimages/web/index.html)
via its official CSV annotation files + direct per-image S3 fetch — no
FiftyOne, no MongoDB.

Ported from the user-supplied reference script (coco_and_open_images_scrap.py
::OpenImagesScraper): download the class-descriptions CSV once (label_id <->
name), download the split's own bbox-annotation CSV once, substring-match
the query against class names, then stream the (large) annotation CSV
collecting every ImageID whose LabelName is in the matched set. Each image
is fetched directly via `{OID_IMAGE_BASE_URL}/{split}/{image_id}.jpg` — no
bulk archive download of any kind. Adapted to this platform's discover()/
fetch() Protocol (discover() never raises; checkpointed+sharded fetch).

Boxes in the CSV are already relative (XMin/XMax/YMin/YMax, 0-1) — converted
straight to YOLO_TXT at fetch time (no native Open Images AnnotationFormat).
"""

import csv
import logging
from collections.abc import Iterator
from pathlib import Path

import requests

from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.core.config import get_settings
from app.core.exceptions import JobStopped, TransientConnectorError
from app.domain.enums import SourceType
from app.services.format_conversion import BBox, ClassRegistry, boxes_to_yolo_lines

logger = logging.getLogger(__name__)

_CLASS_DESCRIPTIONS_URL = "https://storage.googleapis.com/openimages/v7/oidv7-class-descriptions-boxable.csv"
_ANNOTATIONS_URLS = {
    "validation": "https://storage.googleapis.com/openimages/v5/validation-annotations-bbox.csv",
    "train": "https://storage.googleapis.com/openimages/v6/oidv6-train-annotations-bbox.csv",
    "test": "https://storage.googleapis.com/openimages/v5/test-annotations-bbox.csv",
}
_IMAGE_BASE_URL = "https://open-images-dataset.s3.amazonaws.com"
_SHARD_SIZE = 20
_LICENSE_NOTE = (
    "Open Images (predominantly CC BY 2.0 — see "
    "https://storage.googleapis.com/openimages/web/factsfigures.html)"
)


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "open_images"


def _download_once(url: str, dest: Path, timeout: int = 300) -> None:
    if dest.exists():
        return
    logger.info(f"Downloading Open Images file ({url})")
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    tmp_path.replace(dest)


def _ensure_class_descriptions_cached() -> dict[str, str]:
    """label_id -> label_name, from the (small, ~50k-row) boxable-classes CSV."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    csv_path = cache_dir / "oidv7-class-descriptions-boxable.csv"
    _download_once(_CLASS_DESCRIPTIONS_URL, csv_path, timeout=60)

    descriptions: dict[str, str] = {}
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) >= 2:
                descriptions[row[0].strip()] = row[1].strip()
    return descriptions


def _ensure_annotations_cached(split: str) -> Path:
    if split not in _ANNOTATIONS_URLS:
        raise ValueError(f"Invalid Open Images split {split!r}; choose from {list(_ANNOTATIONS_URLS)}")
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    csv_path = cache_dir / f"{split}-annotations-bbox.csv"
    _download_once(_ANNOTATIONS_URLS[split], csv_path)
    return csv_path


@register_connector(SourceType.OPEN_IMAGES)
class OpenImagesConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64), split: str = "validation"):
        self.min_w, self.min_h = min_resolution
        self.split = split

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_classes: list[str] | None = None
    ) -> DiscoveryPage:
        try:
            descriptions = _ensure_class_descriptions_cached()
        except Exception as e:
            logger.warning(f"Open Images class descriptions unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download Open Images class descriptions: {e}",
            )

        if explicit_classes:
            wanted = {c.strip().lower() for c in explicit_classes if c.strip()}
            matched_ids = {lid for lid, name in descriptions.items() if name.lower() in wanted}
        else:
            matched_ids = self._match_classes(query, descriptions)
        if not matched_ids:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No Open Images classes matched query {query!r} — its taxonomy is "
                    "narrower than free text; use explicit class overrides"
                ),
            )

        try:
            ann_csv_path = _ensure_annotations_cached(self.split)
        except Exception as e:
            logger.warning(f"Open Images annotations unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download Open Images {self.split} annotations: {e}",
            )

        image_ids: list[str] = []
        seen: set[str] = set()
        with open(ann_csv_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("LabelName") in matched_ids and row["ImageID"] not in seen:
                    seen.add(row["ImageID"])
                    image_ids.append(row["ImageID"])
                    if len(image_ids) >= limit:
                        break

        if not image_ids:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [image_ids[i : i + _SHARD_SIZE] for i in range(0, len(image_ids), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.OPEN_IMAGES,
                external_ref={"image_ids": shard, "split": self.split, "label_ids": sorted(matched_ids)},
                license_hint=_LICENSE_NOTE,
                dataset_identifier=f"open-images-{self.split}",
            )
            for shard in shards
        ]
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    @staticmethod
    def _match_classes(query: str, descriptions: dict[str, str]) -> set[str]:
        """Two-directional substring match (case-insensitive, prefer exact
        when multiple hit) — same convention as every other connector added
        this session. The reference script's original one-directional check
        (`keyword in name`) missed plain plurals like "cats" not being a
        substring of "cat", which a real job caught."""
        keyword_lower = query.strip().lower()
        matches = [
            (lid, name) for lid, name in descriptions.items()
            if keyword_lower in name.lower() or name.lower() in keyword_lower
        ]
        if len(matches) > 1:
            exact = [(lid, name) for lid, name in matches if name.lower() == keyword_lower]
            if exact:
                matches = exact
        return {lid for lid, _ in matches}

    # -- Download ----------------------------------------------------------------

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        from PIL import Image

        split = item.external_ref.get("split", self.split)
        image_ids = item.external_ref["image_ids"]
        label_ids = set(item.external_ref.get("label_ids", []))

        try:
            descriptions = _ensure_class_descriptions_cached()
            ann_csv_path = _ensure_annotations_cached(split)
        except Exception as e:
            raise TransientConnectorError(f"Open Images metadata unavailable: {e}") from e

        # Re-scan the (already-cached-locally) CSV for just this shard's
        # boxes — cheap relative to the initial download, and avoids holding
        # the whole file's rows in memory across shard tasks. Kept as plain
        # relative (0-1) tuples, not BBox (whose xmin/ymin/xmax/ymax contract
        # is absolute pixels) — the image's own pixel size isn't known until
        # after download, so scaling to absolute happens per-image below.
        image_id_set = set(image_ids)
        rel_boxes_by_image: dict[str, list[tuple[str, float, float, float, float]]] = {
            iid: [] for iid in image_ids
        }
        with open(ann_csv_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                img_id = row.get("ImageID")
                if img_id not in image_id_set or row.get("LabelName") not in label_ids:
                    continue
                rel_boxes_by_image[img_id].append(
                    (
                        descriptions.get(row["LabelName"], "unknown"),
                        float(row.get("XMin", 0)),
                        float(row.get("YMin", 0)),
                        float(row.get("XMax", 0)),
                        float(row.get("YMax", 0)),
                    )
                )

        for i, image_id in enumerate(image_ids):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during Open Images shard walk")

            url = f"{_IMAGE_BASE_URL}/{split}/{image_id}.jpg"
            dest = workdir / f"{image_id}.jpg"
            try:
                with requests.get(url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(dest, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            f.write(chunk)
                with Image.open(dest) as img:
                    img.verify()
                with Image.open(dest) as img:
                    w, h = img.size
                if w < self.min_w or h < self.min_h:
                    on_progress({"rejected": 1})
                    continue
            except Exception as e:
                logger.warning(f"Open Images image {image_id} download/validation failed: {e}")
                on_progress({"rejected": 1})
                continue

            registry = ClassRegistry()
            abs_boxes = [
                BBox(class_name=name, xmin=xmin * w, ymin=ymin * h, xmax=xmax * w, ymax=ymax * h)
                for name, xmin, ymin, xmax, ymax in rel_boxes_by_image.get(image_id, [])
            ]
            ann_path = None
            annotation_format = None
            if abs_boxes:
                lines = boxes_to_yolo_lines(abs_boxes, w, h, registry)
                ann_path = workdir / f"{image_id}.txt"
                ann_path.write_text("\n".join(lines))
                annotation_format = "yolo_txt"

            draft = RawAssetDraft(
                source_item_id=f"oi_{split}_{image_id}",
                source_url=url,
                dataset_identifier=item.dataset_identifier,
                license=item.license_hint,
                local_tmp_path=dest,
                original_name=dest.name,
                annotation_tmp_path=ann_path,
                annotation_format=annotation_format,
                width=w,
                height=h,
                classes=registry.names,
                metadata={"open_images_image_id": image_id, "split": split},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
