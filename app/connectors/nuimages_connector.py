"""NuImagesConnector — registered as SourceType.NUSCENES (kept user-facing
as "nuScenes" per the request), but actually pulls from **nuImages**
(Motional's sibling 2D-detection dataset, same organization, same public
bucket family: motional-nuscenes.s3.amazonaws.com), not nuScenes' native
lidar/3D-box data. This platform has no 3D annotation representation and
projecting 3D boxes onto camera images is out of scope — nuImages ships
genuine 2D camera images with 2D bounding boxes directly, no projection
needed, so it's the right fit for "nuScenes" in spirit. Flagged explicitly
here and in the connector's own dataset_identifier ("nuimages-v1.0-mini"),
not silently substituted.

Uses the public nuImages v1.0-mini split (~118MB, no login/signing needed —
an ordinary public S3 object) rather than the full-size splits (each
30GB+), matching every other new connector's "bounded default" convention.
"""

import json
import logging
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

_ARCHIVE_URL = "https://motional-nuscenes.s3.amazonaws.com/public/nuimages-v1.0/nuimages-v1.0-mini.tgz"
_SHARD_SIZE = 20
_LICENSE_NOTE = (
    "nuImages v1.0-mini (non-commercial research use — Motional terms, "
    "see https://www.nuscenes.org/terms-of-use)"
)


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "nuimages"


def _ensure_dataset_cached() -> Path:
    """Downloads+extracts the nuImages v1.0-mini tgz once (~118MB) and
    returns the cache root (containing v1.0-mini/*.json + samples/)."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    meta_dir = cache_dir / "v1.0-mini"
    if meta_dir.exists():
        return cache_dir

    tgz_path = cache_dir / "nuimages-v1.0-mini.tgz"
    if not tgz_path.exists():
        logger.info(f"Downloading nuImages v1.0-mini ({_ARCHIVE_URL})")
        tmp_path = tgz_path.with_suffix(".tgz.tmp")
        try:
            with requests.get(_ARCHIVE_URL, stream=True, timeout=120, headers=HTTP_HEADERS) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
        except requests.HTTPError as e:
            raise FatalConnectorError(f"nuImages archive not accessible: {e}") from e
        tmp_path.replace(tgz_path)

    import tarfile

    with tarfile.open(tgz_path) as tf:
        tf.extractall(cache_dir, filter="data")

    return cache_dir


def _load_tables(cache_dir: Path) -> tuple[dict, dict, list]:
    meta_dir = cache_dir / "v1.0-mini"
    categories = {c["token"]: c["name"] for c in json.loads((meta_dir / "category.json").read_text())}
    sample_data = {s["token"]: s for s in json.loads((meta_dir / "sample_data.json").read_text())}
    object_ann = json.loads((meta_dir / "object_ann.json").read_text())
    return categories, sample_data, object_ann


def _ensure_class_index_cached(cache_dir: Path) -> dict[str, list[str]]:
    """category_name -> [sample_data_token, ...], built once from
    object_ann.json + category.json."""
    index_path = cache_dir / "class_index.json"
    if index_path.exists():
        return json.loads(index_path.read_text())

    categories, _sample_data, object_ann = _load_tables(cache_dir)
    index: dict[str, list[str]] = defaultdict(list)
    for ann in object_ann:
        name = categories.get(ann["category_token"])
        token = ann["sample_data_token"]
        if name and token not in index[name]:
            index[name].append(token)

    index_path.write_text(json.dumps(index))
    return dict(index)


@register_connector(SourceType.NUSCENES)
class NuImagesConnector:
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
            logger.warning(f"nuImages dataset unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/index nuImages: {e}",
            )

        vocabulary = list(class_index.keys())
        if explicit_categories:
            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
            matched = {c for c in vocabulary if c.lower() in wanted}
        else:
            keyword_lower = query.strip().lower()
            matched = {c for c in vocabulary if keyword_lower in c.lower() or c.lower() in keyword_lower}
        if not matched:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No nuImages categories matched query {query!r} — its taxonomy is "
                    "driving-scene specific (vehicle.*, human.pedestrian.*, ...); use "
                    "explicit category overrides"
                ),
            )

        tokens = sorted({t for cls in matched for t in class_index.get(cls, [])})[:limit]
        if not tokens:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [tokens[i : i + _SHARD_SIZE] for i in range(0, len(tokens), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.NUSCENES,
                external_ref={"sample_data_tokens": shard},
                license_hint=_LICENSE_NOTE,
                dataset_identifier="nuimages-v1.0-mini",
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
            categories, sample_data, object_ann = _load_tables(cache_dir)
        except Exception as e:
            raise TransientConnectorError(f"nuImages dataset unavailable: {e}") from e

        anns_by_token: dict[str, list[dict]] = defaultdict(list)
        for ann in object_ann:
            anns_by_token[ann["sample_data_token"]].append(ann)

        tokens = item.external_ref["sample_data_tokens"]

        for i, token in enumerate(tokens):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during nuImages shard walk")

            sd = sample_data.get(token)
            if sd is None:
                on_progress({"rejected": 1})
                continue

            src_img = cache_dir / sd["filename"]
            if not src_img.exists():
                on_progress({"rejected": 1})
                continue

            dest = workdir / f"nuimages_{token}{src_img.suffix}"
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
                logger.warning(f"nuImages sample {token} copy/validate failed: {e}")
                on_progress({"rejected": 1})
                continue

            registry = ClassRegistry()
            boxes = [
                BBox(
                    class_name=categories.get(ann["category_token"], "unknown"),
                    xmin=ann["bbox"][0], ymin=ann["bbox"][1], xmax=ann["bbox"][2], ymax=ann["bbox"][3],
                )
                for ann in anns_by_token.get(token, [])
            ]
            ann_path = None
            annotation_format = None
            if boxes:
                lines = boxes_to_yolo_lines(boxes, w, h, registry)
                ann_path = workdir / f"nuimages_{token}.txt"
                ann_path.write_text("\n".join(lines))
                annotation_format = "yolo_txt"

            draft = RawAssetDraft(
                source_item_id=f"nuimages_{token}",
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
                metadata={"nuimages_sample_data_token": token},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
