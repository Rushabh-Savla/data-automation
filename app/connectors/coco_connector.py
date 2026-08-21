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
"""

import hashlib
import json
import logging
import zipfile
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
    redundant work; not worth a lock for this."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    json_path = cache_dir / f"instances_{split}.json"
    if json_path.exists():
        return json_path

    zip_path = cache_dir / "annotations_trainval2017.zip"
    if not zip_path.exists():
        logger.info(f"Downloading COCO annotations ({_ANNOTATIONS_ZIP_URL})")
        tmp_path = zip_path.with_suffix(".zip.tmp")
        with requests.get(_ANNOTATIONS_ZIP_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        tmp_path.replace(zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        member = f"annotations/instances_{split}.json"
        if member not in zf.namelist():
            raise FatalConnectorError(f"COCO split '{split}' not found in annotations archive")
        tmp_json = json_path.with_suffix(".json.tmp")
        with zf.open(member) as src, open(tmp_json, "wb") as dst:
            dst.write(src.read())
        tmp_json.replace(json_path)

    return json_path


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

        try:
            coco_data = json.loads(_ensure_annotations_cached(self.split).read_text())
        except Exception as e:
            logger.warning(f"COCO annotations unavailable: {e}")
            return DiscoveryPage(
                items=[],
                next_cursor=None,
                exhausted=True,
                error_reason=f"Failed to download/parse COCO annotations: {e}",
            )

        if explicit_categories:
            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
            matched_ids = {
                cat["id"] for cat in coco_data.get("categories", []) if cat["name"].lower() in wanted
            }
        else:
            matched_ids = self._match_categories(query, coco_data.get("categories", []))
        if not matched_ids:
            return DiscoveryPage(
                items=[],
                next_cursor=None,
                exhausted=True,
                error_reason=(
                    f"No COCO categories matched query {query!r} — COCO's 80-class "
                    "vocabulary is narrower than free text; use explicit category overrides"
                ),
            )

        image_ids = sorted(
            {
                ann["image_id"]
                for ann in coco_data.get("annotations", [])
                if ann.get("category_id") in matched_ids
            }
        )[:limit]
        if not image_ids:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [image_ids[i : i + _SHARD_SIZE] for i in range(0, len(image_ids), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.COCO,
                external_ref={"image_ids": shard, "split": self.split},
                license_hint=None,
                dataset_identifier=f"coco-{self.split}",
            )
            for shard in shards
        ]
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    @staticmethod
    def _match_categories(query: str, categories: list[dict]) -> set[int]:
        """Two-directional substring match (case-insensitive, prefer exact
        when multiple hit) — same convention as every other connector added
        this session (pascal_voc_connector.py, visual_genome_connector.py,
        etc.). The reference script's original one-directional check
        (`keyword in name`) missed plain plurals like "cats" not being a
        substring of "cat", which a real job caught (query="cats" failed to
        match COCO's "cat" category while every other source succeeded)."""
        keyword_lower = query.strip().lower()
        matches = [
            cat for cat in categories
            if keyword_lower in cat["name"].lower() or cat["name"].lower() in keyword_lower
        ]
        if len(matches) > 1:
            exact = [c for c in matches if c["name"].lower() == keyword_lower]
            if exact:
                matches = exact
        return {c["id"] for c in matches}

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

        try:
            coco_data = json.loads(_ensure_annotations_cached(split).read_text())
        except FatalConnectorError:
            raise
        except Exception as e:
            raise TransientConnectorError(f"COCO annotations unavailable: {e}") from e

        images_by_id = {im["id"]: im for im in coco_data.get("images", [])}
        licenses = {lic["id"]: lic["name"] for lic in coco_data.get("licenses", [])}

        # Small per-shard subset (not the whole cached file) written once,
        # shared as annotation_tmp_path by every draft in this shard — cheap
        # since a shard is capped at _SHARD_SIZE images.
        image_id_set = set(image_ids)
        subset = {
            "images": [images_by_id[iid] for iid in image_ids if iid in images_by_id],
            "annotations": [
                ann for ann in coco_data.get("annotations", []) if ann.get("image_id") in image_id_set
            ],
            "categories": coco_data.get("categories", []),
        }
        shard_hash = hashlib.md5(str(image_ids).encode()).hexdigest()[:12]
        subset_path = workdir / f"coco_subset_{shard_hash}.json"
        subset_path.write_text(json.dumps(subset))

        for i, image_id in enumerate(image_ids):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during COCO shard walk")

            image_info = images_by_id.get(image_id)
            if image_info is None:
                on_progress({"rejected": 1})
                continue

            url = image_info.get("coco_url")
            dest = workdir / image_info["file_name"]
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
                logger.warning(f"COCO image {image_id} download/validation failed: {e}")
                on_progress({"rejected": 1})
                continue

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
                metadata={"coco_image_id": image_id, "split": split},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
