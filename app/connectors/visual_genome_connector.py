"""VisualGenomeConnector — targeted downloads from Visual Genome
(visualgenome.org), using its `objects.json` per-image object-box layer,
not the dense region-caption/relationship graph.

Ported from the user-supplied reference script (scraper_for_pascal_and_gnome.py
::VisualGenomeScraper): download+cache the small `objects.json` +
`image_data.json` metadata files once (never the ~20GB bulk image archives),
substring-match the query against each object's `names` *and* `synsets` (a
nice addition this reference script has, kept here), then fetch each
matched image directly via its own `url` field from image_data.json. Boxes
are absolute `x,y,w,h` -> converted to YOLO_TXT via
app/services/format_conversion.py::BBox/boxes_to_yolo_lines() (no closed
vocabulary here, unlike COCO/VOC, so no native AnnotationFormat fits).
"""

import json
import logging
import zipfile
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
from app.core.exceptions import JobStopped, TransientConnectorError
from app.domain.enums import SourceType
from app.services.format_conversion import BBox, ClassRegistry, boxes_to_yolo_lines

logger = logging.getLogger(__name__)

_OBJECTS_URL = "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/objects.json.zip"
_IMAGE_DATA_URL = "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/image_data.json.zip"
_SHARD_SIZE = 20
_LICENSE_NOTE = "Visual Genome (CC BY 4.0 — see https://visualgenome.org/api/v0/api_home.html)"


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "visual_genome"


def _download_and_unzip(url: str, dest_json: Path) -> None:
    tmp_zip = dest_json.with_suffix(".zip.tmp")
    with requests.get(url, stream=True, timeout=300, headers=HTTP_HEADERS) as r:
        r.raise_for_status()
        with open(tmp_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    with zipfile.ZipFile(tmp_zip) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".json"))
        with zf.open(member) as src, open(dest_json.with_suffix(".json.tmp"), "wb") as dst:
            dst.write(src.read())
    dest_json.with_suffix(".json.tmp").replace(dest_json)
    tmp_zip.unlink(missing_ok=True)


def _ensure_metadata_cached() -> tuple[Path, Path]:
    """Downloads+caches Visual Genome's small metadata files once
    (objects.json ~300MB uncompressed, image_data.json ~10MB) — never the
    ~20GB bulk image archives."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    objects_path = cache_dir / "objects.json"
    image_data_path = cache_dir / "image_data.json"
    if not objects_path.exists():
        logger.info(f"Downloading Visual Genome objects.json ({_OBJECTS_URL})")
        _download_and_unzip(_OBJECTS_URL, objects_path)
    if not image_data_path.exists():
        logger.info(f"Downloading Visual Genome image_data.json ({_IMAGE_DATA_URL})")
        _download_and_unzip(_IMAGE_DATA_URL, image_data_path)
    return objects_path, image_data_path


def _matches_keyword(label: str, keyword_lower: str) -> bool:
    """Substring match, either direction — ported from the reference
    script's BaseScraper._matches_keyword()."""
    label_lower = label.lower()
    return keyword_lower in label_lower or label_lower in keyword_lower


@register_connector(SourceType.VISUAL_GENOME)
class VisualGenomeConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_categories: list[str] | None = None
    ) -> DiscoveryPage:
        try:
            objects_path, image_data_path = _ensure_metadata_cached()
            objects_by_image = json.loads(objects_path.read_text())
        except Exception as e:
            logger.warning(f"Visual Genome metadata unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/parse Visual Genome metadata: {e}",
            )

        if explicit_categories:
            keywords = [c.strip().lower() for c in explicit_categories if c.strip()]
        else:
            keywords = [query.strip().lower()] if query.strip() else []
        if not keywords:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Could not derive a search term from query {query!r}",
            )

        image_ids: list[int] = []
        for entry in objects_by_image:
            all_labels = [
                label
                for obj in entry.get("objects", [])
                for label in [*obj.get("names", []), *obj.get("synsets", [])]
            ]
            if any(_matches_keyword(label, kw) for label in all_labels for kw in keywords):
                image_ids.append(entry["image_id"])
                if len(image_ids) >= limit:
                    break

        if not image_ids:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No Visual Genome objects matched query {query!r} — "
                    "use explicit category overrides"
                ),
            )

        shards = [image_ids[i : i + _SHARD_SIZE] for i in range(0, len(image_ids), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.VISUAL_GENOME,
                external_ref={
                    "image_ids": shard,
                    "metadata_path": str(image_data_path),
                    "objects_path": str(objects_path),
                    "keywords": keywords,
                },
                license_hint=_LICENSE_NOTE,
                dataset_identifier="visual-genome",
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
        from PIL import Image

        image_ids = set(item.external_ref["image_ids"])
        keywords = item.external_ref.get("keywords", [])
        try:
            image_data = {
                e["image_id"]: e for e in json.loads(Path(item.external_ref["metadata_path"]).read_text())
            }
            objects_by_id = {
                e["image_id"]: e["objects"]
                for e in json.loads(Path(item.external_ref["objects_path"]).read_text())
                if e["image_id"] in image_ids
            }
        except Exception as e:
            raise TransientConnectorError(f"Visual Genome metadata unavailable: {e}") from e

        ordered_ids = item.external_ref["image_ids"]
        for i, image_id in enumerate(ordered_ids):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during Visual Genome shard walk")

            info = image_data.get(image_id)
            if info is None:
                on_progress({"rejected": 1})
                continue

            url = info["url"]
            dest = workdir / f"vg_{image_id}.jpg"
            try:
                with requests.get(url, stream=True, timeout=30, headers=HTTP_HEADERS) as r:
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
                logger.warning(f"Visual Genome image {image_id} download/validation failed: {e}")
                on_progress({"rejected": 1})
                continue

            registry = ClassRegistry()
            boxes = []
            for obj in objects_by_id.get(image_id, []):
                names = obj.get("names", [])
                synsets = obj.get("synsets", [])
                matched_label = next(
                    (label for label in [*names, *synsets] if any(_matches_keyword(label, kw) for kw in keywords)),
                    None,
                )
                if matched_label is None:
                    continue
                boxes.append(
                    BBox(
                        class_name=matched_label,
                        xmin=obj["x"], ymin=obj["y"], xmax=obj["x"] + obj["w"], ymax=obj["y"] + obj["h"],
                    )
                )

            ann_path = None
            annotation_format = None
            if boxes:
                lines = boxes_to_yolo_lines(boxes, w, h, registry)
                ann_path = workdir / f"vg_{image_id}.txt"
                ann_path.write_text("\n".join(lines))
                annotation_format = "yolo_txt"

            draft = RawAssetDraft(
                source_item_id=f"vg_{image_id}",
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
                metadata={"visual_genome_image_id": image_id},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
