"""PascalVocConnector — targeted, category-filtered downloads from the
official Pascal VOC 2012 dataset (host.robots.ox.ac.uk/pascal/VOC).

Ported from the user-supplied reference script (scraper_for_pascal_and_gnome.py
::PascalVOCScraper): download+extract the trainval tar once, substring-match
the query against the fixed 20-class vocabulary, then use
`ImageSets/Main/{class}_{split}.txt` (space-separated `image_id flag`,
flag==1 means the class is present) for direct per-class image lookup —
avoids scanning all ~17k annotation XML files, the genuine efficiency win
this reference script has over a naive full scan. Falls back to a full XML
scan only if `ImageSets/Main` is missing. Reuses the native VOC_XML
AnnotationFormat via app/services/format_conversion.py::parse_voc_xml()
verbatim — zero new parsing code.
"""

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

logger = logging.getLogger(__name__)

_ARCHIVE_URL = "http://host.robots.ox.ac.uk/pascal/VOC/voc2012/VOCtrainval_11-May-2012.tar"
_SHARD_SIZE = 20
_VOC_CLASSES = [
    "aeroplane", "bicycle", "bird", "boat", "bottle", "bus", "car", "cat", "chair", "cow",
    "diningtable", "dog", "horse", "motorbike", "person", "pottedplant", "sheep", "sofa",
    "train", "tvmonitor",
]
_LICENSE_NOTE = "Pascal VOC 2012 (CC BY 2.5 dataset; source images subject to original Flickr owners' terms)"


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "pascal_voc"


def _ensure_dataset_cached() -> Path:
    """Downloads+extracts the VOC2012 trainval tar once (~1.9GB) and returns
    the extracted VOCdevkit/VOC2012 directory."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    devkit_dir = cache_dir / "VOCdevkit" / "VOC2012"
    if devkit_dir.exists():
        return devkit_dir

    tar_path = cache_dir / "VOCtrainval_11-May-2012.tar"
    if not tar_path.exists():
        logger.info(f"Downloading Pascal VOC 2012 trainval ({_ARCHIVE_URL})")
        tmp_path = tar_path.with_suffix(".tar.tmp")
        try:
            with requests.get(_ARCHIVE_URL, stream=True, timeout=300, headers=HTTP_HEADERS) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
        except requests.HTTPError as e:
            raise FatalConnectorError(f"Pascal VOC archive not accessible: {e}") from e
        tmp_path.replace(tar_path)

    import tarfile

    with tarfile.open(tar_path) as tf:
        tf.extractall(cache_dir, filter="data")

    return devkit_dir


def _match_classes(query: str) -> set[str]:
    """Substring match (case-insensitive, prefer exact when multiple hit) —
    ported from the reference script's PascalVOCScraper._matches_keyword()."""
    keyword_lower = query.strip().lower()
    matches = [c for c in _VOC_CLASSES if keyword_lower in c or c in keyword_lower]
    if len(matches) > 1 and keyword_lower in matches:
        matches = [keyword_lower]
    return set(matches)


def _image_ids_for_classes(devkit_dir: Path, classes: set[str]) -> list[str]:
    """Uses ImageSets/Main/{class}_{split}.txt for direct per-class lookup —
    the reference script's efficiency win over scanning every XML file."""
    imagesets_dir = devkit_dir / "ImageSets" / "Main"
    if not imagesets_dir.exists():
        return _image_ids_via_xml_scan(devkit_dir, classes)

    target_ids: set[str] = set()
    for cls in classes:
        for split in ("trainval", "train", "val"):
            set_file = imagesets_dir / f"{cls}_{split}.txt"
            if not set_file.exists():
                continue
            for line in set_file.read_text().splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1] == "1":
                    target_ids.add(parts[0])
            break  # first available split for this class is enough
    return sorted(target_ids)


def _image_ids_via_xml_scan(devkit_dir: Path, classes: set[str]) -> list[str]:
    from app.services.format_conversion import parse_voc_xml

    target_ids: set[str] = set()
    for xml_path in (devkit_dir / "Annotations").glob("*.xml"):
        try:
            _, _, boxes = parse_voc_xml(xml_path)
        except Exception as e:
            logger.warning(f"Skipping unreadable VOC annotation {xml_path.name}: {e}")
            continue
        if any(b.class_name in classes for b in boxes):
            target_ids.add(xml_path.stem)
    return sorted(target_ids)


@register_connector(SourceType.PASCAL_VOC)
class PascalVocConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_categories: list[str] | None = None
    ) -> DiscoveryPage:
        if explicit_categories:
            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
            matched = {c for c in _VOC_CLASSES if c in wanted}
        else:
            matched = _match_classes(query)
        if not matched:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No Pascal VOC categories matched query {query!r} — VOC's 20-class "
                    "vocabulary is narrower than free text; use explicit category overrides"
                ),
            )

        try:
            devkit_dir = _ensure_dataset_cached()
            image_ids = _image_ids_for_classes(devkit_dir, matched)[:limit]
        except FatalConnectorError as e:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True, error_reason=str(e))
        except Exception as e:
            logger.warning(f"Pascal VOC dataset unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/index Pascal VOC 2012: {e}",
            )

        if not image_ids:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [image_ids[i : i + _SHARD_SIZE] for i in range(0, len(image_ids), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.PASCAL_VOC,
                external_ref={"image_ids": shard},
                license_hint=_LICENSE_NOTE,
                dataset_identifier="pascal-voc2012",
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

        if should_stop():
            raise JobStopped("should_stop() signalled before Pascal VOC shard fetch began")

        try:
            devkit_dir = _ensure_dataset_cached()
        except Exception as e:
            raise TransientConnectorError(f"Pascal VOC dataset unavailable: {e}") from e

        image_ids = item.external_ref["image_ids"]

        for i, image_id in enumerate(image_ids):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during Pascal VOC shard walk")

            src_img = devkit_dir / "JPEGImages" / f"{image_id}.jpg"
            src_xml = devkit_dir / "Annotations" / f"{image_id}.xml"
            if not src_img.exists() or not src_xml.exists():
                on_progress({"rejected": 1})
                continue

            dest_img = workdir / src_img.name
            dest_xml = workdir / src_xml.name
            try:
                shutil.copy2(src_img, dest_img)
                shutil.copy2(src_xml, dest_xml)
                with Image.open(dest_img) as img:
                    img.verify()
                with Image.open(dest_img) as img:
                    w, h = img.size
                if w < self.min_w or h < self.min_h:
                    on_progress({"rejected": 1})
                    continue
            except Exception as e:
                logger.warning(f"Pascal VOC image {image_id} copy/validate failed: {e}")
                on_progress({"rejected": 1})
                continue

            draft = RawAssetDraft(
                source_item_id=f"voc2012_{image_id}",
                source_url=None,
                dataset_identifier=item.dataset_identifier,
                license=item.license_hint,
                local_tmp_path=dest_img,
                original_name=src_img.name,
                annotation_tmp_path=dest_xml,
                annotation_format="voc_xml",
                width=w,
                height=h,
                metadata={"voc_image_id": image_id},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
