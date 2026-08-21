"""DavisConnector — targeted downloads from DAVIS 2017
(davischallenge.org/davis2017/code.html), a class-agnostic video object
segmentation benchmark: ground truth is a per-frame palette PNG mask (pixel
value = object index within that video sequence), not a bounding box, and
objects carry no semantic class name at all — unlike COCO/VOC/Open Images,
DAVIS has no vocabulary of "dog"/"car"/"person" to match a query against.

Two deliberate, flagged compromises to fit this platform's bbox-only
detection pipeline (AnnotationFormat has no segmentation-mask representation):
  1. Query matching happens against DAVIS's ~60 video *sequence names*
     (e.g. "dog", "car-roundabout", "boat") via plain substring — the only
     content-descriptive strings DAVIS ships, an imperfect proxy for a real
     class vocabulary.
  2. Each object's bounding box is derived from its segmentation mask's
     bounding rectangle (a standard, lossy but common mask->bbox reduction)
     and labeled with its *sequence* name, since individual objects within
     a sequence have no finer semantic label of their own.
"""

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
from app.services.format_conversion import BBox, ClassRegistry, boxes_to_yolo_lines

logger = logging.getLogger(__name__)

_ARCHIVE_URL = "https://data.vision.ee.ethz.ch/csergi/share/davis/DAVIS-2017-trainval-480p.zip"
_SHARD_SIZE = 20
_LICENSE_NOTE = (
    "DAVIS 2017 (non-commercial research use only — "
    "see https://davischallenge.org/davis2017/code.html)"
)


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "davis"


def _ensure_dataset_cached() -> Path:
    """Downloads+extracts the DAVIS 2017 trainval-480p zip once (~830MB) and
    returns the extracted DAVIS/ directory."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    davis_dir = cache_dir / "DAVIS"
    if davis_dir.exists():
        return davis_dir

    zip_path = cache_dir / "DAVIS-2017-trainval-480p.zip"
    if not zip_path.exists():
        logger.info(f"Downloading DAVIS 2017 trainval-480p ({_ARCHIVE_URL})")
        tmp_path = zip_path.with_suffix(".zip.tmp")
        try:
            with requests.get(_ARCHIVE_URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
        except requests.HTTPError as e:
            raise FatalConnectorError(f"DAVIS archive not accessible: {e}") from e
        tmp_path.replace(zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(cache_dir)

    return davis_dir


def _match_sequences(query: str, sequences: list[str]) -> set[str]:
    """Substring match, either direction (query in name, or name in query)
    — same convention as the other connectors added this session."""
    keyword_lower = query.strip().lower()
    return {s for s in sequences if keyword_lower in s.lower() or s.lower() in keyword_lower}


def _mask_to_boxes(mask_path: Path, sequence_name: str) -> list[BBox]:
    """Bounding-rectangle-per-labeled-region reduction of a DAVIS palette
    mask (pixel value = object index, 0 = background)."""
    import numpy as np
    from PIL import Image

    with Image.open(mask_path) as img:
        arr = np.array(img)
    if arr.ndim == 3:  # some masks are RGB rather than palette-indexed
        arr = arr[:, :, 0]

    boxes = []
    for object_id in np.unique(arr):
        if object_id == 0:
            continue
        ys, xs = np.where(arr == object_id)
        if ys.size == 0:
            continue
        boxes.append(
            BBox(
                class_name=sequence_name,
                xmin=float(xs.min()), ymin=float(ys.min()),
                xmax=float(xs.max()) + 1, ymax=float(ys.max()) + 1,
            )
        )
    return boxes


@register_connector(SourceType.DAVIS)
class DavisConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(
        self, query: str, cursor: str | None, limit: int = 200, explicit_categories: list[str] | None = None
    ) -> DiscoveryPage:
        try:
            davis_dir = _ensure_dataset_cached()
            sequences = sorted(p.name for p in (davis_dir / "JPEGImages" / "480p").iterdir() if p.is_dir())
        except FatalConnectorError as e:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True, error_reason=str(e))
        except Exception as e:
            logger.warning(f"DAVIS dataset unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/index DAVIS 2017: {e}",
            )

        if explicit_categories:
            wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
            matched_sequences = {s for s in sequences if s.lower() in wanted}
        else:
            matched_sequences = _match_sequences(query, sequences)
        if not matched_sequences:
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=(
                    f"No DAVIS video sequences matched query {query!r} — DAVIS has no "
                    "semantic class vocabulary, only ~60 sequence names; use explicit "
                    "category overrides with a sequence name"
                ),
            )

        frames: list[dict] = []
        for seq in sorted(matched_sequences):
            frame_dir = davis_dir / "JPEGImages" / "480p" / seq
            for frame_path in sorted(frame_dir.glob("*.jpg")):
                frames.append({"sequence": seq, "frame": frame_path.stem})
                if len(frames) >= limit:
                    break
            if len(frames) >= limit:
                break
        if not frames:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        shards = [frames[i : i + _SHARD_SIZE] for i in range(0, len(frames), _SHARD_SIZE)]
        items = [
            SourceItem(
                source_type=SourceType.DAVIS,
                external_ref={"frames": shard},
                license_hint=_LICENSE_NOTE,
                dataset_identifier="davis-2017-trainval-480p",
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

        try:
            davis_dir = _ensure_dataset_cached()
        except Exception as e:
            raise TransientConnectorError(f"DAVIS dataset unavailable: {e}") from e

        frames = item.external_ref["frames"]

        for i, frame_ref in enumerate(frames):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during DAVIS shard walk")

            seq, frame = frame_ref["sequence"], frame_ref["frame"]
            src_img = davis_dir / "JPEGImages" / "480p" / seq / f"{frame}.jpg"
            src_mask = davis_dir / "Annotations" / "480p" / seq / f"{frame}.png"
            if not src_img.exists():
                on_progress({"rejected": 1})
                continue

            dest = workdir / f"davis_{seq}_{frame}.jpg"
            try:
                import shutil

                shutil.copy2(src_img, dest)
                with Image.open(dest) as img:
                    img.verify()
                with Image.open(dest) as img:
                    w, h = img.size
                if w < self.min_w or h < self.min_h:
                    on_progress({"rejected": 1})
                    continue
            except Exception as e:
                logger.warning(f"DAVIS frame {seq}/{frame} copy/validate failed: {e}")
                on_progress({"rejected": 1})
                continue

            ann_path = None
            annotation_format = None
            registry = ClassRegistry()
            if src_mask.exists():
                try:
                    boxes = _mask_to_boxes(src_mask, seq)
                except Exception as e:
                    logger.warning(f"DAVIS mask {seq}/{frame} unreadable: {e}")
                    boxes = []
                if boxes:
                    lines = boxes_to_yolo_lines(boxes, w, h, registry)
                    ann_path = workdir / f"davis_{seq}_{frame}.txt"
                    ann_path.write_text("\n".join(lines))
                    annotation_format = "yolo_txt"

            draft = RawAssetDraft(
                source_item_id=f"davis_{seq}_{frame}",
                source_url=None,
                dataset_identifier=item.dataset_identifier,
                license=item.license_hint,
                local_tmp_path=dest,
                original_name=dest.name,
                annotation_tmp_path=ann_path,
                annotation_format=annotation_format,
                width=w,
                height=h,
                classes=registry.names,
                metadata={"davis_sequence": seq, "davis_frame": frame},
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
