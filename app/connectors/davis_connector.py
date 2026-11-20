"""DavisConnector — targeted downloads from DAVIS 2017
(davischallenge.org/davis2017/code.html), a class-agnostic video object
segmentation benchmark: ground truth is a per-frame palette PNG mask (pixel
value = object index within that video sequence), not a bounding box, and
objects carry no semantic class name at all — unlike COCO/VOC/Open Images,
DAVIS has no vocabulary of "dog"/"car"/"person" to match a query against.

Two deliberate, flagged compromises to fit this platform's bbox-only
detection pipeline (AnnotationFormat has no segmentation-mask representation):
  1. Query matching happens against DAVIS's video *sequence names* (90 in
     the trainval split, e.g. "dog", "car-roundabout", "boat") per query term,
     plus a small hand-checked class->sequence map for classes the names never
     mention (e.g. "person") — the only content-descriptive strings DAVIS
     ships, an imperfect proxy for a real class vocabulary.
  2. Each object's bounding box is derived from its segmentation mask's
     bounding rectangle (a standard, lossy but common mask->bbox reduction)
     and labeled with its *sequence* name, since individual objects within
     a sequence have no finer semantic label of their own.
"""

import logging
import re
import zipfile
from collections.abc import Iterator
from pathlib import Path

from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    ensure_archive,
    exclusive_cache_lock,
    extract_into_cache,
    register_connector,
)
from app.connectors.query_terms import expand_terms, singularize
from app.core.config import get_settings
from app.core.exceptions import CandidateWindowFull, FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType
from app.services.format_conversion import BBox, ClassRegistry, boxes_to_yolo_lines

logger = logging.getLogger(__name__)

_ARCHIVE_URL = "https://data.vision.ee.ethz.ch/csergi/share/davis/DAVIS-2017-trainval-480p.zip"
_SHARD_SIZE = 50
_LICENSE_NOTE = (
    "DAVIS 2017 (non-commercial research use only — "
    "see https://davischallenge.org/davis2017/code.html)"
)


def _cache_dir() -> Path:
    return get_settings().work_root / "_shared_cache" / "davis"


def _ensure_dataset_cached() -> Path:
    """Downloads+extracts the DAVIS 2017 trainval-480p zip once (~830MB) and
    returns the extracted DAVIS/ directory.

    discover() and every download shard call this concurrently, so the
    download/extract runs under an exclusive lock (see exclusive_cache_lock)."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    davis_dir = cache_dir / "DAVIS"
    if davis_dir.exists():
        return davis_dir

    with exclusive_cache_lock(cache_dir / ".dataset.lock"):
        if davis_dir.exists():
            return davis_dir

        zip_path = cache_dir / "DAVIS-2017-trainval-480p.zip"
        ensure_archive(
            _ARCHIVE_URL, zip_path, label="DAVIS 2017 trainval-480p", timeout=300, validate=zipfile.is_zipfile
        )
        extract_into_cache(zip_path, cache_dir, sentinel="DAVIS")

    return davis_dir


# DAVIS sequences carry no class labels, so a query for a class ("person") can
# only be answered from what the sequences visibly contain. These lists were
# checked against the actual frames -- name alone is not enough ("libby" is a
# dog, "color-run" is a blur of hands) -- and kept conservative: only sequences
# where the class is the clear subject. Names missing from the cache are
# skipped, so this stays harmless if the dataset version changes.
_PERSON_SEQUENCES = (
    "boxing-fisheye", "breakdance", "breakdance-flare", "cat-girl", "crossing", "dance-jump",
    "dance-twirl", "dancing", "disc-jockey", "hike", "india", "judo", "kid-football", "lab-coat",
    "lady-running", "lindy-hop", "loading", "lucia", "parkour", "schoolgirls", "stroller",
    "swing", "tuk-tuk", "upside-down", "walking",
)
# Sequences named after a car are found by name token ("car-roundabout",
# "classic-car"); these are the cars whose names don't say so.
_CAR_SEQUENCES = ("drift-chicane", "drift-straight", "drift-turn", "night-race", "rallye")
_OTHER_VEHICLE_SEQUENCES = ("bus", "motorbike", "tractor-sand", "train", "tuk-tuk")
_CLASS_TO_SEQUENCES: dict[str, tuple[str, ...]] = {
    "person": _PERSON_SEQUENCES,
    "man": _PERSON_SEQUENCES,
    "woman": _PERSON_SEQUENCES,
    "human": _PERSON_SEQUENCES,
    "pedestrian": _PERSON_SEQUENCES,
    "car": _CAR_SEQUENCES,
    "vehicle": _CAR_SEQUENCES + _OTHER_VEHICLE_SEQUENCES,
}


def _match_sequences(query: str, sequences: list[str]) -> set[str]:
    """Sequences relevant to any term of the query.

    The whole query is still tried as a substring in either direction (query in
    name, or name in query) -- that is what "a dog running" -> "dog" relies on.
    On top of that each subject term is matched separately ("car and person"
    -> "car", "person") against the hyphen-separated tokens of every sequence
    name, and against _CLASS_TO_SEQUENCES for classes DAVIS never names.

    Activity words ("running", "dancing") are left to the whole-query test:
    as separate terms they would match unrelated sequences, so "a dog running"
    would also return "lady-running" and dilute the dog frames.
    """
    keyword_lower = query.strip().lower()
    # A sequence name inside the query counts only as a whole word: "bus" is not in "business".
    matched = {
        s for s in sequences
        if keyword_lower in s.lower() or re.search(rf"\b{re.escape(s.lower())}\b", keyword_lower)
    }

    available = set(sequences)
    for term in expand_terms(query):
        if term.endswith("ing") and len(term) > 5:
            continue
        matched |= {s for s in sequences if term in {singularize(t) for t in s.lower().split("-")}}
        matched |= {s for s in _CLASS_TO_SEQUENCES.get(term, ()) if s in available}
    return matched


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


def _evenly_sample(paths: list[Path], k: int) -> list[Path]:
    """Up to k items spread evenly across the (already-sorted) list, rather
    than the first k — see discover()'s frame-sampling comment for why."""
    n = len(paths)
    if n <= k:
        return paths
    stride = n / k
    return [paths[int(i * stride)] for i in range(k)]


def _sample_frames(davis_dir: Path, sequences, limit: int) -> list[dict]:
    """Up to `limit` frames, spread evenly over `sequences` and across each one.

    Evenly-spaced sampling per sequence, not the first N consecutive frames:
    consecutive video frames are near-identical (same scene, a few pixels of motion
    apart), so grabbing them in a single burst floods the job with near-duplicates
    of one source before dedup ever runs (dedup only runs once every source in the
    job is terminal -- see app/tasks/orchestration_tasks.py). Spreading the same
    budget across the whole sequence gives real visual variety instead.
    """
    sequences = sorted(sequences)
    if not sequences or limit <= 0:
        return []
    # limit // n each, with the remainder handed to the first few sequences: 3 frames over
    # 2 sequences used to be 1 + 1, which under-filled the limit by the remainder.
    base, extra = divmod(limit, len(sequences))
    frames: list[dict] = []
    for position, seq in enumerate(sequences):
        per_sequence_limit = max(1, base + (1 if position < extra else 0))
        frame_dir = davis_dir / "JPEGImages" / "480p" / seq
        for frame_path in _evenly_sample(sorted(frame_dir.glob("*.jpg")), per_sequence_limit):
            frames.append({"sequence": seq, "frame": frame_path.stem})
        if len(frames) >= limit:
            break
    return frames[:limit]


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
            if not matched_sequences:
                return DiscoveryPage(
                    items=[], next_cursor=None, exhausted=True,
                    error_reason=f"No DAVIS video sequences named {explicit_categories!r}",
                )
        else:
            matched_sequences = _match_sequences(query, sequences)

        labelled = _sample_frames(davis_dir, matched_sequences, limit)

        # DAVIS has no class vocabulary, only ~90 sequence names, so most queries name
        # none of them. Fill whatever the matched sequences cannot supply from every other
        # sequence, and let the relevance filter decide (open_vocab.py). Not for explicit
        # sequence overrides, which are an exact request.
        scanned: list[dict] = []
        if not explicit_categories and len(labelled) < limit:
            others = [s for s in sequences if s not in matched_sequences]
            scanned = _sample_frames(davis_dir, others, limit - len(labelled))

        if not labelled and not scanned:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        items = [
            SourceItem(
                source_type=SourceType.DAVIS,
                external_ref={"frames": batch[i : i + _SHARD_SIZE], "unlabelled": unlabelled},
                license_hint=_LICENSE_NOTE,
                dataset_identifier="davis-2017-trainval-480p",
            )
            for batch, unlabelled in ((labelled, False), (scanned, True))
            for i in range(0, len(batch), _SHARD_SIZE)
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
        admit_candidate=None,
    ) -> Iterator[RawAssetDraft]:
        from PIL import Image

        if should_stop():
            raise JobStopped("should_stop() signalled before DAVIS shard fetch began")

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
            source_item_id = f"davis_{seq}_{frame}"
            admission = None
            if admit_candidate is not None:
                admission = admit_candidate(source_item_id)
                raw_status = getattr(admission, "status", "")
                status = str(getattr(raw_status, "value", raw_status)).upper()
                if status == "DUPLICATE":
                    on_progress({"checkpoint": {"index": i + 1}})
                    continue
                if status == "WINDOW_FULL":
                    on_progress({"checkpoint": {"index": i}})
                    raise CandidateWindowFull(
                        getattr(admission, "window_id", None),
                        getattr(admission, "window_index", None),
                        i,
                    )
                if status == "INACTIVE":
                    raise JobStopped(f"Candidate admission closed for {source_item_id}")
                if status != "ADMITTED":
                    continue

            src_img = davis_dir / "JPEGImages" / "480p" / seq / f"{frame}.jpg"
            src_mask = davis_dir / "Annotations" / "480p" / seq / f"{frame}.png"
            if not src_img.exists():
                if admission is not None and getattr(admission, "reservation_id", None):
                    from app.services.candidate_reservation_service import mark_candidate_failed
                    mark_candidate_failed(admission.reservation_id)
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
                    if admission is not None and getattr(admission, "reservation_id", None):
                        from app.services.candidate_reservation_service import mark_candidate_failed
                        mark_candidate_failed(admission.reservation_id)
                    on_progress({"rejected": 1})
                    continue
            except Exception as e:
                if admission is not None and getattr(admission, "reservation_id", None):
                    from app.services.candidate_reservation_service import mark_candidate_failed
                    mark_candidate_failed(admission.reservation_id)
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

            metadata = {
                "davis_sequence": seq,
                "davis_frame": frame,
                # "vlm": picked by the relevance model among images not labelled for this query.
                "matched_by": "vlm" if item.external_ref.get("unlabelled") else "label",
            }
            if admission is not None:
                metadata["candidate_reservation_id"] = str(admission.reservation_id)
                metadata["candidate_window_id"] = str(admission.window_id)
                metadata["candidate_window_index"] = int(admission.window_index)

            draft = RawAssetDraft(
                source_item_id=source_item_id,
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
                metadata=metadata,
            )
            on_progress({"checkpoint": {"index": i + 1}, "downloaded": 1})
            yield draft
