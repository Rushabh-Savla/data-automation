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

Two datasets, chosen per job with job.config["nuscenes"]["dataset"]:

  * "full" (default): the annotated train+val key frames (~90k images). Two
    public S3 objects, 0.6GB of metadata and 16.4GB of images, needing roughly
    34GB free while it is fetched (the image archive is deleted once extracted,
    so ~17GB stays). Large enough for a top-up loop to keep finding new
    matches round after round.
  * "mini": the v1.0-mini split (~118MB, 50 images). Enough to try the
    connector, far too small to reach any real target -- a job for 200 images
    can only ever get ~45 of them from it.

The metadata JSON is too big to re-parse per download shard (object_ann.json
alone is 355MB for train), so it is loaded once into a SQLite index and every
discovery/shard query reads from that.
"""

import json
import logging
import re
import sqlite3
from collections import defaultdict
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
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

_BASE_URL = "https://motional-nuscenes.s3.amazonaws.com/public/nuimages-v1.0"
_SHARD_SIZE = 20
_INDEX_FILENAME = "index.sqlite3"
_LICENSE_NOTE = (
    "nuImages (non-commercial research use — Motional terms, "
    "see https://www.nuscenes.org/terms-of-use)"
)


@dataclass(frozen=True)
class _Archive:
    filename: str
    # Top-level entry that only appears once the archive is fully extracted.
    sentinel: str
    size_bytes: int
    # Rough size once unpacked; with size_bytes it is the free space needed.
    extracted_bytes: int
    # Too big to keep both the archive and its contents.
    remove_after_extract: bool = False


@dataclass(frozen=True)
class _Dataset:
    identifier: str
    cache_subdir: str
    archives: tuple[_Archive, ...]


_DATASETS = {
    "mini": _Dataset(
        identifier="nuimages-v1.0-mini",
        cache_subdir="nuimages",
        archives=(_Archive("nuimages-v1.0-mini.tgz", "v1.0-mini", 117_873_411, 150_000_000),),
    ),
    "full": _Dataset(
        identifier="nuimages-v1.0-all",
        cache_subdir="nuimages_full",
        archives=(
            _Archive("nuimages-v1.0-all-metadata.tgz", "v1.0-train", 639_386_088, 3_500_000_000, True),
            _Archive("nuimages-v1.0-all-samples.tgz", "samples", 16_381_401_772, 16_500_000_000, True),
        ),
    ),
}
_DEFAULT_DATASET = "full"


def _dataset(name: str) -> _Dataset:
    try:
        return _DATASETS[name]
    except KeyError:
        raise FatalConnectorError(f"Unknown nuImages dataset {name!r}; use one of {sorted(_DATASETS)}") from None


def _dataset_for_identifier(identifier: str | None) -> _Dataset:
    """The dataset a discovered item came from. Items created before the
    dataset choice existed carry the mini identifier."""
    for dataset in _DATASETS.values():
        if dataset.identifier == identifier:
            return dataset
    return _DATASETS["mini"]


def _cache_dir(dataset: _Dataset) -> Path:
    return get_settings().work_root / "_shared_cache" / dataset.cache_subdir


def _ensure_dataset_cached(dataset: _Dataset) -> Path:
    """Downloads+extracts each of the dataset's archives once and returns the
    cache root (containing the v1.0-* metadata dirs + samples/).

    discover() and every download shard call this concurrently, so the
    download/extract runs under an exclusive lock (see exclusive_cache_lock)."""
    cache_dir = _cache_dir(dataset)
    cache_dir.mkdir(parents=True, exist_ok=True)

    for archive in dataset.archives:
        if (cache_dir / archive.sentinel).exists():
            continue
        with exclusive_cache_lock(cache_dir / ".dataset.lock"):
            if (cache_dir / archive.sentinel).exists():
                continue
            archive_path = cache_dir / archive.filename
            ensure_archive(
                f"{_BASE_URL}/{archive.filename}",
                archive_path,
                label=f"nuImages {archive.filename}",
                timeout=120,
                required_free_bytes=archive.size_bytes + archive.extracted_bytes,
            )
            extract_into_cache(
                archive_path, cache_dir, sentinel=archive.sentinel, remove_archive=archive.remove_after_extract
            )

    return cache_dir


def _build_index(cache_dir: Path, index_path: Path) -> None:
    """Loads every annotated split's JSON tables into a SQLite file.

    images(token, filename, width, height) -- key frames that have at least one box
    anns(token, category, xmin, ymin, xmax, ymax) -- one row per 2D box

    Written to a temp file and renamed, so a reader never sees a half-built index.
    """
    tmp_path = index_path.with_name(index_path.name + ".tmp")
    tmp_path.unlink(missing_ok=True)
    conn = sqlite3.connect(tmp_path)
    try:
        # Renamed into place only when complete, so a crash just discards it: no need to sync.
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("CREATE TABLE images (token TEXT PRIMARY KEY, filename TEXT NOT NULL, width INTEGER, height INTEGER)")
        conn.execute(
            "CREATE TABLE anns (token TEXT NOT NULL, category TEXT NOT NULL, "
            "xmin REAL, ymin REAL, xmax REAL, ymax REAL)"
        )

        for split_dir in sorted(cache_dir.glob("v1.0-*")):
            if not (split_dir / "object_ann.json").exists():
                continue  # the test split ships no annotations

            categories = {c["token"]: c["name"] for c in json.loads((split_dir / "category.json").read_text())}
            # Loaded and dropped one table at a time: object_ann.json is 355MB for train.
            key_frames = {
                s["token"]: (s["filename"], s["width"], s["height"])
                for s in json.loads((split_dir / "sample_data.json").read_text())
                if s.get("is_key_frame", True)
            }
            annotated: set[str] = set()
            batch: list[tuple] = []
            for ann in json.loads((split_dir / "object_ann.json").read_text()):
                token = ann["sample_data_token"]
                name = categories.get(ann["category_token"])
                if name is None or token not in key_frames:
                    continue
                annotated.add(token)
                batch.append((token, name, *ann["bbox"]))
                if len(batch) >= 50_000:
                    conn.executemany("INSERT INTO anns VALUES (?, ?, ?, ?, ?, ?)", batch)
                    batch.clear()
            if batch:
                conn.executemany("INSERT INTO anns VALUES (?, ?, ?, ?, ?, ?)", batch)
            conn.executemany(
                "INSERT OR IGNORE INTO images VALUES (?, ?, ?, ?)",
                ((token, *key_frames[token]) for token in annotated),
            )

        conn.execute("CREATE INDEX idx_anns_token ON anns (token)")
        conn.execute("CREATE INDEX idx_anns_category ON anns (category)")
        conn.commit()
    except BaseException:
        conn.close()
        tmp_path.unlink(missing_ok=True)
        raise
    conn.close()
    tmp_path.replace(index_path)


def _ensure_index(cache_dir: Path) -> Path:
    """The SQLite index over the cached metadata, built once."""
    index_path = cache_dir / _INDEX_FILENAME
    if index_path.exists():
        return index_path
    with exclusive_cache_lock(cache_dir / ".index.lock"):
        if not index_path.exists():
            _build_index(cache_dir, index_path)
    return index_path


# Words users type for things nuImages names differently. Everything else is
# matched directly against the dot/underscore-separated tokens of a category
# name ("vehicle.car" -> vehicle, car; "bicycle_rack" -> bicycle, rack).
_TERM_ALIASES = {
    "person": "pedestrian",
    "man": "pedestrian",
    "woman": "pedestrian",
    "human": "pedestrian",
    "motorbike": "motorcycle",
    "bike": "bicycle",
    "cone": "trafficcone",
}


def _match_categories(query: str, vocabulary: list[str]) -> set[str]:
    """nuImages categories relevant to any term of the query.

    Each subject term is matched on its own ("car and person" -> "car",
    "person") against the tokens of every dotted category name. The previous
    whole-query substring test could never match a multi-word query against a
    taxonomy like "vehicle.car" / "human.pedestrian.adult", and turned a plain
    "person" into "human.pedestrian.personal_mobility". A category whose full
    name appears verbatim in the query still matches, and is taken out of the
    query before it is split so "vehicle.car" doesn't also pull in every other
    "vehicle.*" via the token "vehicle".
    """
    remainder = query.strip().lower()
    matched = {c for c in vocabulary if c.lower() in remainder}
    for category in matched:
        remainder = remainder.replace(category.lower(), " ")
    wanted = {_TERM_ALIASES.get(term, term) for term in expand_terms(remainder)}
    for category in vocabulary:
        tokens = {singularize(t) for t in re.split(r"[._]", category.lower())}
        if tokens & wanted:
            matched.add(category)
    return matched


@register_connector(SourceType.NUSCENES)
class NuImagesConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(
        self,
        query: str,
        cursor: str | None,
        limit: int = 200,
        explicit_categories: list[str] | None = None,
        dataset: str | None = None,
    ) -> DiscoveryPage:
        try:
            selected = _dataset(dataset or _DEFAULT_DATASET)
            index_path = _ensure_index(_ensure_dataset_cached(selected))
        except FatalConnectorError as e:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True, error_reason=str(e))
        except Exception as e:
            logger.warning(f"nuImages dataset unavailable: {e}")
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=True,
                error_reason=f"Failed to download/index nuImages: {e}",
            )

        with closing(sqlite3.connect(index_path)) as conn:
            vocabulary = [row[0] for row in conn.execute("SELECT DISTINCT category FROM anns")]
            if explicit_categories:
                wanted = {c.strip().lower() for c in explicit_categories if c.strip()}
                matched = {c for c in vocabulary if c.lower() in wanted}
                if not matched:
                    return DiscoveryPage(
                        items=[], next_cursor=None, exhausted=True,
                        error_reason=f"No nuImages categories matched {explicit_categories!r} — its taxonomy is driving-scene specific",
                    )
            else:
                matched = _match_categories(query, vocabulary)

            # Ordered by token: a stable, effectively random spread over the whole
            # dataset, so a larger `limit` in a later top-up round is a superset of
            # this one (the earlier images are then skipped as duplicates).
            tokens: list[str] = []
            if matched:
                placeholders = ",".join("?" * len(matched))
                tokens = [
                    row[0]
                    for row in conn.execute(
                        f"SELECT DISTINCT token FROM anns WHERE category IN ({placeholders}) ORDER BY token LIMIT ?",
                        (*sorted(matched), limit),
                    )
                ]

            # The categories could not fill the limit, or none names this query
            # ("kitchen", "golden retriever"): fill the rest from every other image and let
            # the relevance filter decide (open_vocab.py). Not for explicit overrides.
            scan_tokens: list[str] = []
            if len(tokens) < limit and not explicit_categories:
                exclude_sql = ""
                exclude_args: tuple = ()
                if matched:
                    placeholders = ",".join("?" * len(matched))
                    exclude_sql = f"WHERE token NOT IN (SELECT token FROM anns WHERE category IN ({placeholders})) "
                    exclude_args = tuple(sorted(matched))
                scan_tokens = [
                    row[0]
                    for row in conn.execute(
                        f"SELECT token FROM images {exclude_sql}ORDER BY token LIMIT ?",
                        (*exclude_args, limit - len(tokens)),
                    )
                ]
        if not tokens and not scan_tokens:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        items = [
            SourceItem(
                source_type=SourceType.NUSCENES,
                external_ref={"sample_data_tokens": batch[i : i + _SHARD_SIZE], "unlabelled": unlabelled},
                license_hint=_LICENSE_NOTE,
                dataset_identifier=selected.identifier,
            )
            for batch, unlabelled in ((tokens, False), (scan_tokens, True))
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
        import shutil

        from PIL import Image

        if should_stop():
            raise JobStopped("should_stop() signalled before nuImages shard fetch began")

        tokens = item.external_ref["sample_data_tokens"]
        try:
            cache_dir = _ensure_dataset_cached(_dataset_for_identifier(item.dataset_identifier))
            index_path = _ensure_index(cache_dir)
            placeholders = ",".join("?" * len(tokens))
            with closing(sqlite3.connect(index_path)) as conn:
                filenames = dict(
                    conn.execute(f"SELECT token, filename FROM images WHERE token IN ({placeholders})", tokens)
                )
                anns_by_token: dict[str, list[BBox]] = defaultdict(list)
                for token, category, xmin, ymin, xmax, ymax in conn.execute(
                    f"SELECT token, category, xmin, ymin, xmax, ymax FROM anns WHERE token IN ({placeholders})",
                    tokens,
                ):
                    anns_by_token[token].append(BBox(class_name=category, xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax))
        except Exception as e:
            raise TransientConnectorError(f"nuImages dataset unavailable: {e}") from e

        for i, token in enumerate(tokens):
            if i < checkpoint.get("index", 0):
                continue
            if should_stop():
                on_progress({"checkpoint": {"index": i}})
                raise JobStopped("should_stop() signalled during nuImages shard walk")

            filename = filenames.get(token)
            if filename is None:
                on_progress({"rejected": 1})
                continue

            src_img = cache_dir / filename
            if not src_img.exists():
                on_progress({"rejected": 1})
                continue

            source_item_id = f"nuimages_{token}"
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
                if status != "ADMITTED":
                    raise FatalConnectorError(
                        f"Candidate admission failed for {source_item_id}: {status}"
                    )

            dest = workdir / f"nuimages_{token}{src_img.suffix}"
            try:
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
                logger.warning(f"nuImages sample {token} copy/validate failed: {e}")
                on_progress({"rejected": 1})
                continue

            registry = ClassRegistry()
            boxes = anns_by_token.get(token, [])
            ann_path = None
            annotation_format = None
            if boxes:
                lines = boxes_to_yolo_lines(boxes, w, h, registry)
                ann_path = workdir / f"nuimages_{token}.txt"
                ann_path.write_text("\n".join(lines))
                annotation_format = "yolo_txt"

            metadata = {"nuimages_token": token}
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
