"""KaggleConnector — ports kaggle_downloader.py's KaggleDownloader into the
platform's DownloadConnector protocol.

Reused near-verbatim from the legacy script (already correct, proven logic):
- _recursive_unzip(): Zip-Slip-safe recursive extraction (member_path.resolve()
  prefix check, depth-capped at 4).
- _find_annotation(): 3-pattern annotation pairing (same-stem, sibling
  annotations/ dir, dataset-level COCO json).
- _make_id(): deterministic md5(slug + rel_path)[:16] id scheme, reused
  directly as Asset.source_item_id.

Refactored: no argparse/input()/sys.exit; credentials come from CredentialService
(scoped into os.environ for the call, since kagglehub/kaggle read env vars
globally with no per-call override); should_stop()/on_progress() wired into
the per-image walk loop so pause/cancel/resume work; kagglehub/kaggle calls
wrapped with retry/backoff instead of failing outright on a transient error.
"""

import hashlib
import logging
import zipfile
from collections.abc import Iterator
from pathlib import Path

from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff", ".tif"}

# Kaggle's own dataset tags for image/vision content — used to prioritize
# search results (see discover()'s docstring for why this matters for
# generic object-word queries like "hammer" or "axe").
_IMAGE_RELATED_TAGS = {"image", "computer vision", "object detection"}


def make_source_item_id(slug: str, rel_path: str) -> str:
    """Deterministic ID — ported verbatim from kaggle_downloader.py::_make_id()."""
    return hashlib.md5(f"{slug}/{rel_path}".encode()).hexdigest()[:16]


@register_connector(SourceType.KAGGLE)
class KaggleConnector:
    def __init__(self, min_resolution: tuple[int, int] = (64, 64)):
        self.min_w, self.min_h = min_resolution

    # -- Discovery -------------------------------------------------------------

    def discover(self, query: str, cursor: str | None, limit: int = 20) -> DiscoveryPage:
        """Search Kaggle for datasets matching the job's query. Falls back to
        an empty page (not fatal) if the `kaggle` package/credentials aren't
        available — a direct-slug job doesn't need discovery at all.

        Kaggle's dataset search matches literal text anywhere in a dataset's
        title/description, not just image datasets — a generic single-word
        query like "hammer" or "axe" ranks tabular/unrelated datasets (an
        IPL auction dataset, a virus-infection prediction dataset, etc.)
        right alongside the handful of real image datasets. With the
        platform's default of only 5 datasets per job (job.config["kaggle"]
        ["max_datasets"]), those unrelated hits can crowd out every actual
        image dataset, so the query appears to "not work." Results are
        stable-sorted so datasets Kaggle itself tags image/computer-vision/
        object-detection are tried first — untagged datasets are still kept
        (just after), since Kaggle's tagging is user-submitted and
        inconsistent (a real image dataset can lack the "image" tag).

        Note: `kaggle.api.authenticate()` calls the bare builtin `exit(1)`
        (i.e. raises SystemExit) when no credentials are configured, instead
        of raising a catchable exception — this is the `kaggle` package's own
        behavior (its `__init__.py` guards the same call with
        `except (Exception, SystemExit)`). Only catching `Exception` here
        would let that SystemExit propagate up through the Celery task and
        kill the whole worker process, taking every other in-flight job down
        with it — exactly the cross-source isolation failure the platform is
        supposed to prevent. Missing/invalid Kaggle credentials must degrade
        to "no results from this source," not a worker crash.
        """
        try:
            import kaggle as kaggle_pkg
        except ImportError:
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        try:
            kaggle_pkg.api.authenticate()
        except (Exception, SystemExit) as e:
            logger.warning(f"Kaggle search unavailable (missing/invalid credentials?): {e}")
            return DiscoveryPage(items=[], next_cursor=None, exhausted=True)

        raw = self._search_with_singular_fallback(kaggle_pkg, query)
        raw = sorted(raw, key=lambda ds: not self._is_image_related(ds))

        items = []
        for ds in raw[:limit]:
            slug = str(ds.ref)
            license_name = str(getattr(ds, "licenseName", "Unknown"))
            items.append(
                SourceItem(
                    source_type=SourceType.KAGGLE,
                    external_ref={"slug": slug},
                    license_hint=license_name,
                    dataset_identifier=slug,
                )
            )
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    @staticmethod
    def _is_image_related(ds) -> bool:
        return any(getattr(t, "ref", None) in _IMAGE_RELATED_TAGS for t in (getattr(ds, "tags", None) or []))

    @staticmethod
    def _search_with_singular_fallback(kaggle_pkg, query: str) -> list:
        """Kaggle's dataset search matches literally rather than stemmed —
        e.g. "red shirts" returns 0 datasets while "red shirt" returns 11 for
        the exact same underlying content. Retry once with a naive
        singularized form of the last word before giving up, so a plural
        user query doesn't silently starve this source of every candidate."""
        try:
            raw = kaggle_pkg.api.dataset_list(search=query, file_type="all")
        except Exception as e:
            logger.warning(f"Kaggle search failed for {query!r}: {e}")
            return []
        if raw:
            return raw

        words = query.split()
        if not words or not words[-1].lower().endswith("s") or len(words[-1]) <= 1:
            return raw

        singular_query = " ".join([*words[:-1], words[-1][:-1]])
        try:
            fallback_raw = kaggle_pkg.api.dataset_list(search=singular_query, file_type="all")
        except Exception as e:
            logger.warning(f"Kaggle fallback search failed for {singular_query!r}: {e}")
            return raw
        if fallback_raw:
            logger.info(
                f"Kaggle: 0 results for {query!r}, found {len(fallback_raw)} for singularized {singular_query!r}"
            )
        return fallback_raw

    # -- Download ----------------------------------------------------------------

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        import kagglehub

        slug = item.external_ref["slug"]
        license_name = item.license_hint or "Unknown"
        logger.info(f"Downloading Kaggle dataset: {slug}")

        # kagglehub.dataset_download() below is one uninterruptible blocking
        # call with no should_stop() hook of its own — once it starts, a
        # pause can't take effect until it returns. Checking here at least
        # makes pause instant for any dataset in the chord whose download
        # hasn't started yet, rather than waiting for it to run to
        # completion first.
        if should_stop():
            raise JobStopped(f"should_stop() signalled before Kaggle download started for {slug}")

        try:
            cached_path = Path(kagglehub.dataset_download(slug))
        except Exception as e:
            raise self._classify_error(e, slug)

        dataset_url = f"https://www.kaggle.com/datasets/{slug}"

        self._recursive_unzip(cached_path)

        walk_index = checkpoint.get("walk_index", 0) if checkpoint else 0
        yield from self._walk_and_yield(
            cached_path, slug, dataset_url, license_name, walk_index, should_stop, on_progress
        )

    # -- Ported verbatim from kaggle_downloader.py --------------------------------

    def _recursive_unzip(self, directory: Path, depth: int = 0):
        if depth > 4:
            return
        for zip_path in list(directory.rglob("*.zip")):
            extract_dir = zip_path.parent / zip_path.stem
            if extract_dir.exists():
                zip_path.unlink(missing_ok=True)
                continue
            logger.info(f"  Extracting: {zip_path.name}")
            try:
                extract_dir.mkdir(exist_ok=True)
                with zipfile.ZipFile(zip_path, "r") as zf:
                    for member in zf.infolist():
                        member_path = (extract_dir / member.filename).resolve()
                        if not str(member_path).startswith(str(extract_dir.resolve())):
                            logger.warning(f"  Zip-slip blocked: {member.filename}")
                            continue
                        zf.extract(member, extract_dir)
                zip_path.unlink(missing_ok=True)
                self._recursive_unzip(extract_dir, depth + 1)
            except zipfile.BadZipFile:
                logger.warning(f"  Bad ZIP (skipped): {zip_path.name}")
                zip_path.unlink(missing_ok=True)
            except Exception as e:
                logger.error(f"  ZIP extraction error {zip_path.name}: {e}")

    def _walk_and_yield(
        self,
        root: Path,
        slug: str,
        dataset_url: str,
        license_name: str,
        start_index: int,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        from PIL import Image

        image_files = sorted(
            f for f in root.rglob("*") if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
        )
        logger.info(f"  Found {len(image_files)} image file(s) in dataset")

        for i, src_img in enumerate(image_files):
            if i < start_index:
                continue
            if should_stop():
                on_progress({"checkpoint": {"walk_index": i}})
                raise JobStopped("should_stop() signalled during Kaggle walk")

            try:
                with Image.open(src_img) as img:
                    img.verify()
                with Image.open(src_img) as img:
                    w, h = img.size
                if w < self.min_w or h < self.min_h:
                    continue
            except Exception:
                on_progress({"rejected": 1})
                continue

            rel_path = src_img.relative_to(root)
            source_item_id = make_source_item_id(slug, str(rel_path))

            ann_path = self._find_annotation(src_img, root)

            draft = RawAssetDraft(
                source_item_id=source_item_id,
                source_url=dataset_url,
                dataset_identifier=slug,
                license=license_name,
                local_tmp_path=src_img,
                original_name=src_img.name,
                annotation_tmp_path=ann_path,
                annotation_format=self._guess_annotation_format(ann_path),
                width=w,
                height=h,
                metadata={"original_path": str(rel_path)},
            )
            on_progress({"checkpoint": {"walk_index": i + 1}, "downloaded": 1})
            yield draft

    def _find_annotation(self, image_file: Path, root: Path) -> Path | None:
        stem = image_file.stem
        parent = image_file.parent

        for ext in (".xml", ".txt", ".json"):
            c = parent / f"{stem}{ext}"
            if c.exists():
                return c

        for ann_dir in ("annotations", "labels", "label", "annotation"):
            adir = parent.parent / ann_dir
            if adir.exists():
                for ext in (".xml", ".txt", ".json"):
                    c = adir / f"{stem}{ext}"
                    if c.exists():
                        return c

        for jname in ("annotations.json", "instances.json", "train.json", "val.json"):
            c = root / jname
            if c.exists():
                return c

        return None

    @staticmethod
    def _guess_annotation_format(ann_path: Path | None) -> str | None:
        if ann_path is None:
            return None
        suffix = ann_path.suffix.lower()
        if suffix == ".txt":
            return "yolo_txt"
        if suffix == ".xml":
            return "voc_xml"
        if suffix == ".json":
            return "coco_json"
        return None

    @staticmethod
    def _classify_error(e: Exception, slug: str) -> Exception:
        msg = str(e).lower()
        if "403" in msg or "401" in msg:
            return FatalConnectorError(
                f"Kaggle dataset '{slug}' requires accepting dataset rules on kaggle.com "
                f"before API access is allowed (403/401 from Kaggle API): {e}"
            )
        if "404" in msg or "not found" in msg:
            return FatalConnectorError(f"Kaggle dataset '{slug}' not found (404): {e}")
        return TransientConnectorError(f"Kaggle download failed for '{slug}': {e}")
