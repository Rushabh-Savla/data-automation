"""RoboflowConnector — ports roboflow_downloader.py's RoboflowDownloader into
the platform's DownloadConnector protocol.

Reused near-verbatim from the legacy script:
- Dual-strategy download (_api_download primary, _sdk_download fallback) and
  its exception-driven fallback ordering.
- _walk_export_dir/_walk_split split-detection (train/valid/test, falling
  back to a flat images/ dir) and the deterministic image_id naming scheme
  (rf_{ws}__{proj}__v{ver}__{split}__{stem}), reused directly as
  Asset.source_item_id.
- _parse_yolo_classes() and the _FORMAT_FALLBACK format-negotiation list.

Refactored: no argparse/input()/sys.exit; the API key comes from
CredentialService (injected via the connector's constructor), not
os.getenv() at import time; exceptions are classified into
TransientConnectorError/FatalConnectorError; the SDK's version.download()
call (which has no timeout parameter) is wrapped in a thread with an
explicit timeout to satisfy "detect stalled exports."
"""

import hashlib
import itertools
import logging
import shutil
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
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
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

RF_UNIVERSE_BASE = "https://universe.roboflow.com"
RF_API_BASE = "https://api.roboflow.com"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff"}
_FORMAT_FALLBACK = ["yolov8", "yolov5pytorch", "coco", "voc"]
_SDK_DOWNLOAD_TIMEOUT_S = 600
# Matches Asset.source_item_id's column width (see the migration widening it
# from 128 -> 512, prompted by real Roboflow exports overflowing 128 once
# search actually started returning real projects/filenames). Kept below the
# column width, not equal to it, so this bound is the thing that trips
# first and predictably, rather than depending on the DB to reject it.
_MAX_SOURCE_ITEM_ID_LEN = 500


@register_connector(SourceType.ROBOFLOW)
class RoboflowConnector:
    def __init__(
        self,
        api_keys: list[str] | None = None,
        export_format: str = "yolov8",
        min_resolution: tuple[int, int] = (64, 64),
    ):
        self.api_keys = api_keys or []
        self._key_cycle = itertools.cycle(self.api_keys) if self.api_keys else None
        self.export_format = export_format
        self.min_w, self.min_h = min_resolution

    def _next_key(self) -> str:
        # Round-robins across every active credential the user has added for
        # Roboflow, so concurrent/sequential requests spread across keys
        # instead of hammering a single one into its rate limit.
        if self._key_cycle is None:
            return ""
        return next(self._key_cycle)

    # -- Discovery -----------------------------------------------------------

    def discover(self, query: str, cursor: str | None, limit: int = 20) -> DiscoveryPage:
        # universe.roboflow.com/search (the old endpoint here) is NOT a real
        # search API — it's the website's own internal Next.js route
        # (Cloudflare-protected against automated traffic, and even when
        # reachable it silently returned one fixed workspace's projects
        # regardless of `q`). The real, documented, authenticated search API
        # is GET /universe/search on api.roboflow.com — confirmed straight
        # from the `roboflow` PyPI SDK's own source
        # (roboflow.adapters.rfapi.search_universe, the function backing
        # `roboflow universe search` / rfapi.search_universe() in Roboflow's
        # own docs), which hits exactly this endpoint with an api_key. It
        # requires an API key (unlike the old endpoint), so this only runs
        # once api_keys is non-empty.
        if not self.api_keys:
            return DiscoveryPage(
                items=[],
                next_cursor=None,
                exhausted=True,
                error_reason="ROBOFLOW_API_KEY not configured — Universe search requires an API key",
            )

        try:
            page_number = int(cursor) if cursor else 1
        except ValueError:
            page_number = 1

        for _ in range(max(1, len(self.api_keys))):
            key = self._next_key()
            params = {"q": query, "limit": limit, "page": page_number, "api_key": key}
            try:
                resp = requests.get(f"{RF_API_BASE}/universe/search", params=params, timeout=15)
            except Exception as e:
                logger.warning(f"Roboflow Universe search request failed for {query!r}: {e}")
                return DiscoveryPage(
                    items=[], next_cursor=None, exhausted=True,
                    error_reason=f"Roboflow Universe search request failed ({e})",
                )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                logger.warning(
                    f"Roboflow Universe search rate-limited (429) for {query!r}"
                    + (f" (Retry-After={retry_after}s)" if retry_after else "")
                    + " — rotating to next key"
                )
                continue

            try:
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(f"Roboflow Universe search failed for {query!r}: {e}")
                return DiscoveryPage(
                    items=[], next_cursor=None, exhausted=True,
                    error_reason=f"Roboflow Universe search failed ({e})",
                )

            results = data.get("results", [])
            items = [parsed for it in results[:limit] if (parsed := self._parse_universe_item(it)) is not None]
            next_cursor = str(page_number + 1) if len(results) >= limit else None
            return DiscoveryPage(items=items, next_cursor=next_cursor, exhausted=next_cursor is None)

        return DiscoveryPage(
            items=[], next_cursor=None, exhausted=True,
            error_reason="Roboflow Universe search rate-limited (429) on every configured API key",
        )

    @staticmethod
    def _parse_universe_item(item: dict) -> SourceItem | None:
        # The response doesn't document a stable "workspace"/"project" field
        # pair, but every result's canonical page is
        # {RF_UNIVERSE_BASE}/{workspace}/{project} (the same shape this
        # connector already builds dataset_url from elsewhere) — parsed from
        # `url`, with explicit fields preferred if the API does provide them.
        owner = item.get("owner") or item.get("workspace") or ""
        slug = item.get("slug") or item.get("project") or ""
        if not owner or not slug:
            url = item.get("url", "")
            path = url.removeprefix(RF_UNIVERSE_BASE).strip("/")
            parts = path.split("/")
            if len(parts) >= 2 and parts[0] and parts[1]:
                owner, slug = parts[0], parts[1]
        if not owner or not slug:
            logger.debug(f"Roboflow Universe search result missing workspace/project, skipping: {item}")
            return None
        return SourceItem(
            source_type=SourceType.ROBOFLOW,
            external_ref={"workspace": owner, "project": slug, "version": 1},
            license_hint=item.get("license", "Unknown"),
            dataset_identifier=f"{owner}/{slug}",
        )

    # -- Download --------------------------------------------------------------

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        if not self.api_keys:
            raise FatalConnectorError("ROBOFLOW_API_KEY not configured")

        # A dispatched-but-not-yet-started task (e.g. the job's target was
        # reached by another project in the same chord while this one sat
        # queued) shouldn't kick off a large export download just to walk
        # through it and skip every image — see the chunked should_stop()
        # check in _api_download below for the same reasoning applied
        # mid-download, not just here at the start.
        if should_stop():
            raise JobStopped("should_stop() signalled before Roboflow download started")

        workspace = item.external_ref["workspace"]
        project = item.external_ref["project"]
        version = item.external_ref.get("version", 1)
        license_name = item.license_hint or "Unknown"
        dataset_url = f"{RF_UNIVERSE_BASE}/{workspace}/{project}"

        try:
            yield from self._api_download(
                workspace, project, version, license_name, dataset_url, workdir, checkpoint, should_stop, on_progress
            )
            return
        except JobStopped:
            raise
        except Exception as e:
            logger.warning(f"Direct API download failed ({e}) — trying SDK")

        yield from self._sdk_download(
            workspace, project, version, license_name, dataset_url, workdir, checkpoint, should_stop, on_progress
        )

    def _api_download(
        self, workspace, project, version, license_name, dataset_url, workdir, checkpoint, should_stop, on_progress
    ) -> Iterator[RawAssetDraft]:
        formats_to_try = [self.export_format] + [f for f in _FORMAT_FALLBACK if f != self.export_format]
        link = None
        for fmt in formats_to_try:
            link = self._try_format(workspace, project, version, fmt)
            if link:
                break

        if not link:
            raise TransientConnectorError(f"No download link for {workspace}/{project} (tried {formats_to_try})")

        extract_dir = workdir / "roboflow_cache" / f"{workspace}__{project}__v{version}"
        extract_dir.mkdir(parents=True, exist_ok=True)
        zip_path = workdir / f"{workspace}__{project}__v{version}.zip"

        # Unlike kagglehub's single opaque blocking call, this download is
        # already a chunked loop we control — checking should_stop() here
        # means a large export in progress actually stops mid-download once
        # the job's target is reached, instead of only reacting to a pause/
        # target-reached signal once the whole zip has already finished.
        with requests.get(link, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if should_stop():
                        zip_path.unlink(missing_ok=True)
                        raise JobStopped(f"should_stop() signalled mid-download for {workspace}/{project}")
                    f.write(chunk)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        zip_path.unlink(missing_ok=True)

        yield from self._walk_export_dir(
            extract_dir, workspace, project, version, license_name, dataset_url, checkpoint, should_stop, on_progress
        )

    def _try_format(self, workspace: str, project: str, version, fmt: str) -> str | None:
        """Requests one export format, rotating to the next API key on a 429
        (rate limited) response instead of giving up — a single key hitting
        its per-account limit shouldn't fail the whole download when other
        active keys are available. Any other failure (bad format, network
        error, no link in the response) is not retried across keys, since
        rotating keys won't fix those."""
        for _ in range(max(1, len(self.api_keys))):
            key = self._next_key()
            url = f"{RF_API_BASE}/{workspace}/{project}/{version}/{fmt}?api_key={key}"
            try:
                resp = requests.get(url, timeout=30)
            except Exception as e:
                logger.debug(f"  Format '{fmt}' failed: {e}")
                return None
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                logger.warning(
                    f"Roboflow API key rate-limited (429) for {workspace}/{project}/{fmt}"
                    + (f" (Retry-After={retry_after}s)" if retry_after else "")
                    + " — rotating to next key"
                )
                continue
            try:
                resp.raise_for_status()
                link = resp.json().get("export", {}).get("link")
            except Exception as e:
                logger.debug(f"  Format '{fmt}' failed: {e}")
                return None
            return link or None
        return None

    def _sdk_download(
        self, workspace, project, version, license_name, dataset_url, workdir, checkpoint, should_stop, on_progress
    ) -> Iterator[RawAssetDraft]:
        # version_obj.download() below is one uninterruptible blocking SDK
        # call with no should_stop() hook of its own (same limitation as
        # kagglehub.dataset_download() — see KaggleConnector.fetch()), so
        # this can only make Pause/target-reached instant when it lands
        # before the download starts, not interrupt one already running.
        if should_stop():
            raise JobStopped(f"should_stop() signalled before Roboflow SDK download started for {workspace}/{project}")

        from roboflow import Roboflow

        rf = Roboflow(api_key=self._next_key())
        try:
            version_obj = rf.workspace(workspace).project(project).version(version)
        except Exception as e:
            if _looks_inaccessible(e):
                # Universe search can surface a project the API itself won't
                # serve (private, deleted, or export-restricted) — that's a
                # quirk of Roboflow's own search index being broader than
                # what it actually lets you fetch, not a problem with this
                # job or anything the user can act on. Treated the same as a
                # candidate that legitimately has zero matching images:
                # yield nothing and don't record it as an error, rather than
                # surfacing raw provider JSON and marking the source
                # "completed (partial)" over something nobody can fix.
                logger.warning(
                    f"Roboflow project '{workspace}/{project}' appeared in search but isn't accessible "
                    f"via the API — skipping it: {e}"
                )
                return
            raise TransientConnectorError(f"Roboflow SDK failed to load {workspace}/{project}: {e}") from e

        export_dir = workdir / "roboflow_cache" / f"{workspace}__{project}__v{version}"
        if export_dir.exists() and not any(export_dir.rglob("*")):
            shutil.rmtree(export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)

        def _do_download():
            try:
                version_obj.download(model_format=self.export_format, location=str(export_dir), overwrite=True)
            except TypeError:
                version_obj.download(model_format=self.export_format, location=str(export_dir))

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_do_download)
            try:
                future.result(timeout=_SDK_DOWNLOAD_TIMEOUT_S)
            except FutureTimeoutError as e:
                raise TransientConnectorError(
                    f"Roboflow SDK export stalled past {_SDK_DOWNLOAD_TIMEOUT_S}s for {workspace}/{project}"
                ) from e

        yield from self._walk_export_dir(
            export_dir, workspace, project, version, license_name, dataset_url, checkpoint, should_stop, on_progress
        )

    def _walk_export_dir(
        self, export_dir, workspace, project, version, license_name, dataset_url, checkpoint, should_stop, on_progress
    ) -> Iterator[RawAssetDraft]:
        splits = ["train", "valid", "test"]
        for split in splits:
            img_dir = export_dir / split / "images"
            lbl_dir = export_dir / split / "labels"
            if not img_dir.exists():
                img_dir = export_dir / "images"
                lbl_dir = export_dir / "labels"
            if not img_dir.exists():
                continue
            start_index = checkpoint.get(split, {}).get("split_index", 0) if checkpoint else 0
            yield from self._walk_split(
                img_dir, lbl_dir, split, workspace, project, version, license_name, dataset_url,
                start_index, should_stop, on_progress,
            )

    def _walk_split(
        self, image_dir, label_dir, split, workspace, project, version, license_name, dataset_url,
        start_index, should_stop, on_progress,
    ) -> Iterator[RawAssetDraft]:
        from PIL import Image

        image_files = sorted(
            f for f in image_dir.rglob("*") if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
        )
        safe_ws = workspace.replace("/", "_")
        safe_proj = project.replace("/", "_")

        for i, src_img in enumerate(image_files):
            if i < start_index:
                continue
            if should_stop():
                on_progress({"checkpoint": {split: {"split_index": i}}})
                raise JobStopped("should_stop() signalled during Roboflow split walk")

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

            ann_path = None
            annotation_format = None
            classes: list = []
            if label_dir.exists():
                lbl_file = label_dir / f"{src_img.stem}.txt"
                if lbl_file.exists():
                    ann_path = lbl_file
                    annotation_format = "yolo_txt"
                    classes = _parse_yolo_classes(lbl_file)
            if ann_path is None:
                for coco_name in ("_annotations.coco.json", "annotations.json"):
                    coco_file = image_dir.parent / coco_name
                    if coco_file.exists():
                        ann_path = coco_file
                        annotation_format = "coco_json"
                        break

            source_item_id = _bounded_source_item_id(
                f"rf_{safe_ws}__{safe_proj}__v{version}__{split}__{src_img.stem}"
            )
            draft = RawAssetDraft(
                source_item_id=source_item_id,
                source_url=dataset_url,
                dataset_identifier=f"{workspace}/{project}",
                license=license_name,
                local_tmp_path=src_img,
                original_name=src_img.name,
                annotation_tmp_path=ann_path,
                annotation_format=annotation_format,
                width=w,
                height=h,
                classes=classes,
                metadata={"split": split},
            )
            on_progress({"checkpoint": {split: {"split_index": i + 1}}, "downloaded": 1})
            yield draft


def _looks_inaccessible(e: Exception) -> bool:
    """True for the SDK's raw RoboflowError({"error": {"message": "...does
    not exist or cannot be loaded due to missing permissions", "type":
    "GraphMethodException", ...}}) — a project Universe search returned that
    the API itself refuses to serve."""
    msg = str(e).lower()
    return "does not exist" in msg or "missing permission" in msg or "graphmethodexception" in msg


def _bounded_source_item_id(source_item_id: str) -> str:
    """Caps the id at _MAX_SOURCE_ITEM_ID_LEN, deterministically — Roboflow
    exports append a `.rf.<32-hex-hash>` suffix on top of already-
    descriptive original filenames, and stock-photo-site names in
    particular can run long enough to overflow even a generously widened
    column. Replacing the tail with a hash keeps the id stable across runs
    (so the (job_id, source_type, source_item_id) uniqueness/dedup logic in
    AssetRepository.upsert still works) while guaranteeing a hard ceiling
    regardless of how long a future filename turns out to be."""
    if len(source_item_id) <= _MAX_SOURCE_ITEM_ID_LEN:
        return source_item_id
    digest = hashlib.md5(source_item_id.encode()).hexdigest()[:16]
    return f"{source_item_id[: _MAX_SOURCE_ITEM_ID_LEN - 17]}_{digest}"


def _parse_yolo_classes(label_file: Path) -> list:
    classes = set()
    try:
        for line in label_file.read_text().strip().splitlines():
            parts = line.strip().split()
            if parts:
                classes.add(parts[0])
    except Exception:
        pass
    return sorted(classes)
