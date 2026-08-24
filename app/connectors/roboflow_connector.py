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

import logging
import shutil
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
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
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

RF_UNIVERSE_BASE = "https://universe.roboflow.com"
RF_API_BASE = "https://api.roboflow.com"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff"}
_FORMAT_FALLBACK = ["yolov8", "yolov5pytorch", "coco", "voc"]
_SDK_DOWNLOAD_TIMEOUT_S = 600


@register_connector(SourceType.ROBOFLOW)
class RoboflowConnector:
    def __init__(self, api_key: str = "", export_format: str = "yolov8", min_resolution: tuple[int, int] = (64, 64)):
        self.api_key = api_key
        self.export_format = export_format
        self.min_w, self.min_h = min_resolution

    # -- Discovery -----------------------------------------------------------

    def discover(self, query: str, cursor: str | None, limit: int = 20) -> DiscoveryPage:
        # Note: api.roboflow.com/search is NOT a real search endpoint — it is
        # Roboflow's GET /:workspace route, and "search" happens to be a real
        # workspace slug someone owns. Every request to it returns that one
        # fixed workspace's own projects regardless of `q`/`type`, completely
        # ignoring the query — verified by hitting it with several unrelated
        # queries and getting byte-identical responses. It was previously
        # kept as a "fallback" but never actually performed a search, so it
        # only risked silently attaching an unrelated workspace's data to a
        # job; removed rather than fixed forward since Roboflow does not
        # appear to expose a public, query-based Universe search API.
        url = RF_UNIVERSE_BASE + "/search"
        params = {"q": query, "t": "object-detection", "p": 1}
        try:
            resp = requests.get(
                url, params=params, headers={**HTTP_HEADERS, "Accept": "application/json"}, timeout=15
            )
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            if "json" not in ct and not resp.text.strip().startswith("{"):
                logger.warning(
                    f"Roboflow Universe search returned non-JSON (likely an anti-bot challenge page) "
                    f"for query {query!r} — this network is probably being blocked, not genuinely "
                    f"returning zero results. Use explicit project overrides instead of auto-search."
                )
                return DiscoveryPage(
                    items=[],
                    next_cursor=None,
                    exhausted=True,
                    error_reason="Roboflow Universe search is unreachable from this network (blocked/anti-bot "
                    "challenge) — use explicit project overrides instead of auto-search",
                )
            data = resp.json()
            items = data.get("results", [])
            return DiscoveryPage(
                items=[self._parse_universe_item(it) for it in items[:limit]], next_cursor=None, exhausted=True
            )
        except Exception as e:
            logger.warning(f"Roboflow Universe search failed for {query!r}: {e}")
            return DiscoveryPage(
                items=[],
                next_cursor=None,
                exhausted=True,
                error_reason=f"Roboflow Universe search request failed ({e}) — use explicit project overrides "
                "instead of auto-search",
            )

    @staticmethod
    def _parse_universe_item(item: dict) -> SourceItem:
        owner = item.get("owner", "")
        slug = item.get("slug", "")
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
        if not self.api_key:
            raise FatalConnectorError("ROBOFLOW_API_KEY not configured")

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
            url = f"{RF_API_BASE}/{workspace}/{project}/{version}/{fmt}?api_key={self.api_key}"
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                link = data.get("export", {}).get("link")
                if link:
                    break
            except Exception as e:
                logger.debug(f"  Format '{fmt}' failed: {e}")
                continue

        if not link:
            raise TransientConnectorError(f"No download link for {workspace}/{project} (tried {formats_to_try})")

        extract_dir = workdir / "roboflow_cache" / f"{workspace}__{project}__v{version}"
        extract_dir.mkdir(parents=True, exist_ok=True)
        zip_path = workdir / f"{workspace}__{project}__v{version}.zip"

        with requests.get(link, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        zip_path.unlink(missing_ok=True)

        yield from self._walk_export_dir(
            extract_dir, workspace, project, version, license_name, dataset_url, checkpoint, should_stop, on_progress
        )

    def _sdk_download(
        self, workspace, project, version, license_name, dataset_url, workdir, checkpoint, should_stop, on_progress
    ) -> Iterator[RawAssetDraft]:
        from roboflow import Roboflow

        rf = Roboflow(api_key=self.api_key)
        version_obj = rf.workspace(workspace).project(project).version(version)

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

            source_item_id = f"rf_{safe_ws}__{safe_proj}__v{version}__{split}__{src_img.stem}"
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
