"""ExportService — packages a job's accepted assets into a downloadable zip,
optionally converting annotations to a consistent YOLO or COCO format across
sources whose annotations arrived in different formats.
"""

import dataclasses
import json
import shutil
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from app.domain.enums import AnnotationFormat, ExportFormat
from app.services.format_conversion import (
    ClassRegistry,
    boxes_to_coco_annotations,
    boxes_to_yolo_lines,
    parse_coco_boxes_for_image,
    parse_voc_xml,
    parse_yolo_txt,
)
from app.storage import asset_files


@dataclass
class ExportAssetView:
    """Plain, session-independent snapshot of the Asset fields export needs.

    Built from the ORM row while its session is still open (see
    app/tasks/export_tasks.py) so the export build — which can run for a
    while over many assets — never touches a detached SQLAlchemy instance.
    """

    id: str
    source_type: str
    source_url: str | None
    dataset_identifier: str | None
    license: str | None
    nas_path: str | None
    annotation_format: str
    annotation_nas_path: str | None
    width: int | None
    height: int | None
    classes: list = field(default_factory=list)
    asset_metadata: dict = field(default_factory=dict)


def _localize(asset: ExportAssetView) -> ExportAssetView:
    """For an asset whose files live on the UI-connected NAS, fetch them into the
    local cache and return a view pointing at the local copies, so the builders
    below (which read paths from disk) work unchanged. Local assets are returned
    as-is — the same object, so nothing changes when no NAS is involved."""
    if not (asset_files.is_remote(asset.nas_path) or asset_files.is_remote(asset.annotation_nas_path)):
        return asset
    changes: dict = {}
    for attr in ("nas_path", "annotation_nas_path"):
        value = getattr(asset, attr)
        if asset_files.is_remote(value):
            try:
                changes[attr] = str(asset_files.materialize(value))
            except FileNotFoundError:
                changes[attr] = None  # gone from the NAS: skipped like a missing local file
    return dataclasses.replace(asset, **changes)


class ExportService:
    def build(
        self,
        assets: list[ExportAssetView],
        export_format: ExportFormat,
        staging_dir: Path,
        zip_path: Path,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        """Writes the export to zip_path. Returns the number of assets included."""
        staging_dir.mkdir(parents=True, exist_ok=True)
        images_dir = staging_dir / "images"
        images_dir.mkdir(exist_ok=True)

        if export_format == ExportFormat.RAW:
            count = self._build_raw(assets, staging_dir, images_dir, on_progress)
        elif export_format == ExportFormat.YOLO:
            count = self._build_yolo(assets, staging_dir, images_dir, on_progress)
        elif export_format == ExportFormat.COCO:
            count = self._build_coco(assets, staging_dir, images_dir, on_progress)
        else:
            raise ValueError(f"Unknown export format: {export_format}")

        self._zip_dir(staging_dir, zip_path)
        shutil.rmtree(staging_dir, ignore_errors=True)
        return count

    # -- raw: images + original annotations, untouched --------------------------

    def _build_raw(self, assets, staging_dir, images_dir, on_progress) -> int:
        annotations_dir = staging_dir / "annotations"
        annotations_dir.mkdir(exist_ok=True)
        manifest = []

        for i, asset in enumerate(assets):
            asset = _localize(asset)
            if not asset.nas_path or not Path(asset.nas_path).exists():
                continue
            dest_img = images_dir / Path(asset.nas_path).name
            shutil.copy2(asset.nas_path, dest_img)

            ann_dest_name = None
            if asset.annotation_nas_path and Path(asset.annotation_nas_path).exists():
                ann_src = Path(asset.annotation_nas_path)
                ann_dest_name = f"{Path(asset.nas_path).stem}{ann_src.suffix}"
                shutil.copy2(ann_src, annotations_dir / ann_dest_name)

            manifest.append(
                {
                    "asset_id": str(asset.id),
                    "source_type": asset.source_type,
                    "source_url": asset.source_url,
                    "dataset_identifier": asset.dataset_identifier,
                    "license": asset.license,
                    "image_file": dest_img.name,
                    "annotation_file": ann_dest_name,
                    "annotation_format": asset.annotation_format,
                    "width": asset.width,
                    "height": asset.height,
                }
            )
            if on_progress:
                on_progress(i + 1)

        (staging_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
        return len(manifest)

    # -- yolo: convert VOC/COCO annotations to YOLO txt, pass through existing --

    def _build_yolo(self, assets, staging_dir, images_dir, on_progress) -> int:
        labels_dir = staging_dir / "labels"
        labels_dir.mkdir(exist_ok=True)
        registry = ClassRegistry()
        coco_cache: dict[str, dict] = {}
        included = 0

        for i, asset in enumerate(assets):
            asset = _localize(asset)
            if not asset.nas_path or not Path(asset.nas_path).exists():
                continue
            dest_img = images_dir / Path(asset.nas_path).name
            shutil.copy2(asset.nas_path, dest_img)
            stem = dest_img.stem

            lines: list[str] = []
            fmt = AnnotationFormat(asset.annotation_format)
            if fmt == AnnotationFormat.YOLO_TXT and asset.annotation_nas_path:
                # Already YOLO — pass through verbatim (class IDs are file-local
                # to its original data.yaml, which we don't have here, so we
                # keep the original numeric IDs rather than remapping them).
                ann_path = Path(asset.annotation_nas_path)
                if ann_path.exists():
                    lines = ann_path.read_text().strip().splitlines()
            elif fmt == AnnotationFormat.VOC_XML and asset.annotation_nas_path:
                width, height, boxes = parse_voc_xml(Path(asset.annotation_nas_path))
                lines = boxes_to_yolo_lines(boxes, width or asset.width or 0, height or asset.height or 0, registry)
            elif fmt == AnnotationFormat.COCO_JSON and asset.annotation_nas_path:
                coco_path = str(asset.annotation_nas_path)
                if coco_path not in coco_cache:
                    coco_cache[coco_path] = json.loads(Path(coco_path).read_text())
                coco_image_id = asset.asset_metadata.get("coco_image_id")
                if coco_image_id is not None:
                    width, height, boxes = parse_coco_boxes_for_image(coco_cache[coco_path], coco_image_id)
                    lines = boxes_to_yolo_lines(boxes, width, height, registry)

            (labels_dir / f"{stem}.txt").write_text("\n".join(lines))
            included += 1
            if on_progress:
                on_progress(i + 1)

        (staging_dir / "classes.txt").write_text("\n".join(registry.names))
        return included

    # -- coco: merge every asset's annotation into one instances.json -----------

    def _build_coco(self, assets, staging_dir, images_dir, on_progress) -> int:
        registry = ClassRegistry()
        coco_cache: dict[str, dict] = {}
        images: list[dict] = []
        annotations: list[dict] = []
        next_ann_id = 1
        included = 0

        for i, asset in enumerate(assets):
            asset = _localize(asset)
            if not asset.nas_path or not Path(asset.nas_path).exists():
                continue
            dest_img = images_dir / Path(asset.nas_path).name
            shutil.copy2(asset.nas_path, dest_img)

            image_id = i + 1
            width, height = asset.width or 0, asset.height or 0
            images.append({"id": image_id, "file_name": dest_img.name, "width": width, "height": height})

            boxes = []
            fmt = AnnotationFormat(asset.annotation_format)
            if fmt == AnnotationFormat.VOC_XML and asset.annotation_nas_path:
                w, h, boxes = parse_voc_xml(Path(asset.annotation_nas_path))
                width, height = w or width, h or height
            elif fmt == AnnotationFormat.YOLO_TXT and asset.annotation_nas_path:
                class_names = asset.classes or []
                boxes = parse_yolo_txt(Path(asset.annotation_nas_path), width, height, class_names)
            elif fmt == AnnotationFormat.COCO_JSON and asset.annotation_nas_path:
                coco_path = str(asset.annotation_nas_path)
                if coco_path not in coco_cache:
                    coco_cache[coco_path] = json.loads(Path(coco_path).read_text())
                coco_image_id = asset.asset_metadata.get("coco_image_id")
                if coco_image_id is not None:
                    w, h, boxes = parse_coco_boxes_for_image(coco_cache[coco_path], coco_image_id)
                    width, height = w or width, h or height

            new_anns, next_ann_id = boxes_to_coco_annotations(boxes, image_id, next_ann_id, registry)
            annotations.extend(new_anns)
            included += 1
            if on_progress:
                on_progress(i + 1)

        coco_output = {
            "images": images,
            "annotations": annotations,
            "categories": [{"id": idx, "name": name} for idx, name in enumerate(registry.names)],
        }
        (staging_dir / "instances.json").write_text(json.dumps(coco_output, indent=2))
        return included

    @staticmethod
    def _zip_dir(source_dir: Path, zip_path: Path) -> None:
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in source_dir.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(source_dir))
