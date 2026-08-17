"""Annotation format conversion between YOLO txt, Pascal VOC XML, and COCO
JSON — used by the export pipeline so a job's assets (whose annotations may
have come from different sources in different formats) can be exported as a
single consistent format. None of the five legacy scripts did any
inter-format conversion — Roboflow can natively export multiple formats via
its own SDK, but nothing reconciled formats *across* sources.
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BBox:
    """Normalized-agnostic box: xmin/ymin/xmax/ymax in absolute pixel coords."""

    class_name: str
    xmin: float
    ymin: float
    xmax: float
    ymax: float


@dataclass
class ClassRegistry:
    """Accumulates a stable class_name -> class_id mapping across an export batch."""

    _names: list[str] = field(default_factory=list)

    def id_for(self, name: str) -> int:
        if name not in self._names:
            self._names.append(name)
        return self._names.index(name)

    @property
    def names(self) -> list[str]:
        return list(self._names)


def parse_voc_xml(xml_path: Path) -> tuple[int, int, list[BBox]]:
    """Returns (width, height, boxes) parsed from a Pascal VOC annotation file."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    size = root.find("size")
    width = int(size.findtext("width", "0")) if size is not None else 0
    height = int(size.findtext("height", "0")) if size is not None else 0

    boxes = []
    for obj in root.findall("object"):
        name = obj.findtext("name", "unknown")
        bnd = obj.find("bndbox")
        if bnd is None:
            continue
        boxes.append(
            BBox(
                class_name=name,
                xmin=float(bnd.findtext("xmin", "0")),
                ymin=float(bnd.findtext("ymin", "0")),
                xmax=float(bnd.findtext("xmax", "0")),
                ymax=float(bnd.findtext("ymax", "0")),
            )
        )
    return width, height, boxes


def parse_yolo_txt(txt_path: Path, width: int, height: int, class_names: list[str]) -> list[BBox]:
    """YOLO txt lines are `class_id cx cy w h`, normalized 0-1. Needs the
    image's pixel dimensions and a class_id -> name list to produce BBoxes."""
    boxes = []
    for line in txt_path.read_text().strip().splitlines():
        parts = line.strip().split()
        if len(parts) != 5:
            continue
        class_id, cx, cy, w, h = parts
        class_id = int(class_id)
        name = class_names[class_id] if 0 <= class_id < len(class_names) else str(class_id)
        cx, cy, w, h = float(cx) * width, float(cy) * height, float(w) * width, float(h) * height
        boxes.append(BBox(class_name=name, xmin=cx - w / 2, ymin=cy - h / 2, xmax=cx + w / 2, ymax=cy + h / 2))
    return boxes


def parse_coco_boxes_for_image(coco_data: dict, coco_image_id: int) -> tuple[int, int, list[BBox]]:
    """Extracts (width, height, boxes) for one image_id from an already-loaded
    COCO JSON dict (categories/images/annotations)."""
    categories = {c["id"]: c["name"] for c in coco_data.get("categories", [])}
    image_info = next((im for im in coco_data.get("images", []) if im["id"] == coco_image_id), None)
    if image_info is None:
        return 0, 0, []
    width, height = image_info.get("width", 0), image_info.get("height", 0)

    boxes = []
    for ann in coco_data.get("annotations", []):
        if ann.get("image_id") != coco_image_id:
            continue
        x, y, w, h = ann["bbox"]
        name = categories.get(ann.get("category_id"), str(ann.get("category_id")))
        boxes.append(BBox(class_name=name, xmin=x, ymin=y, xmax=x + w, ymax=y + h))
    return width, height, boxes


def boxes_to_yolo_lines(boxes: list[BBox], width: int, height: int, registry: ClassRegistry) -> list[str]:
    if not width or not height:
        return []
    lines = []
    for b in boxes:
        cx = (b.xmin + b.xmax) / 2 / width
        cy = (b.ymin + b.ymax) / 2 / height
        w = (b.xmax - b.xmin) / width
        h = (b.ymax - b.ymin) / height
        lines.append(f"{registry.id_for(b.class_name)} {cx:.6f} {cy:.6f} {w:.6f} {h:.6f}")
    return lines


def boxes_to_coco_annotations(
    boxes: list[BBox], image_id: int, next_annotation_id: int, registry: ClassRegistry
) -> tuple[list[dict], int]:
    anns = []
    ann_id = next_annotation_id
    for b in boxes:
        w, h = b.xmax - b.xmin, b.ymax - b.ymin
        anns.append(
            {
                "id": ann_id,
                "image_id": image_id,
                "category_id": registry.id_for(b.class_name),
                "bbox": [b.xmin, b.ymin, w, h],
                "area": w * h,
                "iscrowd": 0,
            }
        )
        ann_id += 1
    return anns, ann_id
