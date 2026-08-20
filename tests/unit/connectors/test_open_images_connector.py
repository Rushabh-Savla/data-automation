"""Tests for OpenImagesConnector's CSV-based class matching, CSV-scan image
discovery, and relative->absolute->YOLO bbox conversion (no FiftyOne, no
MongoDB — see the module docstring for why)."""

import csv
from io import BytesIO, StringIO
from pathlib import Path

from PIL import Image

from app.connectors import open_images_connector as oi_mod
from app.connectors.base import SourceItem
from app.connectors.open_images_connector import OpenImagesConnector
from app.domain.enums import SourceType

_DESCRIPTIONS = {"/m/0bt9lr": "Dog", "/m/0k4j": "Car"}
_ANNOTATION_ROWS = [
    {"ImageID": "img1", "LabelName": "/m/0bt9lr", "XMin": "0.1", "XMax": "0.5", "YMin": "0.2", "YMax": "0.6"},
    {"ImageID": "img2", "LabelName": "/m/0k4j", "XMin": "0.0", "XMax": "1.0", "YMin": "0.0", "YMax": "1.0"},
]


class TestMatchClasses:
    def test_matches_class_case_insensitively(self):
        matched = OpenImagesConnector._match_classes("dog", _DESCRIPTIONS)
        assert matched == {"/m/0bt9lr"}

    def test_no_match_returns_empty_set(self):
        matched = OpenImagesConnector._match_classes("xylophone", _DESCRIPTIONS)
        assert matched == set()


def _write_annotations_csv(path: Path):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ImageID", "LabelName", "XMin", "XMax", "YMin", "YMax"])
        writer.writeheader()
        for row in _ANNOTATION_ROWS:
            writer.writerow(row)


def _patch_cache(monkeypatch, tmp_path: Path):
    ann_csv = tmp_path / "validation-annotations-bbox.csv"
    _write_annotations_csv(ann_csv)
    monkeypatch.setattr(oi_mod, "_ensure_class_descriptions_cached", lambda: dict(_DESCRIPTIONS))
    monkeypatch.setattr(oi_mod, "_ensure_annotations_cached", lambda split: ann_csv)
    return ann_csv


class TestDiscover:
    def test_finds_matching_images_and_shards(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = OpenImagesConnector()
        page = connector.discover("dog", cursor=None, limit=200)

        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {"img1"}
        assert page.error_reason is None

    def test_no_class_match_returns_error_reason(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = OpenImagesConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class _FakeResponse:
    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        buf = BytesIO()
        Image.new("RGB", (200, 100)).save(buf, format="JPEG")
        yield buf.getvalue()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class TestFetch:
    def test_converts_relative_bbox_to_yolo(self, monkeypatch, tmp_path):
        ann_csv = _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(oi_mod.requests, "get", lambda *a, **k: _FakeResponse())

        item = SourceItem(
            source_type=SourceType.OPEN_IMAGES,
            external_ref={"image_ids": ["img1"], "split": "validation", "label_ids": ["/m/0bt9lr"]},
            license_hint="test license",
            dataset_identifier="open-images-validation",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = OpenImagesConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["Dog"]
        assert draft.source_item_id == "oi_validation_img1"

        lines = draft.annotation_tmp_path.read_text().strip().splitlines()
        assert len(lines) == 1
        class_id, cx, cy, w, h = lines[0].split()
        assert class_id == "0"
        # relative box [0.1,0.2,0.5,0.6] on 200x100 -> abs xmin=20,ymin=20,xmax=100,ymax=60
        # center=(60,40), w=80,h=40 -> normalized cx=0.3,cy=0.4,w=0.4,h=0.4
        assert abs(float(cx) - 0.3) < 1e-4
        assert abs(float(cy) - 0.4) < 1e-4
        assert abs(float(w) - 0.4) < 1e-4
        assert abs(float(h) - 0.4) < 1e-4
