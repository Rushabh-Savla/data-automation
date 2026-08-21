"""Tests for CocoConnector's category matching and native COCO-format
annotation handling (deliberately no eager YOLO conversion at fetch time —
see the module docstring in app/connectors/coco_connector.py for why)."""

import json
from io import BytesIO
from pathlib import Path

from PIL import Image

from app.connectors import coco_connector as coco_mod
from app.connectors.base import SourceItem
from app.connectors.coco_connector import CocoConnector
from app.domain.enums import SourceType
from app.services.format_conversion import parse_coco_boxes_for_image

_FAKE_COCO_DATA = {
    "images": [
        {"id": 1, "file_name": "img1.jpg", "width": 640, "height": 480, "coco_url": "http://x/img1.jpg", "license": 1},
        {"id": 2, "file_name": "img2.jpg", "width": 320, "height": 240, "coco_url": "http://x/img2.jpg", "license": 1},
        {"id": 3, "file_name": "img3.jpg", "width": 100, "height": 100, "coco_url": "http://x/img3.jpg", "license": 2},
    ],
    "annotations": [
        {"id": 100, "image_id": 1, "category_id": 18, "bbox": [10, 20, 30, 40]},  # dog
        {"id": 101, "image_id": 2, "category_id": 18, "bbox": [5, 5, 15, 15]},  # dog
        {"id": 102, "image_id": 3, "category_id": 3, "bbox": [0, 0, 50, 50]},  # car
    ],
    "categories": [
        {"id": 18, "name": "dog"},
        {"id": 3, "name": "car"},
        {"id": 37, "name": "sports ball"},
    ],
    "licenses": [{"id": 1, "name": "CC BY 4.0"}, {"id": 2, "name": "Unknown"}],
}


class TestMatchCategories:
    def test_matches_single_word_category(self):
        matched = CocoConnector._match_categories("dog", _FAKE_COCO_DATA["categories"])
        assert matched == {18}

    def test_matches_multi_word_category_via_substring(self):
        matched = CocoConnector._match_categories("ball", _FAKE_COCO_DATA["categories"])
        assert matched == {37}

    def test_matches_plural_query_against_singular_category(self):
        # Real bug caught in production: query="cats" against COCO's "cat"
        # category failed under the old one-directional-only substring
        # check ("cats" is not a substring of "cat"). Reproduced here with
        # the fixture's "dog" category via query="dogs".
        matched = CocoConnector._match_categories("dogs", _FAKE_COCO_DATA["categories"])
        assert matched == {18}

    def test_no_match_returns_empty_set(self):
        matched = CocoConnector._match_categories("xylophone", _FAKE_COCO_DATA["categories"])
        assert matched == set()


def _patch_cache(monkeypatch, tmp_path: Path):
    json_path = tmp_path / "instances_val2017.json"
    json_path.write_text(json.dumps(_FAKE_COCO_DATA))
    monkeypatch.setattr(coco_mod, "_ensure_annotations_cached", lambda split: json_path)


class TestDiscover:
    def test_finds_matching_images_and_shards(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector()
        page = connector.discover("dog", cursor=None, limit=200)

        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {1, 2}
        assert page.error_reason is None

    def test_explicit_categories_bypass_query_matching(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector()
        page = connector.discover("xyz unrelated", cursor=None, limit=200, explicit_categories=["car"])

        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {3}
        assert page.error_reason is None

    def test_train2017_without_opt_in_returns_error_reason_not_raise(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector(split="train2017")
        page = connector.discover("dog", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None
        assert "train2017" in page.error_reason

    def test_reports_error_reason_when_nothing_matches(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class _FakeResponse:
    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        buf = BytesIO()
        Image.new("RGB", (640, 480)).save(buf, format="JPEG")
        yield buf.getvalue()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class TestFetch:
    def test_writes_native_coco_json_not_yolo(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(coco_mod.requests, "get", lambda *a, **k: _FakeResponse())

        item = SourceItem(
            source_type=SourceType.COCO,
            external_ref={"image_ids": [1], "split": "val2017"},
            license_hint=None,
            dataset_identifier="coco-val2017",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = CocoConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "coco_json"
        assert draft.metadata["coco_image_id"] == 1
        assert draft.source_item_id == "coco_val2017_1"
        assert draft.license == "CC BY 4.0"

        subset = json.loads(draft.annotation_tmp_path.read_text())
        width, height, boxes = parse_coco_boxes_for_image(subset, 1)
        assert (width, height) == (640, 480)
        assert len(boxes) == 1
        assert boxes[0].class_name == "dog"
        assert (boxes[0].xmin, boxes[0].ymin) == (10, 20)
