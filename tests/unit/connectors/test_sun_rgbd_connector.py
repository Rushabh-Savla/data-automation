"""Tests for SunRgbdConnector's lexical class-index matching and its 2D bbox
[x,y,w,h] -> YOLO conversion."""

from pathlib import Path

from PIL import Image

from app.connectors import sun_rgbd_connector as sun_mod
from app.connectors.base import SourceItem
from app.connectors.sun_rgbd_connector import SunRgbdConnector
from app.domain.enums import SourceType

_RECORDS = [
    {"sequence": "SUNRGBD/kv2/seq1", "rgbname": "0001.jpg", "boxes": [{"class": "chair", "bbox": [10, 20, 30, 40]}]},
    {"sequence": "SUNRGBD/kv2/seq2", "rgbname": "0002.jpg", "boxes": [{"class": "table", "bbox": [0, 0, 5, 5]}]},
]


def _build_fake_images(tmp_path: Path) -> Path:
    cache_dir = tmp_path / "sun_rgbd"
    img_dir = cache_dir / "SUNRGBD" / "kv2" / "seq1" / "image"
    img_dir.mkdir(parents=True)
    Image.new("RGB", (640, 480)).save(img_dir / "0001.jpg")
    return cache_dir


class TestDiscover:
    def test_matches_class_and_shards(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)

        connector = SunRgbdConnector()
        page = connector.discover("a chair in the room", cursor=None, limit=200)

        assert page.error_reason is None
        indices = {i for item in page.items for i in item.external_ref["record_indices"]}
        assert indices == {0}

    def test_no_match_returns_error_reason(self, monkeypatch):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)

        connector = SunRgbdConnector()
        page = connector.discover("xylophone repair diagram", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class TestFetch:
    def test_converts_xywh_bbox_to_yolo(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_images(tmp_path)
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)
        monkeypatch.setattr(sun_mod, "_ensure_images_cached", lambda: cache_dir)

        item = SourceItem(
            source_type=SourceType.SUN_RGBD,
            external_ref={"record_indices": [0]},
            license_hint="test license",
            dataset_identifier="sun-rgbd",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = SunRgbdConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["chair"]
        assert draft.source_item_id == "sunrgbd_0"

        lines = draft.annotation_tmp_path.read_text().strip().splitlines()
        class_id, cx, cy, w, h = lines[0].split()
        # bbox [x=10,y=20,w=30,h=40] on 640x480 -> xmin=10,ymin=20,xmax=40,ymax=60
        # center=(25,40), w=30,h=40
        assert abs(float(cx) - 25 / 640) < 1e-4
        assert abs(float(cy) - 40 / 480) < 1e-4
        assert abs(float(w) - 30 / 640) < 1e-4
        assert abs(float(h) - 40 / 480) < 1e-4
