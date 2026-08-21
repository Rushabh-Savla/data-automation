"""Tests for KittiConnector's label-line parsing and class index."""

from pathlib import Path

from PIL import Image

from app.connectors import kitti_connector as kitti_mod
from app.connectors.base import SourceItem
from app.connectors.kitti_connector import KittiConnector, _parse_kitti_label
from app.domain.enums import SourceType

# type truncated occluded alpha left top right bottom h w l x y z ry
_LABEL_LINE = "Car 0.00 0 -1.50 100.00 200.00 300.00 400.00 1.5 1.6 3.8 1.0 2.0 10.0 -1.5\n"
_DONTCARE_LINE = "DontCare -1 -1 -10 500 300 550 350 -1 -1 -1 -1000 -1000 -1000 -10\n"


def _build_fake_cache(tmp_path: Path) -> Path:
    cache_dir = tmp_path / "kitti"
    (cache_dir / "training" / "image_2").mkdir(parents=True)
    (cache_dir / "training" / "label_2").mkdir(parents=True)
    (cache_dir / "training" / "label_2" / "000001.txt").write_text(_LABEL_LINE + _DONTCARE_LINE)
    Image.new("RGB", (1242, 375)).save(cache_dir / "training" / "image_2" / "000001.png")
    return cache_dir


class TestParseKittiLabel:
    def test_parses_2d_bbox_and_skips_dontcare(self, tmp_path):
        label_path = tmp_path / "000001.txt"
        label_path.write_text(_LABEL_LINE + _DONTCARE_LINE)

        boxes = _parse_kitti_label(label_path)
        assert len(boxes) == 1
        assert boxes[0].class_name == "Car"
        assert (boxes[0].xmin, boxes[0].ymin, boxes[0].xmax, boxes[0].ymax) == (100.0, 200.0, 300.0, 400.0)


class TestDiscover:
    def test_matches_class_and_shards(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_cache(tmp_path)
        monkeypatch.setattr(kitti_mod, "_ensure_dataset_cached", lambda: cache_dir)

        connector = KittiConnector()
        page = connector.discover("a car on the road", cursor=None, limit=200)

        assert page.error_reason is None
        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {"000001"}

    def test_no_match_returns_error_reason(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_cache(tmp_path)
        monkeypatch.setattr(kitti_mod, "_ensure_dataset_cached", lambda: cache_dir)

        connector = KittiConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class TestFetch:
    def test_converts_kitti_label_to_yolo(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_cache(tmp_path)
        monkeypatch.setattr(kitti_mod, "_ensure_dataset_cached", lambda: cache_dir)

        item = SourceItem(
            source_type=SourceType.KITTI,
            external_ref={"image_ids": ["000001"]},
            license_hint="test license",
            dataset_identifier="kitti-object-2d",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = KittiConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["Car"]

        lines = draft.annotation_tmp_path.read_text().strip().splitlines()
        assert len(lines) == 1
        class_id, cx, cy, w, h = lines[0].split()
        assert class_id == "0"
        # bbox [100,200,300,400] on 1242x375 image -> center (200,300), w=200,h=200
        assert abs(float(cx) - 200 / 1242) < 1e-4
        assert abs(float(cy) - 300 / 375) < 1e-4
