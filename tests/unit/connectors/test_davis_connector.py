"""Tests for DavisConnector's mask->bbox reduction and sequence-name-based
matching (DAVIS has no semantic class vocabulary of its own)."""

from pathlib import Path

import numpy as np
from PIL import Image

from app.connectors import davis_connector as davis_mod
from app.connectors.base import SourceItem
from app.connectors.davis_connector import DavisConnector, _mask_to_boxes, _match_sequences
from app.domain.enums import SourceType


def _build_fake_dataset(tmp_path: Path) -> Path:
    davis_dir = tmp_path / "DAVIS"
    seq_img_dir = davis_dir / "JPEGImages" / "480p" / "dog"
    seq_mask_dir = davis_dir / "Annotations" / "480p" / "dog"
    seq_img_dir.mkdir(parents=True)
    seq_mask_dir.mkdir(parents=True)

    Image.new("RGB", (100, 80)).save(seq_img_dir / "00000.jpg")

    mask = np.zeros((80, 100), dtype=np.uint8)
    mask[10:30, 20:50] = 1  # object id 1: x in [20,49], y in [10,29]
    Image.fromarray(mask, mode="P").save(seq_mask_dir / "00000.png")

    return davis_dir


class TestMatchSequences:
    def test_matches_sequence_name(self):
        assert _match_sequences("a dog running", ["dog", "car-roundabout"]) == {"dog"}

    def test_no_match_returns_empty_set(self):
        assert _match_sequences("xylophone", ["dog", "car-roundabout"]) == set()


class TestMaskToBoxes:
    def test_bounding_rect_of_labeled_region(self, tmp_path):
        mask_path = tmp_path / "mask.png"
        mask = np.zeros((80, 100), dtype=np.uint8)
        mask[10:30, 20:50] = 1
        Image.fromarray(mask, mode="P").save(mask_path)

        boxes = _mask_to_boxes(mask_path, "dog")
        assert len(boxes) == 1
        b = boxes[0]
        assert b.class_name == "dog"
        assert (b.xmin, b.ymin, b.xmax, b.ymax) == (20.0, 10.0, 50.0, 30.0)

    def test_background_only_mask_yields_no_boxes(self, tmp_path):
        mask_path = tmp_path / "empty.png"
        Image.fromarray(np.zeros((10, 10), dtype=np.uint8), mode="P").save(mask_path)
        assert _mask_to_boxes(mask_path, "dog") == []


class TestDiscover:
    def test_matches_sequence_name_and_shards(self, monkeypatch, tmp_path):
        davis_dir = _build_fake_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        connector = DavisConnector()
        page = connector.discover("a dog running", cursor=None, limit=200)

        assert page.error_reason is None
        frames = [f for item in page.items for f in item.external_ref["frames"]]
        assert frames == [{"sequence": "dog", "frame": "00000"}]

    def test_no_match_returns_error_reason(self, monkeypatch, tmp_path):
        davis_dir = _build_fake_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        connector = DavisConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class TestFetch:
    def test_converts_mask_to_yolo_box_labeled_by_sequence(self, monkeypatch, tmp_path):
        davis_dir = _build_fake_dataset(tmp_path)
        monkeypatch.setattr(davis_mod, "_ensure_dataset_cached", lambda: davis_dir)

        item = SourceItem(
            source_type=SourceType.DAVIS,
            external_ref={"frames": [{"sequence": "dog", "frame": "00000"}]},
            license_hint="test license",
            dataset_identifier="davis-2017-trainval-480p",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = DavisConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["dog"]
        assert draft.source_item_id == "davis_dog_00000"
