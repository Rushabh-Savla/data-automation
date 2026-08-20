"""Tests for VisualGenomeConnector's objects.json-based lexical matching
(names + synsets) and per-object bbox -> YOLO conversion."""

import json
from io import BytesIO
from pathlib import Path

from PIL import Image

from app.connectors import visual_genome_connector as vg_mod
from app.connectors.base import SourceItem
from app.connectors.visual_genome_connector import VisualGenomeConnector
from app.domain.enums import SourceType

_OBJECTS = [
    {
        "image_id": 1,
        "objects": [{"names": ["dog"], "synsets": ["dog.n.01"], "x": 10, "y": 20, "w": 30, "h": 40, "object_id": 1}],
    },
    {
        "image_id": 2,
        "objects": [{"names": ["sky"], "synsets": ["sky.n.01"], "x": 0, "y": 0, "w": 5, "h": 5, "object_id": 2}],
    },
]
_IMAGE_DATA = [
    {"image_id": 1, "url": "http://x/1.jpg", "width": 800, "height": 600},
    {"image_id": 2, "url": "http://x/2.jpg", "width": 800, "height": 600},
]


def _patch_cache(monkeypatch, tmp_path: Path):
    objects_path = tmp_path / "objects.json"
    image_data_path = tmp_path / "image_data.json"
    objects_path.write_text(json.dumps(_OBJECTS))
    image_data_path.write_text(json.dumps(_IMAGE_DATA))
    monkeypatch.setattr(vg_mod, "_ensure_metadata_cached", lambda: (objects_path, image_data_path))
    return objects_path, image_data_path


class TestDiscover:
    def test_matches_object_name_and_shards(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = VisualGenomeConnector()
        page = connector.discover("dog", cursor=None, limit=200)

        assert page.error_reason is None
        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {1}

    def test_matches_via_synset(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = VisualGenomeConnector()
        page = connector.discover("sky.n.01", cursor=None, limit=200)

        assert page.error_reason is None
        image_ids = {iid for item in page.items for iid in item.external_ref["image_ids"]}
        assert image_ids == {2}

    def test_no_match_returns_error_reason(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = VisualGenomeConnector()
        page = connector.discover("xylophone repair diagram", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class _FakeResponse:
    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        buf = BytesIO()
        Image.new("RGB", (800, 600)).save(buf, format="JPEG")
        yield buf.getvalue()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class TestFetch:
    def test_converts_object_boxes_to_yolo(self, monkeypatch, tmp_path):
        objects_path, image_data_path = _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(vg_mod.requests, "get", lambda *a, **k: _FakeResponse())

        item = SourceItem(
            source_type=SourceType.VISUAL_GENOME,
            external_ref={
                "image_ids": [1],
                "metadata_path": str(image_data_path),
                "objects_path": str(objects_path),
                "keywords": ["dog"],
            },
            license_hint="test license",
            dataset_identifier="visual-genome",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = VisualGenomeConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["dog"]
        assert draft.source_item_id == "vg_1"
