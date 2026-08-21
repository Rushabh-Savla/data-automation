"""Tests for NuImagesConnector's category-token index and object_ann -> YOLO
conversion. Registered as SourceType.NUSCENES (data is nuImages, not native
nuScenes 3D — see the connector's module docstring)."""

from pathlib import Path

from PIL import Image

from app.connectors import nuimages_connector as nu_mod
from app.connectors.base import SourceItem
from app.connectors.nuimages_connector import NuImagesConnector
from app.domain.enums import SourceType

_CATEGORIES = [{"token": "cat_car", "name": "vehicle.car"}, {"token": "cat_ped", "name": "human.pedestrian.adult"}]
_SAMPLE_DATA = [
    {"token": "sd1", "filename": "samples/CAM_FRONT/img1.jpg", "width": 1600, "height": 900},
]
_OBJECT_ANN = [
    {"token": "ann1", "category_token": "cat_car", "bbox": [100, 200, 300, 400], "sample_data_token": "sd1"},
]


def _build_fake_cache(tmp_path: Path) -> Path:
    cache_dir = tmp_path / "nuimages"
    img_dir = cache_dir / "samples" / "CAM_FRONT"
    img_dir.mkdir(parents=True)
    Image.new("RGB", (1600, 900)).save(img_dir / "img1.jpg")
    return cache_dir


def _patch_tables(monkeypatch):
    monkeypatch.setattr(
        nu_mod,
        "_load_tables",
        lambda cd: (
            {c["token"]: c["name"] for c in _CATEGORIES},
            {s["token"]: s for s in _SAMPLE_DATA},
            _OBJECT_ANN,
        ),
    )


class TestDiscover:
    def test_matches_category_and_shards(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_cache(tmp_path)
        monkeypatch.setattr(nu_mod, "_ensure_dataset_cached", lambda: cache_dir)
        _patch_tables(monkeypatch)

        connector = NuImagesConnector()
        page = connector.discover("car", cursor=None, limit=200)

        assert page.error_reason is None
        tokens = {t for item in page.items for t in item.external_ref["sample_data_tokens"]}
        assert tokens == {"sd1"}

    def test_no_match_returns_error_reason(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_cache(tmp_path)
        monkeypatch.setattr(nu_mod, "_ensure_dataset_cached", lambda: cache_dir)
        _patch_tables(monkeypatch)

        connector = NuImagesConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.items == []
        assert page.error_reason is not None


class TestFetch:
    def test_converts_object_ann_to_yolo(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_cache(tmp_path)
        monkeypatch.setattr(nu_mod, "_ensure_dataset_cached", lambda: cache_dir)
        _patch_tables(monkeypatch)

        item = SourceItem(
            source_type=SourceType.NUSCENES,
            external_ref={"sample_data_tokens": ["sd1"]},
            license_hint="test license",
            dataset_identifier="nuimages-v1.0-mini",
        )
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = NuImagesConnector()
        drafts = list(connector.fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["vehicle.car"]
        assert draft.source_item_id == "nuimages_sd1"
