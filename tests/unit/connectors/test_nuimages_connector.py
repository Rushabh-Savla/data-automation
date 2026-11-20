"""Tests for NuImagesConnector's SQLite metadata index, category matching and
object_ann -> YOLO conversion. Registered as SourceType.NUSCENES (data is
nuImages, not native nuScenes 3D — see the connector's module docstring)."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from PIL import Image

from app.connectors import nuimages_connector as nu_mod
from app.connectors.base import SourceItem
from app.connectors.nuimages_connector import NuImagesConnector, _match_categories
from app.core.exceptions import FatalConnectorError
from app.domain.enums import SourceType

_CATEGORIES = [
    {"token": "cat_car", "name": "vehicle.car"},
    {"token": "cat_ped", "name": "human.pedestrian.adult"},
    {"token": "cat_truck", "name": "vehicle.truck"},
]


def _sample(token: str, filename: str, key_frame: bool = True) -> dict:
    return {"token": token, "filename": filename, "width": 1600, "height": 900, "is_key_frame": key_frame}


def _ann(token: str, category_token: str, bbox=(100, 200, 300, 400)) -> dict:
    return {"token": f"ann_{token}_{category_token}", "category_token": category_token, "bbox": list(bbox), "sample_data_token": token}


def _write_split(cache_dir: Path, split: str, samples: list[dict], anns: list[dict] | None) -> None:
    split_dir = cache_dir / f"v1.0-{split}"
    split_dir.mkdir(parents=True)
    (split_dir / "category.json").write_text(json.dumps(_CATEGORIES))
    (split_dir / "sample_data.json").write_text(json.dumps(samples))
    if anns is not None:  # the real test split has no object_ann.json
        (split_dir / "object_ann.json").write_text(json.dumps(anns))


def _build_fake_cache(tmp_path: Path) -> Path:
    """Two annotated splits (as in the full dataset), one un-annotated split,
    and real image files for the first sample."""
    cache_dir = tmp_path / "nuimages"
    img_dir = cache_dir / "samples" / "CAM_FRONT"
    img_dir.mkdir(parents=True)
    Image.new("RGB", (1600, 900)).save(img_dir / "img1.jpg")

    _write_split(
        cache_dir, "train",
        [_sample("sd1", "samples/CAM_FRONT/img1.jpg"), _sample("sd3", "samples/CAM_FRONT/img3.jpg"),
         _sample("sweep1", "sweeps/CAM_FRONT/sw.jpg", key_frame=False)],
        [_ann("sd1", "cat_car"), _ann("sd3", "cat_truck"), _ann("sweep1", "cat_car")],
    )
    _write_split(
        cache_dir, "val",
        [_sample("sd2", "samples/CAM_FRONT/img2.jpg")],
        [_ann("sd2", "cat_ped"), _ann("sd2", "cat_car", bbox=(1, 2, 3, 4))],
    )
    _write_split(cache_dir, "test", [_sample("sd9", "samples/CAM_FRONT/img9.jpg")], None)
    return cache_dir


@pytest.fixture
def cache_dir(tmp_path, monkeypatch) -> Path:
    cache = _build_fake_cache(tmp_path)
    monkeypatch.setattr(nu_mod, "_ensure_dataset_cached", lambda dataset, *args, **kwargs: cache)
    return cache


class TestBuildIndex:
    def test_indexes_every_annotated_split_and_skips_test_and_sweeps(self, cache_dir):
        index_path = nu_mod._ensure_index(cache_dir)

        with closing(sqlite3.connect(index_path)) as conn:
            images = {row[0] for row in conn.execute("SELECT token FROM images")}
            categories = {row[0] for row in conn.execute("SELECT DISTINCT category FROM anns")}

        assert images == {"sd1", "sd2", "sd3"}  # not "sd9" (test split, no annotations) or "sweep1" (not a key frame)
        assert categories == {"vehicle.car", "human.pedestrian.adult", "vehicle.truck"}

    def test_index_is_built_once(self, cache_dir, monkeypatch):
        nu_mod._ensure_index(cache_dir)
        monkeypatch.setattr(nu_mod, "_build_index", lambda *a: pytest.fail("index was rebuilt"))
        nu_mod._ensure_index(cache_dir)

    def test_failed_build_leaves_no_index_behind(self, cache_dir):
        (cache_dir / "v1.0-train" / "object_ann.json").write_text("{not json")

        with pytest.raises(json.JSONDecodeError):
            nu_mod._ensure_index(cache_dir)

        assert not (cache_dir / "index.sqlite3").exists()
        assert not (cache_dir / "index.sqlite3.tmp").exists()


class TestDatasets:
    def test_full_is_the_default_and_mini_is_selectable(self):
        assert nu_mod._DEFAULT_DATASET == "full"
        assert nu_mod._dataset("mini").identifier == "nuimages-v1.0-mini"
        assert nu_mod._dataset("full").identifier == "nuimages-v1.0-all"

    def test_unknown_dataset_is_a_fatal_error(self):
        with pytest.raises(FatalConnectorError):
            nu_mod._dataset("huge")

    def test_items_map_back_to_their_dataset_and_legacy_items_are_mini(self):
        assert nu_mod._dataset_for_identifier("nuimages-v1.0-all") is nu_mod._DATASETS["full"]
        assert nu_mod._dataset_for_identifier("nuimages-v1.0-mini") is nu_mod._DATASETS["mini"]
        assert nu_mod._dataset_for_identifier(None) is nu_mod._DATASETS["mini"]

    def test_full_dataset_needs_a_disk_check_covering_both_archives(self):
        # The image archive is 16GB: it must be checked before starting, and removed
        # after extraction so the cache doesn't hold two copies of it.
        metadata, samples = nu_mod._DATASETS["full"].archives
        assert samples.size_bytes + samples.extracted_bytes > 30_000_000_000
        assert samples.remove_after_extract and metadata.remove_after_extract
        assert samples.sentinel == "samples" and metadata.sentinel == "v1.0-train"


_TAXONOMY = [
    "human.pedestrian.adult",
    "human.pedestrian.construction_worker",
    "human.pedestrian.personal_mobility",
    "static_object.bicycle_rack",
    "vehicle.bicycle",
    "vehicle.bus.rigid",
    "vehicle.car",
    "vehicle.truck",
]
_PEDESTRIANS = {c for c in _TAXONOMY if c.startswith("human.pedestrian.")}


class TestMatchCategories:
    def test_multi_term_query_matches_every_term(self):
        # Regression: "car and person" is a substring of no dotted category
        # name, so this job used to end with 0 discovered.
        assert _match_categories("car and person", _TAXONOMY) == {"vehicle.car"} | _PEDESTRIANS
        assert _match_categories("car person", _TAXONOMY) == {"vehicle.car"} | _PEDESTRIANS

    def test_plurals_match(self):
        assert _match_categories("cars", _TAXONOMY) == {"vehicle.car"}
        assert _match_categories("people", _TAXONOMY) == _PEDESTRIANS
        assert _match_categories("buses", _TAXONOMY) == {"vehicle.bus.rigid"}

    def test_person_maps_to_pedestrian_not_a_substring_of_it(self):
        # "person" used to hit only ...personal_mobility, by substring accident.
        assert _match_categories("person", _TAXONOMY) == _PEDESTRIANS

    def test_matches_whole_tokens_including_underscore_parts(self):
        assert _match_categories("bicycle", _TAXONOMY) == {"vehicle.bicycle", "static_object.bicycle_rack"}
        # "vehicle" stands for car/truck/bus/motorcycle/bicycle, so it also reaches the bicycle rack.
        assert {c for c in _TAXONOMY if c.startswith("vehicle.")} <= _match_categories("vehicle", _TAXONOMY)

    def test_full_category_name_in_query_matches_only_that_category(self):
        assert _match_categories("vehicle.car", _TAXONOMY) == {"vehicle.car"}
        assert _match_categories("vehicle.car and person", _TAXONOMY) == {"vehicle.car"} | _PEDESTRIANS

    def test_no_match_returns_empty_set(self):
        assert _match_categories("xylophone", _TAXONOMY) == set()
        assert _match_categories("and the", _TAXONOMY) == set()


def _discovered_tokens(page, unlabelled=False) -> set[str]:
    """The labelled tokens by default; pass unlabelled=True for the scanned ones, None for both."""
    return {
        t
        for item in page.items
        if unlabelled in (None, item.external_ref.get("unlabelled", False))
        for t in item.external_ref["sample_data_tokens"]
    }


class TestDiscover:
    def test_multi_term_query_discovers_both_classes_across_splits(self, cache_dir):
        connector = NuImagesConnector()
        page = connector.discover("car and person", cursor=None, limit=200)

        assert page.error_reason is None
        assert _discovered_tokens(page) == {"sd1", "sd2"}  # sd1 (train car) + sd2 (val car and pedestrian)

    def test_explicit_categories_still_require_exact_names(self, cache_dir):
        connector = NuImagesConnector()
        page = connector.discover("anything", cursor=None, limit=200, explicit_categories=["vehicle.truck"])

        assert _discovered_tokens(page) == {"sd3"}

    def test_matches_category_and_shards(self, cache_dir):
        connector = NuImagesConnector()
        page = connector.discover("car", cursor=None, limit=200)

        assert page.error_reason is None
        assert _discovered_tokens(page) == {"sd1", "sd2"}

    def test_limit_caps_results_and_a_larger_limit_is_a_superset(self, cache_dir):
        # Top-up rounds re-run discovery with a bigger limit; the first round's
        # images must be inside it, or the loop would fetch unrelated images each time.
        connector = NuImagesConnector()
        small = _discovered_tokens(connector.discover("car", cursor=None, limit=1))
        large = _discovered_tokens(connector.discover("car", cursor=None, limit=200))

        assert len(small) == 1
        assert small < large

    def test_items_carry_the_dataset_identifier(self, cache_dir):
        connector = NuImagesConnector()

        assert connector.discover("car", None, 200).items[0].dataset_identifier == "nuimages-v1.0-all"
        assert connector.discover("car", None, 200, dataset="mini").items[0].dataset_identifier == "nuimages-v1.0-mini"

    def test_a_query_no_category_names_scans_instead_of_failing(self, cache_dir):
        # "kitchen" is not a driving-scene category, but that is not a reason to return
        # nothing: the relevance filter decides from the image.
        page = NuImagesConnector().discover("xylophone", cursor=None, limit=200)

        assert page.error_reason is None
        assert _discovered_tokens(page, unlabelled=False) == set()
        assert _discovered_tokens(page, unlabelled=True) == {"sd1", "sd2", "sd3"}

    def test_the_scan_follows_the_labelled_images_and_never_repeats_them(self, cache_dir):
        page = NuImagesConnector().discover("truck", cursor=None, limit=200)

        assert _discovered_tokens(page, unlabelled=False) == {"sd3"}
        assert _discovered_tokens(page, unlabelled=True) == {"sd1", "sd2"}
        assert [i.external_ref["unlabelled"] for i in page.items] == [False, True]

    def test_the_scan_is_bounded_by_the_limit(self, cache_dir):
        page = NuImagesConnector().discover("xylophone", cursor=None, limit=2)

        assert len(_discovered_tokens(page, unlabelled=True)) == 2

    def test_a_larger_limit_extends_the_same_scan_order(self, cache_dir):
        connector = NuImagesConnector()

        small = _discovered_tokens(connector.discover("xylophone", None, 1), unlabelled=True)
        large = _discovered_tokens(connector.discover("xylophone", None, 3), unlabelled=True)

        assert small < large

    def test_explicit_categories_are_exact_and_never_scanned(self, cache_dir):
        page = NuImagesConnector().discover("anything", None, 200, explicit_categories=["vehicle.truck"])

        assert _discovered_tokens(page, unlabelled=True) == set()

    def test_explicit_categories_that_match_nothing_still_report_it(self, cache_dir):
        page = NuImagesConnector().discover("anything", None, 200, explicit_categories=["vehicle.spaceship"])

        assert page.items == [] and "vehicle.spaceship" in page.error_reason

    def test_synonyms_reach_the_category(self, cache_dir):
        page = NuImagesConnector().discover("automobile", cursor=None, limit=200)

        assert _discovered_tokens(page, unlabelled=False) == {"sd1", "sd2"}  # automobile -> car

    def test_unknown_dataset_is_reported_not_raised(self, cache_dir):
        page = NuImagesConnector().discover("car", None, 200, dataset="huge")
        assert page.items == []
        assert "huge" in page.error_reason

    def test_insufficient_disk_is_reported_to_the_user(self, monkeypatch):
        def refuse(dataset, *args, **kwargs):
            raise FatalConnectorError("Not enough free disk space to fetch nuImages")

        monkeypatch.setattr(nu_mod, "_ensure_dataset_cached", refuse)
        page = NuImagesConnector().discover("car", None, 200)

        assert page.items == []
        assert "Not enough free disk space" in page.error_reason


class TestEnsureDatasetCached:
    def _dataset_with_tmp_cache(self, monkeypatch, tmp_path):
        monkeypatch.setattr(nu_mod, "_cache_dir", lambda dataset: tmp_path / dataset.cache_subdir)
        return nu_mod._DATASETS["full"]

    def test_fetches_each_archive_with_a_disk_check_and_removes_it_after_extraction(self, monkeypatch, tmp_path):
        dataset = self._dataset_with_tmp_cache(monkeypatch, tmp_path)
        ensured, extracted = [], []
        monkeypatch.setattr(nu_mod, "ensure_archive", lambda url, path, **kw: ensured.append((url, kw)))
        monkeypatch.setattr(
            nu_mod, "extract_into_cache", lambda path, cache, sentinel, remove_archive: extracted.append((path.name, sentinel, remove_archive))
        )

        nu_mod._ensure_dataset_cached(dataset)

        assert [u.rsplit("/", 1)[1] for u, _ in ensured] == ["nuimages-v1.0-all-metadata.tgz", "nuimages-v1.0-all-samples.tgz"]
        assert all(kw["required_free_bytes"] > 0 for _, kw in ensured)
        assert extracted == [
            ("nuimages-v1.0-all-metadata.tgz", "v1.0-train", True),
            ("nuimages-v1.0-all-samples.tgz", "samples", True),
        ]

    def test_an_already_extracted_archive_is_not_fetched_again(self, monkeypatch, tmp_path):
        dataset = self._dataset_with_tmp_cache(monkeypatch, tmp_path)
        cache = tmp_path / dataset.cache_subdir
        (cache / "v1.0-train").mkdir(parents=True)
        (cache / "samples").mkdir()
        monkeypatch.setattr(nu_mod, "ensure_archive", lambda *a, **kw: pytest.fail("downloaded again"))

        assert nu_mod._ensure_dataset_cached(dataset) == cache

    def test_resumes_with_only_the_missing_archive(self, monkeypatch, tmp_path):
        dataset = self._dataset_with_tmp_cache(monkeypatch, tmp_path)
        (tmp_path / dataset.cache_subdir / "v1.0-train").mkdir(parents=True)
        ensured = []
        monkeypatch.setattr(nu_mod, "ensure_archive", lambda url, path, **kw: ensured.append(path.name))
        monkeypatch.setattr(nu_mod, "extract_into_cache", lambda *a, **kw: None)

        nu_mod._ensure_dataset_cached(dataset)

        assert ensured == ["nuimages-v1.0-all-samples.tgz"]


class TestFetch:
    def _item(self, tokens, identifier="nuimages-v1.0-all"):
        return SourceItem(
            source_type=SourceType.NUSCENES,
            external_ref={"sample_data_tokens": tokens},
            license_hint="test license",
            dataset_identifier=identifier,
        )

    def test_converts_object_ann_to_yolo(self, cache_dir, tmp_path):
        workdir = tmp_path / "work"
        workdir.mkdir()

        connector = NuImagesConnector()
        drafts = list(connector.fetch(self._item(["sd1"]), workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
        draft = drafts[0]
        assert draft.annotation_format == "yolo_txt"
        assert draft.classes == ["vehicle.car"]
        assert draft.source_item_id == "nuimages_sd1"
        assert draft.dataset_identifier == "nuimages-v1.0-all"

    def test_image_missing_from_the_cache_is_counted_as_rejected(self, cache_dir, tmp_path):
        workdir = tmp_path / "work"
        workdir.mkdir()
        progress = []

        # sd2's row is in the index but its file was never written.
        drafts = list(NuImagesConnector().fetch(self._item(["sd2"]), workdir, {}, lambda: False, progress.append))

        assert drafts == []
        assert {"rejected": 1} in progress

    def test_unknown_token_is_counted_as_rejected(self, cache_dir, tmp_path):
        workdir = tmp_path / "work"
        workdir.mkdir()
        progress = []

        drafts = list(NuImagesConnector().fetch(self._item(["nope"]), workdir, {}, lambda: False, progress.append))

        assert drafts == []
        assert {"rejected": 1} in progress

    def test_shard_does_not_reload_the_json_tables(self, cache_dir, tmp_path, monkeypatch):
        # object_ann.json is 355MB for train; re-parsing it per shard is what the
        # index exists to avoid.
        nu_mod._ensure_index(cache_dir)
        (cache_dir / "v1.0-train" / "object_ann.json").unlink()
        workdir = tmp_path / "work"
        workdir.mkdir()

        drafts = list(NuImagesConnector().fetch(self._item(["sd1"]), workdir, {}, lambda: False, lambda d: None))

        assert len(drafts) == 1
