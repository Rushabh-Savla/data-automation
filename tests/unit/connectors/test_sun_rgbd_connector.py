"""Tests for SunRgbdConnector's lexical class-index matching and its 2D bbox
[x,y,w,h] -> YOLO conversion."""

import io
from pathlib import Path

import pytest
import requests
from PIL import Image

from app.connectors import sun_rgbd_connector as sun_mod
from app.connectors.base import SourceItem
from app.connectors.sun_rgbd_connector import SunRgbdConnector
from app.core.exceptions import TransientConnectorError
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


def _labelled(page) -> set[int]:
    return {i for item in page.items if not item.external_ref["unlabelled"] for i in item.external_ref["record_indices"]}


def _scanned(page) -> list[int]:
    return [i for item in page.items if item.external_ref["unlabelled"] for i in item.external_ref["record_indices"]]


def _many_records(n=100, person_ids=(3, 7)):
    return [
        {
            "sequence": f"SUNRGBD/kv2/seq{i}",
            "rgbname": f"{i:04d}.jpg",
            "boxes": [{"class": "person" if i in person_ids else "chair", "bbox": [1, 2, 3, 4]}],
        }
        for i in range(n)
    ]


class TestDiscover:
    def test_matches_class_and_shards(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)

        connector = SunRgbdConnector()
        page = connector.discover("a chair in the room", cursor=None, limit=200)

        assert page.error_reason is None
        assert _labelled(page) == {0}

    def test_a_query_no_label_names_scans_instead_of_failing(self, monkeypatch):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)

        connector = SunRgbdConnector()
        page = connector.discover("xylophone repair diagram", cursor=None, limit=200)

        assert page.error_reason is None
        assert _labelled(page) == set()
        assert sorted(_scanned(page)) == [0, 1]


class TestUnlabelledScan:
    """When labels cannot supply the limit, unlabelled images are offered too and
    the relevance gate decides. "Exactly n images" cannot come from labels alone:
    for "car and person" SUN RGB-D labels 93 people and no cars."""

    def test_fills_the_limit_with_unlabelled_images_after_the_labelled_ones(self, monkeypatch):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records())

        page = SunRgbdConnector().discover("a person", cursor=None, limit=30)

        assert _labelled(page) == {3, 7}
        scanned = _scanned(page)
        assert len(scanned) == 28
        assert set(scanned).isdisjoint({3, 7})
        assert len(set(scanned)) == len(scanned)

    def test_scanned_shards_are_marked_and_kept_apart_from_labelled_ones(self, monkeypatch):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records())

        page = SunRgbdConnector().discover("a person", cursor=None, limit=30)

        flags = [item.external_ref["unlabelled"] for item in page.items]
        assert flags[0] is False and flags.count(False) == 1  # the 2 labelled ones
        assert all(flags[1:])

    def test_a_larger_limit_extends_the_same_order(self, monkeypatch):
        # Top-up rounds re-run discovery with a bigger limit; the images already
        # fetched must be the front of it, or the round would refetch duplicates.
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records())

        small = _scanned(SunRgbdConnector().discover("a person", cursor=None, limit=30))
        large = _scanned(SunRgbdConnector().discover("a person", cursor=None, limit=60))

        assert large[: len(small)] == small
        assert len(large) == 58

    def test_the_order_is_not_simply_by_index(self, monkeypatch):
        # The dataset is grouped by scene, so the first N by index would be one room type.
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records())

        scanned = _scanned(SunRgbdConnector().discover("a person", cursor=None, limit=30))

        assert scanned != sorted(scanned)

    def test_no_scan_when_labels_already_fill_the_limit(self, monkeypatch):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records(person_ids=range(50)))

        page = SunRgbdConnector().discover("a person", cursor=None, limit=20)

        assert len(_labelled(page)) == 20
        assert _scanned(page) == []

    def test_a_query_matching_no_label_scans_up_to_the_limit(self, monkeypatch):
        # Any query works: the relevance filter, not the label list, decides.
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records())

        page = SunRgbdConnector().discover("xylophone", cursor=None, limit=50)

        assert page.error_reason is None
        assert _labelled(page) == set()
        assert len(_scanned(page)) == 50

    def test_a_word_inside_a_longer_word_is_not_a_label_match(self, monkeypatch):
        # "phone" must not match "xylophone" (nor "car" match "scarf").
        records = [
            {"sequence": "SUNRGBD/a", "rgbname": "1.jpg", "boxes": [{"class": "phone", "bbox": [1, 2, 3, 4]}]},
            {"sequence": "SUNRGBD/b", "rgbname": "2.jpg", "boxes": [{"class": "chair", "bbox": [1, 2, 3, 4]}]},
        ]
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: records)

        page = SunRgbdConnector().discover("xylophone", cursor=None, limit=200)

        assert _labelled(page) == set()

    def test_explicit_categories_are_an_exact_request_and_never_scanned(self, monkeypatch):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _many_records())

        page = SunRgbdConnector().discover("anything", cursor=None, limit=50, explicit_categories=["person"])

        assert _labelled(page) == {3, 7}
        assert _scanned(page) == []

    def test_candidate_order_is_stable_between_calls(self):
        assert sun_mod._unlabelled_candidates(50, {1, 2}) == sun_mod._unlabelled_candidates(50, {1, 2})
        assert set(sun_mod._unlabelled_candidates(50, {1, 2})) == set(range(50)) - {1, 2}


class TestFetch:
    def test_converts_xywh_bbox_to_yolo(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_images(tmp_path)
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)
        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: cache_dir)
        monkeypatch.setattr(sun_mod, "_ensure_image_index", dict)

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


def _jpeg_bytes(size=(640, 480)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size).save(buf, format="JPEG")
    return buf.getvalue()


_MEMBER_0 = "SUNRGBD/kv2/seq1/image/0001.jpg"
_MEMBER_1 = "SUNRGBD/kv2/seq2/image/0002.jpg"
_INDEX = {_MEMBER_0: [100, 10, 10, 8, 1], _MEMBER_1: [200, 10, 10, 8, 2]}


class TestFetchFromRemoteArchive:
    """With no unpacked copy on disk, images come from the archive by range
    request instead of a 6.9GB download."""

    def _item(self, indices):
        return SourceItem(
            source_type=SourceType.SUN_RGBD,
            external_ref={"record_indices": indices},
            license_hint="test license",
            dataset_identifier="sun-rgbd",
        )

    def _setup(self, monkeypatch, tmp_path, read_member):
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)
        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: tmp_path / "empty_cache")
        monkeypatch.setattr(sun_mod, "_ensure_image_index", lambda: _INDEX)
        monkeypatch.setattr(sun_mod.remote_zip, "read_member", read_member)
        workdir = tmp_path / "work"
        workdir.mkdir()
        return workdir

    def test_downloads_only_the_shards_images_by_range_and_builds_drafts(self, monkeypatch, tmp_path):
        requested = []

        def read_member(url, entry, *, headers):
            requested.append(tuple(entry))
            return _jpeg_bytes()

        workdir = self._setup(monkeypatch, tmp_path, read_member)
        drafts = list(SunRgbdConnector().fetch(self._item([0, 1]), workdir, {}, lambda: False, lambda d: None))

        assert sorted(requested) == [(100, 10, 10, 8, 1), (200, 10, 10, 8, 2)]
        assert [d.source_item_id for d in drafts] == ["sunrgbd_0", "sunrgbd_1"]
        assert drafts[0].classes == ["chair"] and drafts[1].classes == ["table"]
        assert (workdir / "sunrgbd_0.jpg").exists()

    def test_resumes_from_the_checkpoint_without_refetching_finished_images(self, monkeypatch, tmp_path):
        requested = []

        def read_member(url, entry, *, headers):
            requested.append(tuple(entry))
            return _jpeg_bytes()

        workdir = self._setup(monkeypatch, tmp_path, read_member)
        drafts = list(SunRgbdConnector().fetch(self._item([0, 1]), workdir, {"index": 1}, lambda: False, lambda d: None))

        assert requested == [(200, 10, 10, 8, 2)]
        assert [d.source_item_id for d in drafts] == ["sunrgbd_1"]

    def test_a_record_missing_from_the_archive_is_rejected_not_fatal(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sun_mod, "_ensure_image_index", lambda: {_MEMBER_1: _INDEX[_MEMBER_1]})
        workdir = self._setup(monkeypatch, tmp_path, lambda url, entry, *, headers: _jpeg_bytes())
        monkeypatch.setattr(sun_mod, "_ensure_image_index", lambda: {_MEMBER_1: _INDEX[_MEMBER_1]})
        progress = []

        drafts = list(SunRgbdConnector().fetch(self._item([0, 1]), workdir, {}, lambda: False, progress.append))

        assert [d.source_item_id for d in drafts] == ["sunrgbd_1"]
        assert {"rejected": 1} in progress

    def test_a_corrupt_image_is_rejected_but_the_rest_of_the_shard_continues(self, monkeypatch, tmp_path):
        import zipfile

        def read_member(url, entry, *, headers):
            if entry[0] == 100:
                raise zipfile.BadZipFile("member failed its size/CRC check")
            return _jpeg_bytes()

        workdir = self._setup(monkeypatch, tmp_path, read_member)
        progress = []
        drafts = list(SunRgbdConnector().fetch(self._item([0, 1]), workdir, {}, lambda: False, progress.append))

        assert [d.source_item_id for d in drafts] == ["sunrgbd_1"]
        assert {"rejected": 1} in progress

    def test_a_network_failure_is_transient_and_keeps_its_place(self, monkeypatch, tmp_path):
        def read_member(url, entry, *, headers):
            if entry[0] == 200:
                raise requests.ConnectionError("reset")
            return _jpeg_bytes()

        workdir = self._setup(monkeypatch, tmp_path, read_member)
        progress = []

        with pytest.raises(TransientConnectorError):
            list(SunRgbdConnector().fetch(self._item([0, 1]), workdir, {}, lambda: False, progress.append))

        # record 0 finished; the retry must resume at record 1, not start over.
        assert {"checkpoint": {"index": 1}} in progress

    def test_an_unpacked_local_copy_is_used_without_any_request(self, monkeypatch, tmp_path):
        cache_dir = _build_fake_images(tmp_path)
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)
        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: cache_dir)
        monkeypatch.setattr(sun_mod, "_ensure_image_index", lambda: _INDEX)
        monkeypatch.setattr(sun_mod.remote_zip, "read_member", lambda *a, **kw: pytest.fail("fetched a local image"))
        (cache_dir / "SUNRGBD/kv2/seq1/image").mkdir(parents=True, exist_ok=True)
        workdir = tmp_path / "work"
        workdir.mkdir()

        drafts = list(SunRgbdConnector().fetch(self._item([0]), workdir, {}, lambda: False, lambda d: None))

        assert [d.source_item_id for d in drafts] == ["sunrgbd_0"]


class TestProvenance:
    """Images the model found by itself carry SUN's boxes for other objects, not
    the queried one, so exports need to be able to tell them from labelled ones."""

    def _fetch_one(self, monkeypatch, tmp_path, unlabelled):
        cache_dir = _build_fake_images(tmp_path)
        monkeypatch.setattr(sun_mod, "_ensure_records_cached", lambda: _RECORDS)
        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: cache_dir)
        monkeypatch.setattr(sun_mod, "_ensure_image_index", dict)
        workdir = tmp_path / "work"
        workdir.mkdir()
        item = SourceItem(
            source_type=SourceType.SUN_RGBD,
            external_ref=(
                {"record_indices": [0]} if unlabelled is None else {"record_indices": [0], "unlabelled": unlabelled}
            ),
            license_hint="test license",
            dataset_identifier="sun-rgbd",
        )
        return next(iter(SunRgbdConnector().fetch(item, workdir, {}, lambda: False, lambda d: None)))

    def test_labelled_images_are_marked_label(self, monkeypatch, tmp_path):
        assert self._fetch_one(monkeypatch, tmp_path, unlabelled=False).metadata["matched_by"] == "label"

    def test_scanned_images_are_marked_vlm(self, monkeypatch, tmp_path):
        assert self._fetch_one(monkeypatch, tmp_path, unlabelled=True).metadata["matched_by"] == "vlm"

    def test_items_queued_before_the_flag_existed_count_as_labelled(self, monkeypatch, tmp_path):
        # A job resumed across a deploy still holds shards with no "unlabelled" key.
        assert self._fetch_one(monkeypatch, tmp_path, unlabelled=None).metadata["matched_by"] == "label"


class TestRequestsAreRateLimited:
    def test_every_image_request_holds_a_host_wide_slot(self, monkeypatch, tmp_path):
        held = []

        class _Slot:
            def __enter__(self):
                held.append("in")

            def __exit__(self, *exc):
                held.append("out")

        def fake_limited(lock_dir, name, slots):
            held.append((name, slots))
            return _Slot()

        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: tmp_path)
        monkeypatch.setattr(sun_mod, "limited_concurrency", fake_limited)
        monkeypatch.setattr(sun_mod.remote_zip, "read_member", lambda *a, **kw: held.append("request") or b"x")

        assert sun_mod._read_image([1, 2, 3, 4, 5]) == b"x"

        assert held == [("sun_rgbd_http", sun_mod._MAX_CONCURRENT_REQUESTS), "in", "request", "out"]

    def test_the_host_wide_limit_is_small(self):
        # ~32 simultaneous connections is what made the host drop requests.
        assert 1 <= sun_mod._MAX_CONCURRENT_REQUESTS <= 6
