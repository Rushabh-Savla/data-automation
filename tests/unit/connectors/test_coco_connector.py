"""Tests for CocoConnector's category matching and native COCO-format
annotation handling (deliberately no eager YOLO conversion at fetch time —
see the module docstring in app/connectors/coco_connector.py for why)."""

import json
from io import BytesIO
from pathlib import Path
from typing import ClassVar

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


def _fake_train_data(n_dogs: int = 6):
    """A train2017 split whose image ids do not collide with val2017's (1-3)."""
    images = [
        {"id": 1000 + i, "file_name": f"t{i}.jpg", "width": 640, "height": 480,
         "coco_url": f"http://x/t{i}.jpg", "license": 1}
        for i in range(n_dogs + 2)
    ]
    annotations = [
        {"id": 5000 + i, "image_id": 1000 + i, "category_id": 18, "bbox": [1, 2, 3, 4]} for i in range(n_dogs)
    ] + [
        {"id": 6000 + i, "image_id": 1000 + n_dogs + i, "category_id": 3, "bbox": [1, 2, 3, 4]} for i in range(2)
    ]
    return {**_FAKE_COCO_DATA, "images": images, "annotations": annotations}


def _patch_cache(monkeypatch, tmp_path: Path, train: dict | None = None):
    """Both splits' annotation JSON in a tmp cache; the index is built from them for real."""
    paths = {}
    for split, data in (("val2017", _FAKE_COCO_DATA), ("train2017", train if train is not None else _fake_train_data())):
        paths[split] = tmp_path / f"instances_{split}.json"
        paths[split].write_text(json.dumps(data))
    calls = []

    def ensure(split):
        calls.append(split)
        return paths[split]

    monkeypatch.setattr(coco_mod, "_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(coco_mod, "_ensure_annotations_cached", ensure)
    return calls


class TestDiscover:
    def test_finds_matching_images_and_shards(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector()
        page = connector.discover("dog", cursor=None, limit=200)

        assert _ids(page, unlabelled=False) == [1, 2]
        assert _ids(page, unlabelled=True) == [3]  # the rest of the dataset, offered after the labelled ones
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

    def test_a_query_no_label_names_scans_the_dataset_instead_of_failing(self, monkeypatch, tmp_path):
        # "xylophone" is not a COCO class, but that is not a reason to return nothing:
        # the relevance filter decides from the image itself.
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector()
        page = connector.discover("xylophone", cursor=None, limit=200)
        assert page.error_reason is None
        assert sorted(_ids(page, unlabelled=True)) == [1, 2, 3]
        assert _ids(page, unlabelled=False) == []


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


class _FakeSession:
    def get(self, *args, **kwargs):
        return _FakeResponse()


class TestFetch:
    def test_writes_native_coco_json_not_yolo(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(coco_mod, "_session", lambda: _FakeSession())

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


def _ids(page, split=None, unlabelled=None):
    return [
        i
        for item in page.items
        if split in (None, item.external_ref["split"])
        and unlabelled in (None, item.external_ref.get("unlabelled", False))
        for i in item.external_ref["image_ids"]
    ]


class TestContinuesIntoTrain2017:
    """val2017 has 5,000 images and only ~535 with a car, so a 1000-image job could
    never reach its target. Discovery now continues into train2017 when val2017's
    matches run out."""

    def test_val_is_offered_first_then_train_fills_the_rest(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("dog", cursor=None, limit=5)

        assert _ids(page, "val2017") == [1, 2]
        assert _ids(page, "train2017") == [1000, 1001, 1002]
        assert [i.external_ref["split"] for i in page.items] == ["val2017", "train2017"]

    def test_each_shard_carries_its_split_and_dataset_identifier(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("dog", cursor=None, limit=200)

        assert {(i.external_ref["split"], i.dataset_identifier) for i in page.items} == {
            ("val2017", "coco-val2017"),
            ("train2017", "coco-train2017"),
        }

    def test_train_is_not_touched_when_val_fills_the_limit(self, monkeypatch, tmp_path):
        # A small job must not pay to build the 600MB train index.
        calls = _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("dog", cursor=None, limit=2)

        assert _ids(page) == [1, 2]
        assert "train2017" not in calls

    def test_train_is_not_touched_when_val_can_fill_the_limit_by_scanning(self, monkeypatch, tmp_path):
        calls = _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("guitar", cursor=None, limit=3)

        assert len(_ids(page)) == 3
        assert "train2017" not in calls

    def test_a_larger_limit_extends_the_same_order(self, monkeypatch, tmp_path):
        # Top-up rounds re-run discovery with a bigger limit; images already fetched
        # must be the front of it, or the round would refetch duplicates.
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector(allow_train2017=True)

        small = _ids(connector.discover("dog", cursor=None, limit=4))
        large = _ids(connector.discover("dog", cursor=None, limit=8))

        assert large[: len(small)] == small
        assert len(large) == 8

    def test_the_limit_is_exact_across_both_splits(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        for limit in (1, 2, 3, 7):
            assert len(_ids(CocoConnector(allow_train2017=True).discover("dog", None, limit))) == limit

    def test_without_opt_in_only_val_is_used(self, monkeypatch, tmp_path):
        calls = _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector().discover("dog", cursor=None, limit=200)

        assert _ids(page, unlabelled=False) == [1, 2]
        assert set(_ids(page)) <= {1, 2, 3}
        assert "train2017" not in calls

    def test_explicit_categories_also_continue_into_train(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("anything", None, 200, explicit_categories=["car"])

        assert _ids(page, "val2017") == [3]
        assert _ids(page, "train2017") == [1006, 1007]

    def test_a_query_matching_no_label_scans_val_then_train(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("xylophone", cursor=None, limit=200)

        assert page.error_reason is None
        assert _ids(page, unlabelled=False) == []
        assert len(_ids(page, "val2017")) == 3 and len(_ids(page, "train2017")) == 8

    def test_train_only_split_still_works_when_allowed(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(split="train2017", allow_train2017=True).discover("dog", None, 200)

        assert _ids(page, "val2017") == []
        assert len(_ids(page, "train2017", unlabelled=False)) == 6

    def test_a_split_that_cannot_be_read_degrades_to_an_error_reason(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        def broken(split):
            raise OSError("disk full")

        monkeypatch.setattr(coco_mod, "_ensure_annotations_cached", broken)
        page = CocoConnector(allow_train2017=True).discover("dog", cursor=None, limit=5)

        assert page.items == []
        assert "disk full" in page.error_reason


class TestIndex:
    def test_built_once_and_reused(self, monkeypatch, tmp_path):
        calls = _patch_cache(monkeypatch, tmp_path)

        first = coco_mod._ensure_index("val2017")
        second = coco_mod._ensure_index("val2017")

        assert first == second
        assert calls == ["val2017"]  # the JSON was parsed once

    def test_a_failed_build_leaves_no_index_behind(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        (tmp_path / "instances_val2017.json").write_text("{not json")

        try:
            coco_mod._ensure_index("val2017")
        except ValueError:
            pass
        else:  # pragma: no cover
            raise AssertionError("expected a JSON error")

        assert not (tmp_path / "index_val2017.sqlite3").exists()
        assert not (tmp_path / "index_val2017.sqlite3.tmp").exists()

    def test_concurrent_callers_build_it_exactly_once(self, monkeypatch, tmp_path):
        import threading
        import time

        _patch_cache(monkeypatch, tmp_path)
        builds = []
        real_build = coco_mod._build_index

        def slow_build(json_path, index_path):
            builds.append(1)
            time.sleep(0.05)
            real_build(json_path, index_path)

        monkeypatch.setattr(coco_mod, "_build_index", slow_build)
        results = []
        threads = [threading.Thread(target=lambda: results.append(coco_mod._ensure_index("val2017"))) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(builds) == 1
        assert len(results) == 6 and len(set(results)) == 1

    def test_images_without_annotations_are_indexed_but_match_no_category(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        import sqlite3
        from contextlib import closing

        with closing(sqlite3.connect(coco_mod._ensure_index("train2017"))) as conn:
            total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
            with_cats = conn.execute("SELECT COUNT(DISTINCT image_id) FROM image_cats").fetchone()[0]

        assert total == 8 and with_cats == 8


class TestFetchFromIndex:
    def _item(self, ids, split):
        return SourceItem(
            source_type=SourceType.COCO,
            external_ref={"image_ids": ids, "split": split},
            license_hint=None,
            dataset_identifier=f"coco-{split}",
        )

    def _fetch(self, monkeypatch, tmp_path, item):
        monkeypatch.setattr(coco_mod, "_session", lambda: _FakeSession())
        workdir = tmp_path / "work"
        workdir.mkdir(exist_ok=True)
        return list(CocoConnector().fetch(item, workdir, {}, lambda: False, lambda d: None))

    def test_a_train_image_is_fetched_with_its_own_split_and_annotations(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        [draft] = self._fetch(monkeypatch, tmp_path, self._item([1002], "train2017"))

        assert draft.source_item_id == "coco_train2017_1002"
        assert draft.dataset_identifier == "coco-train2017"
        assert draft.metadata == {"coco_image_id": 1002, "split": "train2017", "matched_by": "label"}
        subset = json.loads(draft.annotation_tmp_path.read_text())
        assert [im["id"] for im in subset["images"]] == [1002]
        assert [a["image_id"] for a in subset["annotations"]] == [1002]
        assert len(subset["categories"]) == 3

    def test_the_subset_holds_only_the_shards_annotations(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        drafts = self._fetch(monkeypatch, tmp_path, self._item([1000, 1001], "train2017"))

        subset = json.loads(drafts[0].annotation_tmp_path.read_text())
        assert {a["image_id"] for a in subset["annotations"]} == {1000, 1001}

    def test_a_shard_does_not_reparse_the_annotation_json(self, monkeypatch, tmp_path):
        # The whole point of the index: train2017's JSON is 448MB.
        _patch_cache(monkeypatch, tmp_path)
        coco_mod._ensure_index("train2017")
        (tmp_path / "instances_train2017.json").unlink()

        def no_json(split):
            raise AssertionError("re-read the annotation JSON")

        monkeypatch.setattr(coco_mod, "_ensure_annotations_cached", no_json)
        drafts = self._fetch(monkeypatch, tmp_path, self._item([1000], "train2017"))

        assert len(drafts) == 1

    def test_an_image_missing_from_the_index_is_rejected_not_fatal(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(coco_mod, "_session", lambda: _FakeSession())
        workdir = tmp_path / "work"
        workdir.mkdir()
        progress = []

        drafts = list(CocoConnector().fetch(self._item([999999], "val2017"), workdir, {}, lambda: False, progress.append))

        assert drafts == []
        assert {"rejected": 1} in progress

    def test_shards_queued_before_the_split_key_existed_read_from_val(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        item = SourceItem(
            source_type=SourceType.COCO,
            external_ref={"image_ids": [1]},
            license_hint=None,
            dataset_identifier="coco-val2017",
        )

        [draft] = self._fetch(monkeypatch, tmp_path, item)

        assert draft.metadata["split"] == "val2017"


class _ScriptedResponse(_FakeResponse):
    def __init__(self, status=200):
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            error = requests.HTTPError(str(self.status_code))
            error.response = self
            raise error


import time as _time

# The tests stub out time.sleep (the retry backoff), which is the same module, so the
# fake server's simulated latency must hold on to the real one.
_REAL_SLEEP = _time.sleep


class _RecordingSession:
    """Serves images, tracking how many requests are in flight at once."""

    def __init__(self, delay=0.0, fail=None):
        import threading

        self.delay = delay
        self.fail = fail or {}  # url -> list of outcomes: an Exception, or an int status
        self.lock = threading.Lock()
        self.in_flight = 0
        self.peak = 0
        self.requested: list[str] = []

    def get(self, url, **kwargs):
        with self.lock:
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
            self.requested.append(url)
        try:
            _REAL_SLEEP(self.delay)
            outcomes = self.fail.get(url)
            if outcomes:
                outcome = outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                return _ScriptedResponse(outcome)
            return _ScriptedResponse()
        finally:
            with self.lock:
                self.in_flight -= 1


def _fetch_all(monkeypatch, tmp_path, session, ids, split="train2017", checkpoint=None, should_stop=lambda: False):
    _patch_cache(monkeypatch, tmp_path)
    monkeypatch.setattr(coco_mod, "_session", lambda: session)
    monkeypatch.setattr(coco_mod.time, "sleep", lambda _: None)
    item = SourceItem(
        source_type=SourceType.COCO,
        external_ref={"image_ids": ids, "split": split},
        license_hint=None,
        dataset_identifier=f"coco-{split}",
    )
    workdir = tmp_path / "work"
    workdir.mkdir(exist_ok=True)
    progress = []
    drafts = list(CocoConnector().fetch(item, workdir, checkpoint or {}, should_stop, progress.append))
    return drafts, progress


class TestConcurrentFetch:
    """A shard fetched its 20 images one after another, ~4s each. Several at once
    overlaps the waiting."""

    IDS: ClassVar[list[int]] = [1000, 1001, 1002, 1003, 1004, 1005]

    def test_several_images_are_in_flight_at_once(self, monkeypatch, tmp_path):
        session = _RecordingSession(delay=0.05)

        drafts, _ = _fetch_all(monkeypatch, tmp_path, session, self.IDS)

        assert len(drafts) == 6
        assert session.peak > 1

    def test_concurrency_is_bounded(self, monkeypatch, tmp_path):
        session = _RecordingSession(delay=0.05)

        _fetch_all(monkeypatch, tmp_path, session, self.IDS + [1006, 1007])

        assert session.peak <= coco_mod._FETCH_CONCURRENCY

    def test_it_is_actually_faster_than_one_at_a_time(self, monkeypatch, tmp_path):
        import time

        session = _RecordingSession(delay=0.1)
        started = time.time()

        _fetch_all(monkeypatch, tmp_path, session, self.IDS)

        # 6 images x 0.1s sequentially would be 0.6s; in parallel it is about one delay (allow margin for CPU load).
        assert time.time() - started < 1.5

    def test_drafts_come_out_in_shard_order_whatever_finishes_first(self, monkeypatch, tmp_path):
        drafts, _ = _fetch_all(monkeypatch, tmp_path, _RecordingSession(delay=0.02), self.IDS)

        assert [d.metadata["coco_image_id"] for d in drafts] == self.IDS

    def test_progress_and_checkpoints_advance_in_order(self, monkeypatch, tmp_path):
        _, progress = _fetch_all(monkeypatch, tmp_path, _RecordingSession(), [1000, 1001, 1002])

        assert [p["checkpoint"]["index"] for p in progress if "checkpoint" in p] == [1, 2, 3]

    def test_resuming_from_a_checkpoint_fetches_only_the_remaining_images(self, monkeypatch, tmp_path):
        session = _RecordingSession()

        drafts, _ = _fetch_all(monkeypatch, tmp_path, session, self.IDS, checkpoint={"index": 4})

        assert [d.metadata["coco_image_id"] for d in drafts] == [1004, 1005]
        assert sorted(session.requested) == ["http://x/t4.jpg", "http://x/t5.jpg"]

    def test_a_stop_keeps_the_place_and_raises_jobstopped(self, monkeypatch, tmp_path):
        import pytest

        from app.core.exceptions import JobStopped

        calls = {"n": 0}

        def stop_after_two():
            calls["n"] += 1
            return calls["n"] > 2

        _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(coco_mod, "_session", lambda: _RecordingSession())
        item = SourceItem(SourceType.COCO, {"image_ids": self.IDS, "split": "train2017"}, None, "coco-train2017")
        workdir = tmp_path / "work"
        workdir.mkdir()
        progress = []

        with pytest.raises(JobStopped):
            list(CocoConnector().fetch(item, workdir, {}, stop_after_two, progress.append))

        assert {"checkpoint": {"index": 2}} in progress


class TestDownloadRetries:
    URL = "http://x/t0.jpg"

    def _patch(self, monkeypatch, session):
        monkeypatch.setattr(coco_mod, "_session", lambda: session)
        monkeypatch.setattr(coco_mod.time, "sleep", lambda _: None)
        dropped = []
        monkeypatch.setattr(coco_mod, "_drop_session", lambda: dropped.append(1))
        return dropped

    def test_a_dropped_connection_is_retried(self, monkeypatch, tmp_path):
        import requests

        session = _RecordingSession(fail={self.URL: [requests.ConnectionError("reset")]})
        self._patch(monkeypatch, session)

        coco_mod._download_image(self.URL, tmp_path / "a.jpg")

        assert session.requested.count(self.URL) == 2

    def test_a_server_error_is_retried(self, monkeypatch, tmp_path):
        session = _RecordingSession(fail={self.URL: [503, 502]})
        self._patch(monkeypatch, session)

        coco_mod._download_image(self.URL, tmp_path / "a.jpg")

        assert session.requested.count(self.URL) == 3

    def test_a_missing_image_is_not_retried(self, monkeypatch, tmp_path):
        import pytest
        import requests

        session = _RecordingSession(fail={self.URL: [404]})
        self._patch(monkeypatch, session)

        with pytest.raises(requests.HTTPError):
            coco_mod._download_image(self.URL, tmp_path / "a.jpg")

        assert session.requested.count(self.URL) == 1

    def test_it_gives_up_after_the_attempt_budget(self, monkeypatch, tmp_path):
        import pytest
        import requests

        session = _RecordingSession(fail={self.URL: [requests.ConnectionError("x")] * 10})
        self._patch(monkeypatch, session)

        with pytest.raises(requests.ConnectionError):
            coco_mod._download_image(self.URL, tmp_path / "a.jpg")

        assert session.requested.count(self.URL) == coco_mod._DOWNLOAD_ATTEMPTS

    def test_the_connection_is_discarded_before_a_retry(self, monkeypatch, tmp_path):
        import requests

        session = _RecordingSession(fail={self.URL: [requests.ConnectionError("reset")]})
        dropped = self._patch(monkeypatch, session)

        coco_mod._download_image(self.URL, tmp_path / "a.jpg")

        assert dropped == [1]

    def test_one_image_that_cannot_be_fetched_costs_only_that_image(self, monkeypatch, tmp_path):
        import requests

        bad = "http://x/t1.jpg"
        session = _RecordingSession(fail={bad: [requests.ConnectionError("x")] * 10})

        drafts, progress = _fetch_all(monkeypatch, tmp_path, session, [1000, 1001, 1002])

        assert [d.metadata["coco_image_id"] for d in drafts] == [1000, 1002]
        assert {"rejected": 1} in progress


class TestOpenVocabularyScan:
    """Any query works: labels come first, and whatever they cannot cover is filled
    from the rest of the dataset for the relevance filter to judge."""

    def test_labelled_images_always_come_before_scanned_ones(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector().discover("dog", cursor=None, limit=200)

        assert [i.external_ref["unlabelled"] for i in page.items] == [False, True]

    def test_the_scan_never_repeats_a_labelled_image(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("dog", cursor=None, limit=200)

        assert set(_ids(page, unlabelled=False)).isdisjoint(_ids(page, unlabelled=True))
        assert len(_ids(page)) == len(set(_ids(page)))

    def test_the_scan_is_bounded_by_the_limit(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        assert len(_ids(CocoConnector(allow_train2017=True).discover("guitar", None, 5))) == 5

    def test_a_larger_limit_extends_the_same_scan_order(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        connector = CocoConnector(allow_train2017=True)

        small = _ids(connector.discover("guitar", None, 4))
        large = _ids(connector.discover("guitar", None, 9))

        assert large[: len(small)] == small

    def test_the_scan_order_is_not_simply_by_id(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        scanned = _ids(CocoConnector(allow_train2017=True).discover("guitar", None, 8))

        assert scanned != sorted(scanned)

    def test_explicit_categories_are_exact_and_never_scanned(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector(allow_train2017=True).discover("anything", None, 200, explicit_categories=["car"])

        assert _ids(page, unlabelled=True) == []

    def test_explicit_categories_that_match_nothing_still_report_it(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector().discover("anything", None, 200, explicit_categories=["guitar"])

        assert page.items == [] and "guitar" in page.error_reason

    def test_synonyms_reach_the_label_instead_of_scanning(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)

        page = CocoConnector().discover("puppy", cursor=None, limit=2)

        assert _ids(page, unlabelled=False) == [1, 2]  # "puppy" -> dog

    def test_scanned_images_are_tagged_so_exports_can_tell(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(coco_mod, "_session", lambda: _FakeSession())
        workdir = tmp_path / "work"
        workdir.mkdir()

        def matched_by(unlabelled):
            item = SourceItem(
                SourceType.COCO, {"image_ids": [1], "split": "val2017", "unlabelled": unlabelled}, None, "coco-val2017"
            )
            [draft] = list(CocoConnector().fetch(item, workdir, {}, lambda: False, lambda d: None))
            return draft.metadata["matched_by"]

        assert matched_by(False) == "label"
        assert matched_by(True) == "vlm"

    def test_a_shard_queued_before_the_flag_existed_counts_as_labelled(self, monkeypatch, tmp_path):
        _patch_cache(monkeypatch, tmp_path)
        monkeypatch.setattr(coco_mod, "_session", lambda: _FakeSession())
        workdir = tmp_path / "work"
        workdir.mkdir()
        item = SourceItem(SourceType.COCO, {"image_ids": [1], "split": "val2017"}, None, "coco-val2017")

        [draft] = list(CocoConnector().fetch(item, workdir, {}, lambda: False, lambda d: None))

        assert draft.metadata["matched_by"] == "label"


class TestCandidateAdmission:
    @staticmethod
    def _item():
        return SourceItem(
            source_type=SourceType.COCO,
            external_ref={"image_ids": [1, 2, 3], "split": "val2017"},
            license_hint=None,
            dataset_identifier="coco-val2017",
        )

    @staticmethod
    def _fake_download(submitted):
        def download(url, dest):
            submitted.append(dest.name)
            dest.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (640, 480)).save(dest, format="JPEG")
        return download

    def test_admission_stops_before_candidate_after_slot_600(
        self, monkeypatch, tmp_path
    ):
        from types import SimpleNamespace
        from app.core.exceptions import CandidateWindowFull

        _patch_cache(monkeypatch, tmp_path)
        submitted = []
        monkeypatch.setattr(
            coco_mod, "_download_image", self._fake_download(submitted)
        )

        calls = []

        def admit(source_item_id):
            calls.append(source_item_id)
            if len(calls) == 1:
                # Simulates this candidate consuming the final slot (600).
                return SimpleNamespace(
                    status="ADMITTED",
                    window_id="test-window",
                    window_index=1,
                    reservation_id="reservation-600",
                )
            # Simulates the next candidate, number 601.
            return SimpleNamespace(
                status="WINDOW_FULL",
                window_id="test-window",
                window_index=1,
                reservation_id=None,
            )

        workdir = tmp_path / "admission-work"
        workdir.mkdir()
        generator = CocoConnector().fetch(
            self._item(),
            workdir,
            {},
            lambda: False,
            lambda delta: None,
            admit_candidate=admit,
        )

        yielded = []
        boundary = None
        try:
            while True:
                yielded.append(next(generator))
        except CandidateWindowFull as exc:
            boundary = exc
        except StopIteration:
            pass

        assert boundary is not None
        assert boundary.window_id == "test-window"
        assert boundary.checkpoint_index == 1
        assert calls == ["coco_val2017_1", "coco_val2017_2"]
        assert submitted == ["img1.jpg"]
        assert [draft.source_item_id for draft in yielded] == [
            "coco_val2017_1"
        ]
        assert yielded[0].metadata["candidate_reservation_id"] == "reservation-600"

    def test_duplicate_is_skipped_without_downloading(
        self, monkeypatch, tmp_path
    ):
        from types import SimpleNamespace

        _patch_cache(monkeypatch, tmp_path)
        submitted = []
        monkeypatch.setattr(
            coco_mod, "_download_image", self._fake_download(submitted)
        )

        def admit(source_item_id):
            if source_item_id == "coco_val2017_1":
                return SimpleNamespace(
                    status="DUPLICATE",
                    window_id="test-window",
                    window_index=1,
                    reservation_id=None,
                )
            return SimpleNamespace(
                status="ADMITTED",
                window_id="test-window",
                window_index=1,
                reservation_id=f"reservation-{source_item_id}",
            )

        workdir = tmp_path / "duplicate-work"
        workdir.mkdir()
        drafts = list(
            CocoConnector().fetch(
                self._item(),
                workdir,
                {},
                lambda: False,
                lambda delta: None,
                admit_candidate=admit,
            )
        )

        assert set(submitted) == {"img2.jpg", "img3.jpg"}
        assert [draft.source_item_id for draft in drafts] == [
            "coco_val2017_2",
            "coco_val2017_3",
        ]
