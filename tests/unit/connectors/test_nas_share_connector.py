import hashlib

import pytest
from PIL import Image

from app.connectors.base import SourceItem
from app.connectors.nas import NasConnectionParams
from app.connectors.nas_share_connector import NasShareConnector
from app.core.exceptions import JobStopped
from app.domain.enums import SourceType


def _img(path, size=(100, 100)):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, (200, 10, 10)).save(path)


@pytest.fixture
def share(tmp_path):
    root = tmp_path / "nas"
    for n in range(3):
        _img(root / "cars" / f"car_{n}.jpg")
    for n in range(4):
        _img(root / "misc" / f"img_{n}.jpg")
    _img(root / "data_automation" / "job" / "coco" / "images" / "stored.jpg")
    _img(root / "tiny" / "small.jpg", size=(10, 10))
    return root


def _connector(root, **kw):
    return NasShareConnector(NasConnectionParams(protocol="local", share=str(root), **kw))


def _all_paths(page):
    return [p for item in page.items for p in item.external_ref["paths"]]


class TestDiscover:
    def test_path_matches_come_first_and_are_not_flagged_unlabelled(self, share):
        page = _connector(share, base_path="").discover("car", None, limit=100)
        matched = [i for i in page.items if not i.external_ref["unlabelled"]]
        assert sorted(p for i in matched for p in i.external_ref["paths"]) == [
            "cars/car_0.jpg", "cars/car_1.jpg", "cars/car_2.jpg"
        ]
        assert all(i.external_ref["unlabelled"] for i in page.items if i not in matched)

    def test_stored_output_folder_is_never_offered_back_as_a_source(self, share):
        paths = _all_paths(_connector(share).discover("anything", None, limit=100))
        assert not any(p.startswith("data_automation/") for p in paths)

    def test_limit_is_respected(self, share):
        assert len(_all_paths(_connector(share).discover("car", None, limit=4))) == 4

    def test_result_is_deterministic_so_topup_rounds_extend_the_same_list(self, share):
        c = _connector(share)
        small = _all_paths(c.discover("zebra", None, limit=3))
        large = _all_paths(c.discover("zebra", None, limit=6))
        assert large[:3] == small

    def test_shards_are_bounded_and_carry_no_credentials(self, share):
        c = NasShareConnector(
            NasConnectionParams(
                protocol="local", share=str(share), auth_mode="password", username="bob", password="s3cret-pw"
            )
        )
        page = c.discover("car", None, limit=100)
        assert page.items and all(len(i.external_ref["paths"]) <= 25 for i in page.items)
        blob = repr([(i.external_ref, i.dataset_identifier, i.license_hint) for i in page.items])
        assert "s3cret-pw" not in blob and "bob" not in blob

    def test_base_path_narrows_the_scan(self, share):
        paths = _all_paths(_connector(share, base_path="cars").discover("x", None, limit=100))
        assert set(paths) == {"cars/car_0.jpg", "cars/car_1.jpg", "cars/car_2.jpg"}

    def test_empty_folder_reports_why(self, tmp_path):
        (tmp_path / "e").mkdir()
        page = _connector(tmp_path / "e").discover("car", None, limit=10)
        assert page.items == [] and "No images found" in page.error_reason

    def test_unreachable_nas_is_an_error_reason_not_an_exception(self, tmp_path):
        page = _connector(tmp_path / "missing").discover("car", None, limit=10)
        assert page.items == [] and page.error_reason


def _item(paths, unlabelled=False):
    return SourceItem(SourceType.NAS_SHARE, {"paths": paths, "unlabelled": unlabelled}, "user-provided", "nas:x")


class TestFetch:
    def _run(self, connector, item, workdir, checkpoint=None, should_stop=lambda: False):
        events = []
        drafts = []
        for d in connector.fetch(item, workdir, checkpoint or {}, should_stop, events.append):
            # the driver promotes the file while the generator is suspended; it must still exist here
            assert d.local_tmp_path.exists()
            drafts.append(d)
        return drafts, events

    def test_yields_drafts_with_provenance_and_stable_ids(self, share, tmp_path):
        c = _connector(share)
        drafts, events = self._run(c, _item(["cars/car_0.jpg"]), tmp_path / "w")
        d = drafts[0]
        assert (d.width, d.height) == (100, 100)
        assert d.original_name == "car_0.jpg"
        assert d.metadata["nas_path"] == "cars/car_0.jpg"
        assert d.source_url.endswith("cars/car_0.jpg") and d.source_url.startswith("nas://")
        again, _ = self._run(c, _item(["cars/car_0.jpg"]), tmp_path / "w2")
        assert again[0].source_item_id == d.source_item_id
        assert d.source_item_id.startswith("nas_") and len(d.source_item_id) <= 512
        assert {"checkpoint": {"index": 1}, "downloaded": 1} in events

    def test_source_item_id_differs_per_path(self, share, tmp_path):
        drafts, _ = self._run(_connector(share), _item(["cars/car_0.jpg", "cars/car_1.jpg"]), tmp_path / "w")
        assert len({d.source_item_id for d in drafts}) == 2

    def test_temp_files_are_freed_as_it_goes(self, share, tmp_path):
        workdir = tmp_path / "w"
        self._run(_connector(share), _item(["cars/car_0.jpg", "cars/car_1.jpg", "cars/car_2.jpg"]), workdir)
        assert list(workdir.iterdir()) == []

    def test_small_images_are_rejected_not_yielded(self, share, tmp_path):
        drafts, events = self._run(_connector(share), _item(["tiny/small.jpg", "cars/car_0.jpg"]), tmp_path / "w")
        assert [d.original_name for d in drafts] == ["car_0.jpg"]
        assert {"rejected": 1} in events

    def test_a_file_deleted_since_discovery_is_skipped_not_fatal(self, share, tmp_path):
        drafts, events = self._run(_connector(share), _item(["cars/gone.jpg", "cars/car_0.jpg"]), tmp_path / "w")
        assert len(drafts) == 1 and {"rejected": 1} in events

    def test_a_non_image_file_is_rejected(self, share, tmp_path):
        (share / "cars" / "fake.jpg").write_text("not an image")
        drafts, events = self._run(_connector(share), _item(["cars/fake.jpg", "cars/car_0.jpg"]), tmp_path / "w")
        assert len(drafts) == 1 and {"rejected": 1} in events

    def test_resumes_from_checkpoint(self, share, tmp_path):
        drafts, _ = self._run(
            _connector(share), _item(["cars/car_0.jpg", "cars/car_1.jpg"]), tmp_path / "w", checkpoint={"index": 1}
        )
        assert [d.original_name for d in drafts] == ["car_1.jpg"]

    def test_stop_saves_checkpoint_and_raises(self, share, tmp_path):
        events = []
        gen = _connector(share).fetch(
            _item(["cars/car_0.jpg", "cars/car_1.jpg"]), tmp_path / "w", {}, lambda: True, events.append
        )
        with pytest.raises(JobStopped):
            next(gen)
        assert {"checkpoint": {"index": 0}} in events

    def test_unlabelled_shards_are_marked_for_the_relevance_filter(self, share, tmp_path):
        drafts, _ = self._run(_connector(share), _item(["misc/img_0.jpg"], unlabelled=True), tmp_path / "w")
        assert drafts[0].metadata["matched_by"] == "vlm"
        labelled, _ = self._run(_connector(share), _item(["cars/car_0.jpg"], unlabelled=False), tmp_path / "w2")
        assert labelled[0].metadata["matched_by"] == "path"


def test_source_item_id_is_not_derived_from_secrets():
    p1 = NasConnectionParams(protocol="smb", host="nas", share="s", password="a")
    p2 = NasConnectionParams(protocol="smb", host="nas", share="s", password="b")
    ident = lambda p: f"{p.protocol}://{p.host}/{p.share}/x.jpg"
    assert hashlib.sha1(ident(p1).encode()).hexdigest() == hashlib.sha1(ident(p2).encode()).hexdigest()
