import os
import time
from types import SimpleNamespace

import pytest

from app.connectors.nas import NasConnectionParams, NasPathError
from app.services.export_service import ExportAssetView, _localize
from app.storage import asset_files


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A 'NAS' that is a directory, plus an isolated cache under tmp_path."""
    nas = tmp_path / "nas"
    nas.mkdir()
    settings = SimpleNamespace(work_root=tmp_path / "work", nas_cache_max_gb=5.0)
    monkeypatch.setattr(asset_files, "get_settings", lambda: settings)
    params = NasConnectionParams(protocol="local", share=str(nas))
    monkeypatch.setattr(asset_files, "get_saved_params", lambda: params)
    return SimpleNamespace(nas=nas, settings=settings, tmp=tmp_path, monkeypatch=monkeypatch)


class TestPathKinds:
    def test_scheme_detection(self):
        assert asset_files.is_remote("nas://data/x.jpg")
        assert not asset_files.is_remote("/abs/local.jpg")
        assert not asset_files.is_remote("")
        assert not asset_files.is_remote(None)

    def test_round_trip(self):
        assert asset_files.remote_rel(asset_files.make_remote_path("/a//b/c.jpg")) == "a/b/c.jpg"

    def test_make_remote_path_refuses_escapes(self):
        with pytest.raises(NasPathError):
            asset_files.make_remote_path("a/../../etc/passwd")


class TestLocalPassThrough:
    """No NAS in play: behaviour must be exactly what callers did before."""

    def test_materialize_returns_the_same_path_without_touching_the_nas(self, monkeypatch):
        def boom():
            raise AssertionError("must not look up the NAS for a local path")

        monkeypatch.setattr(asset_files, "get_saved_params", boom)
        assert str(asset_files.materialize("/some/local/file.jpg")) == "/some/local/file.jpg"
        assert asset_files.local_path_for_read("/some/local/file.jpg") == "/some/local/file.jpg"

    def test_delete_unlinks_and_tolerates_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(asset_files, "get_saved_params", lambda: pytest.fail("no NAS lookup for local"))
        f = tmp_path / "a.jpg"
        f.write_bytes(b"x")
        asset_files.delete(str(f))
        assert not f.exists()
        asset_files.delete(str(f))  # already gone: no error

    def test_exists_checks_the_local_disk(self, tmp_path):
        f = tmp_path / "a.jpg"
        assert not asset_files.exists(str(f))
        f.write_bytes(b"x")
        assert asset_files.exists(str(f))


class TestMaterialize:
    def _put(self, env, rel, data=b"image-bytes"):
        p = env.nas / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def test_fetches_a_remote_image_into_the_cache(self, env):
        self._put(env, "data/job/coco/images/x.jpg")
        local = asset_files.materialize("nas://data/job/coco/images/x.jpg")
        assert local.read_bytes() == b"image-bytes"
        assert str(local).startswith(str(env.settings.work_root / "nas_cache"))
        assert local.suffix == ".jpg"

    def test_second_read_is_served_from_cache(self, env):
        self._put(env, "data/x.jpg")
        first = asset_files.materialize("nas://data/x.jpg")
        (env.nas / "data" / "x.jpg").unlink()  # NAS copy gone; cache must still answer
        assert asset_files.materialize("nas://data/x.jpg") == first

    def test_missing_remote_file_is_file_not_found(self, env):
        with pytest.raises(FileNotFoundError):
            asset_files.materialize("nas://data/nope.jpg")

    def test_no_nas_configured_is_reported_as_unreachable(self, env, monkeypatch):
        monkeypatch.setattr(asset_files, "get_saved_params", lambda: None)
        with pytest.raises(asset_files.NasUnreachable, match="no NAS connection"):
            asset_files.materialize("nas://data/x.jpg")

    def test_unreachable_nas_is_reported_as_unreachable(self, env, monkeypatch):
        params = NasConnectionParams(protocol="local", share=str(env.tmp / "unmounted"))
        monkeypatch.setattr(asset_files, "get_saved_params", lambda: params)
        with pytest.raises(asset_files.NasUnreachable):
            asset_files.materialize("nas://data/x.jpg")

    def test_thumbnail_cache_is_separate_from_the_image_cache(self):
        a = asset_files.thumbnail_cache_file("nas://data/x.jpg")
        assert "thumbs" in a.parts and a.name.endswith(".thumb.jpg")
        assert a != asset_files._cache_file("nas://data/x.jpg")


class TestEviction:
    def test_oldest_files_are_evicted_past_the_limit_but_recent_ones_are_protected(self, env):
        env.settings.nas_cache_max_gb = 10 / 1024**3  # 10 bytes
        for n in range(3):
            self._old(env, f"data/{n}.jpg", age_s=1000 + n)
        recent = env.nas / "data" / "fresh.jpg"
        recent.write_bytes(b"12345678")  # 8 bytes, fetched just now
        for n in range(3):
            asset_files.materialize(f"nas://data/{n}.jpg")
        # age the first three past the grace window, oldest first
        now = time.time()
        for i, n in enumerate(range(3)):
            f = asset_files._cache_file(f"nas://data/{n}.jpg")
            os.utime(f, (now - 1000 + i, now - 1000 + i))
        fresh = asset_files.materialize("nas://data/fresh.jpg")
        assert fresh.exists(), "a file that was just handed to a reader must never be evicted"
        survivors = [n for n in range(3) if asset_files._cache_file(f"nas://data/{n}.jpg").exists()]
        assert len(survivors) < 3

    def _old(self, env, rel, age_s):
        p = env.nas / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"12345678")

    def test_thumbnails_are_never_evicted(self, env):
        env.settings.nas_cache_max_gb = 1 / 1024**3
        thumb = asset_files.thumbnail_cache_file("nas://data/x.jpg")
        thumb.parent.mkdir(parents=True, exist_ok=True)
        thumb.write_bytes(b"t" * 1000)
        old = time.time() - 10_000
        os.utime(thumb, (old, old))
        (env.nas / "data").mkdir()
        (env.nas / "data" / "y.jpg").write_bytes(b"y")
        asset_files.materialize("nas://data/y.jpg")
        assert thumb.exists()


class TestRemoteDelete:
    def test_removes_the_file_and_its_cached_copies(self, env):
        (env.nas / "data").mkdir()
        (env.nas / "data" / "x.jpg").write_bytes(b"img")
        cached = asset_files.materialize("nas://data/x.jpg")
        thumb = asset_files.thumbnail_cache_file("nas://data/x.jpg")
        thumb.parent.mkdir(parents=True, exist_ok=True)
        thumb.write_bytes(b"t")
        asset_files.delete("nas://data/x.jpg")
        assert not (env.nas / "data" / "x.jpg").exists()
        assert not cached.exists() and not thumb.exists()

    def test_a_nas_failure_never_raises(self, env, monkeypatch):
        monkeypatch.setattr(asset_files, "get_saved_params", lambda: None)
        asset_files.delete("nas://data/x.jpg")  # best-effort, like every other cleanup

    def test_strict_delete_reports_a_nas_that_cannot_be_reached(self, env, monkeypatch):
        monkeypatch.setattr(asset_files, "get_saved_params", lambda: None)
        with pytest.raises(asset_files.NasUnreachable):
            asset_files.delete("nas://data/x.jpg", strict=True)

    def test_strict_delete_still_treats_an_already_missing_file_as_success(self, env):
        (env.nas / "data").mkdir()
        asset_files.delete("nas://data/never-existed.jpg", strict=True)

    def test_exists_reflects_the_nas(self, env):
        assert not asset_files.exists("nas://data/x.jpg")
        (env.nas / "data").mkdir()
        (env.nas / "data" / "x.jpg").write_bytes(b"i")
        assert asset_files.exists("nas://data/x.jpg")


class TestExportLocalize:
    def _view(self, **kw):
        base = dict(
            id="1", source_type="coco", source_url=None, dataset_identifier=None, license=None,
            nas_path="/local/a.jpg", annotation_format="none", annotation_nas_path=None, width=1, height=1,
        )
        return ExportAssetView(**{**base, **kw})

    def test_local_asset_comes_back_as_the_very_same_object(self):
        v = self._view()
        assert _localize(v) is v

    def test_remote_image_and_annotation_are_pointed_at_local_copies(self, env):
        (env.nas / "d").mkdir()
        (env.nas / "d" / "a.jpg").write_bytes(b"i")
        (env.nas / "d" / "a.txt").write_bytes(b"0 0.5 0.5 1 1")
        v = self._view(nas_path="nas://d/a.jpg", annotation_nas_path="nas://d/a.txt", annotation_format="yolo_txt")
        out = _localize(v)
        assert out is not v and v.nas_path == "nas://d/a.jpg", "the original view must not be mutated"
        assert open(out.nas_path, "rb").read() == b"i"
        assert open(out.annotation_nas_path, "rb").read() == b"0 0.5 0.5 1 1"

    def test_image_missing_on_the_nas_becomes_none_so_it_is_skipped(self, env):
        assert _localize(self._view(nas_path="nas://d/gone.jpg")).nas_path is None

    def test_unreachable_nas_fails_the_export_loudly(self, env, monkeypatch):
        monkeypatch.setattr(asset_files, "get_saved_params", lambda: None)
        with pytest.raises(asset_files.NasUnreachable):
            _localize(self._view(nas_path="nas://d/a.jpg"))
