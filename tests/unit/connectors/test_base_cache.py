"""Tests for the shared-cache helpers in app/connectors/base.py
(exclusive_cache_lock / ensure_archive / extract_into_cache) and the SUN RGB-D
index cache that uses the lock.

The bug they exist for: a job's download shards run concurrently, each one
fetched the whole archive with no lock, and they all appended to the same
`SUNRGBD.zip.tmp` -- a 15GB file for a 6.9GB archive that then failed every
shard with `Corrupt zip64 end of central directory record`, permanently, because
the "does the zip exist" check trusted whatever was on disk.
"""

import tarfile
import threading
import time
import zipfile
from pathlib import Path

import pytest
import requests

from app.connectors import base as base_mod
from app.connectors import sun_rgbd_connector as sun_mod
from app.connectors.base import (
    ensure_archive,
    exclusive_cache_lock,
    extract_into_cache,
    limited_concurrency,
)
from app.core.exceptions import FatalConnectorError, TransientConnectorError


def _write_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)


def _write_tgz(path: Path, members: dict[str, str], tmp_path: Path) -> None:
    src = tmp_path / "_tgz_src"
    for name, content in members.items():
        (src / name).parent.mkdir(parents=True, exist_ok=True)
        (src / name).write_text(content)
    with tarfile.open(path, "w:gz") as tf:
        for entry in sorted(src.iterdir()):
            tf.add(entry, arcname=entry.name)


class TestExclusiveCacheLock:
    def test_serialises_concurrent_holders(self, tmp_path):
        lock_path = tmp_path / ".cache.lock"
        inside = 0
        max_inside = 0
        state_lock = threading.Lock()

        def worker():
            nonlocal inside, max_inside
            with exclusive_cache_lock(lock_path):
                with state_lock:
                    inside += 1
                    max_inside = max(max_inside, inside)
                time.sleep(0.02)
                with state_lock:
                    inside -= 1

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert max_inside == 1

    def test_released_after_an_exception(self, tmp_path):
        lock_path = tmp_path / ".cache.lock"
        with pytest.raises(RuntimeError), exclusive_cache_lock(lock_path):
            raise RuntimeError("boom")

        acquired = threading.Event()

        def reacquire():
            with exclusive_cache_lock(lock_path):
                acquired.set()

        t = threading.Thread(target=reacquire)
        t.start()
        t.join(timeout=2)
        assert acquired.is_set()

    def test_creates_missing_parent_directory(self, tmp_path):
        with exclusive_cache_lock(tmp_path / "new" / "dir" / ".lock"):
            pass
        assert (tmp_path / "new" / "dir" / ".lock").exists()


class TestLimitedConcurrency:
    """Bounds simultaneous requests to one small server across all workers."""

    def _run(self, tmp_path, slots, threads, hold=0.03):
        inside = 0
        peak = 0
        state = threading.Lock()

        def worker():
            nonlocal inside, peak
            with limited_concurrency(tmp_path, "srv", slots):
                with state:
                    inside += 1
                    peak = max(peak, inside)
                time.sleep(hold)
                with state:
                    inside -= 1

        pool = [threading.Thread(target=worker) for _ in range(threads)]
        for t in pool:
            t.start()
        for t in pool:
            t.join()
        return peak

    def test_never_exceeds_the_slot_count(self, tmp_path):
        assert self._run(tmp_path, slots=3, threads=12) <= 3

    def test_actually_runs_that_many_at_once(self, tmp_path):
        # A limiter that serialised everything would also pass the test above.
        assert self._run(tmp_path, slots=3, threads=12) == 3

    def test_one_slot_is_a_mutex(self, tmp_path):
        assert self._run(tmp_path, slots=1, threads=6) == 1

    def test_slot_is_released_after_an_exception(self, tmp_path):
        with pytest.raises(RuntimeError), limited_concurrency(tmp_path, "srv", 1):
            raise RuntimeError("boom")

        acquired = threading.Event()

        def again():
            with limited_concurrency(tmp_path, "srv", 1):
                acquired.set()

        t = threading.Thread(target=again)
        t.start()
        t.join(timeout=2)
        assert acquired.is_set()

    def test_different_names_do_not_share_slots(self, tmp_path):
        with limited_concurrency(tmp_path, "a", 1):
            done = threading.Event()

            def other():
                with limited_concurrency(tmp_path, "b", 1):
                    done.set()

            t = threading.Thread(target=other)
            t.start()
            t.join(timeout=2)
            assert done.is_set()

    def test_creates_the_lock_directory(self, tmp_path):
        with limited_concurrency(tmp_path / "new" / "dir", "srv", 2):
            pass
        assert (tmp_path / "new" / "dir").is_dir()


class TestExtractIntoCache:
    def test_extracts_zip_and_leaves_no_staging_dir(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.zip"
        _write_zip(archive, {"DAVIS/a.txt": "a", "DAVIS/sub/b.txt": "b"})

        extract_into_cache(archive, cache, sentinel="DAVIS")

        assert (cache / "DAVIS" / "a.txt").read_text() == "a"
        assert (cache / "DAVIS" / "sub" / "b.txt").read_text() == "b"
        assert not list(cache.glob(".extract-*"))

    def test_extracts_tgz(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.tgz"
        _write_tgz(archive, {"v1.0-mini/category.json": "[]", "samples/img.jpg": "x"}, tmp_path)

        extract_into_cache(archive, cache, sentinel="v1.0-mini")

        assert (cache / "v1.0-mini" / "category.json").exists()
        assert (cache / "samples" / "img.jpg").exists()

    def test_sentinel_is_moved_into_place_last(self, tmp_path, monkeypatch):
        # The sentinel is what connectors treat as "cache is complete", so it
        # must not appear before the other top-level entries do.
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.tgz"
        _write_tgz(archive, {"v1.0-mini/category.json": "[]", "samples/img.jpg": "x", "zzz/z.txt": "z"}, tmp_path)

        moved: list[str] = []
        real_replace = Path.replace

        def recording_replace(self, target):
            moved.append(Path(target).name)
            return real_replace(self, target)

        monkeypatch.setattr(Path, "replace", recording_replace)
        extract_into_cache(archive, cache, sentinel="v1.0-mini")

        assert moved[-1] == "v1.0-mini"
        assert set(moved) == {"v1.0-mini", "samples", "zzz"}

    def test_replaces_a_partial_entry_left_by_a_crashed_run(self, tmp_path):
        cache = tmp_path / "cache"
        (cache / "samples").mkdir(parents=True)
        (cache / "samples" / "half_written.jpg").write_text("partial")
        archive = cache / "data.tgz"
        _write_tgz(archive, {"v1.0-mini/category.json": "[]", "samples/img.jpg": "x"}, tmp_path)

        extract_into_cache(archive, cache, sentinel="v1.0-mini")

        assert not (cache / "samples" / "half_written.jpg").exists()
        assert (cache / "samples" / "img.jpg").exists()

    def test_removes_stale_staging_dirs_from_killed_runs(self, tmp_path):
        cache = tmp_path / "cache"
        (cache / ".extract-dead").mkdir(parents=True)
        (cache / ".extract-dead" / "junk").write_text("x")
        archive = cache / "data.zip"
        _write_zip(archive, {"DAVIS/a.txt": "a"})

        extract_into_cache(archive, cache, sentinel="DAVIS")

        assert not (cache / ".extract-dead").exists()

    def test_corrupt_archive_is_deleted_and_reported_as_transient(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.zip"
        archive.write_bytes(b"this is not a zip file")

        with pytest.raises(TransientConnectorError):
            extract_into_cache(archive, cache, sentinel="DAVIS")

        assert not archive.exists()  # so the retry downloads a fresh copy
        assert not (cache / "DAVIS").exists()
        assert not list(cache.glob(".extract-*"))

    def test_archive_without_the_sentinel_is_a_fatal_error(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.zip"
        _write_zip(archive, {"other/a.txt": "a"})

        with pytest.raises(FatalConnectorError):
            extract_into_cache(archive, cache, sentinel="DAVIS")


class TestArchiveSafeguards:
    def test_extract_can_remove_the_archive_afterwards(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.zip"
        _write_zip(archive, {"DAVIS/a.txt": "a"})

        extract_into_cache(archive, cache, sentinel="DAVIS", remove_archive=True)

        assert (cache / "DAVIS" / "a.txt").exists()
        assert not archive.exists()

    def test_archive_is_kept_by_default(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        archive = cache / "data.zip"
        _write_zip(archive, {"DAVIS/a.txt": "a"})

        extract_into_cache(archive, cache, sentinel="DAVIS")

        assert archive.exists()

    def test_refuses_to_start_a_download_that_will_not_fit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(base_mod, "download_with_retry", lambda *a, **kw: pytest.fail("started downloading"))
        monkeypatch.setattr(base_mod.shutil, "disk_usage", lambda path: type("U", (), {"free": 5_000_000_000})())

        with pytest.raises(FatalConnectorError, match="Not enough free disk space"):
            ensure_archive(
                "http://example.com/x.tgz", tmp_path / "x.tgz", label="X", timeout=5,
                required_free_bytes=33_000_000_000,
            )

    def test_a_partial_download_counts_towards_the_space_needed(self, tmp_path, monkeypatch):
        # Resuming with 4KB already on disk only needs the rest of the space.
        (tmp_path / "x.tgz.tmp").write_bytes(b"\0" * 4_000)
        calls = []
        monkeypatch.setattr(base_mod.shutil, "disk_usage", lambda path: type("U", (), {"free": 5_000})())
        monkeypatch.setattr(base_mod, "download_with_retry", lambda url, tmp, **kw: calls.append(url))

        # 5_000 free + 4_000 already downloaded covers 9_000 ...
        ensure_archive("http://example.com/x.tgz", tmp_path / "x.tgz", label="X", timeout=5, required_free_bytes=9_000)
        assert calls == ["http://example.com/x.tgz"]

        # ... but not 9_001.
        (tmp_path / "y.tgz.tmp").write_bytes(b"\0" * 4_000)
        with pytest.raises(FatalConnectorError):
            ensure_archive(
                "http://example.com/y.tgz", tmp_path / "y.tgz", label="Y", timeout=5, required_free_bytes=9_001
            )

    def test_no_disk_check_when_none_is_requested(self, tmp_path, monkeypatch):
        monkeypatch.setattr(base_mod.shutil, "disk_usage", lambda path: pytest.fail("checked the disk"))
        monkeypatch.setattr(base_mod, "download_with_retry", lambda url, tmp, **kw: _write_zip(tmp, {"X/a.txt": "a"}))

        ensure_archive("http://example.com/x.zip", tmp_path / "x.zip", label="X", timeout=5)

        assert (tmp_path / "x.zip").exists()


class TestEnsureArchive:
    def _patch_download(self, monkeypatch, members):
        calls = []

        def fake_download(url, tmp_path, *, timeout, headers):
            calls.append(url)
            _write_zip(tmp_path, members)

        monkeypatch.setattr(base_mod, "download_with_retry", fake_download)
        return calls

    def test_downloads_when_missing_and_renames_into_place(self, tmp_path, monkeypatch):
        calls = self._patch_download(monkeypatch, {"X/a.txt": "a"})
        archive = tmp_path / "x.zip"

        ensure_archive("http://example.com/x.zip", archive, label="X", timeout=5, validate=zipfile.is_zipfile)

        assert calls == ["http://example.com/x.zip"]
        assert zipfile.is_zipfile(archive)
        assert not (tmp_path / "x.zip.tmp").exists()

    def test_valid_cached_archive_is_not_downloaded_again(self, tmp_path, monkeypatch):
        calls = self._patch_download(monkeypatch, {"X/a.txt": "a"})
        archive = tmp_path / "x.zip"
        _write_zip(archive, {"X/old.txt": "old"})

        ensure_archive("http://example.com/x.zip", archive, label="X", timeout=5, validate=zipfile.is_zipfile)

        assert calls == []

    def test_corrupt_cached_archive_is_discarded_and_redownloaded(self, tmp_path, monkeypatch):
        # The production state: an unreadable SUNRGBD.zip already on disk.
        calls = self._patch_download(monkeypatch, {"X/a.txt": "a"})
        archive = tmp_path / "x.zip"
        archive.write_bytes(b"corrupt " * 1000)

        ensure_archive("http://example.com/x.zip", archive, label="X", timeout=5, validate=zipfile.is_zipfile)

        assert len(calls) == 1
        assert zipfile.is_zipfile(archive)

    def test_corrupt_download_is_dropped_and_raises_transient(self, tmp_path, monkeypatch):
        def bad_download(url, tmp_path, *, timeout, headers):
            tmp_path.write_bytes(b"truncated")

        monkeypatch.setattr(base_mod, "download_with_retry", bad_download)
        archive = tmp_path / "x.zip"

        with pytest.raises(TransientConnectorError):
            ensure_archive("http://example.com/x.zip", archive, label="X", timeout=5, validate=zipfile.is_zipfile)

        assert not archive.exists()
        assert not (tmp_path / "x.zip.tmp").exists()  # retry must start from byte zero, not resume junk

    def test_http_error_becomes_fatal(self, tmp_path, monkeypatch):
        def refused(url, tmp_path, *, timeout, headers):
            raise requests.HTTPError("404")

        monkeypatch.setattr(base_mod, "download_with_retry", refused)

        with pytest.raises(FatalConnectorError):
            ensure_archive("http://example.com/x.zip", tmp_path / "x.zip", label="X", timeout=5)

    def test_without_validate_an_existing_archive_is_trusted(self, tmp_path, monkeypatch):
        calls = self._patch_download(monkeypatch, {"X/a.txt": "a"})
        archive = tmp_path / "x.tgz"
        archive.write_bytes(b"whatever")

        ensure_archive("http://example.com/x.tgz", archive, label="X", timeout=5)

        assert calls == []
        assert archive.read_bytes() == b"whatever"


class TestSunRgbdEnsureImageIndex:
    """SUN RGB-D no longer downloads its 6.9GB archive; the member index is
    built once (from the zip's central directory) under the same lock."""

    def _patch(self, monkeypatch, tmp_path, delay: float = 0.0):
        cache = tmp_path / "sun_rgbd"
        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: cache)
        builds = []
        builds_lock = threading.Lock()

        def fake_build(url, keep, *, headers):
            with builds_lock:
                builds.append(url)
            time.sleep(delay)
            assert keep("SUNRGBD/kv2/seq1/image/0001.jpg") and not keep("SUNRGBD/kv2/seq1/depth/0001.png")
            return {"SUNRGBD/kv2/seq1/image/0001.jpg": [100, 10, 10, 8, 1]}

        monkeypatch.setattr(sun_mod.remote_zip, "build_member_index", fake_build)
        return cache, builds

    def test_concurrent_shards_build_the_index_exactly_once(self, tmp_path, monkeypatch):
        cache, builds = self._patch(monkeypatch, tmp_path, delay=0.05)
        results: list[dict] = []
        errors: list[Exception] = []

        def shard():
            try:
                results.append(sun_mod._ensure_image_index())
            except Exception as e:  # noqa: BLE001 - collected and asserted on below
                errors.append(e)

        threads = [threading.Thread(target=shard) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(builds) == 1
        assert len(results) == 8 and all(r == results[0] for r in results)
        assert (cache / "image_index.json").exists()
        assert not (cache / "image_index.json.tmp").exists()

    def test_cached_index_is_reused(self, tmp_path, monkeypatch):
        _, builds = self._patch(monkeypatch, tmp_path)

        first = sun_mod._ensure_image_index()
        second = sun_mod._ensure_image_index()

        assert first == second and len(builds) == 1

    def test_a_failed_build_leaves_nothing_cached(self, tmp_path, monkeypatch):
        cache = tmp_path / "sun_rgbd"
        monkeypatch.setattr(sun_mod, "_cache_dir", lambda: cache)

        def boom(url, keep, *, headers):
            raise requests.ConnectionError("reset")

        monkeypatch.setattr(sun_mod.remote_zip, "build_member_index", boom)

        with pytest.raises(requests.ConnectionError):
            sun_mod._ensure_image_index()

        assert not (cache / "image_index.json").exists()
