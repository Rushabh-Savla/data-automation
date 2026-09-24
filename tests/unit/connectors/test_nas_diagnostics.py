import os

import pytest
from PIL import Image

from app.connectors.nas import NasConnectionParams
from app.connectors.nas.diagnostics import run_connection_test


def _params(root, **kw):
    return NasConnectionParams(protocol="local", share=str(root), **kw)


@pytest.fixture
def share(tmp_path):
    root = tmp_path / "nas"
    (root / "photos").mkdir(parents=True)
    Image.new("RGB", (80, 80)).save(root / "photos" / "one.jpg")
    return root


class TestConnectionTest:
    def test_healthy_share_reads_and_writes(self, share):
        out = run_connection_test(_params(share, base_path="photos"), require_write=True)
        assert out.ok and out.can_read and out.can_write
        assert out.sample_files == ["photos/one.jpg"]
        assert out.latency_ms is not None
        assert "read and written" in out.message

    def test_leaves_no_probe_file_behind(self, share):
        run_connection_test(_params(share), require_write=True)
        leftovers = [p for p in share.rglob(".data_automation_probe_*")]
        assert leftovers == []

    def test_creates_the_storage_folder_it_probes(self, share):
        run_connection_test(_params(share, storage_path="filtered"), require_write=True)
        assert (share / "filtered").is_dir()

    def test_missing_mount_fails_with_a_message(self, tmp_path):
        out = run_connection_test(_params(tmp_path / "gone"), require_write=True)
        assert not out.ok and "does not exist" in out.message

    def test_missing_source_folder_is_only_a_note_when_the_nas_is_used_for_storage(self, share):
        out = run_connection_test(_params(share, base_path="typo"), require_write=True)
        assert out.ok and out.can_write and not out.can_read
        assert "typo" in out.message and "not found" in out.message

    def test_missing_source_folder_is_reported_but_names_the_folder(self, share):
        out = run_connection_test(_params(share, base_path="nope"), require_write=False)
        assert not out.ok and not out.can_read and "nope" in out.message

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_read_only_share_fails_when_storage_is_wanted_and_passes_when_not(self, share):
        share.chmod(0o555)
        try:
            wanted = run_connection_test(_params(share, base_path="photos"), require_write=True)
            not_wanted = run_connection_test(_params(share, base_path="photos"), require_write=False)
        finally:
            share.chmod(0o755)
        assert wanted.can_read and not wanted.can_write and not wanted.ok
        assert "cannot write" in wanted.message
        assert not_wanted.ok and not not_wanted.can_write

    def test_empty_source_folder_is_still_ok_but_says_so(self, tmp_path):
        root = tmp_path / "empty"
        root.mkdir()
        out = run_connection_test(_params(root), require_write=True)
        assert out.ok and "no images" in out.message
