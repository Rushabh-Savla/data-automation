"""NAS adapter behaviour, exercised through LocalClient (a real directory), which
runs the shared NasClient logic — walk, safe paths, atomic download — that every
protocol adapter inherits."""

import pytest

from app.connectors.nas import (
    IMAGE_EXTENSIONS,
    NasConnectionParams,
    NasNotFoundError,
    NasPathError,
    normalize_relpath,
    open_client,
)


@pytest.fixture
def share(tmp_path):
    root = tmp_path / "nas"
    (root / "cars").mkdir(parents=True)
    (root / "cars" / "a.jpg").write_bytes(b"aaaa")
    (root / "cars" / "b.PNG").write_bytes(b"bb")
    (root / "cars" / "notes.txt").write_text("skip me")
    (root / "@eaDir").mkdir()
    (root / "@eaDir" / "thumb.jpg").write_bytes(b"t")
    (root / "#recycle").mkdir()
    (root / "#recycle" / "old.jpg").write_bytes(b"o")
    (root / "top.jpeg").write_bytes(b"cccccc")
    return root


def _client(root):
    return open_client(NasConnectionParams(protocol="local", share=str(root)))


class TestNormalizeRelpath:
    def test_cleans_slashes_and_dots(self):
        assert normalize_relpath("/a//b/./c/") == "a/b/c"
        assert normalize_relpath("a\\b") == "a/b"
        assert normalize_relpath("") == ""

    @pytest.mark.parametrize("bad", ["../etc", "a/../../b", "..", "a/.."])
    def test_rejects_parent_segments(self, bad):
        with pytest.raises(NasPathError):
            normalize_relpath(bad)


class TestWalk:
    def test_yields_only_images_and_skips_nas_housekeeping_dirs(self, share):
        with _client(share) as c:
            found = [f.path for f in c.walk("", True, IMAGE_EXTENSIONS)]
        # a folder's own files first, then its subfolders; sorted within each
        assert found == ["top.jpeg", "cars/a.jpg", "cars/b.PNG"]

    @pytest.mark.parametrize(
        "bin_name", ["@eaDir", "#recycle", "@Recycle", "$RECYCLE.BIN", ".recycle", ".deleted", ".Trash", ".Trash-1000"]
    )
    def test_recycle_bins_and_thumbnail_folders_are_never_offered_as_images(self, tmp_path, bin_name):
        root = tmp_path / "nas"
        (root / bin_name / "old" / "deep").mkdir(parents=True)
        (root / bin_name / "old" / "deep" / "deleted.jpg").write_bytes(b"x")
        (root / "keep.jpg").write_bytes(b"k")
        with _client(root) as c:
            assert [f.path for f in c.walk("", True, IMAGE_EXTENSIONS)] == ["keep.jpg"]

    def test_order_is_stable_across_walks(self, share):
        with _client(share) as c:
            first = [f.path for f in c.walk("", True, IMAGE_EXTENSIONS)]
            second = [f.path for f in c.walk("", True, IMAGE_EXTENSIONS)]
        assert first == second

    def test_non_recursive_stays_in_the_folder(self, share):
        with _client(share) as c:
            assert [f.path for f in c.walk("", False, IMAGE_EXTENSIONS)] == ["top.jpeg"]

    def test_max_files_stops_early(self, share):
        with _client(share) as c:
            assert len(list(c.walk("", True, IMAGE_EXTENSIONS, max_files=2))) == 2

    def test_max_dirs_bounds_the_scan(self, share):
        with _client(share) as c:
            # only the root listing is allowed, so the subfolder's files are never reached
            assert [f.path for f in c.walk("", True, IMAGE_EXTENSIONS, max_dirs=1)] == ["top.jpeg"]

    def test_reports_size(self, share):
        with _client(share) as c:
            sizes = {f.path: f.size for f in c.walk("", True, IMAGE_EXTENSIONS)}
        assert sizes["cars/a.jpg"] == 4


class TestTransfers:
    def test_download_copies_bytes_and_leaves_no_part_file(self, share, tmp_path):
        dest = tmp_path / "out" / "a.jpg"
        with _client(share) as c:
            c.download("cars/a.jpg", dest)
        assert dest.read_bytes() == b"aaaa"
        assert not list(dest.parent.glob("*.part"))

    def test_download_missing_file_raises_not_found_and_leaves_nothing(self, share, tmp_path):
        dest = tmp_path / "out" / "x.jpg"
        with _client(share) as c, pytest.raises(NasNotFoundError):
            c.download("cars/nope.jpg", dest)
        assert not dest.exists() and not list(dest.parent.glob("*.part"))

    def test_upload_creates_parent_folders_and_size_matches(self, share, tmp_path):
        src = tmp_path / "src.jpg"
        src.write_bytes(b"12345")
        with _client(share) as c:
            c.upload(src, "data_automation/job1/coco/images/x.jpg")
            assert c.size("data_automation/job1/coco/images/x.jpg") == 5
            assert c.exists("data_automation/job1/coco/images/x.jpg")

    def test_upload_leaves_no_part_file_behind_on_success(self, share, tmp_path):
        src = tmp_path / "src.jpg"
        src.write_bytes(b"12345")
        with _client(share) as c:
            c.upload(src, "data_automation/job1/coco/images/x.jpg")
        assert not list((share / "data_automation/job1/coco/images").glob("*.part"))

    def test_upload_overwrites_an_existing_file_atomically(self, share, tmp_path):
        src = tmp_path / "src.jpg"
        src.write_bytes(b"newbytes")
        with _client(share) as c:
            c.upload(src, "top.jpeg")  # already exists in the fixture as b"cccccc"
            assert c.size("top.jpeg") == len(b"newbytes")

    def test_a_transfer_that_dies_mid_write_leaves_nothing_at_the_final_path(self, share, tmp_path, monkeypatch):
        # Simulate a weak NAS dropping the connection partway through the data
        # transfer (after _mkdirs/the directory already exists, before the
        # file is ever fully written) — this is the exact bug reported: a
        # directory left behind with no (or a corrupt) file in it.
        src = tmp_path / "src.jpg"
        src.write_bytes(b"12345")
        import app.connectors.nas.local_client as local_client_module

        real_upload = local_client_module.LocalClient._upload

        def dying_upload(self, local, rel):
            real_upload(self, local, rel)  # the temp file gets written...
            raise ConnectionError("NAS dropped the connection")  # ...but we never get to rename it into place

        monkeypatch.setattr(local_client_module.LocalClient, "_upload", dying_upload)
        with _client(share) as c, pytest.raises(ConnectionError):
            c.upload(src, "data_automation/job1/coco/images/x.jpg")
        final = share / "data_automation/job1/coco/images/x.jpg"
        assert not final.exists(), "a failed transfer must never leave a file at the final path"
        assert not list(final.parent.glob("*.part")), "the temp file must be cleaned up too"

    def test_delete_is_idempotent(self, share):
        with _client(share) as c:
            c.delete("top.jpeg")
            c.delete("top.jpeg")
            assert not c.exists("top.jpeg")
            assert c.size("top.jpeg") is None


class TestPathSafety:
    def test_symlink_pointing_outside_the_root_is_refused(self, share, tmp_path):
        outside = tmp_path / "secret.txt"
        outside.write_text("secret")
        (share / "link.jpg").symlink_to(outside)
        with _client(share) as c, pytest.raises(NasPathError):
            c.download("link.jpg", tmp_path / "stolen.jpg")

    def test_sibling_folder_with_shared_prefix_is_not_inside_the_root(self, tmp_path):
        root = tmp_path / "nas"
        root.mkdir()
        (tmp_path / "nas2").mkdir()
        (tmp_path / "nas2" / "x.jpg").write_bytes(b"x")
        (root / "hop").symlink_to(tmp_path / "nas2")
        with _client(root) as c, pytest.raises(NasPathError):
            c.exists("hop/x.jpg")

    def test_missing_mount_is_a_clear_error(self, tmp_path):
        with pytest.raises(NasNotFoundError):
            _client(tmp_path / "does-not-exist").connect()

    def test_blank_mount_path_is_a_clear_error(self):
        with pytest.raises(NasNotFoundError):
            open_client(NasConnectionParams(protocol="local", share="")).connect()


class TestParams:
    def test_password_never_appears_in_repr(self):
        p = NasConnectionParams(protocol="smb", host="nas", share="s", auth_mode="password", username="u", password="hunter2")
        assert "hunter2" not in repr(p)

    def test_redact_strips_the_password_from_messages(self):
        p = NasConnectionParams(protocol="smb", password="hunter2")
        assert p.redact("login failed for hunter2 on host") == "login failed for *** on host"

    def test_redact_with_no_password_leaves_text_alone(self):
        assert NasConnectionParams(protocol="smb").redact("hello") == "hello"

    def test_nfs_and_local_never_use_a_password(self):
        assert not NasConnectionParams(protocol="nfs", auth_mode="password").uses_password
        assert not NasConnectionParams(protocol="local", auth_mode="password").uses_password
        assert NasConnectionParams(protocol="smb", auth_mode="password").uses_password
        assert not NasConnectionParams(protocol="smb", auth_mode="anonymous").uses_password


def test_nfs_without_the_optional_library_explains_the_alternative():
    from app.connectors.nas import NasUnavailableError

    params = NasConnectionParams(protocol="nfs", host="10.0.0.5", share="/export/photos")
    try:
        import libnfs  # noqa: F401
    except ImportError:
        with pytest.raises(NasUnavailableError, match="Mounted path"):
            open_client(params).connect()
    else:
        pytest.skip("libnfs is installed")
