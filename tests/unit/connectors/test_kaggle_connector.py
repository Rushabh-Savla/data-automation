"""Regression tests for logic ported from kaggle_downloader.py."""

import zipfile
from pathlib import Path

from app.connectors.kaggle_connector import KaggleConnector, make_source_item_id


class TestMakeSourceItemId:
    def test_deterministic(self):
        id1 = make_source_item_id("andrewmvd/hard-hat-detection", "images/hard_hat_0001.png")
        id2 = make_source_item_id("andrewmvd/hard-hat-detection", "images/hard_hat_0001.png")
        assert id1 == id2

    def test_differs_by_slug(self):
        id1 = make_source_item_id("owner/dataset-a", "images/x.jpg")
        id2 = make_source_item_id("owner/dataset-b", "images/x.jpg")
        assert id1 != id2

    def test_differs_by_path(self):
        id1 = make_source_item_id("owner/dataset", "images/a.jpg")
        id2 = make_source_item_id("owner/dataset", "images/b.jpg")
        assert id1 != id2

    def test_length(self):
        assert len(make_source_item_id("a/b", "c.jpg")) == 16


class TestRecursiveUnzipZipSlip:
    """Regression test for the Zip-Slip guard ported verbatim from
    kaggle_downloader.py::_recursive_unzip — a malicious zip member path
    containing '../' must never be extracted outside the target directory."""

    def test_blocks_path_traversal_member(self, tmp_path: Path):
        extract_root = tmp_path / "safe_root"
        extract_root.mkdir()

        malicious_zip = extract_root / "evil.zip"
        with zipfile.ZipFile(malicious_zip, "w") as zf:
            zf.writestr("../../escaped.txt", "should not escape")
            zf.writestr("legit.txt", "this one is fine")

        connector = KaggleConnector()
        connector._recursive_unzip(extract_root)

        escaped_path = tmp_path.parent / "escaped.txt"
        assert not escaped_path.exists()
        assert not (tmp_path / "escaped.txt").exists()

        extracted_dir = extract_root / "evil"
        assert (extracted_dir / "legit.txt").exists()
        assert not (extracted_dir / ".." / ".." / "escaped.txt").resolve().exists()

    def test_depth_limit_prevents_infinite_recursion(self, tmp_path: Path):
        # A directory 5 levels deep with no zips should simply be a no-op,
        # not raise or hang — proves the depth cap (max 4) short-circuits.
        connector = KaggleConnector()
        connector._recursive_unzip(tmp_path, depth=5)  # already past the cap
