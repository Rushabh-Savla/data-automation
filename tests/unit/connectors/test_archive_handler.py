import io
import tarfile
import zipfile
from pathlib import Path
from PIL import Image
import pytest

from app.connectors.archive_handler import is_archive, safe_extract_archive, discover_valid_images


def test_is_archive_detection(tmp_path: Path):
    z = tmp_path / "test.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("a.txt", "data")
    assert is_archive(z) is True

    t = tmp_path / "test.tar.gz"
    with tarfile.open(t, "w:gz") as tf:
        info = tarfile.TarInfo(name="b.txt")
        info.size = 4
        tf.addfile(info, io.BytesIO(b"data"))
    assert is_archive(t) is True

    txt = tmp_path / "test.txt"
    txt.write_text("plain text")
    assert is_archive(txt) is False


def test_safe_extract_archive_basic(tmp_path: Path):
    z = tmp_path / "bundle.zip"
    dest = tmp_path / "extracted"

    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("sub/item.txt", "hello")
        zf.writestr("__MACOSX/._item.txt", "garbage")

    extracted_dir = safe_extract_archive(z, dest, remove_archive=False)
    assert extracted_dir == dest
    assert (dest / "sub" / "item.txt").exists()
    assert (dest / "sub" / "item.txt").read_text() == "hello"
    assert not (dest / "__MACOSX").exists()


def test_safe_extract_archive_zip_slip_prevention(tmp_path: Path):
    z = tmp_path / "malicious.zip"
    dest = tmp_path / "safe_dest"
    dest.mkdir()

    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("../../evil.txt", "evil")

    extracted_dir = safe_extract_archive(z, dest, remove_archive=False)
    # The zip-slip file must be rejected and not written outside dest
    assert not (tmp_path / "evil.txt").exists()
    assert not (dest / "evil.txt").exists()


def test_discover_valid_images_nested(tmp_path: Path):
    root = tmp_path / "images_root"
    sub = root / "train" / "images"
    sub.mkdir(parents=True)

    img_path = sub / "valid.jpg"
    im = Image.new("RGB", (200, 200), color="blue")
    im.save(img_path)

    corrupt_path = sub / "corrupt.jpg"
    corrupt_path.write_bytes(b"not an image")

    discovered = list(discover_valid_images(root, min_resolution=(100, 100)))
    assert len(discovered) == 1
    p, w, h = discovered[0]
    assert p == img_path
    assert (w, h) == (200, 200)
