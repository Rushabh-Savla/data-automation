"""Archive handling and extraction utility for all data connectors.

Provides safe, recursive extraction of archives (.zip, .tar, .tar.gz, .tgz,
.tar.bz2, .tbz2, .tar.xz, .txz, .gz) with Zip-Slip/Tar-Slip protection,
disk-space preservation, corrupt-file tolerance, and recursive discovery
of valid images across deeply nested directories (train, test, valid, etc.).
"""

from __future__ import annotations

import logging
import os
import shutil
import tarfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

from PIL import Image

logger = logging.getLogger(__name__)

SUPPORTED_IMAGE_EXTENSIONS = frozenset(
    {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff", ".tif"}
)

ARCHIVE_EXTENSIONS = frozenset(
    {
        ".zip",
        ".tar",
        ".tar.gz",
        ".tgz",
        ".tar.bz2",
        ".tbz2",
        ".tar.xz",
        ".txz",
        ".gz",
        ".bz2",
        ".xz",
    }
)

# Archive magic byte signatures
MAGIC_ZIP = b"PK\x03\x04"
MAGIC_GZIP = b"\x1f\x8b"
MAGIC_BZIP2 = b"BZh"
MAGIC_XZ = b"\xfd7zXZ\x00"


def is_archive(path: Path) -> bool:
    """Returns True if path points to a supported archive file by extension or magic bytes."""
    if not path.is_file():
        return False

    name_lower = path.name.lower()
    for ext in ARCHIVE_EXTENSIONS:
        if name_lower.endswith(ext):
            return True

    # Check magic bytes for archives that may have missing or generic extensions
    try:
        with open(path, "rb") as f:
            header = f.read(8)
            if header.startswith(MAGIC_ZIP) or header.startswith(MAGIC_GZIP) or header.startswith(MAGIC_BZIP2) or header.startswith(MAGIC_XZ):
                return True
            if len(header) >= 2 and header[:2] == b"\x1f\x8b":
                return True
            # Check for POSIX tar magic 'ustar' at offset 257
            f.seek(257)
            if f.read(5) == b"ustar":
                return True
    except Exception:
        pass

    return False


def is_safe_path(target_dir: Path, path: Path) -> bool:
    """Ensures that extracting path does not escape target_dir (Zip-Slip/Tar-Slip protection)."""
    try:
        target_abs = target_dir.resolve()
        dest_abs = path.resolve()
        return dest_abs == target_abs or target_abs in dest_abs.parents
    except Exception:
        return False


def safe_extract_archive(
    archive_path: Path,
    target_dir: Path | None = None,
    *,
    remove_archive: bool = False,
    max_depth: int = 5,
    current_depth: int = 0,
) -> Path:
    """Safely extracts a ZIP or TAR archive into target_dir.

    Features:
    - Zip-Slip and Tar-Slip protection preventing extraction outside target_dir.
    - Recursive unpacking of nested archives up to max_depth.
    - Graceful handling of corrupted/truncated archives.
    - Optional automatic removal of source archive to preserve disk space.
    """
    if target_dir is None:
        target_dir = archive_path.parent / f"{archive_path.stem}_extracted"

    target_dir.mkdir(parents=True, exist_ok=True)

    extracted_any = False

    # 1. Try ZIP extraction
    if zipfile.is_zipfile(archive_path):
        try:
            with zipfile.ZipFile(archive_path, "r") as zf:
                for member in zf.infolist():
                    # Skip macOS resource fork metadata
                    if member.filename.startswith("__MACOSX/") or "/__MACOSX/" in member.filename:
                        continue
                    if os.path.basename(member.filename).startswith("._"):
                        continue

                    dest = target_dir / member.filename
                    if not is_safe_path(target_dir, dest):
                        logger.warning(
                            f"[ARCHIVE] Blocked Zip-Slip attempt for {member.filename} in {archive_path.name}"
                        )
                        continue

                    zf.extract(member, target_dir)
            extracted_any = True
        except (zipfile.BadZipFile, EOFError) as e:
            logger.warning(f"[ARCHIVE] Corrupt or truncated ZIP file {archive_path.name}: {e}")
        except Exception as e:
            logger.error(f"[ARCHIVE] Failed to extract ZIP {archive_path.name}: {e}")

    # 2. Try TAR extraction (.tar, .tar.gz, .tgz, .tar.bz2, etc.)
    elif tarfile.is_tarfile(archive_path):
        try:
            with tarfile.open(archive_path, "r:*") as tf:
                for member in tf.getmembers():
                    if member.name.startswith("__MACOSX/") or "/__MACOSX/" in member.name:
                        continue
                    if os.path.basename(member.name).startswith("._"):
                        continue

                    dest = target_dir / member.name
                    if not is_safe_path(target_dir, dest):
                        logger.warning(
                            f"[ARCHIVE] Blocked Tar-Slip attempt for {member.name} in {archive_path.name}"
                        )
                        continue

                    # Extract single member safely
                    tf.extract(member, target_dir, filter="data" if hasattr(tarfile, "data_filter") else None)
            extracted_any = True
        except (tarfile.TarError, EOFError) as e:
            logger.warning(f"[ARCHIVE] Corrupt or truncated TAR file {archive_path.name}: {e}")
        except Exception as e:
            logger.error(f"[ARCHIVE] Failed to extract TAR {archive_path.name}: {e}")

    if extracted_any and remove_archive:
        try:
            archive_path.unlink(missing_ok=True)
            logger.debug(f"[ARCHIVE] Cleaned up extracted archive: {archive_path.name}")
        except Exception as e:
            logger.warning(f"[ARCHIVE] Could not remove archive {archive_path.name}: {e}")

    # 3. Recursively extract any nested archives discovered inside target_dir
    if current_depth < max_depth:
        for nested in list(target_dir.rglob("*")):
            if nested.is_file() and is_archive(nested) and nested != archive_path:
                nested_target = nested.parent / f"{nested.stem}_extracted"
                safe_extract_archive(
                    nested,
                    nested_target,
                    remove_archive=True,
                    max_depth=max_depth,
                    current_depth=current_depth + 1,
                )

    return target_dir


def discover_valid_images(
    root_dir: Path,
    min_resolution: tuple[int, int] = (64, 64),
    extract_archives_first: bool = True,
) -> Iterator[tuple[Path, int, int]]:
    """Recursively walks root_dir, extracting any archives found, and yields
    all valid, uncorrupted images with dimensions meeting min_resolution.

    Yields:
        (image_file_path, width, height)
    """
    if not root_dir.exists():
        return

    min_w, min_h = min_resolution

    if extract_archives_first:
        # Check if root_dir itself is an archive
        if root_dir.is_file() and is_archive(root_dir):
            extracted = safe_extract_archive(root_dir, remove_archive=False)
            yield from discover_valid_images(extracted, min_resolution, extract_archives_first=False)
            return

        # Extract any nested archives inside directory
        for arch in list(root_dir.rglob("*")):
            if arch.is_file() and is_archive(arch):
                safe_extract_archive(arch, remove_archive=True)

    for file_path in sorted(root_dir.rglob("*")):
        if not file_path.is_file():
            continue

        # Skip hidden files and macOS metadata
        if file_path.name.startswith(".") or file_path.name.startswith("._") or "__MACOSX" in file_path.parts:
            continue

        if file_path.suffix.lower() not in SUPPORTED_IMAGE_EXTENSIONS:
            continue

        try:
            with Image.open(file_path) as img:
                img.verify()
            with Image.open(file_path) as img:
                w, h = img.size

            if w < min_w or h < min_h:
                continue

            yield file_path, w, h

        except Exception as e:
            logger.debug(f"[ARCHIVE DISCOVERY] Skipping invalid/corrupt image {file_path.name}: {e}")
            continue

