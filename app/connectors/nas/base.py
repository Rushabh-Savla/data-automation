"""Shared NAS client interface, connection parameters and path safety.

Path convention used by every adapter: callers pass POSIX-style paths that are
*relative to the configured root* ("" is the root itself). What "root" means is
protocol-specific and lives in NasConnectionParams.share (SMB share name, SFTP/FTP
base directory, WebDAV path prefix, NFS export, or the mounted directory for
"local"). Adapters translate to their native form; nothing above them ever
builds a protocol-specific path.
"""

from __future__ import annotations

import os
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from app.core.exceptions import FatalConnectorError, TransientConnectorError

PROTOCOLS = ("smb", "sftp", "ftp", "webdav", "nfs", "local")

IMAGE_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"})

# Directories NAS firmwares create beside user files. Walking into them would
# feed thumbnails and recycle-bin copies of the same photos into the pipeline.
_SKIPPED_DIR_NAMES = frozenset(
    {
        "@eaDir",  # Synology thumbnails
        "#recycle",  # Synology recycle bin
        "@Recycle",  # QNAP recycle bin
        "$RECYCLE.BIN",  # Windows
        ".recycle",  # Samba (vfs recycle) default
        ".deleted",  # Samba recycle bin as configured by common Docker images
        ".@__thumb",
        "System Volume Information",
        ".AppleDouble",
    }
)


def _is_skipped_dir(name: str) -> bool:
    # ".Trash", ".Trash-1000", ... — freedesktop trash folders are per-user named.
    return name in _SKIPPED_DIR_NAMES or name.startswith(".Trash")

CONNECT_TIMEOUT_S = 15
IO_TIMEOUT_S = 60
_MAX_WALK_DEPTH = 12


class NasAuthError(FatalConnectorError):
    """Wrong or missing credentials — retrying cannot help."""


class NasNotFoundError(FatalConnectorError):
    """A path (or the share itself) does not exist on the NAS."""


class NasPathError(FatalConnectorError):
    """A path tried to escape the configured root (or was otherwise unsafe)."""


class NasUnavailableError(FatalConnectorError):
    """The protocol's optional client library is not installed."""


class NasConnectionError(TransientConnectorError):
    """Host unreachable, timed out or dropped the connection — worth retrying."""


@dataclass(frozen=True)
class NasConnectionParams:
    protocol: str
    host: str = ""
    port: int | None = None
    share: str = ""
    base_path: str = ""
    storage_path: str = "data_automation"
    auth_mode: str = "anonymous"
    username: str = ""
    domain: str = ""
    # repr=False: the password must never show up in a log line or traceback.
    password: str = field(default="", repr=False)
    use_tls: bool = False
    verify_tls: bool = True

    @property
    def uses_password(self) -> bool:
        return self.auth_mode == "password" and self.protocol not in ("nfs", "local")

    def redact(self, text: str) -> str:
        """Strip the password out of any message before it leaves the process."""
        return text.replace(self.password, "***") if self.password else text


@dataclass(frozen=True)
class RemoteEntry:
    name: str
    is_dir: bool
    size: int = 0
    mtime: float = 0.0


@dataclass(frozen=True)
class RemoteFile:
    path: str  # relative to the configured root, POSIX separators
    size: int = 0
    mtime: float = 0.0


def normalize_relpath(path: str) -> str:
    """Canonicalise a user/config-supplied path to a safe root-relative POSIX
    path ("" for the root). Rejects '..' outright rather than trying to resolve
    it — a NAS path that needs '..' is a mistake or an escape attempt."""
    parts: list[str] = []
    for segment in path.replace("\\", "/").split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            raise NasPathError(f"Path '{path}' must not contain '..'")
        if "\x00" in segment:
            raise NasPathError("Path contains a NUL byte")
        parts.append(segment)
    return "/".join(parts)


def join_relpath(*parts: str) -> str:
    return normalize_relpath("/".join(p for p in parts if p))


class NasClient(ABC):
    """One connection to the NAS. Use as a context manager (connects on enter)."""

    def __init__(self, params: NasConnectionParams):
        self.params = params

    # -- adapter primitives ----------------------------------------------------
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def list_dir(self, rel: str) -> list[RemoteEntry]: ...

    @abstractmethod
    def _download(self, rel: str, local: Path) -> None: ...

    @abstractmethod
    def _upload(self, local: Path, rel: str) -> None:
        """Write `local` to `rel`, creating any missing parent directories."""

    @abstractmethod
    def delete(self, rel: str) -> None:
        """Remove a file. Missing is not an error (deletes are idempotent)."""

    @abstractmethod
    def exists(self, rel: str) -> bool: ...

    @abstractmethod
    def size(self, rel: str) -> int | None:
        """Size in bytes, or None if the file does not exist."""

    # -- shared behaviour ------------------------------------------------------
    def download(self, rel: str, local: Path) -> None:
        """Download via a .part file so an interrupted transfer never leaves a
        truncated image at the final path."""
        rel = normalize_relpath(rel)
        local = Path(local)
        local.parent.mkdir(parents=True, exist_ok=True)
        # Unique per call: two workers fetching the same file must not share a temp file.
        tmp = local.with_name(f"{local.name}.{uuid.uuid4().hex[:8]}.part")
        try:
            self._download(rel, tmp)
            os.replace(tmp, local)
        finally:
            tmp.unlink(missing_ok=True)

    def upload(self, local: Path, rel: str) -> None:
        self._upload(Path(local), normalize_relpath(rel))

    def walk(
        self,
        rel: str = "",
        recursive: bool = True,
        extensions: frozenset[str] | None = None,
        max_files: int | None = None,
        max_dirs: int | None = None,
    ) -> Iterator[RemoteFile]:
        """Yield files under `rel` in a deterministic (sorted) order. NAS
        housekeeping directories are skipped; depth is capped so a symlink loop
        on the server cannot spin forever, and `max_dirs` bounds how many
        directories are listed so a quick probe of a huge share stays quick."""
        stack: list[tuple[str, int]] = [(normalize_relpath(rel), 0)]
        yielded = 0
        listed = 0
        while stack:
            if max_dirs is not None and listed >= max_dirs:
                return
            current, depth = stack.pop()
            listed += 1
            entries = sorted(self.list_dir(current), key=lambda e: e.name)
            subdirs: list[tuple[str, int]] = []
            for entry in entries:
                child = f"{current}/{entry.name}" if current else entry.name
                if entry.is_dir:
                    if recursive and depth < _MAX_WALK_DEPTH and not _is_skipped_dir(entry.name):
                        subdirs.append((child, depth + 1))
                    continue
                if extensions is not None and Path(entry.name).suffix.lower() not in extensions:
                    continue
                yield RemoteFile(path=child, size=entry.size, mtime=entry.mtime)
                yielded += 1
                if max_files is not None and yielded >= max_files:
                    return
            # reversed so the stack pops directories back in sorted order
            stack.extend(reversed(subdirs))

    def __enter__(self) -> "NasClient":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def open_client(params: NasConnectionParams) -> NasClient:
    """Instantiate the adapter for params.protocol. Imports are lazy so a
    missing optional library only affects the protocol that needs it."""
    protocol = params.protocol
    if protocol == "local":
        from app.connectors.nas.local_client import LocalClient

        return LocalClient(params)
    if protocol == "smb":
        from app.connectors.nas.smb_client import SmbClient

        return SmbClient(params)
    if protocol == "sftp":
        from app.connectors.nas.sftp_client import SftpClient

        return SftpClient(params)
    if protocol == "ftp":
        from app.connectors.nas.ftp_client import FtpClient

        return FtpClient(params)
    if protocol == "webdav":
        from app.connectors.nas.webdav_client import WebDavClient

        return WebDavClient(params)
    if protocol == "nfs":
        from app.connectors.nas.nfs_client import NfsClient

        return NfsClient(params)
    raise FatalConnectorError(f"Unsupported NAS protocol '{protocol}'")
