"""LocalClient — a NAS that is already mounted as a directory.

This is the universal fallback: any NAS the host can mount (NFS, CIFS, iSCSI
volume, USB disk) and bind-mount into the container is reachable through it with
no extra library. It is also what the adapter tests run against."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from app.connectors.nas.base import (
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    NasPathError,
    RemoteEntry,
)


class LocalClient(NasClient):
    def connect(self) -> None:
        self._root = Path(self.params.share).expanduser()
        if not self.params.share.strip():
            raise NasNotFoundError("Enter the mounted folder path")
        if not self._root.is_dir():
            raise NasNotFoundError(f"Mounted path '{self.params.share}' does not exist or is not a folder")

    def close(self) -> None:
        return None

    def _abs(self, rel: str) -> Path:
        root = self._root.resolve()
        candidate = (root / rel).resolve()
        # Resolved (symlinks followed) and compared as a path, not a string
        # prefix, so '/mnt/nas2' can never pass as being inside '/mnt/nas'.
        if candidate != root and root not in candidate.parents:
            raise NasPathError(f"Path '{rel}' escapes the NAS root")
        return candidate

    def list_dir(self, rel: str) -> list[RemoteEntry]:
        target = self._abs(rel)
        if not target.is_dir():
            raise NasNotFoundError(f"Folder '{rel or '/'}' not found on the NAS")
        entries: list[RemoteEntry] = []
        try:
            with os.scandir(target) as it:
                for e in it:
                    try:
                        st = e.stat()
                        entries.append(RemoteEntry(e.name, e.is_dir(), st.st_size, st.st_mtime))
                    except OSError:
                        continue  # vanished or unreadable entry: skip, don't fail the walk
        except OSError as exc:
            raise NasConnectionError(f"Could not list '{rel or '/'}': {exc}") from exc
        return entries

    def _download(self, rel: str, local: Path) -> None:
        src = self._abs(rel)
        if not src.is_file():
            raise NasNotFoundError(f"File '{rel}' not found on the NAS")
        try:
            shutil.copyfile(src, local)
        except OSError as exc:
            raise NasConnectionError(f"Could not read '{rel}': {exc}") from exc

    def _upload(self, local: Path, rel: str) -> None:
        dest = self._abs(rel)
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local, dest)
        except OSError as exc:
            raise NasConnectionError(f"Could not write '{rel}': {exc}") from exc

    def delete(self, rel: str) -> None:
        try:
            self._abs(rel).unlink(missing_ok=True)
        except OSError as exc:
            raise NasConnectionError(f"Could not delete '{rel}': {exc}") from exc

    def exists(self, rel: str) -> bool:
        return self._abs(rel).exists()

    def size(self, rel: str) -> int | None:
        target = self._abs(rel)
        return target.stat().st_size if target.is_file() else None
