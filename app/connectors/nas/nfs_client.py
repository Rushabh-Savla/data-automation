"""NfsClient — user-space NFS through the optional `libnfs` binding (no kernel
mount, so no container privileges).

`libnfs` wraps a C library that is not pip-installable on its own, so it is an
optional extra. When it is missing, connect() fails with a message pointing to
the "Mounted path" option, which reaches any NFS export the host has mounted.
NFS has no password: access is decided by the server's export rules for this
machine's address, so the config's auth mode is ignored for this protocol.

Not exercised by the automated tests (they cannot load the C library); the
protocol-independent behaviour it relies on is covered through LocalClient."""

from __future__ import annotations

import stat as statmod
from pathlib import Path

from app.connectors.nas.base import (
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    NasUnavailableError,
    RemoteEntry,
)

_CHUNK = 1024 * 1024


class NfsClient(NasClient):
    def connect(self) -> None:
        p = self.params
        if not p.host.strip():
            raise NasNotFoundError("Enter the NAS host name or IP address")
        if not p.share.strip():
            raise NasNotFoundError("Enter the NFS export path, e.g. /volume1/photos")
        try:
            import libnfs
        except ImportError as exc:
            raise NasUnavailableError(
                "NFS support needs the optional 'libnfs' library on the server. "
                "Mount the export on the host and use the 'Mounted path' option instead."
            ) from exc
        export = "/" + p.share.strip().strip("/")
        try:
            self._nfs = libnfs.NFS(f"nfs://{p.host}{export}")
            self._nfs.listdir("/")
        except Exception as exc:
            raise NasConnectionError(f"Could not open NFS export {p.host}:{export}: {exc}") from exc

    def close(self) -> None:
        self._nfs = None

    @staticmethod
    def _abs(rel: str) -> str:
        return "/" + rel if rel else "/"

    def list_dir(self, rel: str) -> list[RemoteEntry]:
        try:
            out: list[RemoteEntry] = []
            for name in self._nfs.listdir(self._abs(rel)):
                if name in (".", ".."):
                    continue
                st = self._nfs.stat(self._abs(f"{rel}/{name}" if rel else name))
                out.append(
                    RemoteEntry(name, statmod.S_ISDIR(int(st["mode"])), int(st["size"]), float(st.get("mtime", 0)))
                )
            return out
        except Exception as exc:
            raise NasConnectionError(f"Could not list '{rel or '/'}': {exc}") from exc

    def _download(self, rel: str, local: Path) -> None:
        try:
            src = self._nfs.open(self._abs(rel), mode="rb")
            with open(local, "wb") as dst:
                while chunk := src.read(_CHUNK):
                    dst.write(chunk)
            src.close()
        except Exception as exc:
            raise NasConnectionError(f"Could not read '{rel}': {exc}") from exc

    def _upload(self, local: Path, rel: str) -> None:
        try:
            built = ""
            for segment in rel.rpartition("/")[0].split("/"):
                if not segment:
                    continue
                built = f"{built}/{segment}" if built else segment
                if not self.exists(built):
                    self._nfs.mkdir(self._abs(built))
            dst = self._nfs.open(self._abs(rel), mode="wb")
            with open(local, "rb") as src:
                while chunk := src.read(_CHUNK):
                    dst.write(chunk)
            dst.close()
        except Exception as exc:
            raise NasConnectionError(f"Could not write '{rel}': {exc}") from exc

    # No _rename override: libnfs's rename support isn't confirmed (this
    # optional dependency isn't installed anywhere this has been checked, and
    # this module is already not exercised by the automated tests). upload()
    # in base.py falls back to a non-atomic direct write for this protocol —
    # same exposure NFS already had, not a regression. Add a real _rename
    # here if libnfs turns out to expose one.

    def delete(self, rel: str) -> None:
        try:
            if self.exists(rel):
                self._nfs.unlink(self._abs(rel))
        except Exception as exc:
            raise NasConnectionError(f"Could not delete '{rel}': {exc}") from exc

    def rmdir(self, rel: str) -> None:
        try:
            self._nfs.rmdir(self._abs(rel))
        except Exception as exc:
            raise NasConnectionError(f"Could not remove directory '{rel}': {exc}") from exc

    def exists(self, rel: str) -> bool:
        return self.size(rel) is not None

    def size(self, rel: str) -> int | None:
        try:
            return int(self._nfs.stat(self._abs(rel))["size"])
        except Exception:
            return None
