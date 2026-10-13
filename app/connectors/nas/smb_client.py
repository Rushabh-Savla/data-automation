"""SmbClient — SMB2/3 (Windows shares, Synology, QNAP, TrueNAS, Samba) over
smbprotocol's pure-Python client: no kernel mount, no extra privileges, so it
works from inside an unprivileged container.

Each client gets its own connection cache instead of smbclient's process-global
one, so two configs with different credentials can never share (or leak) a
session, and close() tears down exactly what this client opened."""

from __future__ import annotations

import socket
import threading
from pathlib import Path

import smbclient
from smbprotocol.exceptions import (
    BadNetworkName,
    LogonFailure,
    SMBAuthenticationError,
    SMBConnectionClosed,
    SMBException,
    SMBOSError,
)
from spnego.exceptions import SpnegoError

from app.connectors.nas.base import (
    CONNECT_TIMEOUT_S,
    NasAuthError,
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    RemoteEntry,
)

_CHUNK = 1024 * 1024
_ENOENT = 2
_GUEST_NEGOTIATE_LOCK = threading.Lock()

# NT status codes worth telling apart: the account is refused vs. the path is missing.
_NT_DENIED = {
    0xC0000022,  # ACCESS_DENIED
    0xC000006D,  # LOGON_FAILURE
    0xC000006A,  # WRONG_PASSWORD
    0xC0000064,  # NO_SUCH_USER
    0xC000006E,  # ACCOUNT_RESTRICTION
    0xC0000072,  # ACCOUNT_DISABLED
    0xC0000234,  # ACCOUNT_LOCKED_OUT
}
_NT_NOT_FOUND = {
    0xC0000034,  # OBJECT_NAME_NOT_FOUND
    0xC000003A,  # OBJECT_PATH_NOT_FOUND
    0xC00000CC,  # BAD_NETWORK_NAME
}


class SmbClient(NasClient):
    def connect(self) -> None:
        p = self.params
        if not p.host.strip():
            raise NasNotFoundError("Enter the NAS host name or IP address")
        if not p.share.strip():
            raise NasNotFoundError("Enter the SMB share name")
        self._cache: dict = {}
        self._port = p.port or 445
        # smbclient finds a session by (server, port) in the cache, and its
        # operations only know the server from the UNC path — so every call must
        # also carry the port, or a non-default port silently misses the session
        # and smbclient tries a fresh, credential-less login.
        self._kw = {"connection_cache": self._cache, "port": self._port}
        if p.uses_password:
            username = f"{p.domain}\\{p.username}" if p.domain else p.username
            password, require_signing = p.password, True
        else:
            # Guest sessions have no key to sign with, so signing must be
            # optional or an open share would refuse the login.
            username, password, require_signing = "guest", "", False
        try:
            smbclient.register_session(
                p.host,
                username=username,
                password=password,
                port=self._port,
                encrypt=True if p.use_tls else None,
                connection_timeout=CONNECT_TIMEOUT_S,
                connection_cache=self._cache,
                require_signing=require_signing,
            )
            # Touch the share now so a wrong share name or a denied login fails
            # here, with a clear message, rather than on the first file.
            if p.uses_password:
                smbclient.stat(self._unc(""), **self._kw)
            else:
                self._connect_share_as_guest()
        except Exception as exc:
            self.close()
            raise self._translate(exc) from exc

    def _connect_share_as_guest(self) -> None:
        """Guest sessions have no session key, so SMB's "secure negotiate"
        integrity check cannot pass for them. It is relaxed only for the moment the
        share is first connected (the tree stays connected for later calls), under a
        lock, and always switched back on."""
        with _GUEST_NEGOTIATE_LOCK:
            smbclient.ClientConfig(require_secure_negotiate=False)
            try:
                smbclient.stat(self._unc(""), **self._kw)
            finally:
                smbclient.ClientConfig(require_secure_negotiate=True)

    def close(self) -> None:
        cache = getattr(self, "_cache", None)
        if cache:
            try:
                smbclient.delete_session(self.params.host, port=self._port, connection_cache=cache)
            except Exception:
                pass
            cache.clear()

    def _unc(self, rel: str) -> str:
        share = self.params.share.strip().strip("/\\").replace("/", "\\")
        base = f"\\\\{self.params.host}\\{share}"
        return f"{base}\\{rel.replace('/', chr(92))}" if rel else base

    def _translate(self, exc: Exception) -> Exception:
        if isinstance(exc, (SMBAuthenticationError, LogonFailure, SpnegoError)):
            return NasAuthError("The NAS rejected the user name or password")
        if isinstance(exc, BadNetworkName):
            return NasNotFoundError(f"Share '{self.params.share}' was not found on the NAS")
        if isinstance(exc, SMBOSError):
            status = getattr(exc, "ntstatus", None)
            if status in _NT_DENIED or exc.errno in (13, 1):  # EACCES / EPERM
                return NasAuthError("Access denied by the NAS — check the account and the share's permissions")
            if status in _NT_NOT_FOUND or exc.errno == _ENOENT:
                return NasNotFoundError("Path not found on the NAS")
            return NasConnectionError(self.params.redact(str(exc)))
        if isinstance(exc, (SMBConnectionClosed, socket.timeout, TimeoutError, ConnectionError, OSError)):
            return NasConnectionError(f"Could not reach {self.params.host}: {self.params.redact(str(exc))}")
        if isinstance(exc, SMBException):
            return NasConnectionError(self.params.redact(str(exc)))
        if isinstance(exc, ValueError) and "connect" in str(exc).lower():
            # smbprotocol raises a bare ValueError("Failed to connect to ...") when the
            # host refuses or cannot be resolved.
            return NasConnectionError(f"Could not reach {self.params.host}: {self.params.redact(str(exc))}")
        return exc

    def list_dir(self, rel: str) -> list[RemoteEntry]:
        try:
            out: list[RemoteEntry] = []
            for e in smbclient.scandir(self._unc(rel), **self._kw):
                if e.name in (".", ".."):
                    continue
                # Size and time come with the directory listing itself, so this costs no
                # extra round trip per file (DirEntry.stat() would make one each).
                info = e.smb_info
                out.append(RemoteEntry(e.name, e.is_dir(), info.end_of_file, info.last_write_time.timestamp()))
            return out
        except Exception as exc:
            raise self._translate(exc) from exc

    def _download(self, rel: str, local: Path) -> None:
        try:
            with smbclient.open_file(self._unc(rel), mode="rb", **self._kw) as src, open(
                local, "wb"
            ) as dst:
                while chunk := src.read(_CHUNK):
                    dst.write(chunk)
        except Exception as exc:
            raise self._translate(exc) from exc

    def _upload(self, local: Path, rel: str) -> None:
        try:
            parent = rel.rpartition("/")[0]
            if parent:
                smbclient.makedirs(self._unc(parent), exist_ok=True, **self._kw)
            with open(local, "rb") as src, smbclient.open_file(
                self._unc(rel), mode="wb", **self._kw
            ) as dst:
                while chunk := src.read(_CHUNK):
                    dst.write(chunk)
        except Exception as exc:
            raise self._translate(exc) from exc

    def _rename(self, tmp_rel: str, final_rel: str) -> None:
        try:
            # Atomic overwrite, like os.replace.
            smbclient.replace(self._unc(tmp_rel), self._unc(final_rel), **self._kw)
        except Exception as exc:
            raise self._translate(exc) from exc

    def delete(self, rel: str) -> None:
        try:
            smbclient.remove(self._unc(rel), **self._kw)
        except SMBOSError as exc:
            if exc.errno != _ENOENT:
                raise self._translate(exc) from exc
        except Exception as exc:
            raise self._translate(exc) from exc

    def exists(self, rel: str) -> bool:
        return self.size(rel) is not None

    def size(self, rel: str) -> int | None:
        try:
            return smbclient.stat(self._unc(rel), **self._kw).st_size
        except SMBOSError as exc:
            if exc.errno == _ENOENT:
                return None
            raise self._translate(exc) from exc
        except Exception as exc:
            raise self._translate(exc) from exc
