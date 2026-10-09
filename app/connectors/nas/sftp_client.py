"""SftpClient — SFTP over SSH (Synology/QNAP with SSH enabled, any Linux box).

SFTP has no anonymous mode, so it always authenticates with a password; the
config validation rejects sftp + open access before it gets here."""

from __future__ import annotations

import posixpath
import socket
import stat as statmod
from pathlib import Path

import paramiko

from app.connectors.base import limited_concurrency
from app.connectors.nas.base import (
    CONNECT_TIMEOUT_S,
    IO_TIMEOUT_S,
    NasAuthError,
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    RemoteEntry,
)
from app.core.config import get_settings


class SftpClient(NasClient):
    def connect(self) -> None:
        # Held for the whole connection's lifetime (released in close()), not just
        # this call: a cheap NAS/router can drop connections or restart under a
        # burst of concurrent SSH sessions, so only a couple may be open at once.
        settings = get_settings()
        self._slot = limited_concurrency(
            settings.work_root / "_shared_cache" / "nas_sftp",
            "nas_sftp_session",
            settings.nas_sftp_max_concurrency,
        )
        self._slot.__enter__()
        # Everything below must go through this one try/except: the slot is only
        # ever released by close(), so any raise before reaching it — including
        # the validation checks — would otherwise leak the slot permanently.
        try:
            p = self.params
            if not p.host.strip():
                raise NasNotFoundError("Enter the NAS host name or IP address")
            if not p.username:
                raise NasAuthError("SFTP needs a user name and password")
            self._ssh = paramiko.SSHClient()
            # A NAS on a LAN has no CA-signed host key and the UI has no place to pin
            # one, so an unknown key is accepted on first use. Credentials still
            # travel encrypted; the trade-off is no protection against an active
            # man-in-the-middle on the first connection.
            self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh.connect(
                p.host,
                port=p.port or 22,
                username=p.username,
                password=p.password,
                timeout=CONNECT_TIMEOUT_S,
                banner_timeout=CONNECT_TIMEOUT_S,
                auth_timeout=CONNECT_TIMEOUT_S,
                look_for_keys=False,
                allow_agent=False,
            )
            self._sftp = self._ssh.open_sftp()
            self._sftp.get_channel().settimeout(IO_TIMEOUT_S)
            self._root = p.share.strip().rstrip("/")
            self._sftp.stat(self._abs(""))
        except Exception as exc:
            self.close()
            raise self._translate(exc) from exc

    def close(self) -> None:
        for attr in ("_sftp", "_ssh"):
            obj = getattr(self, attr, None)
            if obj is not None:
                try:
                    obj.close()
                except Exception:
                    pass
        slot = getattr(self, "_slot", None)
        if slot is not None:
            slot.__exit__(None, None, None)
            self._slot = None

    def _abs(self, rel: str) -> str:
        if self._root:
            return posixpath.join(self._root, rel) if rel else self._root
        return rel or "."

    def _translate(self, exc: Exception) -> Exception:
        if isinstance(exc, paramiko.AuthenticationException):
            return NasAuthError("The NAS rejected the user name or password")
        if isinstance(exc, FileNotFoundError):
            return NasNotFoundError("Path not found on the NAS")
        if isinstance(exc, PermissionError):
            return NasAuthError("Access denied on the NAS — check the account's permissions")
        if isinstance(exc, (paramiko.SSHException, socket.timeout, TimeoutError, OSError, EOFError)):
            return NasConnectionError(f"Could not reach {self.params.host}: {self.params.redact(str(exc))}")
        return exc

    def list_dir(self, rel: str) -> list[RemoteEntry]:
        try:
            return [
                RemoteEntry(a.filename, statmod.S_ISDIR(a.st_mode or 0), a.st_size or 0, float(a.st_mtime or 0))
                for a in self._sftp.listdir_attr(self._abs(rel))
            ]
        except Exception as exc:
            raise self._translate(exc) from exc

    def _download(self, rel: str, local: Path) -> None:
        try:
            self._sftp.get(self._abs(rel), str(local))
        except Exception as exc:
            raise self._translate(exc) from exc

    def _mkdirs(self, rel_dir: str) -> None:
        built = ""
        for segment in rel_dir.split("/"):
            built = f"{built}/{segment}" if built else segment
            target = self._abs(built)
            try:
                self._sftp.stat(target)
            except FileNotFoundError:
                self._sftp.mkdir(target)

    def _upload(self, local: Path, rel: str) -> None:
        try:
            parent = rel.rpartition("/")[0]
            if parent:
                self._mkdirs(parent)
            self._sftp.put(str(local), self._abs(rel))
        except Exception as exc:
            raise self._translate(exc) from exc

    def delete(self, rel: str) -> None:
        try:
            self._sftp.remove(self._abs(rel))
        except FileNotFoundError:
            return
        except Exception as exc:
            raise self._translate(exc) from exc

    def exists(self, rel: str) -> bool:
        return self.size(rel) is not None

    def size(self, rel: str) -> int | None:
        try:
            return self._sftp.stat(self._abs(rel)).st_size
        except FileNotFoundError:
            return None
        except Exception as exc:
            raise self._translate(exc) from exc
