"""FtpClient — plain FTP or explicit FTPS (use_tls), with anonymous login
support. Built on the standard library, so it adds no dependency."""

from __future__ import annotations

import ftplib
import socket
import ssl
from datetime import datetime, timezone
from pathlib import Path

from app.connectors.nas.base import (
    CONNECT_TIMEOUT_S,
    IO_TIMEOUT_S,
    NasAuthError,
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    RemoteEntry,
)


def _parse_mlsd_time(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.strptime(value[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return 0.0


class FtpClient(NasClient):
    def connect(self) -> None:
        p = self.params
        if not p.host.strip():
            raise NasNotFoundError("Enter the NAS host name or IP address")
        try:
            if p.use_tls:
                ctx = ssl.create_default_context()
                if not p.verify_tls:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                self._ftp: ftplib.FTP = ftplib.FTP_TLS(context=ctx, timeout=IO_TIMEOUT_S)
            else:
                self._ftp = ftplib.FTP(timeout=IO_TIMEOUT_S)
            self._ftp.connect(p.host, p.port or 21, timeout=CONNECT_TIMEOUT_S)
            if p.uses_password:
                self._ftp.login(p.username, p.password)
            else:
                self._ftp.login()  # anonymous
            if p.use_tls:
                self._ftp.prot_p()  # encrypt the data channel too, not just the login
            self._ftp.sock.settimeout(IO_TIMEOUT_S)
            self._root = "/" + p.share.strip().strip("/") if p.share.strip().strip("/") else ""
            self._ftp.voidcmd("TYPE I")
        except Exception as exc:
            self.close()
            raise self._translate(exc) from exc

    def close(self) -> None:
        ftp = getattr(self, "_ftp", None)
        if ftp is not None:
            try:
                ftp.quit()
            except Exception:
                try:
                    ftp.close()
                except Exception:
                    pass

    def _abs(self, rel: str) -> str:
        full = f"{self._root}/{rel}" if rel else self._root
        return full or "/"

    def _translate(self, exc: Exception) -> Exception:
        if isinstance(exc, ftplib.error_perm):
            code = str(exc)[:3]
            if code in ("530", "331", "332"):
                return NasAuthError("The NAS rejected the login")
            if code == "550":
                return NasNotFoundError("Path not found on the NAS (or access denied)")
            return NasConnectionError(self.params.redact(str(exc)))
        if isinstance(exc, ssl.SSLError):
            return NasConnectionError(f"TLS error talking to {self.params.host}: {exc}")
        if isinstance(exc, (ftplib.Error, socket.timeout, TimeoutError, OSError, EOFError)):
            return NasConnectionError(f"Could not reach {self.params.host}: {self.params.redact(str(exc))}")
        return exc

    def list_dir(self, rel: str) -> list[RemoteEntry]:
        path = self._abs(rel)
        try:
            out: list[RemoteEntry] = []
            for name, facts in self._ftp.mlsd(path, facts=["type", "size", "modify"]):
                kind = facts.get("type", "")
                if kind in ("cdir", "pdir") or name in (".", ".."):
                    continue
                out.append(
                    RemoteEntry(name, kind == "dir", int(facts.get("size") or 0), _parse_mlsd_time(facts.get("modify")))
                )
            return out
        except ftplib.error_perm as exc:
            if str(exc)[:3] in ("500", "501", "502", "504"):
                return self._list_dir_fallback(path)  # server has no MLSD
            raise self._translate(exc) from exc
        except Exception as exc:
            raise self._translate(exc) from exc

    def _list_dir_fallback(self, path: str) -> list[RemoteEntry]:
        try:
            out: list[RemoteEntry] = []
            for name in self._ftp.nlst(path):
                base = name.rsplit("/", 1)[-1]
                if base in (".", "..", ""):
                    continue
                full = f"{path.rstrip('/')}/{base}"
                try:
                    out.append(RemoteEntry(base, False, self._ftp.size(full) or 0, 0.0))
                except ftplib.error_perm:
                    out.append(RemoteEntry(base, True, 0, 0.0))  # SIZE fails on directories
            return out
        except Exception as exc:
            raise self._translate(exc) from exc

    def _download(self, rel: str, local: Path) -> None:
        try:
            with open(local, "wb") as fh:
                self._ftp.retrbinary(f"RETR {self._abs(rel)}", fh.write)
        except Exception as exc:
            raise self._translate(exc) from exc

    def _mkdirs(self, rel_dir: str) -> None:
        built = ""
        for segment in rel_dir.split("/"):
            built = f"{built}/{segment}" if built else segment
            try:
                self._ftp.mkd(self._abs(built))
            except ftplib.error_perm:
                pass  # already exists (or genuinely denied — the STOR below then reports it)

    def _upload(self, local: Path, rel: str) -> None:
        try:
            parent = rel.rpartition("/")[0]
            if parent:
                self._mkdirs(parent)
            with open(local, "rb") as fh:
                self._ftp.storbinary(f"STOR {self._abs(rel)}", fh)
        except Exception as exc:
            raise self._translate(exc) from exc

    def _rename(self, tmp_rel: str, final_rel: str) -> None:
        # FTP's RNFR/RNTO has no atomic-overwrite guarantee — best effort:
        # clear the destination first, then rename. A crash between the two
        # could leave nothing at final_rel, same exposure as a crash during a
        # direct write would have had; it cannot leave a *partial* file there,
        # which is the failure this whole temp-name scheme exists to prevent.
        try:
            try:
                self._ftp.delete(self._abs(final_rel))
            except ftplib.error_perm:
                pass  # didn't exist
            self._ftp.rename(self._abs(tmp_rel), self._abs(final_rel))
        except Exception as exc:
            raise self._translate(exc) from exc

    def delete(self, rel: str) -> None:
        try:
            self._ftp.delete(self._abs(rel))
        except ftplib.error_perm as exc:
            if str(exc)[:3] != "550":
                raise self._translate(exc) from exc
        except Exception as exc:
            raise self._translate(exc) from exc

    def rmdir(self, rel: str) -> None:
        try:
            self._ftp.rmd(self._abs(rel))
        except ftplib.error_perm as exc:
            if str(exc)[:3] != "550":
                raise self._translate(exc) from exc
        except Exception as exc:
            raise self._translate(exc) from exc

    def exists(self, rel: str) -> bool:
        return self.size(rel) is not None

    def size(self, rel: str) -> int | None:
        try:
            return self._ftp.size(self._abs(rel))
        except ftplib.error_perm as exc:
            if str(exc)[:3] == "550":
                return None
            raise self._translate(exc) from exc
        except Exception as exc:
            raise self._translate(exc) from exc
