"""WebDavClient — WebDAV over HTTP(S) (Synology WebDAV Server, QNAP, Nextcloud,
Apache/nginx dav). Built on `requests` plus defusedxml for the PROPFIND
replies, since those come from a remote server we do not control.

Redirects are never followed: a NAS that answers a WebDAV call with a redirect
is usually an http->https mismatch, and silently following it would resend
credentials somewhere the user did not configure."""

from __future__ import annotations

import posixpath
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import requests
from defusedxml import ElementTree as SafeET

from app.connectors.nas.base import (
    CONNECT_TIMEOUT_S,
    IO_TIMEOUT_S,
    NasAuthError,
    NasClient,
    NasConnectionError,
    NasNotFoundError,
    RemoteEntry,
)

_DAV = "{DAV:}"
_PROPFIND_BODY = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<propfind xmlns="DAV:"><prop><resourcetype/><getcontentlength/><getlastmodified/></prop></propfind>'
)
_CHUNK = 1024 * 1024


class WebDavClient(NasClient):
    def connect(self) -> None:
        p = self.params
        if not p.host.strip():
            raise NasNotFoundError("Enter the NAS host name or IP address")
        scheme = "https" if p.use_tls else "http"
        netloc = f"{p.host}:{p.port}" if p.port else p.host
        prefix = "/" + p.share.strip().strip("/") if p.share.strip().strip("/") else ""
        self._base_path = prefix
        self._origin = f"{scheme}://{netloc}"
        self._session = requests.Session()
        self._session.verify = p.verify_tls
        if p.uses_password:
            self._session.auth = (p.username, p.password)
        # Proving the root answers PROPFIND catches wrong URL, wrong protocol and
        # bad credentials in one call, with a message the user can act on.
        self._propfind("", depth=0)

    def close(self) -> None:
        session = getattr(self, "_session", None)
        if session is not None:
            session.close()

    def _path(self, rel: str, collection: bool = False) -> str:
        path = f"{self._base_path}/{quote(rel)}" if rel else (self._base_path or "/")
        # Servers such as Apache answer a collection URL that lacks the trailing
        # slash with a 301; redirects are never followed (see the module note), so
        # collections are addressed with the slash from the start.
        return path if not collection or path.endswith("/") else path + "/"

    def _request(self, method: str, rel: str, *, collection: bool = False, **kwargs) -> requests.Response:
        try:
            resp = self._session.request(
                method,
                self._origin + self._path(rel, collection),
                timeout=(CONNECT_TIMEOUT_S, IO_TIMEOUT_S),
                allow_redirects=False,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise NasConnectionError(f"Could not reach {self.params.host}: {self.params.redact(str(exc))}") from exc
        if resp.status_code in (401, 403):
            resp.close()
            raise NasAuthError("The NAS rejected the login or denied access")
        if resp.status_code == 404:
            resp.close()
            raise NasNotFoundError("Path not found on the NAS")
        if 300 <= resp.status_code < 400:
            resp.close()
            raise NasConnectionError(
                f"The NAS redirected the request (HTTP {resp.status_code}) — check the http/https setting and the path"
            )
        if resp.status_code >= 500:
            resp.close()
            raise NasConnectionError(f"The NAS returned HTTP {resp.status_code}")
        return resp

    def _propfind(self, rel: str, depth: int) -> list[tuple[str, RemoteEntry]]:
        resp = self._request(
            "PROPFIND",
            rel,
            collection=depth > 0,
            headers={"Depth": str(depth), "Content-Type": "application/xml"},
            data=_PROPFIND_BODY,
        )
        if resp.status_code != 207:
            raise NasConnectionError(f"Unexpected WebDAV response (HTTP {resp.status_code}) — is this a WebDAV path?")
        try:
            root = SafeET.fromstring(resp.content)
        except Exception as exc:
            raise NasConnectionError("The NAS sent a WebDAV reply that could not be read") from exc
        out: list[tuple[str, RemoteEntry]] = []
        for node in root.findall(f"{_DAV}response"):
            href = node.findtext(f"{_DAV}href") or ""
            href_path = unquote(urlparse(href).path).rstrip("/")
            prop = node.find(f"{_DAV}propstat/{_DAV}prop")
            if prop is None:
                continue
            is_dir = prop.find(f"{_DAV}resourcetype/{_DAV}collection") is not None
            try:
                size = int(prop.findtext(f"{_DAV}getcontentlength") or 0)
            except ValueError:
                size = 0
            mtime = 0.0
            modified = prop.findtext(f"{_DAV}getlastmodified")
            if modified:
                try:
                    mtime = parsedate_to_datetime(modified).timestamp()
                except (TypeError, ValueError):
                    pass
            out.append((href_path, RemoteEntry(posixpath.basename(href_path), is_dir, size, mtime)))
        return out

    def list_dir(self, rel: str) -> list[RemoteEntry]:
        own = unquote(self._path(rel)).rstrip("/")
        return [entry for href_path, entry in self._propfind(rel, depth=1) if href_path != own and entry.name]

    def _download(self, rel: str, local: Path) -> None:
        resp = self._request("GET", rel, stream=True)
        try:
            if resp.status_code != 200:
                raise NasConnectionError(f"Unexpected response downloading '{rel}' (HTTP {resp.status_code})")
            with open(local, "wb") as fh:
                for chunk in resp.iter_content(_CHUNK):
                    fh.write(chunk)
        except requests.RequestException as exc:
            raise NasConnectionError(f"Download of '{rel}' was interrupted") from exc
        finally:
            resp.close()

    def _mkdirs(self, rel_dir: str) -> None:
        built = ""
        for segment in rel_dir.split("/"):
            built = f"{built}/{segment}" if built else segment
            resp = self._request("MKCOL", built, collection=True)
            resp.close()
            # 201 created; 405 already exists. Anything else that is not a
            # success will surface when the PUT into it fails.

    def _upload(self, local: Path, rel: str) -> None:
        parent = rel.rpartition("/")[0]
        if parent:
            self._mkdirs(parent)
        with open(local, "rb") as fh:
            resp = self._request("PUT", rel, data=fh)
        resp.close()
        if resp.status_code not in (200, 201, 204):
            raise NasConnectionError(f"The NAS refused the upload of '{rel}' (HTTP {resp.status_code})")

    def _rename(self, tmp_rel: str, final_rel: str) -> None:
        dest_url = self._origin + self._path(final_rel)
        resp = self._request("MOVE", tmp_rel, headers={"Destination": dest_url, "Overwrite": "T"})
        resp.close()
        if resp.status_code not in (200, 201, 204):
            raise NasConnectionError(f"The NAS refused to finalize the upload of '{final_rel}' (HTTP {resp.status_code})")

    def delete(self, rel: str) -> None:
        try:
            resp = self._request("DELETE", rel)
        except NasNotFoundError:
            return
        resp.close()

    def exists(self, rel: str) -> bool:
        return self.size(rel) is not None

    def size(self, rel: str) -> int | None:
        try:
            entries = self._propfind(rel, depth=0)
        except NasNotFoundError:
            return None
        return entries[0][1].size if entries else None
