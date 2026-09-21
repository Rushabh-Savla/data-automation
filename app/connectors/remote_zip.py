"""Reading individual members out of a large zip over HTTP range requests.

SUN RGB-D ships as one 6.9GB SUNRGBD.zip with no per-image URLs. A job usually
wants a few dozen to a few thousand of its ~10,000 images, but the only way to
get them used to be downloading the whole archive (about 45 minutes at the
~2.5MB/s the host allows per connection) and unpacking another 7GB to disk.

A zip keeps an index of its members (the central directory) at the end of the
file, and each member is stored contiguously. So the index can be fetched on its
own (66MB for this archive), and then each wanted image is one small range
request -- minutes instead of most of an hour, and no archive on disk.
"""

import io
import logging
import random
import struct
import threading
import time
import zipfile
import zlib
from collections.abc import Callable

import requests

logger = logging.getLogger(__name__)

# A member is [local header][data]; the local header is 30 fixed bytes plus the
# file name and an extra field. Asking for this much beyond the compressed size
# covers a header far longer than any real one, so a member is one request.
_LOCAL_HEADER_SLACK = 2048
_LOCAL_HEADER = struct.Struct("<4s5H3I2H")

# Long enough (~30s of backoff in total) to ride out a server that is shedding
# load; the shard's own Celery retry covers anything longer.
_MAX_ATTEMPTS = 6
_MAX_BACKOFF_S = 30
_MIN_BLOCK_BYTES = 64 * 1024

_local = threading.local()


def _session() -> requests.Session:
    """One keep-alive session per thread.

    A fresh connection per request means a TCP + TLS handshake for a ~50KB
    image, and hundreds of them opening at once is exactly what a small
    academic server sheds. A session reuses its connection instead.
    """
    session = getattr(_local, "session", None)
    if session is None:
        session = _local.session = requests.Session()
    return session


def _drop_session() -> None:
    """Forget this thread's connection, so a retry opens a new one rather than
    reusing one the server has already closed."""
    session = getattr(_local, "session", None)
    _local.session = None
    if session is not None:
        session.close()

# (header_offset, compress_size, file_size, compress_type, crc32)
MemberEntry = tuple[int, int, int, int, int]


class RangeNotSupportedError(Exception):
    """The server answered a range request with the whole file (or nothing usable)."""


def _get_range(url: str, start: int, end: int | None, headers: dict, timeout: int) -> bytes:
    """Bytes [start, end] (inclusive; open-ended when `end` is None), retried on
    connection errors and 5xx. Anything but a 206 is refused: a 200 means the
    server ignored the Range header and is sending the whole 6.9GB."""
    range_value = f"bytes={start}-" if end is None else f"bytes={start}-{end}"
    last_error: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            response = _session().get(url, headers={**headers, "Range": range_value}, timeout=timeout)
            if response.status_code >= 500:
                raise requests.ConnectionError(f"HTTP {response.status_code}")
            if response.status_code != 206:
                raise RangeNotSupportedError(f"{url} answered {response.status_code} to {range_value}")
            return response.content
        except RangeNotSupportedError:
            raise
        except requests.RequestException as e:
            last_error = e
            _drop_session()
            if attempt < _MAX_ATTEMPTS - 1:
                # Jittered, so shards that failed together do not retry together.
                time.sleep(min(2**attempt, _MAX_BACKOFF_S) * (0.5 + random.random()))
    raise last_error


class HttpRangeFile(io.RawIOBase):
    """A read-only, seekable file over a URL, fetching with range requests.

    Just enough for zipfile.ZipFile to read the central directory: it seeks to
    the end to find the trailer, then reads the index in one go. The last block
    fetched is kept, so the trailer probes (a few small reads at the end) do not
    each cost a round trip.
    """

    def __init__(self, url: str, *, headers: dict, timeout: int = 120):
        self._url = url
        self._headers = headers
        self._timeout = timeout
        self._position = 0
        self._block_start = 0
        self._block = b""
        response = _session().head(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        self._size = int(response.headers["Content-Length"])

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._position = offset
        elif whence == io.SEEK_CUR:
            self._position += offset
        else:
            self._position = self._size + offset
        self._position = max(0, min(self._position, self._size))
        return self._position

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self._size - self._position
        size = min(size, self._size - self._position)
        if size <= 0:
            return b""

        block_end = self._block_start + len(self._block)
        if not (self._block_start <= self._position and self._position + size <= block_end):
            # Fetch at least a minimum block: small sequential reads are common.
            fetch = min(max(size, _MIN_BLOCK_BYTES), self._size - self._position)
            self._block = _get_range(
                self._url, self._position, self._position + fetch - 1, self._headers, self._timeout
            )
            self._block_start = self._position
        offset = self._position - self._block_start
        data = self._block[offset : offset + size]
        self._position += len(data)
        return data


def build_member_index(
    url: str, keep: Callable[[str], bool], *, headers: dict, timeout: int = 120
) -> dict[str, MemberEntry]:
    """name -> MemberEntry for the archive's members that `keep` accepts.

    Reads only the archive's central directory (66MB for SUNRGBD.zip), never the
    member data. The result is small enough to cache as JSON.
    """
    with HttpRangeFile(url, headers=headers, timeout=timeout) as remote, zipfile.ZipFile(remote) as archive:
        return {
            info.filename: (info.header_offset, info.compress_size, info.file_size, info.compress_type, info.CRC)
            for info in archive.infolist()
            if keep(info.filename)
        }


def read_member(url: str, entry: MemberEntry, *, headers: dict, timeout: int = 120) -> bytes:
    """The uncompressed bytes of one member, in a single range request.

    Raises zipfile.BadZipFile if what came back is not that member (bad local
    header, short data, CRC mismatch) so a truncated or misplaced response is
    never returned as an image.
    """
    header_offset, compress_size, file_size, compress_type, crc = entry
    raw = _get_range(url, header_offset, header_offset + _LOCAL_HEADER_SLACK + compress_size, headers, timeout)

    if len(raw) < _LOCAL_HEADER.size:
        raise zipfile.BadZipFile("short read for local file header")
    signature, *_, name_length, extra_length = _LOCAL_HEADER.unpack(raw[: _LOCAL_HEADER.size])
    if signature != b"PK\x03\x04":
        raise zipfile.BadZipFile("no local file header at the indexed offset")

    start = _LOCAL_HEADER.size + name_length + extra_length
    data = raw[start : start + compress_size]
    if len(data) != compress_size:
        raise zipfile.BadZipFile(f"expected {compress_size} bytes of member data, got {len(data)}")

    if compress_type == zipfile.ZIP_STORED:
        content = data
    elif compress_type == zipfile.ZIP_DEFLATED:
        try:
            content = zlib.decompress(data, -zlib.MAX_WBITS)
        except zlib.error as e:
            raise zipfile.BadZipFile(f"member data does not decompress: {e}") from e
    else:
        raise zipfile.BadZipFile(f"unsupported compression method {compress_type}")

    if len(content) != file_size or zlib.crc32(content) != crc:
        raise zipfile.BadZipFile("member failed its size/CRC check")
    return content
