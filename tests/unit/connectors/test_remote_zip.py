"""Tests for reading single members out of a remote zip via HTTP range requests
(app/connectors/remote_zip.py) -- what lets SUN RGB-D fetch the ~50KB images a
job needs instead of downloading a 6.9GB archive.

An in-memory zip sits behind a fake `requests` that honours Range headers, so
the offsets/index logic runs against real zip bytes."""

import io
import re
import struct
import zipfile

import pytest
import requests

from app.connectors import remote_zip as rz

URL = "http://example.com/data.zip"


def _build_zip() -> tuple[bytes, dict[str, bytes]]:
    members = {
        "SUNRGBD/kv2/seq1/image/0001.jpg": b"jpeg-bytes-one" * 500,
        "SUNRGBD/kv2/seq1/image/0002.jpg": bytes(range(256)) * 40,
        "SUNRGBD/kv2/seq1/depth/0001.png": b"depth" * 2000,
        "SUNRGBD/kv2/seq2/image/0003.jpg": b"jpeg-bytes-three" * 300,
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            # Mix stored and deflated members, as real archives do.
            method = zipfile.ZIP_STORED if name.endswith("0002.jpg") else zipfile.ZIP_DEFLATED
            zf.writestr(zipfile.ZipInfo(name), data, compress_type=method)
    return buf.getvalue(), members


class _Response:
    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class _FakeServer:
    """Serves `blob` with Range support; `mode` switches on failure behaviours."""

    def __init__(self, blob: bytes):
        self.blob = blob
        self.mode = "ok"
        self.range_requests: list[tuple[int, int | None]] = []
        self.failures_left = 0

    def head(self, url, headers=None, timeout=None, allow_redirects=True):
        return _Response(200, headers={"Content-Length": str(len(self.blob))})

    def get(self, url, headers=None, timeout=None):
        if self.failures_left > 0:
            self.failures_left -= 1
            raise requests.ConnectionError("reset by peer")
        if self.mode == "server_error":
            return _Response(503)
        if self.mode == "ignores_range":
            return _Response(200, self.blob)

        match = re.fullmatch(r"bytes=(\d+)-(\d*)", headers["Range"])
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else None
        self.range_requests.append((start, end))
        stop = len(self.blob) if end is None else min(end + 1, len(self.blob))
        return _Response(206, self.blob[start:stop])


@pytest.fixture
def server(monkeypatch):
    blob, members = _build_zip()
    fake = _FakeServer(blob)
    fake.members = members
    monkeypatch.setattr(rz, "_session", lambda: fake)
    monkeypatch.setattr(rz.time, "sleep", lambda _: None)
    return fake


def _is_rgb(name: str) -> bool:
    return "/image/" in name and name.endswith((".jpg", ".png"))


class TestBuildMemberIndex:
    def test_indexes_only_the_members_wanted(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})

        assert set(index) == {n for n in server.members if "/image/" in n}
        assert "SUNRGBD/kv2/seq1/depth/0001.png" not in index

    def test_reads_only_the_index_never_the_member_data(self, server):
        rz.build_member_index(URL, _is_rgb, headers={})

        # The end-of-central-directory record stores where the index starts;
        # nothing before that (i.e. no member data) may be requested.
        eocd = server.blob.rfind(b"PK\x05\x06")
        index_start = struct.unpack("<I", server.blob[eocd + 16 : eocd + 20])[0]
        assert server.range_requests
        assert all(start >= index_start for start, _ in server.range_requests)

    def test_entries_carry_what_is_needed_to_read_a_member(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        offset, compress_size, file_size, compress_type, _crc = index["SUNRGBD/kv2/seq1/image/0001.jpg"]

        assert file_size == len(server.members["SUNRGBD/kv2/seq1/image/0001.jpg"])
        assert compress_type == zipfile.ZIP_DEFLATED and compress_size < file_size
        assert offset == 0


class TestReadMember:
    def test_returns_each_members_exact_bytes_stored_and_deflated(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})

        for name, entry in index.items():
            assert rz.read_member(URL, entry, headers={}) == server.members[name]

    def test_is_a_single_request_per_member(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        server.range_requests.clear()

        rz.read_member(URL, index["SUNRGBD/kv2/seq2/image/0003.jpg"], headers={})

        assert len(server.range_requests) == 1

    def test_only_the_requested_range_is_asked_for(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        server.range_requests.clear()
        entry = index["SUNRGBD/kv2/seq2/image/0003.jpg"]

        rz.read_member(URL, entry, headers={})

        start, end = server.range_requests[0]
        assert start == entry[0]
        assert end - start < entry[1] + 4096  # the member plus header slack, not the archive

    def test_crc_mismatch_is_rejected_rather_than_returned(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        offset, csize, fsize, ctype, crc = index["SUNRGBD/kv2/seq2/image/0003.jpg"]

        with pytest.raises(zipfile.BadZipFile):
            rz.read_member(URL, (offset, csize, fsize, ctype, crc ^ 1), headers={})

    def test_an_offset_that_is_not_a_member_is_rejected(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        offset, csize, fsize, ctype, crc = index["SUNRGBD/kv2/seq2/image/0003.jpg"]

        with pytest.raises(zipfile.BadZipFile):
            rz.read_member(URL, (offset + 7, csize, fsize, ctype, crc), headers={})

    def test_truncated_data_is_rejected(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        _offset, csize, fsize, ctype, crc = index["SUNRGBD/kv2/seq2/image/0003.jpg"]
        # Claims more compressed bytes than the archive holds past this offset.
        with pytest.raises(zipfile.BadZipFile):
            rz.read_member(URL, (len(server.blob) - 10, csize, fsize, ctype, crc), headers={})

    def test_unsupported_compression_is_rejected(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        offset, csize, fsize, _ctype, crc = index["SUNRGBD/kv2/seq2/image/0003.jpg"]

        with pytest.raises(zipfile.BadZipFile):
            rz.read_member(URL, (offset, csize, fsize, zipfile.ZIP_BZIP2, crc), headers={})


class TestRequestFailures:
    def test_a_server_that_ignores_range_is_refused_not_downloaded(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        server.mode = "ignores_range"

        with pytest.raises(rz.RangeNotSupportedError):
            rz.read_member(URL, index["SUNRGBD/kv2/seq2/image/0003.jpg"], headers={})

    def test_transient_connection_errors_are_retried(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        server.failures_left = 2

        data = rz.read_member(URL, index["SUNRGBD/kv2/seq2/image/0003.jpg"], headers={})

        assert data == server.members["SUNRGBD/kv2/seq2/image/0003.jpg"]

    def test_gives_up_after_the_retry_budget(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        server.failures_left = 99

        with pytest.raises(requests.ConnectionError):
            rz.read_member(URL, index["SUNRGBD/kv2/seq2/image/0003.jpg"], headers={})

    def test_server_errors_are_retried_then_raised(self, server):
        index = rz.build_member_index(URL, _is_rgb, headers={})
        server.mode = "server_error"

        with pytest.raises(requests.ConnectionError):
            rz.read_member(URL, index["SUNRGBD/kv2/seq2/image/0003.jpg"], headers={})


class TestHttpRangeFile:
    def test_behaves_like_a_seekable_file(self, server):
        with rz.HttpRangeFile(URL, headers={}) as remote:
            assert remote.seekable() and remote.readable()
            remote.seek(-22, io.SEEK_END)
            assert remote.read(4) == b"PK\x05\x06"
            assert remote.tell() == len(server.blob) - 22 + 4
            remote.seek(0)
            assert remote.read(4) == b"PK\x03\x04"
            assert remote.read(0) == b""

    def test_small_sequential_reads_share_one_request(self, server):
        with rz.HttpRangeFile(URL, headers={}) as remote:
            remote.seek(0)
            for _ in range(10):
                remote.read(100)

        assert len(server.range_requests) == 1

    def test_reading_past_the_end_returns_what_is_left(self, server):
        with rz.HttpRangeFile(URL, headers={}) as remote:
            remote.seek(-5, io.SEEK_END)
            assert len(remote.read(1000)) == 5
            assert remote.read(10) == b""


class TestPoliteClient:
    """A small academic server started dropping connections (and then refused this
    host) when ~32 fresh connections hit it at once."""

    def test_a_thread_reuses_one_keep_alive_session(self):
        rz._drop_session()
        first = rz._session()

        assert rz._session() is first
        rz._drop_session()

    def test_each_thread_gets_its_own_session(self):
        import threading

        rz._drop_session()
        seen = []
        thread = threading.Thread(target=lambda: seen.append(rz._session()))
        thread.start()
        thread.join()

        assert seen[0] is not rz._session()
        rz._drop_session()

    def test_a_failed_request_discards_the_connection_before_retrying(self, monkeypatch):
        # The server may have closed the kept-alive connection; a retry on it would fail again.
        blob, _members = _build_zip()
        server = _FakeServer(blob)
        server.failures_left = 2
        created = []

        def new_session():
            created.append(1)
            return server

        monkeypatch.setattr(rz, "_session", new_session)
        monkeypatch.setattr(rz.time, "sleep", lambda _: None)
        drops = []
        monkeypatch.setattr(rz, "_drop_session", lambda: drops.append(1))

        rz._get_range(URL, 0, 10, {}, 5)

        assert len(drops) == 2  # once per failed attempt

    def test_backoff_is_jittered_and_bounded(self, server, monkeypatch):
        sleeps = []
        monkeypatch.setattr(rz.time, "sleep", sleeps.append)
        server.failures_left = 99

        with pytest.raises(requests.ConnectionError):
            rz._get_range(URL, 0, 10, {}, 5)

        assert len(sleeps) == rz._MAX_ATTEMPTS - 1
        assert all(0 < d <= rz._MAX_BACKOFF_S * 1.5 for d in sleeps)
        assert sleeps[-1] > sleeps[0] * 2 or sleeps[-1] >= rz._MAX_BACKOFF_S * 0.5  # grows, then plateaus

    def test_it_rides_out_a_burst_of_dropped_connections(self, server):
        server.failures_left = rz._MAX_ATTEMPTS - 1

        assert rz._get_range(URL, 0, 10, {}, 5)
