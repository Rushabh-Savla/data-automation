"""Tests for download_with_retry (app/connectors/base.py) — retry-with-resume
for the large one-time archive downloads (KITTI/DAVIS/Pascal VOC/SUN RGB-D/
Visual Genome/nuImages) that previously failed the whole discovery task
outright on a single dropped connection."""

import requests
import pytest

from app.connectors import base as base_mod
from app.connectors.base import download_with_retry


class _FakeResponse:
    def __init__(self, chunks: list[bytes], status_code: int = 200, http_error: bool = False):
        self._chunks = chunks
        self.status_code = status_code
        self._http_error = http_error

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self._http_error:
            raise requests.HTTPError(f"{self.status_code} error")

    def iter_content(self, chunk_size):
        yield from self._chunks


class _DroppedConnection(Exception):
    """Stands in for requests.exceptions.ChunkedEncodingError/ConnectionError
    wrapping IncompleteRead — the actual production symptom."""


def _make_get(responses):
    """responses: list of either _FakeResponse or an Exception instance/class
    to raise, consumed one per call to requests.get."""
    calls = []

    def fake_get(url, stream, timeout, headers):
        calls.append({"url": url, "headers": dict(headers), "timeout": timeout})
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    fake_get.calls = calls
    return fake_get


class TestDownloadWithRetry:
    def test_succeeds_on_first_attempt_no_retry_needed(self, tmp_path, monkeypatch):
        dest = tmp_path / "file.zip.tmp"
        fake_get = _make_get([_FakeResponse([b"hello", b"world"])])
        monkeypatch.setattr(base_mod.requests, "get", fake_get)

        download_with_retry("http://example.com/f.zip", dest, headers={})

        assert dest.read_bytes() == b"helloworld"
        assert len(fake_get.calls) == 1
        assert "Range" not in fake_get.calls[0]["headers"]

    def test_retries_after_a_dropped_connection_and_succeeds(self, tmp_path, monkeypatch):
        dest = tmp_path / "file.zip.tmp"
        fake_get = _make_get(
            [
                _DroppedConnection("boom"),
                _FakeResponse([b"hello", b"world"]),
            ]
        )
        monkeypatch.setattr(base_mod.requests, "get", fake_get)
        monkeypatch.setattr(base_mod.time, "sleep", lambda _: None)

        download_with_retry("http://example.com/f.zip", dest, headers={}, max_attempts=3)

        assert dest.read_bytes() == b"helloworld"
        assert len(fake_get.calls) == 2

    def test_resumes_from_partial_bytes_via_range_header(self, tmp_path, monkeypatch):
        dest = tmp_path / "file.zip.tmp"
        dest.write_bytes(b"hello")  # simulates a previous attempt's partial write

        fake_get = _make_get([_FakeResponse([b"world"], status_code=206)])
        monkeypatch.setattr(base_mod.requests, "get", fake_get)

        download_with_retry("http://example.com/f.zip", dest, headers={})

        assert dest.read_bytes() == b"helloworld"  # appended, not overwritten
        assert fake_get.calls[0]["headers"]["Range"] == "bytes=5-"

    def test_restarts_from_scratch_if_server_ignores_range(self, tmp_path, monkeypatch):
        dest = tmp_path / "file.zip.tmp"
        dest.write_bytes(b"stale-partial-data")

        # Server responds 200 (full content) even though Range was requested.
        fake_get = _make_get([_FakeResponse([b"fresh-full-content"], status_code=200)])
        monkeypatch.setattr(base_mod.requests, "get", fake_get)

        download_with_retry("http://example.com/f.zip", dest, headers={})

        assert dest.read_bytes() == b"fresh-full-content"

    def test_http_error_is_not_retried(self, tmp_path, monkeypatch):
        dest = tmp_path / "file.zip.tmp"
        fake_get = _make_get([_FakeResponse([], status_code=404, http_error=True)])
        monkeypatch.setattr(base_mod.requests, "get", fake_get)

        with pytest.raises(requests.HTTPError):
            download_with_retry("http://example.com/f.zip", dest, headers={}, max_attempts=5)

        assert len(fake_get.calls) == 1  # no retry attempted

    def test_exhausting_retries_reraises_the_last_error(self, tmp_path, monkeypatch):
        dest = tmp_path / "file.zip.tmp"
        fake_get = _make_get([_DroppedConnection("1"), _DroppedConnection("2"), _DroppedConnection("3")])
        monkeypatch.setattr(base_mod.requests, "get", fake_get)
        monkeypatch.setattr(base_mod.time, "sleep", lambda _: None)

        with pytest.raises(_DroppedConnection):
            download_with_retry("http://example.com/f.zip", dest, headers={}, max_attempts=3)

        assert len(fake_get.calls) == 3
