"""Regression test for the confirmed bug in youtube_downloader.py's
_cleanup_videos(), which only globbed *.mp4 and silently left .mkv/.webm/etc.
files on disk. The ported YouTubeConnector._cleanup_video_files must cover
every media extension yt-dlp/ffmpeg can produce."""

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.connectors import youtube_connector
from app.connectors.base import SourceItem
from app.connectors.youtube_connector import YouTubeConnector, _ErrorCapturingLogger, _MEDIA_EXTENSIONS, search_videos
from app.core.exceptions import FatalConnectorError, RateLimitedError, TransientConnectorError
from app.domain.enums import SourceType


class TestCleanupVideoFiles:
    def test_removes_every_media_extension_for_the_video_id(self, tmp_path: Path):
        video_id = "abc123XYZ"
        for ext in _MEDIA_EXTENSIONS:
            (tmp_path / f"{video_id}{ext}").write_bytes(b"fake media bytes")
        # A different video's file must be left untouched.
        other = tmp_path / "other_video.mp4"
        other.write_bytes(b"unrelated")

        YouTubeConnector._cleanup_video_files(tmp_path, video_id)

        for ext in _MEDIA_EXTENSIONS:
            assert not (tmp_path / f"{video_id}{ext}").exists()
        assert other.exists()

    def test_no_error_when_no_files_present(self, tmp_path: Path):
        YouTubeConnector._cleanup_video_files(tmp_path, "nonexistent_id")


class TestErrorCapturingLogger:
    """Regression test for a real production case where a video intermittently
    produced no file, every time with last_error left None -- the only
    diagnostic yt-dlp had actually logged was a warning (report_warning(),
    not report_error()), which the old logger silently discarded, leaving
    fetch() with nothing better to say than the unhelpful literal
    "unknown reason (no file produced, no error captured)"."""

    def test_captures_warning_independently_of_error(self):
        logger = _ErrorCapturingLogger()
        logger.warning("unable to download format: HTTP Error 429: Too Many Requests")
        assert logger.last_warning == "unable to download format: HTTP Error 429: Too Many Requests"
        assert logger.last_error is None

    def test_error_is_not_masked_by_a_later_warning(self):
        logger = _ErrorCapturingLogger()
        logger.error("real cause")
        logger.warning("unrelated benign warning")
        assert logger.last_error == "real cause"

    def test_embedded_carriage_return_is_stripped(self):
        # Regression: production surfaced "ERROR: \r[download] Got error: ..."
        # verbatim in job_source_tasks.error_message -- a raw control
        # character rendered on the job detail page. Only the text after the
        # last \r (yt-dlp's progress-redraw convention) is the real message.
        logger = _ErrorCapturingLogger()
        logger.error("ERROR: \r[download] Got error: 906 bytes read, 10315295 more expected. Giving up after 5 retries")
        assert "\r" not in logger.last_error
        assert logger.last_error == "[download] Got error: 906 bytes read, 10315295 more expected. Giving up after 5 retries"


class TestClassifyFfmpegError:
    """Regression test for the raw, undiagnosed 'ffmpeg failed for X: Command
    [...] returned non-zero exit status 183' error surfaced to users verbatim
    (job_source_tasks.error_message -> JobSourceTaskTable.tsx). Root cause of
    that specific exit code: YouTube serves a truncated stream for some
    videos under the android client's format restrictions (SABR-only), which
    ffmpeg reports as a malformed/incomplete input -- a deterministic,
    non-retryable failure, not a transient one."""

    def test_truncated_stream_is_fatal_and_message_has_no_raw_command(self, tmp_path: Path):
        stderr = (
            "[mov,mp4,m4a,3gp,3g2,mj2] stream 0, offset 0x792e4: partial file\n"
            "Cannot determine format of input 0:0 after EOF\n"
            "Conversion failed!"
        )
        e = subprocess.CalledProcessError(183, ["ffmpeg", "-i", "/work/secret-path/vid.mp4"], stderr=stderr)

        result = YouTubeConnector._classify_ffmpeg_error(e, tmp_path / "cyaRkOXDhOk.mp4")

        assert isinstance(result, FatalConnectorError)
        assert "/work/secret-path" not in str(result)  # no internal container path leaked
        assert "cyaRkOXDhOk" in str(result)
        assert "Cannot determine format" in str(result)

    def test_other_ffmpeg_failures_stay_transient(self, tmp_path: Path):
        e = subprocess.CalledProcessError(1, ["ffmpeg"], stderr="Error opening filters!\nConversion failed!")

        result = YouTubeConnector._classify_ffmpeg_error(e, tmp_path / "someVideo.mp4")

        assert isinstance(result, TransientConnectorError)
        assert "Error opening filters" in str(result)

    def test_no_stderr_captured_still_produces_a_message(self, tmp_path: Path):
        e = subprocess.CalledProcessError(1, ["ffmpeg"], stderr="")

        result = YouTubeConnector._classify_ffmpeg_error(e, tmp_path / "someVideo.mp4")

        assert isinstance(result, TransientConnectorError)
        assert "someVideo" in str(result)


_VIDEO_ID = "abc123XYZ"


def _item() -> SourceItem:
    return SourceItem(
        source_type=SourceType.YOUTUBE,
        external_ref={"video_id": _VIDEO_ID, "url": f"https://www.youtube.com/watch?v={_VIDEO_ID}"},
        license_hint="creativeCommon",
    )


class _FakeYoutubeDL:
    """Stands in for yt_dlp.YoutubeDL: "downloads" by writing the video file."""

    downloads: list[str] = []

    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def extract_info(self, url, download=True):
        type(self).downloads.append(url)
        out_dir = Path(self.opts["outtmpl"]).parent
        (out_dir / f"{_VIDEO_ID}.mp4").write_bytes(b"video")
        return {"id": _VIDEO_ID, "license": "Creative Commons Attribution license"}


class _FakeRedis:
    """Minimal in-memory stand-in for get_redis()'s real redis.Redis client --
    just enough of the API the rate-limit cooldown gate uses (set/exists/ttl)
    to keep these tests off a live Redis instance. Does not actually expire
    keys over time; ttl() just returns whatever `ex` was last set with."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def set(self, key, value, ex=None):
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    def exists(self, key):
        return 1 if key in self.store else 0

    def ttl(self, key):
        if key not in self.store:
            return -2
        return self.ttls.get(key, -1)


@pytest.fixture
def fetch_env(monkeypatch):
    """A connector whose yt-dlp and frame extraction are faked; records what fetch() did."""
    import yt_dlp

    _FakeYoutubeDL.downloads = []
    monkeypatch.setattr(yt_dlp, "YoutubeDL", _FakeYoutubeDL)

    extracted = SimpleNamespace(calls=[])

    def fake_extract(self, video_id, video_path, video_url, license_name, frames_dir, start_index, *_):
        extracted.calls.append({"video_path": video_path, "start_index": start_index, "license": license_name})
        return iter(())

    monkeypatch.setattr(YouTubeConnector, "_extract_frames", fake_extract)
    # fetch() checks the rate-limit cooldown gate against real Redis before
    # every download -- keep these tests off that live dependency, cooldown
    # inactive by default. TestRateLimitCooldown flips fake_redis to exercise
    # the active path through this same fake instead of a second mock.
    fake_redis = _FakeRedis()
    monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
    return SimpleNamespace(
        connector=YouTubeConnector(), extracted=extracted, downloads=_FakeYoutubeDL.downloads, fake_redis=fake_redis
    )


def _run_fetch(connector, workdir, checkpoint, progress=None):
    on_progress = progress.append if progress is not None else (lambda _p: None)
    return list(connector.fetch(_item(), workdir, checkpoint, lambda: False, on_progress))


class TestLocalVideo:
    def test_finds_the_video_by_id(self, tmp_path: Path):
        video = tmp_path / f"{_VIDEO_ID}.webm"
        video.write_bytes(b"x")
        assert YouTubeConnector._local_video(tmp_path, _VIDEO_ID) == video

    def test_ignores_non_media_files_and_other_videos(self, tmp_path: Path):
        (tmp_path / f"{_VIDEO_ID}.info.json").write_text("{}")
        (tmp_path / "other.mp4").write_bytes(b"x")
        assert YouTubeConnector._local_video(tmp_path, _VIDEO_ID) is None

    def test_empty_id_matches_nothing(self, tmp_path: Path):
        (tmp_path / ".mp4").write_bytes(b"x")
        assert YouTubeConnector._local_video(tmp_path, "") is None


class TestFetchResume:
    def test_missing_video_on_extracting_resume_is_downloaded_again(self, tmp_path: Path, fetch_env):
        # The state that used to end the task with "No local video file to extract frames from":
        # the checkpoint says "extracting", but the scratch video is gone.
        archive = tmp_path / "yt_archive.txt"
        archive.write_text(f"youtube {_VIDEO_ID}\n")
        progress: list[dict] = []

        _run_fetch(fetch_env.connector, tmp_path, {"stage": "extracting", "frame_index": 7}, progress)

        assert len(fetch_env.downloads) == 1
        # The archive entry must be gone, otherwise yt-dlp skips the video and produces no file.
        assert not archive.exists()
        (call,) = fetch_env.extracted.calls
        assert call["video_path"].name == f"{_VIDEO_ID}.mp4"
        # Frame numbering restarts with the fresh download rather than continuing from a stale index.
        assert call["start_index"] == 0
        assert progress[0]["checkpoint"]["stage"] == "extracting"

    def test_missing_video_on_resume_only_forgets_its_own_archive_entry(self, tmp_path: Path, fetch_env):
        # The archive file is shared by every video in this job_source_task's workdir
        # (same workdir, one archive_file per task, not per video) -- resuming one
        # video must not wipe out another already-recorded video's entry.
        archive = tmp_path / "yt_archive.txt"
        other_video_id = "other999ZZZ"
        archive.write_text(f"youtube {other_video_id}\nyoutube {_VIDEO_ID}\n")

        _run_fetch(fetch_env.connector, tmp_path, {"stage": "extracting", "frame_index": 7})

        assert archive.exists()
        assert archive.read_text().splitlines() == [f"youtube {other_video_id}"]

    def test_present_video_on_extracting_resume_is_not_downloaded_again(self, tmp_path: Path, fetch_env):
        video_dir = tmp_path / "videos"
        video_dir.mkdir()
        (video_dir / f"{_VIDEO_ID}.mp4").write_bytes(b"video")

        _run_fetch(fetch_env.connector, tmp_path, {"stage": "extracting", "frame_index": 7})

        assert fetch_env.downloads == []
        (call,) = fetch_env.extracted.calls
        assert call["start_index"] == 7
        assert call["license"] == "creativeCommon"

    def test_fresh_fetch_downloads_then_extracts(self, tmp_path: Path, fetch_env):
        progress: list[dict] = []

        _run_fetch(fetch_env.connector, tmp_path, {}, progress)

        assert len(fetch_env.downloads) == 1
        (call,) = fetch_env.extracted.calls
        assert call["start_index"] == 0
        assert call["license"] == "Creative Commons Attribution license"
        assert progress[0] == {"checkpoint": {"stage": "extracting", "frame_index": 0}, "downloaded": 1}

    def test_download_that_produces_no_file_is_still_an_error(self, tmp_path: Path, fetch_env, monkeypatch):
        # The fallback must not paper over a genuine download failure.
        monkeypatch.setattr(_FakeYoutubeDL, "extract_info", lambda self, url, download=True: {"id": _VIDEO_ID})

        with pytest.raises(TransientConnectorError):
            _run_fetch(fetch_env.connector, tmp_path, {"stage": "extracting"})

        assert fetch_env.extracted.calls == []

    def test_no_file_produced_falls_back_to_the_captured_warning(self, tmp_path: Path, fetch_env, monkeypatch):
        # Regression: a video that intermittently produced no file, every time
        # with nothing logged at error level -- only a warning (yt-dlp's
        # report_warning(), not report_error()) actually said why. Before
        # capturing warnings too, this surfaced as the useless literal
        # "unknown reason (no file produced, no error captured)".
        def fake_extract_info(self, url, download=True):
            self.opts["logger"].warning("unable to download format: HTTP Error 429: Too Many Requests")
            return {"id": _VIDEO_ID}

        monkeypatch.setattr(_FakeYoutubeDL, "extract_info", fake_extract_info)

        with pytest.raises(TransientConnectorError, match="HTTP Error 429"):
            _run_fetch(fetch_env.connector, tmp_path, {"stage": "extracting"})


class TestRateLimitCooldown:
    """YouTube rate-limits/blocks by source IP, not per-video or per-job, so
    once one task sees a 429/403, every other concurrent or subsequent
    YouTube task on this host is equally likely to hit the identical wall.
    Each task's own Celery retry_backoff exhausts in well under a minute --
    far short of a real rate-limit window -- and does nothing to stop
    sibling tasks making the same doomed request meanwhile. These test the
    shared cooldown gate that now makes discover()/fetch() fail fast instead."""

    def test_inactive_by_default(self, monkeypatch):
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: _FakeRedis())
        assert youtube_connector._rate_limit_cooldown_active() is False

    def test_start_makes_it_active(self, monkeypatch):
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)

        assert youtube_connector._rate_limit_cooldown_active() is False
        youtube_connector._start_rate_limit_cooldown(300)
        assert youtube_connector._rate_limit_cooldown_active() is True

    def test_raise_if_active_is_a_noop_when_inactive(self, monkeypatch):
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: _FakeRedis())
        youtube_connector._raise_if_rate_limit_cooldown_active()  # must not raise

    def test_raise_if_active_raises_when_active(self, monkeypatch):
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
        youtube_connector._start_rate_limit_cooldown(300)

        with pytest.raises(RateLimitedError):
            youtube_connector._raise_if_rate_limit_cooldown_active()

    def test_raise_if_active_carries_the_real_remaining_ttl(self, monkeypatch):
        # Regression: this used to raise RateLimitedError with no
        # retry_after_s at all, so _run_item_download's generic ~1-31s
        # exponential backoff (nowhere close to a 300s cooldown) exhausted
        # every item caught mid-cooldown and permanently recorded it as
        # failed instead of just waiting the cooldown out.
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
        youtube_connector._start_rate_limit_cooldown(42)

        with pytest.raises(RateLimitedError) as exc_info:
            youtube_connector._raise_if_rate_limit_cooldown_active()

        assert exc_info.value.retry_after_s == 42

    def test_429_is_classified_as_rate_limited_and_starts_the_cooldown(self, monkeypatch):
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)

        result = YouTubeConnector._classify_error(
            RuntimeError("HTTP Error 429: Too Many Requests"), "https://youtube.com/watch?v=x", video_id="x"
        )

        assert isinstance(result, RateLimitedError)
        assert youtube_connector._rate_limit_cooldown_active() is True

    def test_403_also_starts_the_cooldown(self, monkeypatch):
        # Not reclassified as RateLimitedError (403 is only "likely" a rate
        # limit per the existing message, not a certain one like 429) but
        # still shares the same cooldown -- both symptoms mean this host's
        # requests are currently being refused.
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)

        result = YouTubeConnector._classify_error(
            RuntimeError("HTTP Error 403: Forbidden"), "https://youtube.com/watch?v=x", video_id="x"
        )

        assert isinstance(result, TransientConnectorError)
        assert youtube_connector._rate_limit_cooldown_active() is True

    def test_incomplete_read_also_starts_the_cooldown(self, monkeypatch):
        # Regression for video _RN5IrQy168: connection cut under 20KB into a
        # ~10.3MB file, 10/10 real attempts, byte offset varying each time --
        # not deterministic enough for FatalConnectorError, but the same
        # class of CDN anti-automation behavior as 403/429.
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)

        result = YouTubeConnector._classify_error(
            RuntimeError("[download] Got error: 906 bytes read, 10315295 more expected. Giving up after 5 retries"),
            "https://youtube.com/watch?v=x",
            video_id="x",
        )

        assert isinstance(result, TransientConnectorError)
        assert not isinstance(result, FatalConnectorError)
        assert youtube_connector._rate_limit_cooldown_active() is True

    def test_other_errors_do_not_start_a_cooldown(self, monkeypatch):
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)

        YouTubeConnector._classify_error(RuntimeError("some unrelated failure"), "https://youtube.com/watch?v=x")

        assert youtube_connector._rate_limit_cooldown_active() is False

    def test_fetch_fails_fast_without_calling_yt_dlp_when_cooldown_active(self, tmp_path: Path, fetch_env):
        youtube_connector._start_rate_limit_cooldown(300)

        with pytest.raises(RateLimitedError):
            _run_fetch(fetch_env.connector, tmp_path, {})

        assert fetch_env.downloads == []  # no real yt-dlp call was made

    def test_fetch_proceeds_normally_once_cooldown_is_not_active(self, tmp_path: Path, fetch_env):
        # fetch_env's fake Redis starts empty (inactive) -- a normal fetch
        # must be unaffected by the mere existence of the cooldown check.
        _run_fetch(fetch_env.connector, tmp_path, {})

        assert len(fetch_env.downloads) == 1

    def test_discover_skips_search_and_reports_the_cooldown_when_active(self, monkeypatch):
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
        youtube_connector._start_rate_limit_cooldown(300)

        def _must_not_be_called(*args, **kwargs):
            raise AssertionError("search must not run while the cooldown is active")

        monkeypatch.setattr(youtube_connector, "search_videos", _must_not_be_called)
        monkeypatch.setattr(youtube_connector, "search_videos_ytdlp", _must_not_be_called)

        page = YouTubeConnector().discover("cats", cursor=None, limit=10)

        assert page.items == []
        # Not exhausted: a cooldown is temporary. discover_youtube previously
        # treated any zero-item page as permanent exhaustion regardless of
        # this flag, ending the whole job's YouTube source over a few
        # minutes' wait -- retry_after_s is what actually fixes that (it
        # tells the caller to reschedule itself instead), checked below.
        assert page.exhausted is False
        assert "cooldown" in (page.error_reason or "").lower()
        assert page.retry_after_s == 300

    def test_discover_cooldown_page_retry_after_s_reflects_remaining_ttl(self, monkeypatch):
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
        youtube_connector._start_rate_limit_cooldown(42)

        page = YouTubeConnector().discover("cats", cursor=None, limit=10)

        assert page.retry_after_s == 42

    def test_cooldown_remaining_floors_to_a_positive_number_when_ttl_is_gone(self, monkeypatch):
        # A cleared/expired key (ttl() == -2) or one Redis reports as -1 must
        # still produce a real, positive countdown -- never a 0/negative
        # retry_after_s, which would just re-run the same check instantly.
        fake_redis = _FakeRedis()
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
        fake_redis.store[youtube_connector._RATE_LIMIT_COOLDOWN_KEY] = "1"  # no ttl recorded -> ttl() returns -1

        assert youtube_connector._rate_limit_cooldown_remaining_s() > 0


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class TestSearchVideosPagination:
    """Regression: search_videos used to request a fixed max 50 results and
    never page further, capping every discovery call at 6 variants x 50 =
    300 videos regardless of how many were actually needed."""

    def test_pages_until_max_results_reached(self, monkeypatch):
        pages = [
            {"items": [{"id": {"videoId": f"v{i}"}} for i in range(50)], "nextPageToken": "p2"},
            {"items": [{"id": {"videoId": f"v{i}"}} for i in range(50, 70)], "nextPageToken": "p3"},
        ]
        calls = {"search": 0}

        def fake_get(url, params=None, timeout=None):
            if url.endswith("/search"):
                page = pages[calls["search"]]
                calls["search"] += 1
                return _FakeResponse(page)
            ids = params["id"].split(",")
            return _FakeResponse({"items": [{"id": vid, "status": {"license": "creativeCommon"}} for vid in ids]})

        monkeypatch.setattr(youtube_connector.requests, "get", fake_get)

        items = search_videos("cars", api_key="key", max_results=70)

        assert len(items) == 70
        assert calls["search"] == 2

    def test_stops_when_no_more_pages_even_if_short_of_max_results(self, monkeypatch):
        def fake_get(url, params=None, timeout=None):
            if url.endswith("/search"):
                return _FakeResponse({"items": [{"id": {"videoId": "only"}}]})  # no nextPageToken
            ids = params["id"].split(",")
            return _FakeResponse({"items": [{"id": vid, "status": {"license": "creativeCommon"}} for vid in ids]})

        monkeypatch.setattr(youtube_connector.requests, "get", fake_get)

        items = search_videos("cars", api_key="key", max_results=1000)

        assert len(items) == 1
