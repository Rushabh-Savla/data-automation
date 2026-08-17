"""Regression test for the confirmed bug in youtube_downloader.py's
_cleanup_videos(), which only globbed *.mp4 and silently left .mkv/.webm/etc.
files on disk. The ported YouTubeConnector._cleanup_video_files must cover
every media extension yt-dlp/ffmpeg can produce."""

from pathlib import Path

from app.connectors.youtube_connector import YouTubeConnector, _MEDIA_EXTENSIONS


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
