"""YouTubeConnector — ports youtube_downloader.py's YouTubeDownloader into the
platform's DownloadConnector protocol.

Reused near-verbatim from the legacy script:
- search_videos(): two-step YouTube Data API v3 call (search -> videos, since
  license lives in contentDetails/status, not the search endpoint).
- build_ytdlp_options(): format cascade, CC match_filter, download_archive,
  politeness sleep/rate-limit settings, ignoreerrors=True, retries=5.
- The three frame-extraction strategies: _extract_scene (PySceneDetect
  ContentDetector(threshold=27.0, min_scene_len=15), grabs the middle frame
  per scene, falls back to fps=1 on zero scenes or scenedetect failure),
  _extract_fps (ffmpeg subprocess), _extract_every_n (pure OpenCV).

Refactored:
- Removed the PATH-shimming for deno/nvm (youtube_downloader.py lines 57-72)
  — the worker Docker image bakes in deno+ffmpeg instead (see
  docker/Dockerfile.worker), so the runtime environment is deterministic.
- Fixed the confirmed bug where _cleanup_videos() only globbed *.mp4 — now
  covers every media extension yt-dlp/ffmpeg might produce.
- No argparse/input(); credentials come from CredentialService.
- should_stop()/on_progress() wired into the frame-extraction loop so
  pause/cancel/resume work mid-video.
"""

import logging
import subprocess
from collections.abc import Iterator
from pathlib import Path

import requests

from app.connectors.base import (
    DiscoveryPage,
    OnProgressFn,
    RawAssetDraft,
    ShouldStopFn,
    SourceItem,
    register_connector,
)
from app.core.exceptions import FatalConnectorError, JobStopped, TransientConnectorError
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

YT_API_BASE = "https://www.googleapis.com/youtube/v3"
_MEDIA_EXTENSIONS = {".mp4", ".mkv", ".webm", ".avi", ".m4a", ".mov", ".flv"}


def search_videos(
    query: str,
    api_key: str,
    max_results: int = 20,
    license_filter: str | None = "creativeCommon",
) -> list[SourceItem]:
    if not api_key:
        return []

    params = {
        "key": api_key,
        "q": query,
        "type": "video",
        "part": "snippet",
        "maxResults": min(max_results, 50),
        "order": "relevance",
    }
    if license_filter:
        params["videoLicense"] = license_filter

    try:
        resp = requests.get(f"{YT_API_BASE}/search", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"YouTube API search failed: {e}")
        return []

    video_ids = [item["id"]["videoId"] for item in data.get("items", []) if item.get("id", {}).get("videoId")]
    if not video_ids:
        return []

    try:
        det_resp = requests.get(
            f"{YT_API_BASE}/videos",
            params={"key": api_key, "id": ",".join(video_ids), "part": "snippet,contentDetails,status"},
            timeout=15,
        )
        det_resp.raise_for_status()
        detail_data = det_resp.json()
    except Exception as e:
        logger.error(f"YouTube API details fetch failed: {e}")
        return []

    items = []
    for v in detail_data.get("items", []):
        vid_id = v["id"]
        status = v.get("status", {})
        vid_license = status.get("license", "youtube")
        if license_filter == "creativeCommon" and vid_license != "creativeCommon":
            continue
        items.append(
            SourceItem(
                source_type=SourceType.YOUTUBE,
                external_ref={"url": f"https://www.youtube.com/watch?v={vid_id}", "video_id": vid_id},
                license_hint=vid_license,
                dataset_identifier=vid_id,
            )
        )
    return items


class _ErrorCapturingLogger:
    """yt-dlp's `ignoreerrors=True` swallows download failures (extract_info
    returns a populated info dict even though no file was ever written), so
    the only way to learn *why* is to intercept the messages yt-dlp would
    otherwise just print — without this, a real cause like a 403 from
    YouTube's CDN gets reported to operators as an opaque "file not found",
    which is indistinguishable from an actual code bug."""

    def __init__(self) -> None:
        self.last_error: str | None = None

    def debug(self, msg: str) -> None:
        pass

    def warning(self, msg: str) -> None:
        pass

    def error(self, msg: str) -> None:
        self.last_error = msg


def build_ytdlp_options(
    output_dir: Path,
    archive_file: Path,
    max_downloads: int = 1,
    rate_limit: str = "5M",
    cc_only: bool = True,
) -> dict:
    opts = {
        "format": (
            "best[ext=mp4][vcodec!=none][acodec!=none][height<=1080]"
            "/best[vcodec!=none][acodec!=none][height<=1080]"
            "/bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]"
            "/bestvideo[height<=1080]+bestaudio"
            "/best"
        ),
        "merge_output_format": "mp4",
        "outtmpl": str(output_dir / "%(id)s.%(ext)s"),
        "download_archive": str(archive_file),
        "limit_rate": rate_limit,
        "sleep_interval": 3,
        "max_sleep_interval": 10,
        "sleep_interval_requests": 1,
        "ignoreerrors": True,
        "no_warnings": True,
        "quiet": True,
        "writeinfojson": True,
        "writesubtitles": False,
        "max_downloads": max_downloads,
        "retries": 5,
        "fragment_retries": 10,
        "file_access_retries": 3,
        "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
        "extractor_args": {"youtube": {"client": ["ios", "android"]}},
    }
    if cc_only:
        opts["match_filter"] = _cc_only_match_filter
    return opts


def _cc_only_match_filter(info_dict: dict, *, incomplete: bool = False) -> str | None:
    """Was previously `match_filter_func("license = creativecommon")`, which
    uses yt-dlp's filter-DSL `=` (exact string equality) operator against
    `info_dict["license"]`. That field is always yt-dlp's human-readable
    license label (observed: "Creative Commons Attribution license (reuse
    allowed)"), which can never equal the literal string "creativecommon" —
    so the old filter silently rejected every video regardless of its real
    license, making cc_only=True (the default) download nothing, ever, with
    no error surfaced (match_filter skips are not failures to yt-dlp). This
    checks for the substring instead, matching what the field actually
    contains."""
    if incomplete:
        return None
    license_name = (info_dict.get("license") or "").lower()
    if "creative commons" in license_name:
        return None
    return f"Skipping non-CC-licensed video (license={info_dict.get('license')!r})"


@register_connector(SourceType.YOUTUBE)
class YouTubeConnector:
    def __init__(
        self,
        frame_mode: str = "scene",
        frames_per_video: int = 100,
        min_resolution: tuple[int, int] = (320, 240),
        cc_only: bool = True,
    ):
        self.frame_mode = frame_mode
        self.frames_per_video = frames_per_video
        self.min_w, self.min_h = min_resolution
        self.cc_only = cc_only

    def discover(self, query: str, cursor: str | None, limit: int, api_key: str = "") -> DiscoveryPage:
        items = search_videos(query, api_key, max_results=limit, license_filter="creativeCommon" if self.cc_only else None)
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True)

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
    ) -> Iterator[RawAssetDraft]:
        import yt_dlp

        url = item.external_ref["url"]
        video_dir = workdir / "videos"
        frames_dir = workdir / "frames"
        video_dir.mkdir(parents=True, exist_ok=True)
        frames_dir.mkdir(parents=True, exist_ok=True)
        archive_file = workdir / "yt_archive.txt"

        stage = checkpoint.get("stage", "downloading")
        video_path: Path | None = None

        if stage == "downloading":
            opts = build_ytdlp_options(video_dir, archive_file, cc_only=self.cc_only)
            error_logger = _ErrorCapturingLogger()
            opts["logger"] = error_logger
            info = None
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
            except yt_dlp.utils.MaxDownloadsReached:
                # We only ever request one URL per fetch() with
                # `max_downloads: 1`, but yt-dlp raises this as its stop
                # signal *after* successfully writing the file, not only
                # when a limit is exceeded mid-playlist — so this fires on
                # every ordinary single-video download, not just when
                # something is actually wrong. Treating it as an error (the
                # previous behavior) discarded videos that had already
                # downloaded successfully to disk. Fall through to the same
                # candidate-file lookup as the success path below, using the
                # item's own video_id since we have no `info` dict here.
                pass
            except Exception as e:
                raise self._classify_error(e, url)

            if info is None and not item.external_ref.get("video_id"):
                raise FatalConnectorError(
                    f"No video info returned for {url} — likely skipped by the CC license filter"
                )

            video_id = info.get("id") if info is not None else item.external_ref["video_id"]
            candidates = [
                p for p in video_dir.glob(f"{video_id}.*")
                if p.suffix.lower() in _MEDIA_EXTENSIONS
            ]
            if not candidates:
                reason = error_logger.last_error or "unknown reason (no file produced, no error captured)"
                raise self._classify_error(RuntimeError(reason), url, video_id=video_id)
            video_path = candidates[0]
            license_name = info.get("license", "unknown") if info is not None else (item.license_hint or "unknown")
            on_progress({"checkpoint": {"stage": "extracting", "frame_index": 0}, "downloaded": 1})
        else:
            video_id = item.external_ref.get("video_id", "")
            candidates = [p for p in video_dir.glob(f"{video_id}.*") if p.suffix.lower() in _MEDIA_EXTENSIONS]
            video_path = candidates[0] if candidates else None
            license_name = item.license_hint or "unknown"

        if video_path is None or not video_path.exists():
            raise FatalConnectorError(f"No local video file to extract frames from for {url}")

        start_index = checkpoint.get("frame_index", 0) if stage == "extracting" else 0
        yield from self._extract_frames(
            video_id, video_path, url, license_name, frames_dir, start_index, should_stop, on_progress
        )

        self._cleanup_video_files(video_dir, video_id)

    def _extract_frames(
        self, video_id, video_path, video_url, license_name, frames_dir, start_index, should_stop, on_progress
    ) -> Iterator[RawAssetDraft]:
        out_dir = frames_dir / video_id
        out_dir.mkdir(parents=True, exist_ok=True)

        if self.frame_mode == "scene":
            frames = self._extract_scene(video_path, out_dir)
        elif self.frame_mode == "fps":
            frames = self._extract_fps(video_path, out_dir)
        else:
            frames = self._extract_every_n(video_path, out_dir)

        for i, (frame_path, frame_number, timestamp_sec, scene_number, w, h) in enumerate(frames):
            if i < start_index:
                continue
            if i >= self.frames_per_video:
                break
            if should_stop():
                on_progress({"checkpoint": {"stage": "extracting", "frame_index": i}})
                raise JobStopped("should_stop() signalled during YouTube frame extraction")
            if w < self.min_w or h < self.min_h:
                continue

            source_item_id = f"{video_id}_f{i:07d}"
            draft = RawAssetDraft(
                source_item_id=source_item_id,
                source_url=video_url,
                dataset_identifier=video_id,
                license=license_name,
                local_tmp_path=frame_path,
                original_name=frame_path.name,
                width=w,
                height=h,
                metadata={
                    "frame_number": frame_number,
                    "timestamp_sec": timestamp_sec,
                    "scene_number": scene_number,
                },
            )
            on_progress({"checkpoint": {"stage": "extracting", "frame_index": i + 1}, "downloaded": 1})
            yield draft

    def _extract_scene(self, vpath: Path, out_dir: Path):
        import cv2

        try:
            from scenedetect import SceneManager, open_video
            from scenedetect.detectors import ContentDetector

            video_obj = open_video(str(vpath))
            manager = SceneManager()
            manager.add_detector(ContentDetector(threshold=27.0, min_scene_len=15))
            manager.detect_scenes(video_obj, show_progress=False)
            scene_list = manager.get_scene_list()
        except Exception as e:
            logger.error(f"Scene detection failed for {vpath.stem}: {e}")
            yield from self._extract_fps(vpath, out_dir, fps=1.0)
            return

        if not scene_list:
            yield from self._extract_fps(vpath, out_dir, fps=1.0)
            return

        cap = cv2.VideoCapture(str(vpath))
        source_fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        for scene_num, (start_time, end_time) in enumerate(scene_list):
            mid_f = (start_time.get_frames() + end_time.get_frames()) // 2
            cap.set(cv2.CAP_PROP_POS_FRAMES, mid_f)
            ret, frame = cap.read()
            if not ret or frame is None:
                continue
            h, w = frame.shape[:2]
            fname = f"scene_{scene_num:05d}_f{mid_f:07d}.jpg"
            fpath = out_dir / fname
            if cv2.imwrite(str(fpath), frame, [cv2.IMWRITE_JPEG_QUALITY, 92]):
                yield fpath, mid_f, round(mid_f / source_fps, 3), scene_num, w, h
        cap.release()

    def _extract_fps(self, vpath: Path, out_dir: Path, fps: float = 1.0):
        import cv2

        out_pattern = str(out_dir / "fps_%07d.jpg")
        cmd = ["ffmpeg", "-i", str(vpath), "-vf", f"fps={fps}", "-q:v", "2", out_pattern, "-loglevel", "quiet", "-y"]
        try:
            subprocess.run(cmd, check=True, timeout=3600)
        except FileNotFoundError:
            raise FatalConnectorError("ffmpeg not found on PATH")
        except subprocess.CalledProcessError as e:
            raise TransientConnectorError(f"ffmpeg failed for {vpath.stem}: {e}")
        except subprocess.TimeoutExpired:
            raise TransientConnectorError(f"ffmpeg timeout for {vpath.stem}")

        cap = cv2.VideoCapture(str(vpath))
        source_fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        cap.release()

        for i, fpath in enumerate(sorted(out_dir.glob("fps_*.jpg"))):
            img = cv2.imread(str(fpath))
            if img is None:
                continue
            h, w = img.shape[:2]
            timestamp = i / fps
            yield fpath, int(timestamp * source_fps), round(timestamp, 3), 0, w, h

    def _extract_every_n(self, vpath: Path, out_dir: Path, every_n: int = 30):
        import cv2

        cap = cv2.VideoCapture(str(vpath))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        source_fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        frame_num = 0
        while frame_num < total_frames:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
            ret, frame = cap.read()
            if not ret:
                break
            h, w = frame.shape[:2]
            fpath = out_dir / f"every_{frame_num:07d}.jpg"
            cv2.imwrite(str(fpath), frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
            yield fpath, frame_num, round(frame_num / source_fps, 3), 0, w, h
            frame_num += every_n
        cap.release()

    @staticmethod
    def _cleanup_video_files(video_dir: Path, video_id: str) -> None:
        """Fixes the legacy script's bug of only globbing *.mp4 — deletes any
        media-extension file for this video_id after successful extraction."""
        for ext in _MEDIA_EXTENSIONS:
            for f in video_dir.glob(f"{video_id}{ext}"):
                f.unlink(missing_ok=True)

    @staticmethod
    def _classify_error(e: Exception, url: str, video_id: str | None = None) -> Exception:
        msg = str(e).lower()
        ref = f"video_id={video_id}" if video_id else url
        if "private" in msg or "unavailable" in msg or "removed" in msg or "copyright" in msg:
            return FatalConnectorError(f"Video unavailable ({ref}): {e}")
        if "403" in msg or "forbidden" in msg:
            return TransientConnectorError(
                f"YouTube CDN refused the video download for {ref} (HTTP 403 — this host's network is "
                f"likely being rate-limited/blocked by YouTube for automated downloads, not a code bug): {e}"
            )
        return TransientConnectorError(f"yt-dlp failed for {ref}: {e}")
