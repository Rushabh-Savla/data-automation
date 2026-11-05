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

Changes (2024-09):
- frame_mode now defaults to "fps" (was "scene") so the connector reliably
  produces exactly one frame per second, matching the documented behaviour.
  "scene" is still available but must be requested explicitly — its frame
  count is variable and its fps=1 fallback was already identical to this
  default, so switching the default costs nothing for the common case.
- extraction_fps constructor param (default 1.0) is forwarded into
  _extract_fps() so callers can adjust density (e.g. 2.0) without a mode
  switch. The scene-fallback path always uses 1.0 regardless.
- frames_per_video defaults to None (no hard cap). At fps=1.0 the video's
  own duration limits output naturally. An explicit integer is still
  honoured as a hard ceiling when set.
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
from app.core.config import get_settings
from app.core.exceptions import (
    CandidateWindowFull,
    FatalConnectorError,
    JobStopped,
    RateLimitedError,
    TransientConnectorError,
)
from app.domain.enums import SourceType

logger = logging.getLogger(__name__)

YT_API_BASE = "https://www.googleapis.com/youtube/v3"
_MEDIA_EXTENSIONS = {".mp4", ".mkv", ".webm", ".avi", ".m4a", ".mov", ".flv"}

# ffmpeg's own diagnosis (captured from stderr, see _classify_ffmpeg_error)
# when the input file is truncated/incomplete rather than genuinely
# unreadable for some other reason. Observed cause: YouTube's evolving
# "SABR-only" server-side restriction (see build_ytdlp_options' extractor_args
# comment) can serve only the first few hundred KB of even itag 18 (the
# fallback 360p muxed format) while yt-dlp still reports "100% downloaded" and
# the container's own duration metadata still claims the full video length.
# Re-downloading the same URL/format reproduces the identical truncated bytes
# every time (verified directly) -- it is not a transient network blip, so
# retrying it (the previous behavior) only burns all 5 retries for a
# guaranteed-identical failure.
_TRUNCATED_STREAM_SIGNATURES = (
    "partial file",
    "moov atom not found",
    "invalid data found when processing input",
    "cannot determine format",
)

# -- Rate-limit cooldown ----------------------------------------------------
# YouTube rate-limits by IP, not per-video — once one task hits a 429/403,
# every other task on this host will too. This is a shared Redis flag: while
# it's set, discover()/fetch() fail fast instead of making a request that
# would likely fail the same way.
_RATE_LIMIT_COOLDOWN_KEY = "youtube:rate_limit_cooldown"


def _start_rate_limit_cooldown(cooldown_s: int) -> None:
    from app.core.redis_client import get_redis

    get_redis().set(_RATE_LIMIT_COOLDOWN_KEY, "1", ex=cooldown_s)


def _rate_limit_cooldown_active() -> bool:
    from app.core.redis_client import get_redis

    return bool(get_redis().exists(_RATE_LIMIT_COOLDOWN_KEY))


def _rate_limit_cooldown_remaining_s() -> float:
    """Seconds left on the cooldown, so callers can reschedule instead of
    failing outright. Floors to 5s if the TTL is already gone."""
    from app.core.redis_client import get_redis

    ttl = get_redis().ttl(_RATE_LIMIT_COOLDOWN_KEY)
    return max(5.0, ttl) if ttl and ttl > 0 else 5.0


def _raise_if_rate_limit_cooldown_active() -> None:
    if _rate_limit_cooldown_active():
        raise RateLimitedError(
            "YouTube rate-limit cooldown is active for this host",
            retry_after_s=_rate_limit_cooldown_remaining_s(),
        )


# A single literal query has a finite pool of matching videos, and cc_only
# filtering (default on) shrinks that further — mirrors web_connector.py's
# _QUERY_VARIANT_SUFFIXES fix for the same "job stops way short of target"
# problem (there: ~16 images instead of the requested 500). Generic
# rephrasings pull differently-ranked search results for the same subject;
# downstream relevance filtering (app/filtering/vlm_filter.py) is what keeps this
# safe — broadening recall here only adds candidates for filtering to
# reject, it can't let anything irrelevant through on its own.
_QUERY_VARIANT_SUFFIXES = (
    "",
    " video",
    " footage",
    " scene",
    " clip",
    " compilation",
)
_DISCOVER_MAX_VARIANT_ATTEMPTS = len(_QUERY_VARIANT_SUFFIXES)


_SEARCH_PAGE_SIZE = 50  # YouTube Data API's own per-page ceiling
_SEARCH_MAX_PAGES = 20  # safety bound on the pagination loop below, not a real cap


def search_videos(
    query: str,
    api_key: str,
    max_results: int = 20,
    license_filter: str | None = "creativeCommon",
) -> list[SourceItem]:
    """YouTube Data API v3 search — requires a valid api_key.

    Pages via nextPageToken until max_results is reached; a single page
    tops out at 50 (the API's own limit), not max_results itself.

    Returns an empty list (not an exception) on any API failure so the
    caller can fall back to search_videos_ytdlp() without special-casing
    the two error modes (missing key vs. API call failed).
    """
    if not api_key:
        return []

    items: list[SourceItem] = []
    page_token = None
    for _ in range(_SEARCH_MAX_PAGES):
        params = {
            "key": api_key,
            "q": query,
            "type": "video",
            "part": "snippet",
            "maxResults": min(_SEARCH_PAGE_SIZE, max_results - len(items)),
            "order": "relevance",
        }
        if license_filter:
            params["videoLicense"] = license_filter
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = requests.get(f"{YT_API_BASE}/search", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"YouTube API search failed: {e}")
            break

        video_ids = [
            item["id"]["videoId"]
            for item in data.get("items", [])
            if item.get("id", {}).get("videoId")
        ]
        if video_ids:
            items.extend(_video_details(video_ids, api_key, license_filter))

        page_token = data.get("nextPageToken")
        if not page_token or len(items) >= max_results:
            break

    return items


def _video_details(video_ids: list[str], api_key: str, license_filter: str | None) -> list[SourceItem]:
    """Resolves search-result video IDs to full SourceItems (license lives
    in contentDetails/status, not the search endpoint)."""
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


def search_videos_ytdlp(
    query: str,
    max_results: int = 20,
    cc_only: bool = False,
) -> list[SourceItem]:
    """yt-dlp based search — no API key required.

    Uses yt-dlp's built-in ytsearch extractor which hits the same internal
    YouTube endpoint the web UI uses, bypassing the Data API v3 quota
    entirely. This is the automatic fallback when no YOUTUBE_API_KEY
    credential is configured.

    License handling: yt-dlp populates info_dict["license"] from the video's
    metadata the same way it does during download (the field is extracted
    from the watch page, not the Data API). We apply the same
    _cc_only_match_filter substring check used in fetch() so CC filtering
    is consistent regardless of which search path produced the SourceItem.

    extract_flat="in_playlist" tells yt-dlp to collect video metadata
    without downloading anything — it returns info dicts with id, title,
    and (crucially) license. quiet=True + ignoreerrors=True suppress
    per-video warnings for age-gated/geo-blocked entries so one bad result
    doesn't abort the whole search.
    """
    import yt_dlp

    search_url = f"ytsearch{max_results}:{query}"
    opts = {
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "extract_flat": "in_playlist",
        # Explicitly request the iOS/android clients so yt-dlp can resolve
        # metadata for videos that block the web client — same
        # extractor_args used in build_ytdlp_options() for actual downloads.
        # Key is "player_client", NOT "client" — the latter is silently
        # ignored by yt-dlp's youtube extractor (not a recognized
        # configuration arg), which is why this was never actually taking
        # effect until this fix.
        "extractor_args": {"youtube": {"player_client": ["ios", "android"]}},
    }

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(search_url, download=False)
    except Exception as e:
        logger.error(f"yt-dlp search failed for query {query!r}: {e}")
        return []

    if info is None:
        return []

    entries = info.get("entries") or []
    items: list[SourceItem] = []
    for entry in entries:
        if entry is None:
            continue
        vid_id = entry.get("id")
        if not vid_id:
            continue

        # yt-dlp's flat extraction may omit "license" for some entries —
        # treat missing as non-CC so we don't accidentally include a
        # standard-license video when cc_only=True.
        license_name: str = entry.get("license") or ""
        if cc_only and "creative commons" not in license_name.lower():
            continue

        license_hint = license_name if license_name else "unknown"
        items.append(
            SourceItem(
                source_type=SourceType.YOUTUBE,
                external_ref={
                    "url": f"https://www.youtube.com/watch?v={vid_id}",
                    "video_id": vid_id,
                },
                license_hint=license_hint,
                dataset_identifier=vid_id,
            )
        )

    logger.info(
        f"yt-dlp search for {query!r}: {len(entries)} candidates, "
        f"{len(items)} passed CC filter (cc_only={cc_only})"
    )
    return items


class _ErrorCapturingLogger:
    """yt-dlp's `ignoreerrors=True` swallows download failures (extract_info
    returns a populated info dict even though no file was ever written), so
    the only way to learn *why* is to intercept the messages yt-dlp would
    otherwise just print — without this, a real cause like a 403 from
    YouTube's CDN gets reported to operators as an opaque "file not found",
    which is indistinguishable from an actual code bug.

    `last_warning` exists because plenty of real download failures under
    ignoreerrors=True are only ever reported via yt-dlp's report_warning()
    (logger.warning()), not report_error() -- observed directly: a video that
    intermittently produced no file at all, with last_error staying None
    every time (fetch()'s only fallback then was the unhelpful literal
    "unknown reason (no file produced, no error captured)"). Kept separate
    from last_error (not overwritten by it) so a real error is never masked
    by an unrelated, often-benign warning (e.g. every single call logs the
    same SABR-format-availability warning regardless of outcome) — fetch()
    only falls back to last_warning when last_error was never set at all.
    """

    def __init__(self) -> None:
        self.last_error: str | None = None
        self.last_warning: str | None = None

    def debug(self, msg: str) -> None:
        pass

    def warning(self, msg: str) -> None:
        self.last_warning = _clean_log_message(msg)

    def error(self, msg: str) -> None:
        self.last_error = _clean_log_message(msg)


def _clean_log_message(msg: str) -> str:
    """Strips the raw '\\r' yt-dlp embeds when a message follows an
    overwritten progress line (observed verbatim in production:
    "ERROR: \\r[download] Got error: ..."), which otherwise flows straight
    into job_source_tasks.error_message and is rendered as-is by
    JobSourceTaskTable.tsx -- a literal carriage return in the middle of the
    string a user reads on the job detail page. '\\r' is a progress-bar
    redraw convention (each segment overwrites the previous on a real
    terminal); only the text after the last one is the actual final message.
    """
    return msg.rsplit("\r", 1)[-1].strip()


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
            "/bestvideo[height<=1080]"
            "/bestvideo"
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
        # Low on purpose: download_youtube_video (ingest_tasks.py) already
        # retries the WHOLE task up to 5x with growing backoff (retry_backoff,
        # capped at 600s) -- that outer retry is what actually gives a flaky
        # connection time to recover. yt-dlp's own retries here fire back-to-
        # back inside a single attempt with no comparable wait, so leaving
        # this at 5 meant a single stubborn video could cost up to 5 (outer)
        # x 5 (inner) = 25 raw HTTP hits to YouTube's CDN before ever giving
        # up (observed directly: video _RN5IrQy168, every one of them an
        # early connection drop under 20KB in). That's needless load on top
        # of being exactly the kind of aggressive-automated-traffic pattern
        # that gets a host rate-limited/blocked in the first place. 2 still
        # absorbs a one-off blip within a single attempt; the outer retry
        # covers everything beyond that, with actual backoff.
        "retries": 2,
        "fragment_retries": 10,
        "file_access_retries": 3,
        # Bounds each socket op so a connection that stalls after the
        # handshake (accepted, no data) fails and retries instead of
        # hanging extract_info() indefinitely.
        "socket_timeout": get_settings().youtube_socket_timeout_s,
        "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
        # Key is "player_client", NOT "client" (see discover_videos_ytdlp's
        # extractor_args for the same fix) — "client" is silently ignored by
        # yt-dlp's youtube extractor, so this client restriction was never
        # actually being applied; yt-dlp fell back to its own default client
        # selection instead, which picks clients (e.g. "ios", "android_vr")
        # whose video-CDN formats require a GVS PO Token yt-dlp doesn't have,
        # causing a hard "HTTP Error 403: Forbidden" on every video's actual
        # data download regardless of video_id — looking exactly like this
        # host's whole network being IP-blocked, but it wasn't. Now that the
        # key actually works, "android" alone gets formats that need no PO
        # token and download cleanly (verified directly against every
        # video_id that was previously 403ing) — capped at itag 18 (360p)
        # since YouTube's SABR-only streaming experiment currently withholds
        # the android client's higher formats without a token, but a working
        # 360p frame source beats a 100%-failing 1080p one.
        "extractor_args": {"youtube": {"player_client": ["android"]}},
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
        # ------------------------------------------------------------------ #
        # frame_mode="fps": extract one frame per second via ffmpeg (default).#
        # This is deterministic, evenly spaced, and matches the documented    #
        # "1 image per second" spec. "scene" (PySceneDetect) and "every_n"   #
        # (raw OpenCV) are still supported but must be requested explicitly.  #
        # ------------------------------------------------------------------ #
        frame_mode: str = "fps",
        # ------------------------------------------------------------------ #
        # extraction_fps: frames per second to extract when frame_mode="fps". #
        # 1.0 = one frame per second (the default). Only affects fps mode;   #
        # scene and every_n modes are not rate-controlled by this param.     #
        # ------------------------------------------------------------------ #
        extraction_fps: float = 1.0,
        # ------------------------------------------------------------------ #
        # frames_per_video: hard upper cap on frames yielded per video.      #
        # None (the default) means no cap — at fps=1.0 the video's own      #
        # duration naturally limits output. Set an integer when you need a   #
        # strict per-video ceiling (e.g. to stop one long video from         #
        # monopolising the job's target_count quota).                        #
        # ------------------------------------------------------------------ #
        frames_per_video: int | None = None,
        min_resolution: tuple[int, int] = (320, 240),
        # cc_only=False: do not restrict to Creative Commons license by default.
        # yt-dlp's flat search metadata often omits the license field entirely,
        # so cc_only=True silently drops every candidate video when no API key
        # is configured (observed: 5 candidates → 0 passed for 'red car').
        # Operators who need strict CC-only ingestion can set
        # config={"youtube": {"cc_only": true}} on the job.
        cc_only: bool = False,
    ):
        self.frame_mode = frame_mode
        self.extraction_fps = extraction_fps
        self.frames_per_video = frames_per_video
        self.min_w, self.min_h = min_resolution
        self.cc_only = cc_only

    def discover(self, query: str, cursor: str | None, limit: int, api_key: str = "") -> DiscoveryPage:
        """Discover YouTube videos for the given query.

        Strategy (API-first, yt-dlp fallback):
        1. If api_key is provided, try the YouTube Data API v3. This is the
           preferred path: it returns richer metadata and can filter by
           license server-side (videoLicense=creativeCommon), reducing the
           number of entries we have to filter ourselves.
        2. If the API key is absent OR the API call returns nothing (quota
           exhausted, key invalid, transient error — search_videos() swallows
           all of these and returns []), fall back to yt-dlp's ytsearch
           extractor. This requires no credentials and is not subject to
           the Data API v3 quota.

        The fallback is transparent to the caller: discover_youtube in
        discovery_tasks.py passes whatever api_key CredentialService resolved
        (may be an empty string) and receives a normal DiscoveryPage either
        way. No task-level changes are needed.

        Loops _QUERY_VARIANT_SUFFIXES (generic rephrasings of the same
        query, e.g. "weapon" -> "weapon footage") accumulating unique
        (by video_id) candidates until `limit` is reached or every variant
        has been tried — a single literal query previously capped discovery
        at whatever one search call returned (as few as 5-20 videos), so a
        job asking for hundreds of accepted frames from a query with heavy
        downstream relevance-filter rejection had no way to make up the
        shortfall; discover() just declared itself exhausted immediately.
        """
        if _rate_limit_cooldown_active():
            # Not exhausted — a cooldown is temporary. discover() never
            # raises, so this reports it via retry_after_s instead, telling
            # the caller to reschedule rather than treat the source as done.
            return DiscoveryPage(
                items=[], next_cursor=None, exhausted=False,
                error_reason="YouTube rate-limit cooldown is active for this host",
                retry_after_s=_rate_limit_cooldown_remaining_s(),
            )

        license_filter = "creativeCommon" if self.cc_only else None
        seen_video_ids: set[str] = set()
        items: list[SourceItem] = []
        api_returned_anything = False

        for suffix in _QUERY_VARIANT_SUFFIXES:
            if len(items) >= limit:
                break
            variant_query = f"{query}{suffix}".strip()

            remaining = limit - len(items)

            # --- Path 1: YouTube Data API v3 ---
            variant_items = search_videos(variant_query, api_key, max_results=remaining, license_filter=license_filter)
            if variant_items:
                api_returned_anything = True
            else:
                # --- Path 2: yt-dlp (no API key needed, or API path came up empty) ---
                variant_items = search_videos_ytdlp(variant_query, max_results=remaining, cc_only=self.cc_only)

            for si in variant_items:
                vid_id = si.external_ref["video_id"]
                if vid_id in seen_video_ids:
                    continue
                seen_video_ids.add(vid_id)
                items.append(si)
                if len(items) >= limit:
                    break

        error_reason: str | None = None
        if not items:
            if api_key and not api_returned_anything:
                logger.warning(
                    f"YouTube Data API returned no results for {query!r} across all query variants "
                    f"(quota exhausted or key invalid?) — yt-dlp fallback also came up empty"
                )
            error_reason = (
                "No YouTube videos found via Data API or yt-dlp search "
                f"for query {query!r} (cc_only={self.cc_only}, tried {_DISCOVER_MAX_VARIANT_ATTEMPTS} query variants)"
            )

        # exhausted=True whenever every variant has been walked (always, in
        # this implementation) — there's no cursor to resume from since each
        # variant is a one-shot search, not a paginated feed.
        return DiscoveryPage(items=items, next_cursor=None, exhausted=True, error_reason=error_reason)

    @staticmethod
    def _local_video(video_dir: Path, video_id: str) -> Path | None:
        """The downloaded video for `video_id` in `video_dir`, if there is one."""
        if not video_id:
            return None
        candidates = [p for p in video_dir.glob(f"{video_id}.*") if p.suffix.lower() in _MEDIA_EXTENSIONS]
        return candidates[0] if candidates else None

    @staticmethod
    def _forget_archived_video(archive_file: Path, video_id: str) -> None:
        """Removes just this video's line from yt-dlp's download_archive file.

        `archive_file` is shared by every video downloaded under the same
        job_source_task (see download_youtube_video: workdir is keyed by
        job_source_task_id, not by video), so a blanket unlink() here would
        make yt-dlp forget every other video already recorded under this
        same task -- including one a sibling fetch() call is concurrently
        mid-download on. Only this video's entry needs to go, so yt-dlp
        stops skipping it as "already downloaded" and actually re-fetches it.
        """
        if not archive_file.exists() or not video_id:
            return
        lines = archive_file.read_text().splitlines()
        kept = [line for line in lines if line.split()[-1:] != [video_id]]
        if kept == lines:
            return
        if kept:
            archive_file.write_text("\n".join(kept) + "\n")
        else:
            archive_file.unlink(missing_ok=True)

    def fetch(
        self,
        item: SourceItem,
        workdir: Path,
        checkpoint: dict,
        should_stop: ShouldStopFn,
        on_progress: OnProgressFn,
        admit_candidate=None,
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

        if stage == "extracting" and self._local_video(video_dir, item.external_ref.get("video_id", "")) is None:
            # Resumed at the "extracting" stage, but the downloaded video is gone: the scratch
            # directory was cleaned up, or the task was re-driven after a restart. This used to be
            # a fatal error ("No local video file to extract frames from"), which lost the video
            # for good -- most recent YouTube jobs ended completed_partial on it. Download it
            # again instead. The archive entry has to go too: yt-dlp would otherwise skip the
            # video as "already downloaded" and produce no file. Frames extracted before the
            # interruption are produced again and skipped as duplicates downstream.
            logger.warning(f"Local video for {url} is missing on resume; downloading it again")
            self._forget_archived_video(archive_file, item.external_ref.get("video_id", ""))
            stage = "downloading"
            checkpoint = {}

        if stage == "downloading":
            _raise_if_rate_limit_cooldown_active()
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
            if info is not None:
                candidates = [
                    p for p in video_dir.glob(f"{video_id}.*")
                    if p.suffix.lower() in _MEDIA_EXTENSIONS
                ]
            else:
                # MaxDownloadsReached fallthrough: no `info` dict, so the id we have
                # may not match the real downloaded filename (e.g. it was parsed from
                # a share-link URL still carrying a tracking query string). video_dir
                # is a fresh per-task workdir holding only this one video, so match
                # on any media file instead of requiring the filename to start with id.
                candidates = [
                    p for p in video_dir.glob("*")
                    if p.suffix.lower() in _MEDIA_EXTENSIONS
                ]
            if not candidates:
                reason = (
                    error_logger.last_error
                    or (f"{error_logger.last_warning} (warning only, no error was logged)" if error_logger.last_warning else None)
                    or "unknown reason (no file produced, no error captured)"
                )
                raise self._classify_error(RuntimeError(reason), url, video_id=video_id)
            video_path = candidates[0]
            license_name = (
                info.get("license", "unknown") if info is not None else (item.license_hint or "unknown")
            )
            # No "downloaded": 1 here -- a video isn't itself an Asset, only
            # the frames extracted from it are, and those are counted below
            # in _extract_frames(). Counting the video too double-counted
            # "downloaded" against "accepted"/"rejected"/"duplicate"/"errors"
            # (which only ever count frames), inflating it by exactly one
            # per video with nothing on the other side to balance it.
            on_progress({"checkpoint": {"stage": "extracting", "frame_index": 0}})
        else:
            video_id = item.external_ref.get("video_id", "")
            video_path = self._local_video(video_dir, video_id)
            license_name = item.license_hint or "unknown"

        if video_path is None or not video_path.exists():
            raise FatalConnectorError(f"No local video file to extract frames from for {url}")

        start_index = checkpoint.get("frame_index", 0) if stage == "extracting" else 0
        try:
            yield from self._extract_frames(
                video_id,
                video_path,
                url,
                license_name,
                frames_dir,
                start_index,
                should_stop,
                on_progress,
                admit_candidate=admit_candidate,
            )
        finally:
            self._cleanup_video_files(video_dir, video_id)
            shutil.rmtree(frames_dir / video_id, ignore_errors=True)

    def _extract_frames(
        self,
        video_id,
        video_path,
        video_url,
        license_name,
        frames_dir,
        start_index,
        should_stop,
        on_progress,
        admit_candidate=None,
    ) -> Iterator[RawAssetDraft]:
        out_dir = frames_dir / video_id
        out_dir.mkdir(parents=True, exist_ok=True)

        if self.frame_mode == "scene":
            frames = self._extract_scene(video_path, out_dir)
        elif self.frame_mode == "fps":
            # Forward the configured rate so callers who set extraction_fps
            # != 1.0 get the density they asked for without a mode switch.
            frames = self._extract_fps(video_path, out_dir, fps=self.extraction_fps)
        else:
            frames = self._extract_every_n(video_path, out_dir)

        for i, (frame_path, frame_number, timestamp_sec, scene_number, w, h) in enumerate(frames):
            if i < start_index:
                continue
            # None means no cap — honour an explicit integer ceiling.
            if self.frames_per_video is not None and i >= self.frames_per_video:
                break
            if should_stop():
                on_progress({"checkpoint": {"stage": "extracting", "frame_index": i}})
                raise JobStopped("should_stop() signalled during YouTube frame extraction")
            if w < self.min_w or h < self.min_h:
                continue

            source_item_id = f"{video_id}_f{i:07d}"

            admission = None
            if admit_candidate is not None:
                admission = admit_candidate(source_item_id)
                raw_status = getattr(admission, "status", "")
                status = str(getattr(raw_status, "value", raw_status)).upper()
                if status == "DUPLICATE":
                    on_progress({"checkpoint": {"stage": "extracting", "frame_index": i + 1}})
                    continue
                if status == "WINDOW_FULL":
                    on_progress({"checkpoint": {"stage": "extracting", "frame_index": i}})
                    raise CandidateWindowFull(
                        getattr(admission, "window_id", None),
                        getattr(admission, "window_index", None),
                        i,
                    )
                if status != "ADMITTED":
                    raise FatalConnectorError(
                        f"Candidate admission failed for {source_item_id}: {status}"
                    )

            metadata = {
                "frame_number": frame_number,
                "timestamp_sec": timestamp_sec,
                "scene_number": scene_number,
            }
            if admission is not None:
                metadata["candidate_reservation_id"] = str(admission.reservation_id)
                metadata["candidate_window_id"] = str(admission.window_id)
                metadata["candidate_window_index"] = int(admission.window_index)

            draft = RawAssetDraft(
                source_item_id=source_item_id,
                source_url=video_url,
                dataset_identifier=video_id,
                license=license_name,
                local_tmp_path=frame_path,
                original_name=frame_path.name,
                width=w,
                height=h,
                metadata=metadata,
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
        """Extract frames at a fixed rate using ffmpeg.

        fps=1.0 produces exactly one frame per second — the default for the
        connector. The fps parameter is explicit (not hard-coded) so both the
        constructor path (self.extraction_fps) and the scene-detection
        fallback path (always 1.0 regardless of self.extraction_fps) can
        share this method without a magic constant buried inside it.
        """
        import cv2

        out_pattern = str(out_dir / "fps_%07d.jpg")
        cmd = [
            "ffmpeg",
            "-i", str(vpath),
            "-vf", f"fps={fps}",
            "-q:v", "2",
            out_pattern,
            # "error" (not "quiet"): quiet discarded ffmpeg's own diagnosis of
            # *why* it failed, leaving only a bare "exit status N" -- captured
            # below so a failure is actually diagnosable instead of requiring
            # a manual repro to find out what ffmpeg was complaining about.
            "-loglevel", "error",
            "-y",
        ]
        try:
            subprocess.run(cmd, check=True, timeout=3600, capture_output=True, text=True)
        except FileNotFoundError:
            raise FatalConnectorError("ffmpeg not found on PATH")
        except subprocess.CalledProcessError as e:
            raise self._classify_ffmpeg_error(e, vpath)
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
    def _classify_ffmpeg_error(e: subprocess.CalledProcessError, vpath: Path) -> Exception:
        """Turns a raw ffmpeg CalledProcessError into a clean, classified one.

        Two things wrong with just re-raising `e` directly (the previous
        behavior): its str() is "Command [<full argv, including this host's
        absolute /work/... container path>] returned non-zero exit status N"
        -- an internal implementation detail with no diagnosis, that then
        gets stored verbatim in job_source_tasks.error_message and rendered
        as-is to the user (see JobSourceTaskTable.tsx). And a permanently
        truncated source (see _TRUNCATED_STREAM_SIGNATURES) was previously
        classified as TransientConnectorError, wasting all 5 retries on an
        identical guaranteed failure instead of failing once, clearly.
        """
        stderr = (e.stderr or "").strip()
        # ffmpeg's actual diagnosis is usually the last line before its generic
        # "Conversion failed!" trailer -- prefer that over the trailer itself.
        lines = [line for line in stderr.splitlines() if line.strip() and line.strip() != "Conversion failed!"]
        reason = lines[-1].strip() if lines else f"ffmpeg exited {e.returncode} (no diagnostic captured)"
        if any(sig in stderr.lower() for sig in _TRUNCATED_STREAM_SIGNATURES):
            return FatalConnectorError(
                f"YouTube served an incomplete/corrupted video stream for {vpath.stem} "
                f"(truncated download, not a network blip -- retrying the same format "
                f"reproduces the identical truncated file): {reason}"
            )
        return TransientConnectorError(f"ffmpeg failed for {vpath.stem}: {reason}")

    @staticmethod
    def _classify_error(e: Exception, url: str, video_id: str | None = None) -> Exception:
        msg = str(e).lower()
        ref = f"video_id={video_id}" if video_id else url
        if "private" in msg or "unavailable" in msg or "removed" in msg or "copyright" in msg:
            return FatalConnectorError(f"Video unavailable ({ref}): {e}")
        if "sign in to confirm" in msg:
            # yt-dlp has no cookies configured in this deployment, so an
            # age/bot-check gate can never succeed no matter how many times
            # it's retried — classifying it as transient just burns 5 retries
            # x up to 600s of backoff before landing in the same place a
            # fatal error would reach immediately (recorded as a per-item
            # error, chord continues with the rest of the job).
            return FatalConnectorError(f"Video requires sign-in/age verification ({ref}): {e}")
        if "429" in msg or "too many requests" in msg:
            _start_rate_limit_cooldown(get_settings().youtube_rate_limit_cooldown_s)
            return RateLimitedError(f"YouTube rate-limited this host for {ref} (HTTP 429): {e}")
        if "403" in msg or "forbidden" in msg:
            _start_rate_limit_cooldown(get_settings().youtube_rate_limit_cooldown_s)
            return TransientConnectorError(f"YouTube CDN refused the download for {ref} (HTTP 403 — likely rate-limited): {e}")
        if "bytes read" in msg and "more expected" in msg:
            # Connection cut early, mid-transfer — same rate-limit signature as 403/429.
            _start_rate_limit_cooldown(get_settings().youtube_rate_limit_cooldown_s)
            return TransientConnectorError(f"YouTube CDN cut the download short for {ref} (likely rate-limited): {e}")
        if "requested format is not available" in msg:
            return FatalConnectorError(f"Requested video format unavailable for {ref}: {e}")
        return TransientConnectorError(f"yt-dlp failed for {ref}: {e}")