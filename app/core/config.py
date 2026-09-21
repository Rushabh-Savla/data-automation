"""Application configuration, loaded from environment variables / .env."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # -- Database ------------------------------------------------------------
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/data_automation"

    # -- Redis (Celery broker/result backend + pub/sub + control flags) ------
    redis_url: str = "redis://localhost:6379/0"

    # -- Storage ---------------------------------------------------------------
    # Root directory the NAS is mounted at (or a local path for dev).
    nas_root: Path = Path("./nas_data")
    # Scratch space for in-flight downloads before they're promoted to NAS.
    work_root: Path = Path("./work_data")

    # -- Auth --------------------------------------------------------------------
    # Comma-separated list of valid API keys. Any one of them is accepted.
    api_keys: str = "dev-local-key"

    # -- CORS --------------------------------------------------------------------
    # Comma-separated list of allowed frontend origins. Only needed when the
    # frontend is served from a different origin than the API with no reverse
    # proxy in front of both (a same-origin dev proxy or prod nginx setup never
    # triggers CORS at all — see frontend/vite.config.ts and docker/nginx.frontend.conf).
    cors_allowed_origins: str = "http://localhost:5173"

    # -- Credential encryption -----------------------------------------------------
    # Fernet key (32 url-safe base64-encoded bytes). Generate with:
    #   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    credential_encryption_key: str = ""

    # -- Celery tuning -------------------------------------------------------------
    # Raised from 1800/2100 (30/35 min): several connectors' shared one-time
    # cache downloads (KITTI ~12GB, SUN RGB-D ~6.9GB) can legitimately take
    # longer than 30 minutes on a real connection — the old limits would
    # kill those tasks outright regardless of the watchdog/heartbeat fix
    # below. watchdog_stall_threshold_s is set well under these so the
    # stall safety net still fires with headroom before Celery's own kill.
    celery_task_soft_time_limit_s: int = 3600
    celery_task_time_limit_s: int = 3900
    watchdog_stall_threshold_s: int = 900

    # Separate, longer threshold for the *post-ingestion* stall: a job whose
    # source tasks are all terminal but which is still RUNNING because the
    # relevance-filter/dedup chain died part-way. That chain has no
    # heartbeat of its own (it is not a JobSourceTask), so
    # watchdog_stall_threshold_s above cannot see it — a job in that state
    # used to sit in `running` indefinitely with nothing able to re-drive
    # it. 10 minutes: a healthy batch of 50 takes ~11s on the GPU and bumps the
    # job's updated_at as it finishes, so ten idle minutes with every source
    # terminal is a dead chain, not a slow one. It was 30 (from when a CPU-only batch
    # could take ~55 min), and a COCO job idled the whole 30 after a restart with all
    # 1507 images already judged. Re-driving a pass that is merely slow is safe and
    # only wastes some inference.
    filtering_stall_threshold_s: int = 600
    # After a restart, work last seen before the latest worker boot is resumed
    # once this long has passed (app/domain/recovery.py), instead of waiting out
    # the two thresholds above. Must exceed the heartbeat interval (60s) so work
    # on a worker that was not restarted has time to heartbeat again first.
    watchdog_restart_grace_s: int = 120

    # yt-dlp's extract_info() is a single blocking call with no timeout of
    # its own — a connection that stalls after the handshake (accepted but
    # no data) can hang indefinitely without this. Bounds each socket
    # operation so yt-dlp's existing retries/backoff can actually kick in.
    youtube_socket_timeout_s: int = 30

    # -- Fast-path source selection --------------------------------------------------
    # For small target counts, fanning out to every connector at once wastes time —
    # Kaggle/Roboflow download whole datasets and YouTube downloads+extracts video
    # frames, all much slower than web scraping for a handful of images. When a job
    # is auto-sourced (no explicit `sources` list) and asks for at most this many
    # images, only web scraping is dispatched first; the remaining connectors are
    # only brought in afterward if web scraping alone didn't reach the target (see
    # JobService.create() and orchestration_tasks.check_and_advance_job()).
    fast_path_enabled: bool = True
    fast_path_max_target_count: int = 50

    # -- Target count vs delivered count ----------------------------------------------
    # Discovery/download stops as soon as target_count PROMOTED assets exist, but
    # relevance filtering and dedup run afterward and can only reduce that count —
    # with no buffer, a job that fetches exactly target_count often delivers fewer
    # once irrelevant/duplicate assets are removed. Fetching this many times
    # target_count up front gives filtering enough raw material that the survivors
    # still land on target_count in the common case (see build_on_progress in
    # app/tasks/_helpers.py); trim_excess_assets (app/tasks/filtering_tasks.py)
    # trims any overshoot back down to exactly target_count afterward.
    # Hard ceiling on discover -> filter -> re-discover rounds per job
    # (app/domain/topup.py). A top-up loop that can run forever is worse than
    # no loop at all: it burns GPU and API quota on a query with no real
    # supply. Three softer brakes usually stop it first -- target met, all
    # sources exhausted, diminishing returns -- and the deadline sweep in
    # watchdog_scan is a fourth. This is the backstop for when none of those
    # fire.
    topup_max_rounds: int = 5

    # First-round prior only. Once a round has completed, the top-up planner
    # measures the query's actual post-filter yield and sizes the next round
    # from that instead -- a fixed multiplier cannot absorb an attrition rate
    # that varies per query (a real job needed ~7x, not 1.5x).
    target_overfetch_buffer: float = 1.5

    # -- Source credentials (fallback if not set via the Credential API) -----------
    roboflow_api_key: str = ""
    kaggle_username: str = ""
    kaggle_key: str = ""
    youtube_api_key: str = ""

    @property
    def api_key_set(self) -> set[str]:
        return {k.strip() for k in self.api_keys.split(",") if k.strip()}

    @property
    def cors_allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
