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
