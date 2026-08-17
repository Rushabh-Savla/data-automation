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

    # -- Credential encryption -----------------------------------------------------
    # Fernet key (32 url-safe base64-encoded bytes). Generate with:
    #   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    credential_encryption_key: str = ""

    # -- Celery tuning -------------------------------------------------------------
    celery_task_soft_time_limit_s: int = 1800
    celery_task_time_limit_s: int = 2100
    watchdog_stall_threshold_s: int = 300

    # -- Source credentials (fallback if not set via the Credential API) -----------
    roboflow_api_key: str = ""
    kaggle_username: str = ""
    kaggle_key: str = ""
    youtube_api_key: str = ""

    @property
    def api_key_set(self) -> set[str]:
        return {k.strip() for k in self.api_keys.split(",") if k.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()
