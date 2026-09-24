"""CredentialService — resolves per-source secrets from the encrypted Credential
table (falling back to Settings/.env for bootstrap), and scopes them into
os.environ only for the duration of a connector call.

Scoping matters because kagglehub/kaggle read credentials from process-global
env vars with no per-call override; without this, two concurrently running
jobs using different Kaggle accounts on the same worker process would clobber
each other's credentials.
"""

import os
from collections.abc import Iterator
from contextlib import contextmanager

from app.core.config import Settings, get_settings
from app.core.security import CredentialCipher
from app.db.repositories.credential_repository import CredentialRepository
from app.domain.enums import SourceType

# Which env var name(s) each source expects, and the Settings fallback attr.
_SOURCE_ENV_KEYS: dict[SourceType, dict[str, str]] = {
    SourceType.ROBOFLOW: {"ROBOFLOW_API_KEY": "roboflow_api_key"},
    SourceType.KAGGLE: {
        "KAGGLE_USERNAME": "kaggle_username",
        "KAGGLE_KEY": "kaggle_key",
    },
    SourceType.YOUTUBE: {"YOUTUBE_API_KEY": "youtube_api_key"},
    SourceType.WEB: {},
    SourceType.COCO: {},
    SourceType.OPEN_IMAGES: {},
    SourceType.PASCAL_VOC: {},
    SourceType.VISUAL_GENOME: {},
    SourceType.DAVIS: {},
    SourceType.SUN_RGBD: {},
    SourceType.NUSCENES: {},
    SourceType.KITTI: {},
    # Configured on its own page (host/share/auth), not as a key/value credential.
    SourceType.NAS_SHARE: {},
}


class CredentialService:
    def __init__(self, credential_repo: CredentialRepository, settings: Settings | None = None):
        self.credential_repo = credential_repo
        self.settings = settings or get_settings()
        self._cipher: CredentialCipher | None = None

    def _cipher_or_none(self) -> CredentialCipher | None:
        if self._cipher is None:
            try:
                self._cipher = CredentialCipher()
            except RuntimeError:
                return None
        return self._cipher

    def resolve(self, source_type: SourceType) -> dict[str, str]:
        """Returns {env_var_name: value} for the given source, preferring
        active DB-stored credentials and falling back to Settings/.env."""
        result: dict[str, str] = {}
        cipher = self._cipher_or_none()
        if cipher is not None:
            for cred in self.credential_repo.list_active_for_source(source_type):
                try:
                    result[cred.key_name] = cipher.decrypt(cred.encrypted_value)
                    self.credential_repo.mark_used(cred.id)
                except RuntimeError:
                    continue

        for env_key, settings_attr in _SOURCE_ENV_KEYS.get(source_type, {}).items():
            if env_key not in result:
                fallback = getattr(self.settings, settings_attr, "")
                if fallback:
                    result[env_key] = fallback
        return result

    def resolve_pool(self, source_type: SourceType, key_name: str) -> list[str]:
        """Returns every active credential's decrypted value for a given
        (source_type, key_name), least-recently-used first — for connectors
        that rotate across several user-added credentials (e.g. multiple
        Roboflow API keys) to spread request load and avoid a single key's
        rate limit. Falls back to the single Settings/.env value only when
        no DB-stored credential exists at all."""
        values: list[str] = []
        cipher = self._cipher_or_none()
        if cipher is not None:
            for cred in self.credential_repo.list_active_for_source(source_type):
                if cred.key_name != key_name:
                    continue
                try:
                    values.append(cipher.decrypt(cred.encrypted_value))
                    self.credential_repo.mark_used(cred.id)
                except RuntimeError:
                    continue

        if not values:
            settings_attr = _SOURCE_ENV_KEYS.get(source_type, {}).get(key_name)
            if settings_attr:
                fallback = getattr(self.settings, settings_attr, "")
                if fallback:
                    values.append(fallback)
        return values

    @contextmanager
    def scoped_env(self, source_type: SourceType) -> Iterator[dict[str, str]]:
        """Context manager: sets resolved credentials into os.environ for the
        duration of the block, restoring prior values afterward."""
        values = self.resolve(source_type)
        previous = {k: os.environ.get(k) for k in values}
        try:
            os.environ.update(values)
            yield values
        finally:
            for k, prev in previous.items():
                if prev is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = prev
