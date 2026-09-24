"""NasConfigService — saves, reads and resolves the single NAS connection.

Every consumer that needs to *reach* the NAS (the connector, the storage
offload, the asset readers, the test endpoint) gets a NasConnectionParams from
here, so the password is decrypted in exactly one place and never travels
through Celery messages, checkpoints or API responses."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.connectors.nas.base import NasConnectionParams
from app.core.security import CredentialCipher
from app.db.models.nas_config import NasConfig
from app.db.repositories.nas_config_repository import NasConfigRepository
from app.db.session import session_scope
from app.schemas.nas import NasConfigInput


class NasPasswordRequired(ValueError):
    """Password-protected mode was chosen but no password was supplied or saved."""


class NasConfigService:
    def __init__(self, db: Session):
        self.repo = NasConfigRepository(db)

    def get(self) -> NasConfig | None:
        return self.repo.get()

    def save(self, data: NasConfigInput) -> NasConfig:
        existing = self.repo.get()
        encrypted = self._resolve_password(data, existing)
        return self.repo.upsert(
            protocol=data.protocol,
            host=data.host,
            port=data.port,
            share=data.share,
            base_path=data.base_path,
            storage_path=data.storage_path,
            use_as_storage=data.use_as_storage,
            auth_mode=data.auth_mode,
            username=data.username,
            domain=data.domain,
            encrypted_password=encrypted,
            use_tls=data.use_tls,
            verify_tls=data.verify_tls,
        )

    def delete(self) -> None:
        self.repo.delete()

    def params_from_input(self, data: NasConfigInput) -> NasConnectionParams:
        """Params for an unsaved form (the "Test connection" button). A blank
        password falls back to the saved one, so testing an existing connection
        does not force the user to retype it."""
        password = data.password_text()
        if password is None and data.auth_mode == "password":
            existing = self.repo.get()
            if existing is not None and existing.encrypted_password:
                password = CredentialCipher().decrypt(existing.encrypted_password)
        return NasConnectionParams(
            protocol=data.protocol,
            host=data.host,
            port=data.port,
            share=data.share,
            base_path=data.base_path,
            storage_path=data.storage_path,
            auth_mode=data.auth_mode,
            username=data.username,
            domain=data.domain,
            password=password or "",
            use_tls=data.use_tls,
            verify_tls=data.verify_tls,
        )

    @staticmethod
    def params_for(config: NasConfig) -> NasConnectionParams:
        password = ""
        if config.auth_mode == "password" and config.encrypted_password:
            password = CredentialCipher().decrypt(config.encrypted_password)
        return NasConnectionParams(
            protocol=config.protocol,
            host=config.host,
            port=config.port,
            share=config.share,
            base_path=config.base_path,
            storage_path=config.storage_path,
            auth_mode=config.auth_mode,
            username=config.username,
            domain=config.domain,
            password=password,
            use_tls=config.use_tls,
            verify_tls=config.verify_tls,
        )

    @staticmethod
    def _resolve_password(data: NasConfigInput, existing: NasConfig | None) -> bytes | None:
        if data.auth_mode == "anonymous":
            return None
        submitted = data.password_text()
        if submitted is not None:
            return CredentialCipher().encrypt(submitted)
        if existing is not None and existing.encrypted_password:
            return existing.encrypted_password  # blank field = keep what is saved
        raise NasPasswordRequired("Password is required for a password-protected NAS")


def get_saved_params() -> NasConnectionParams | None:
    """Params for the saved NAS regardless of the storage toggle — what readers
    need to fetch an image that already lives on the NAS."""
    with session_scope() as db:
        config = NasConfigRepository(db).get()
        return NasConfigService.params_for(config) if config is not None else None


def get_active_storage_params() -> NasConnectionParams | None:
    """Params when new images should be written to the NAS, else None.

    This single answer is what decides "NAS connected -> store on the NAS, not
    connected -> local": every write path asks here and nowhere else."""
    with session_scope() as db:
        config = NasConfigRepository(db).get()
        if config is None or not config.use_as_storage:
            return None
        return NasConfigService.params_for(config)


def is_nas_configured() -> bool:
    with session_scope() as db:
        return NasConfigRepository(db).get() is not None


def is_storage_active() -> bool:
    """Cheap check (no decryption) for "should new images go to the NAS?"."""
    with session_scope() as db:
        config = NasConfigRepository(db).get()
        return config is not None and config.use_as_storage
