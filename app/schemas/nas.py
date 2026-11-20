"""Request/response schemas for the NAS connection API.

The password is write-only: it is accepted as a SecretStr on input and is never
part of any response — clients only learn whether one is saved (has_password)."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator

from app.connectors.nas.base import NasPathError, normalize_relpath

NasProtocol = Literal["smb", "sftp", "ftp", "webdav", "nfs", "local"]
NasAuthMode = Literal["anonymous", "password"]

_NEEDS_USERNAME = ("smb", "sftp", "ftp", "webdav")


class NasConfigInput(BaseModel):
    protocol: NasProtocol
    host: str = ""
    port: int | None = Field(default=None, ge=1, le=65535)
    share: str = ""
    base_path: str = ""
    storage_path: str = "data_automation"
    use_as_storage: bool = True
    auth_mode: NasAuthMode = "anonymous"
    username: str = ""
    domain: str = ""
    # None or blank means "keep the saved password" (or none, for open access).
    password: SecretStr | None = None
    use_tls: bool = False
    verify_tls: bool = True

    @field_validator("host", "share", "username", "domain")
    @classmethod
    def _strip(cls, value: str) -> str:
        return value.strip()

    @field_validator("host")
    @classmethod
    def _host_is_bare(cls, value: str) -> str:
        # A pasted URL ("smb://nas/photos") is the most common mistake; say so
        # instead of failing later with an unresolvable-host error.
        if "://" in value or "/" in value or "\\" in value or " " in value:
            raise ValueError("Enter only the host name or IP address, without smb://, http:// or a path")
        return value

    @field_validator("base_path", "storage_path")
    @classmethod
    def _safe_folder(cls, value: str) -> str:
        try:
            return normalize_relpath(value)
        except NasPathError as exc:
            raise ValueError(str(exc)) from exc

    @model_validator(mode="after")
    def _check_protocol_rules(self) -> "NasConfigInput":
        if self.protocol != "local" and not self.host:
            raise ValueError("Host is required")
        if self.protocol in ("smb", "nfs", "local") and not self.share:
            raise ValueError(
                {"smb": "Share name is required", "nfs": "NFS export path is required", "local": "Mounted folder path is required"}[
                    self.protocol
                ]
            )
        if self.protocol == "local" and not self.share.startswith("/"):
            raise ValueError("Mounted folder path must be an absolute path, e.g. /mnt/nas")

        # NFS and mounted paths have no login; the fields that do not apply are
        # cleared rather than rejected so a protocol switch in the form stays easy.
        if self.protocol in ("nfs", "local"):
            self.auth_mode = "anonymous"
        if self.auth_mode == "anonymous":
            self.username = ""
            self.password = None
        if self.protocol != "smb":
            self.domain = ""

        if self.protocol == "sftp" and self.auth_mode == "anonymous":
            raise ValueError("SFTP always needs a user name and password")
        if self.auth_mode == "password" and self.protocol in _NEEDS_USERNAME and not self.username:
            raise ValueError("User name is required for a password-protected NAS")
        if self.use_as_storage and not self.storage_path:
            raise ValueError("Storage folder is required")
        return self

    def password_text(self) -> str | None:
        """The submitted password, or None when blank/omitted (= keep saved)."""
        if self.password is None:
            return None
        value = self.password.get_secret_value()
        return value if value != "" else None


class NasConfigResponse(BaseModel):
    configured: bool
    protocol: NasProtocol = "sftp"
    host: str = ""
    port: int | None = None
    share: str = ""
    base_path: str = ""
    storage_path: str = "data_automation"
    use_as_storage: bool = True
    auth_mode: NasAuthMode = "anonymous"
    username: str = ""
    domain: str = ""
    has_password: bool = False
    use_tls: bool = False
    verify_tls: bool = True
    updated_at: datetime | None = None


class NasStatusResponse(BaseModel):
    configured: bool
    connected: bool
    protocol: NasProtocol | None = None
    host: str | None = None
    port: int | None = None
    latency_ms: float | None = None
    message: str | None = None


class NasTestResponse(BaseModel):
    ok: bool
    can_read: bool
    can_write: bool
    message: str
    latency_ms: int | None = None
    sample_files: list[str] = []


class NasTransferInput(BaseModel):
    direction: Literal["local_to_nas", "nas_to_local"] = "local_to_nas"
    source_folder: str = ""
    destination_folder: str = ""
    max_images: int = Field(default=1000, ge=1, le=5000)


class NasTransferResponse(BaseModel):
    ok: bool
    direction: str
    transferred_count: int = 0
    total_bytes: int = 0
    duration_s: float = 0.0
    message: str = ""
    sample_files: list[str] = []


class NasFoldersResponse(BaseModel):
    current_path: str = ""
    parent_path: str | None = None
    folders: list[str] = Field(default_factory=list)
    tree: list[str] = Field(default_factory=list)


class NasVerifyFolderRequest(BaseModel):
    folder_path: str = ""


class NasVerifyFolderResponse(BaseModel):
    exists: bool
    path: str
    message: str = ""


