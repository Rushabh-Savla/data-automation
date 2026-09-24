"""NAS protocol adapters — one small client per protocol behind a shared
NasClient interface, so the connector, the storage backend and the
"test connection" endpoint never know which protocol they are talking to."""

from app.connectors.nas.base import (
    IMAGE_EXTENSIONS,
    PROTOCOLS,
    NasAuthError,
    NasClient,
    NasConnectionError,
    NasConnectionParams,
    NasNotFoundError,
    NasPathError,
    NasUnavailableError,
    RemoteFile,
    normalize_relpath,
    open_client,
)

__all__ = [
    "IMAGE_EXTENSIONS",
    "PROTOCOLS",
    "NasAuthError",
    "NasClient",
    "NasConnectionError",
    "NasConnectionParams",
    "NasNotFoundError",
    "NasPathError",
    "NasUnavailableError",
    "RemoteFile",
    "normalize_relpath",
    "open_client",
]
