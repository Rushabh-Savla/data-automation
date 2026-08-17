"""API-key auth dependency and credential-value encryption helpers."""

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Header, HTTPException, status

from app.core.config import get_settings


def require_api_key(x_api_key: str | None = Header(default=None)) -> str:
    """FastAPI dependency: validates the X-API-Key header against configured keys."""
    settings = get_settings()
    if not x_api_key or x_api_key not in settings.api_key_set:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
        )
    return x_api_key


def require_api_key_query(api_key: str | None = None) -> str:
    """Variant for the WebSocket endpoint, which passes the key as a query param
    (browsers cannot set custom headers during the WS handshake)."""
    settings = get_settings()
    if not api_key or api_key not in settings.api_key_set:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
        )
    return api_key


class CredentialCipher:
    """Thin wrapper around Fernet for encrypting/decrypting stored credential values."""

    def __init__(self, key: str | None = None):
        settings = get_settings()
        raw_key = key or settings.credential_encryption_key
        if not raw_key:
            raise RuntimeError(
                "CREDENTIAL_ENCRYPTION_KEY is not set. Generate one with:\n"
                "  python -c \"from cryptography.fernet import Fernet; "
                'print(Fernet.generate_key().decode())"'
            )
        self._fernet = Fernet(raw_key.encode() if isinstance(raw_key, str) else raw_key)

    def encrypt(self, plaintext: str) -> bytes:
        return self._fernet.encrypt(plaintext.encode("utf-8"))

    def decrypt(self, ciphertext: bytes) -> str:
        try:
            return self._fernet.decrypt(ciphertext).decode("utf-8")
        except InvalidToken as e:
            raise RuntimeError("Failed to decrypt credential value — wrong encryption key?") from e
