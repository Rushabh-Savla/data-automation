"""NasConfig — the single, UI-managed NAS connection (host, share, auth).

One row at most (id is pinned to 1 by a check constraint): the platform has one
global NAS, used both as an optional image source and as the storage the
filtered images are moved to. The password is Fernet-encrypted with the same
cipher as Credential.encrypted_value and is never read back by the API."""

from datetime import datetime, timezone

from sqlalchemy import Boolean, CheckConstraint, DateTime, Integer, LargeBinary, String, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class NasConfig(Base):
    __tablename__ = "nas_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False, default=1)

    protocol: Mapped[str] = mapped_column(String(16), nullable=False)
    host: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # SMB share name / NFS export / WebDAV path prefix / mounted path for "local".
    share: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    # Folder scanned when the NAS is used as an image *source*.
    base_path: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    # Folder filtered images are moved into when the NAS is used as *storage*.
    storage_path: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    use_as_storage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    auth_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="anonymous")
    username: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    domain: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    encrypted_password: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)

    use_tls: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    verify_tls: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), onupdate=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (CheckConstraint("id = 1", name="singleton"),)
