"""ImageBatch & NasTransferBatch models for configurable batch processing,
durable local staging, and sequential verified NAS transfers.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.domain.enums import BatchStatus, TransferStatus

if TYPE_CHECKING:
    from app.db.models.asset import Asset
    from app.db.models.job import Job


class ImageBatch(Base):
    __tablename__ = "image_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    batch_index: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    target_image_count: Mapped[int] = mapped_column(Integer, nullable=False, default=600)
    accepted_image_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_image_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_image_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    uncertain_image_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_image_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    local_staging_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    manifest_path: Mapped[str | None] = mapped_column(Text, nullable=True)

    status: Mapped[BatchStatus] = mapped_column(
        Enum(BatchStatus, native_enum=False, length=32, values_callable=lambda e: [m.value for m in e]),
        nullable=False,
        default=BatchStatus.DISCOVERING,
    )

    batch_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), onupdate=text("now()"), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    job: Mapped[Job] = relationship(back_populates="image_batches")
    assets: Mapped[list[Asset]] = relationship(back_populates="batch")
    transfers: Mapped[list[NasTransferBatch]] = relationship(back_populates="batch", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_image_batches_job_id", "job_id"),
        Index("ix_image_batches_status", "status"),
        UniqueConstraint("job_id", "batch_index", name="uq_image_batches_job_id_batch_index"),
    )


class NasTransferBatch(Base):
    __tablename__ = "nas_transfer_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("image_batches.id", ondelete="CASCADE"), nullable=False
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    remote_path: Mapped[str] = mapped_column(Text, nullable=False)
    transfer_status: Mapped[TransferStatus] = mapped_column(
        Enum(TransferStatus, native_enum=False, length=32, values_callable=lambda e: [m.value for m in e]),
        nullable=False,
        default=TransferStatus.PENDING,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    total_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    uploaded_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    failed_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    pending_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    bytes_transferred: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    verification_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    transfer_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    staging_cleanup_status: Mapped[str] = mapped_column(String(32), nullable=False, default="none", server_default="none")
    staging_cleaned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), onupdate=text("now()"), nullable=False)

    batch: Mapped[ImageBatch] = relationship(back_populates="transfers")

    __table_args__ = (
        Index("ix_nas_transfer_batches_batch_id", "batch_id"),
        Index("ix_nas_transfer_batches_job_id", "job_id"),
        Index("ix_nas_transfer_batches_transfer_status", "transfer_status"),
        Index("ix_nas_transfer_batches_staging_cleanup_status", "staging_cleanup_status"),
    )

