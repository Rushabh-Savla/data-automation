"""Durable acquisition windows within an image batch."""

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CandidateWindow(Base):
    __tablename__ = "candidate_windows"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("image_batches.id", ondelete="CASCADE"),
        nullable=False,
    )

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )

    window_index: Mapped[int] = mapped_column(
        Integer, nullable=False
    )

    candidate_limit: Mapped[int] = mapped_column(
        Integer, nullable=False, default=600
    )

    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPEN",
        server_default="OPEN",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "batch_id",
            "window_index",
            name="uq_candidate_windows_batch_index",
        ),
        Index(
            "ix_candidate_windows_batch_status",
            "batch_id",
            "status",
        ),
        Index(
            "ix_candidate_windows_job_status",
            "job_id",
            "status",
        ),
    )
