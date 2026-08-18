"""Job — a user-submitted request: a free-text query, which sources to pull from,
and how it should stop (target image count, a deadline, or both)."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.domain.enums import JobCompletionReason, JobStatus


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    sources: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)

    target_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scheduled_start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[JobStatus] = mapped_column(
        String(32), nullable=False, default=JobStatus.DRAFT
    )
    completion_reason: Mapped[JobCompletionReason | None] = mapped_column(String(32), nullable=True)

    # Per-source overrides: min_resolution, license allow-list, yolo_world_conf_threshold,
    # dedup_hamming_threshold, img2dataset concurrency, etc.
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    discovered_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    downloaded_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # server_default must be text("now()") (a live function-call expression),
    # not the bare string "now()" — SQLAlchemy/Postgres treats a plain quoted
    # string default as a constant, evaluated once when the DDL runs and then
    # frozen into the column's catalog default forever after, so every row
    # ever inserted without an explicit value gets that same table-creation-
    # time timestamp instead of its own insert time (this is exactly the bug
    # that made every job's created_at identical and job listings unsortable
    # by recency).
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), onupdate=lambda: datetime.utcnow()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    source_tasks: Mapped[list["JobSourceTask"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    assets: Mapped[list["Asset"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    export_jobs: Mapped[list["ExportJob"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_jobs_status_scheduled_start_at", "status", "scheduled_start_at"),
        Index("ix_jobs_status", "status"),
    )
