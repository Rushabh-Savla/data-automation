"""JobSourceTask — one row per (job, source). This is the concrete mechanism behind
"a failure in one source must not interrupt the remaining jobs": each source's
discovery/download/standardization progresses and fails independently."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.domain.enums import JobSourceTaskStatus, SourceType


class JobSourceTask(Base):
    __tablename__ = "job_source_tasks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )

    source_type: Mapped[SourceType] = mapped_column(String(32), nullable=False)
    status: Mapped[JobSourceTaskStatus] = mapped_column(
        String(32), nullable=False, default=JobSourceTaskStatus.PENDING
    )

    # Resume cursor: small index/cursor documents, not full processed-ID lists.
    # See app/domain/completion.py and the plan §5 for the schema per source.
    checkpoint: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    discovered_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    downloaded_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    job: Mapped["Job"] = relationship(back_populates="source_tasks")
    assets: Mapped[list["Asset"]] = relationship(back_populates="job_source_task")

    __table_args__ = (
        UniqueConstraint("job_id", "source_type", name="uq_job_source_tasks_job_id_source_type"),
    )
