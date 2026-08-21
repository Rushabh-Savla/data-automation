"""Asset — the unified image/annotation record replacing the four divergent
manifest.jsonl schemas from the legacy scripts. One row per accepted item,
with full provenance: source, source URL, dataset/project identifier,
license, resolution, exact NAS path, processing status."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.domain.enums import AnnotationFormat, AssetStatus, SourceType


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    job_source_task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job_source_tasks.id", ondelete="CASCADE"), nullable=False
    )

    source_type: Mapped[SourceType] = mapped_column(String(32), nullable=False)
    # Deterministic per-source ID — see app/connectors/*.py for the generation scheme
    # of each source (reused from the legacy scripts where one already existed).
    source_item_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_identifier: Mapped[str | None] = mapped_column(String(255), nullable=True)
    license: Mapped[str | None] = mapped_column(String(255), nullable=True)
    original_name: Mapped[str | None] = mapped_column(String(500), nullable=True)

    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    file_format: Mapped[str | None] = mapped_column(String(16), nullable=True)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    phash: Mapped[str | None] = mapped_column(String(16), nullable=True)
    # CLIP image embedding (512-dim, L2-normalized) — the additive "vector
    # matching" signal for hybrid duplicate detection (app/filtering/dedup.py),
    # alongside phash. Nullable: computation can legitimately fail/be skipped.
    embedding: Mapped[list[float] | None] = mapped_column(JSONB, nullable=True)

    nas_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    annotation_format: Mapped[AnnotationFormat] = mapped_column(
        String(16), nullable=False, default=AnnotationFormat.NONE
    )
    annotation_nas_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    classes: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    status: Mapped[AssetStatus] = mapped_column(
        String(32), nullable=False, default=AssetStatus.DISCOVERED
    )
    relevance_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_duplicate_of: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("assets.id", ondelete="SET NULL"), nullable=True
    )

    # Catch-all for source-specific fields (YouTube frame_number/scene_number/
    # timestamp_sec, Roboflow split, Kaggle original_path, coco_image_id, etc.)
    asset_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"))
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    job: Mapped["Job"] = relationship(back_populates="assets")
    job_source_task: Mapped["JobSourceTask"] = relationship(back_populates="assets")

    __table_args__ = (
        UniqueConstraint(
            "job_id", "source_type", "source_item_id",
            name="uq_assets_job_id_source_type_source_item_id",
        ),
    )
