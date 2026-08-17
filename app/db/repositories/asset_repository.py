import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db.models.asset import Asset
from app.domain.enums import ASSET_ACCEPTED_STATUSES, AssetStatus, SourceType


class AssetRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert(self, asset: Asset) -> tuple[Asset, bool]:
        """Idempotent insert keyed on (job_id, source_type, source_item_id).

        Returns (asset, created). On conflict, does nothing and returns the
        existing row — this is the DB-level guarantee that makes resumed/
        re-run ingestion safe to duplicate-call without duplicating data.
        """
        stmt = (
            pg_insert(Asset)
            .values(
                id=asset.id,
                job_id=asset.job_id,
                job_source_task_id=asset.job_source_task_id,
                source_type=asset.source_type,
                source_item_id=asset.source_item_id,
                source_url=asset.source_url,
                dataset_identifier=asset.dataset_identifier,
                license=asset.license,
                original_name=asset.original_name,
                width=asset.width,
                height=asset.height,
                file_format=asset.file_format,
                checksum_sha256=asset.checksum_sha256,
                phash=asset.phash,
                nas_path=asset.nas_path,
                annotation_format=asset.annotation_format,
                annotation_nas_path=asset.annotation_nas_path,
                classes=asset.classes,
                status=asset.status,
                asset_metadata=asset.asset_metadata,
                promoted_at=asset.promoted_at,
            )
            .on_conflict_do_nothing(
                index_elements=["job_id", "source_type", "source_item_id"]
            )
            .returning(Asset.id)
        )
        result = self.db.execute(stmt)
        row = result.first()
        self.db.flush()
        if row is None:
            existing = self.get_by_source_item(asset.job_id, asset.source_type, asset.source_item_id)
            return existing, False
        return self.db.get(Asset, row[0]), True

    def get(self, asset_id: uuid.UUID) -> Asset | None:
        return self.db.get(Asset, asset_id)

    def get_by_source_item(
        self, job_id: uuid.UUID, source_type: SourceType, source_item_id: str
    ) -> Asset | None:
        stmt = select(Asset).where(
            Asset.job_id == job_id,
            Asset.source_type == source_type,
            Asset.source_item_id == source_item_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def list_for_job(
        self,
        job_id: uuid.UUID,
        status: AssetStatus | None = None,
        source_type: SourceType | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Asset]:
        stmt = (
            select(Asset)
            .where(Asset.job_id == job_id)
            .order_by(Asset.discovered_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if status is not None:
            stmt = stmt.where(Asset.status == status)
        if source_type is not None:
            stmt = stmt.where(Asset.source_type == source_type)
        return list(self.db.execute(stmt).scalars().all())

    def list_for_export(self, job_id: uuid.UUID, min_relevance_score: float | None = None) -> list[Asset]:
        stmt = select(Asset).where(Asset.job_id == job_id, Asset.status.in_(ASSET_ACCEPTED_STATUSES))
        if min_relevance_score is not None:
            stmt = stmt.where(
                (Asset.relevance_score.is_(None)) | (Asset.relevance_score >= min_relevance_score)
            )
        return list(self.db.execute(stmt).scalars().all())

    def list_relevance_pending(self, job_id: uuid.UUID, limit: int = 500) -> list[Asset]:
        stmt = (
            select(Asset)
            .where(Asset.job_id == job_id, Asset.status == AssetStatus.RELEVANCE_PENDING)
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def list_for_dedup(self, job_id: uuid.UUID) -> list[Asset]:
        stmt = select(Asset).where(
            Asset.job_id == job_id,
            Asset.status.in_([AssetStatus.PROMOTED, AssetStatus.RELEVANCE_PASSED]),
            Asset.phash.is_not(None),
        )
        return list(self.db.execute(stmt).scalars().all())

    def set_status(self, asset_id: uuid.UUID, status: AssetStatus, **extra) -> None:
        asset = self.db.get(Asset, asset_id)
        if asset is None:
            return
        asset.status = status
        for k, v in extra.items():
            setattr(asset, k, v)

    def delete(self, asset_id: uuid.UUID) -> None:
        asset = self.db.get(Asset, asset_id)
        if asset is not None:
            self.db.delete(asset)
