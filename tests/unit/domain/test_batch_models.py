"""Unit tests for Stage 1: Batch Configuration and Database Models."""

import uuid
import pytest
from app.core.config import Settings, get_settings
from app.db.base import Base
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.db.models.asset import Asset
from app.domain.enums import BatchStatus, TransferStatus, AssetStatus, SourceType
from app.db.repositories.image_batch_repository import ImageBatchRepository, NasTransferBatchRepository
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def test_batch_settings(monkeypatch):
    monkeypatch.delenv("LOCAL_STAGING_MAX_BYTES", raising=False)
    s = Settings(_env_file=None)
    assert s.batch_accepted_target == 600
    assert s.download_candidate_window == 600
    assert s.ram_buffer_max_bytes == 1024 * 1024 * 1024
    assert s.local_staging_max_bytes == 50 * 1024 * 1024 * 1024
    assert s.local_staging_high_watermark == 0.80
    assert s.local_staging_resume_watermark == 0.65
    assert s.download_pause_when_storage_full is True
    assert s.nas_max_concurrency == 1
    assert s.nas_upload_max_batch_files == 600
    assert s.nas_upload_retry_limit == 5
    assert s.nas_upload_cooldown_seconds == 180


def test_image_batch_and_nas_transfer_models_sqlite():
    engine = create_engine("sqlite:///:memory:")
    # Register 'now' function for SQLite so server_default=text("now()") works seamlessly
    from sqlalchemy import event
    @event.listens_for(engine, "connect")
    def register_sqlite_functions(dbapi_connection, connection_record):
        import datetime
        dbapi_connection.create_function("now", 0, lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())

    from sqlalchemy.dialects.sqlite import JSON
    from sqlalchemy.dialects.postgresql import JSONB, UUID
    for table in Base.metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    job_id = uuid.uuid4()
    job = Job(id=job_id, query="fire", sources=["web"], target_count=1200)
    session.add(job)
    session.flush()

    batch_repo = ImageBatchRepository(session)
    batch = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=1,
        target_image_count=600,
        status=BatchStatus.DISCOVERING,
    )
    batch_repo.create(batch)

    assert batch.batch_index == 1
    assert batch.target_image_count == 600
    assert batch.status == BatchStatus.DISCOVERING

    # Test status transitions
    batch_repo.set_status(batch.id, BatchStatus.DOWNLOADING)
    fetched = batch_repo.get(batch.id)
    assert fetched.status == BatchStatus.DOWNLOADING

    batch_repo.update_counts(batch.id, accepted=550, rejected=40, duplicate=10)
    fetched = batch_repo.get(batch.id)
    assert fetched.accepted_image_count == 550
    assert fetched.rejected_image_count == 40
    assert fetched.duplicate_image_count == 10

    # Test transfer batch
    transfer_repo = NasTransferBatchRepository(session)
    transfer = NasTransferBatch(
        id=uuid.uuid4(),
        batch_id=batch.id,
        job_id=job_id,
        remote_path="ai_data_collection/fire_batch_1",
        transfer_status=TransferStatus.PENDING,
    )
    transfer_repo.create(transfer)

    pending = transfer_repo.list_pending()
    assert len(pending) == 1
    assert pending[0].remote_path == "ai_data_collection/fire_batch_1"

    transfer_repo.mark_uploading(transfer.id)
    assert transfer_repo.get(transfer.id).transfer_status == TransferStatus.UPLOADING
    assert transfer_repo.get(transfer.id).attempt_count == 1

    transfer_repo.mark_verified(transfer.id, checksum="abc123sha")
    verified = transfer_repo.get(transfer.id)
    assert verified.transfer_status == TransferStatus.VERIFIED
    assert verified.checksum == "abc123sha"
    assert verified.verified_at is not None

    session.close()
