import uuid
import datetime
import pytest
from pathlib import Path
from unittest.mock import MagicMock
from sqlalchemy import create_engine, select, func, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.exceptions import CandidateWindowFull
from app.db.base import Base
from app.db.models.asset import Asset
from app.db.models.candidate_reservation import CandidateReservation
from app.db.models.candidate_window import CandidateWindow
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import BatchStatus, JobSourceTaskStatus, JobStatus, SourceType, AssetStatus
from app.services.candidate_reservation_service import (
    CandidateAdmissionStatus,
    admit_candidate_for_task,
    mark_candidate_staged,
    mark_candidate_failed,
    set_candidate_window_status,
)
from app.storage.window_staging import WindowStagingManager


@pytest.fixture
def mock_db(monkeypatch, tmp_path):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def register_sqlite_functions(dbapi_connection, connection_record):
        dbapi_connection.create_function(
            "now", 0, lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
        )

    from sqlalchemy.dialects.sqlite import JSON
    from sqlalchemy.dialects.postgresql import JSONB
    for table in Base.metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    import contextlib
    @contextlib.contextmanager
    def _mock_scope():
        session = Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("app.services.candidate_reservation_service.session_scope", _mock_scope)
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", _mock_scope)
    return Session


def test_window_waiting_checkpoint_persistence(mock_db, monkeypatch):
    """Proves that when a window fills, source checkpoint preserves next candidate and task enters WINDOW_WAITING without deleting workdir."""
    with mock_db() as db:
        job = Job(id=uuid.uuid4(), name="test_job", query="dogs", status=JobStatus.RUNNING)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.DOWNLOADING)
        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job.id,
            source_type=SourceType.COCO.value,
            status=JobSourceTaskStatus.DOWNLOADING,
            checkpoint={"index": 10},
        )
        window = CandidateWindow(
            id=uuid.uuid4(),
            batch_id=batch.id,
            job_id=job.id,
            window_index=1,
            candidate_limit=2,
            status="OPEN",
        )
        db.add_all([job, batch, task, window])
        db.commit()
        task_id = task.id
        job_id = job.id

    # Admit 2 items to fill the window
    adm1 = admit_candidate_for_task(task_id, SourceType.COCO, "item_1")
    adm2 = admit_candidate_for_task(task_id, SourceType.COCO, "item_2")
    assert adm1.status == CandidateAdmissionStatus.ADMITTED
    assert adm2.status == CandidateAdmissionStatus.ADMITTED

    # 3rd item encounters WINDOW_FULL
    adm3 = admit_candidate_for_task(task_id, SourceType.COCO, "item_3")
    assert adm3.status == CandidateAdmissionStatus.WINDOW_FULL

    # In orchestration chord callback: had_window_full triggers JobSourceTaskStatus.WINDOW_WAITING
    # Let's verify on_source_finished behavior
    from app.tasks.orchestration_tasks import on_source_finished

    cleanup_mock = MagicMock()
    monkeypatch.setattr("app.tasks.cleanup_tasks.cleanup_local.delay", cleanup_mock)
    advance_mock = MagicMock()
    monkeypatch.setattr("app.tasks.orchestration_tasks.check_and_advance_job", advance_mock)

    # Simulated chord results where one task returned window_full
    chord_results = [
        {"item": "shard_001", "accepted": 2, "rejected": 0, "window_full": True, "checkpoint_index": 12}
    ]

    with mock_db() as db:
        repo_task = db.get(JobSourceTask, task_id)
        repo_task.checkpoint = {"index": 12}
        db.commit()

    on_source_finished(chord_results, str(job_id), str(task_id))

    with mock_db() as db:
        saved_task = db.get(JobSourceTask, task_id)
        assert saved_task.status == JobSourceTaskStatus.WINDOW_WAITING
        assert saved_task.checkpoint["index"] == 12
        # Verify cleanup_local was NOT dispatched for WINDOW_WAITING task
        cleanup_mock.assert_not_called()
        advance_mock.assert_called_once_with(str(job_id))


def test_window_only_staging_and_cleanup(tmp_path):
    """Verify window staging directory isolation and cleanup only affects the target window."""
    manager = WindowStagingManager(staging_path=tmp_path)
    job_id = uuid.uuid4()

    # Stage image in window 1
    dir_w1 = manager.get_window_staging_dir(job_id, 1, 1)
    file_w1 = manager.stage_accepted_image(
        job_id, 1, 1, uuid.uuid4(), "jpg", b"fake_image_bytes_w1"
    )
    assert file_w1.exists()

    # Stage image in window 2
    dir_w2 = manager.get_window_staging_dir(job_id, 1, 2)
    file_w2 = manager.stage_accepted_image(
        job_id, 1, 2, uuid.uuid4(), "jpg", b"fake_image_bytes_w2"
    )
    assert file_w2.exists()

    # Clean only window 1
    cleaned = manager.cleanup_window_staging(job_id, 1, 1)
    assert cleaned is True
    assert not dir_w1.exists()
    # Window 2 must remain completely intact
    assert dir_w2.exists()
    assert file_w2.exists()


def test_empty_window_manifest_rejection(tmp_path):
    """Verify enqueue_nas_window fails if manifest is missing or empty."""
    from app.storage.nas_writer import enqueue_nas_window
    import json

    manifest_path = tmp_path / "empty_manifest.json"
    manifest_path.write_text(json.dumps({"items": []}))

    with pytest.raises(ValueError, match="Candidate-window manifest has no items"):
        enqueue_nas_window(
            window_id=uuid.uuid4(),
            batch_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            batch_index=1,
            window_index=1,
            manifest_path=manifest_path,
        )
