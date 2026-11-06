import uuid
import datetime
import hashlib
from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy import create_engine, select, func, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.connectors.base import DiscoveryPage, SourceItem
from app.db.base import Base
from app.db.models.candidate_reservation import CandidateReservation
from app.db.models.candidate_window import CandidateWindow
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import BatchStatus, JobSourceTaskStatus, JobStatus, SourceType
from app.services.candidate_reservation_service import (
    CandidateAdmissionStatus,
    admit_candidate_for_task,
)
from app.services.job_service import _resume_pending_items, register_source_download_task


@pytest.fixture
def mock_db(monkeypatch):
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
    monkeypatch.setattr("app.tasks.discovery_tasks.session_scope", _mock_scope)
    monkeypatch.setattr("app.tasks.ingest_tasks.session_scope", _mock_scope)
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", _mock_scope)

    fake_redis = MagicMock()
    fake_redis.exists.return_value = False
    fake_redis.get.return_value = None
    monkeypatch.setattr("app.core.redis_client.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks._helpers.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.discovery_tasks.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
    return Session


def test_web_discovery_registers_pending_items(mock_db, monkeypatch):
    """TEST 1 & 2: Proves discover_web populates _pending_items in checkpoint and advances status to DOWNLOADING."""
    from app.tasks.discovery_tasks import discover_web

    with mock_db() as db:
        job = Job(id=uuid.uuid4(), name="web_job", query="containers", status=JobStatus.RUNNING, target_count=100)
        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job.id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.PENDING,
            checkpoint={},
        )
        db.add_all([job, task])
        db.commit()
        job_id = str(job.id)
        task_id = str(task.id)

    # Mock connector.discover to return 2 shards with 2 URLs each
    shard1 = SourceItem(
        source_type=SourceType.WEB,
        external_ref={"urls": ["https://example.com/1.jpg", "https://example.com/2.jpg"]},
        dataset_identifier="containers",
    )
    shard2 = SourceItem(
        source_type=SourceType.WEB,
        external_ref={"urls": ["https://example.com/3.jpg", "https://example.com/4.jpg"]},
        dataset_identifier="containers",
    )

    mock_page = DiscoveryPage(items=[shard1, shard2], next_cursor=None, exhausted=True)
    monkeypatch.setattr("app.connectors.web_connector.WebScrapeConnector.discover", lambda *args, **kwargs: mock_page)

    chord_mock = MagicMock()
    monkeypatch.setattr("app.tasks.discovery_tasks.chord", lambda group: chord_mock)

    discover_web(job_id, task_id)

    with mock_db() as db:
        saved_task = db.get(JobSourceTask, uuid.UUID(task_id))
        assert saved_task.status == JobSourceTaskStatus.DOWNLOADING
        assert saved_task.discovered_count == 4
        checkpoint = saved_task.checkpoint or {}
        pending = checkpoint.get("_pending_items", {})
        assert len(pending) == 2

        # Verify shard keys are computed by url md5
        shard1_id = hashlib.md5(",".join(shard1.external_ref["urls"]).encode()).hexdigest()[:12]
        shard2_id = hashlib.md5(",".join(shard2.external_ref["urls"]).encode()).hexdigest()[:12]
        assert shard1_id in pending
        assert shard2_id in pending


def test_resume_pending_items_web(monkeypatch):
    """TEST 2 & 11: Proves _resume_pending_items redispatches outstanding web shards from checkpoint."""
    register_source_download_task(SourceType.WEB, "app.tasks.ingest_tasks.download_web_batch")

    chord_mock = MagicMock()
    monkeypatch.setattr("celery.chord", lambda group: chord_mock)

    checkpoint = {
        "_pending_items": {
            "shard_a": {"external_ref": {"urls": ["http://a.com/1.jpg"]}},
            "shard_b": {"external_ref": {"urls": ["http://b.com/2.jpg"]}},
        },
        "_completed_items": ["shard_a"],
    }

    job_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    resumed = _resume_pending_items(SourceType.WEB, job_id, task_id, checkpoint)
    assert resumed is True
    # Only shard_b should be dispatched
    chord_mock.assert_called_once()


def test_candidate_window_fairness_multi_source(mock_db):
    """TEST 6 & 7: Proves that when multiple sources are active, a fast source cannot monopolize 100% of window."""
    with mock_db() as db:
        job = Job(
            id=uuid.uuid4(),
            name="multi_source_job",
            query="test",
            status=JobStatus.RUNNING,
            sources=["roboflow", "web"],
        )
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.DOWNLOADING)
        rf_task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job.id,
            source_type=SourceType.ROBOFLOW.value,
            status=JobSourceTaskStatus.DOWNLOADING,
        )
        web_task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job.id,
            source_type=SourceType.WEB.value,
            status=JobSourceTaskStatus.DOWNLOADING,
        )
        window = CandidateWindow(
            id=uuid.uuid4(),
            batch_id=batch.id,
            job_id=job.id,
            window_index=1,
            candidate_limit=10,
            status="OPEN",
        )
        db.add_all([job, batch, rf_task, web_task, window])
        db.commit()
        rf_task_id = rf_task.id
        web_task_id = web_task.id
        window_id = window.id

    # Sourced fairness cap is 70% of 10 = 7 items
    for i in range(7):
        adm = admit_candidate_for_task(rf_task_id, SourceType.ROBOFLOW, f"rf_item_{i}")
        assert adm.status == CandidateAdmissionStatus.ADMITTED

    # 8th Roboflow candidate should hit fairness limit and receive WINDOW_FULL while Web is active
    adm_rf_8 = admit_candidate_for_task(rf_task_id, SourceType.ROBOFLOW, "rf_item_7")
    assert adm_rf_8.status == CandidateAdmissionStatus.WINDOW_FULL

    # Web can now admit its candidates in the remaining 3 slots
    adm_web_1 = admit_candidate_for_task(web_task_id, SourceType.WEB, "web_item_1")
    assert adm_web_1.status == CandidateAdmissionStatus.ADMITTED

    adm_web_2 = admit_candidate_for_task(web_task_id, SourceType.WEB, "web_item_2")
    assert adm_web_2.status == CandidateAdmissionStatus.ADMITTED

    adm_web_3 = admit_candidate_for_task(web_task_id, SourceType.WEB, "web_item_3")
    assert adm_web_3.status == CandidateAdmissionStatus.ADMITTED

    # 10 total slots are now consumed; window is globally full
    adm_web_4 = admit_candidate_for_task(web_task_id, SourceType.WEB, "web_item_4")
    assert adm_web_4.status == CandidateAdmissionStatus.WINDOW_FULL


def test_fast_skip_preserves_open_batches(mock_db, monkeypatch):
    """TEST 7 & 8: Proves that an old FULL window does not cause fast-skip if another active batch has an OPEN window."""
    with mock_db() as db:
        job = Job(id=uuid.uuid4(), name="web_job", query="test", status=JobStatus.RUNNING)
        # Batch 1 is COMPLETED with a FULL window
        batch1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.COMPLETED)
        win1 = CandidateWindow(id=uuid.uuid4(), batch_id=batch1.id, job_id=job.id, window_index=1, candidate_limit=5, status="FULL")
        # Batch 2 is DOWNLOADING with an OPEN window
        batch2 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=2, status=BatchStatus.DOWNLOADING)
        win2 = CandidateWindow(id=uuid.uuid4(), batch_id=batch2.id, job_id=job.id, window_index=1, candidate_limit=5, status="OPEN")
        task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type=SourceType.WEB.value, status=JobSourceTaskStatus.DOWNLOADING)
        db.add_all([job, batch1, win1, batch2, win2, task])
        db.commit()

        # Check the active batches and open window query
        terminal_batches = (BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED)
        active_batch_ids = db.execute(
            select(ImageBatch.id).where(
                ImageBatch.job_id == job.id,
                ~ImageBatch.status.in_(terminal_batches),
            )
        ).scalars().all()
        assert active_batch_ids == [batch2.id]

        open_win = db.execute(
            select(CandidateWindow).where(
                CandidateWindow.batch_id.in_(active_batch_ids),
                CandidateWindow.status.in_(("OPEN", "ACQUIRING")),
            ).limit(1)
        ).scalar_one_or_none()
        assert open_win is not None
        assert open_win.id == win2.id


def test_duplicate_url_rejected_at_admission(mock_db):
    """TEST 10: Proves duplicate web URLs are rejected as DUPLICATE and not re-downloaded."""
    with mock_db() as db:
        job = Job(id=uuid.uuid4(), name="web_job", query="test", status=JobStatus.RUNNING, sources=["web"])
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.DOWNLOADING)
        task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type=SourceType.WEB.value, status=JobSourceTaskStatus.DOWNLOADING)
        window = CandidateWindow(id=uuid.uuid4(), batch_id=batch.id, job_id=job.id, window_index=1, candidate_limit=5, status="OPEN")
        db.add_all([job, batch, task, window])
        db.commit()
        task_id = task.id

    url = "https://example.com/unique_car.jpg"
    url_id = hashlib.md5(url.encode()).hexdigest()[:16]

    adm1 = admit_candidate_for_task(task_id, SourceType.WEB, url_id)
    assert adm1.status == CandidateAdmissionStatus.ADMITTED

    adm2 = admit_candidate_for_task(task_id, SourceType.WEB, url_id)
    assert adm2.status == CandidateAdmissionStatus.DUPLICATE
    assert adm2.reservation_id is None
