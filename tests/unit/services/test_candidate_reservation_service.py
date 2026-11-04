import uuid
import datetime
import pytest
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
from app.domain.enums import BatchStatus, JobSourceTaskStatus, JobStatus, SourceType
from app.services.candidate_reservation_service import (
    MAX_CANDIDATES_PER_WINDOW,
    CandidateAdmission,
    CandidateAdmissionStatus,
    admit_candidate_for_task,
    ensure_candidate_window,
    get_candidate_window_stats,
    mark_candidate_failed,
    mark_candidate_staged,
    open_candidate_window,
    reserve_candidate,
    set_candidate_window_status,
)


@pytest.fixture
def isolated_db(monkeypatch):
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
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("app.services.candidate_reservation_service.session_scope", _mock_scope)
    return Session


def _setup_job_and_batch(Session, candidate_limit=600):
    with Session() as db:
        job = Job(id=uuid.uuid4(), name="test_job", query="cars", status=JobStatus.RUNNING)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.DOWNLOADING)
        task = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job.id,
            source_type=SourceType.COCO.value,
            status=JobSourceTaskStatus.DOWNLOADING,
        )
        window = CandidateWindow(
            id=uuid.uuid4(),
            batch_id=batch.id,
            job_id=job.id,
            window_index=1,
            candidate_limit=candidate_limit,
            status="OPEN",
        )
        db.add_all([job, batch, task, window])
        db.commit()
        return job.id, batch.id, task.id, window.id


def test_admit_candidate_success(isolated_db):
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=5)
    admission = admit_candidate_for_task(task_id, SourceType.COCO, "item_1")

    assert admission.status == CandidateAdmissionStatus.ADMITTED
    assert admission.reservation_id is not None
    assert admission.window_id == window_id
    assert admission.window_index == 1

    with isolated_db() as db:
        res = db.get(CandidateReservation, admission.reservation_id)
        assert res is not None
        assert res.source_item_id == "item_1"
        assert res.status == "RESERVED"


def test_admit_candidate_duplicate_detection(isolated_db):
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=5)
    adm1 = admit_candidate_for_task(task_id, SourceType.COCO, "item_dup")
    assert adm1.status == CandidateAdmissionStatus.ADMITTED

    # Second attempt with same source_item_id in same batch must be DUPLICATE
    adm2 = admit_candidate_for_task(task_id, SourceType.COCO, "item_dup")
    assert adm2.status == CandidateAdmissionStatus.DUPLICATE
    assert adm2.reservation_id is None


def test_admit_candidate_existing_asset_duplicate(isolated_db):
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=5)
    with isolated_db() as db:
        # Pre-existing Asset in the DB
        asset = Asset(
            id=uuid.uuid4(),
            job_id=job_id,
            job_source_task_id=task_id,
            source_type=SourceType.COCO.value,
            source_item_id="existing_asset_1",
            nas_path="/tmp/fake.jpg",
        )
        db.add(asset)
        db.commit()

    adm = admit_candidate_for_task(task_id, SourceType.COCO, "existing_asset_1")
    assert adm.status == CandidateAdmissionStatus.DUPLICATE


def test_admit_candidate_capacity_and_window_full(isolated_db):
    # Window limit = 2
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=2)

    adm1 = admit_candidate_for_task(task_id, SourceType.COCO, "item_1")
    assert adm1.status == CandidateAdmissionStatus.ADMITTED

    adm2 = admit_candidate_for_task(task_id, SourceType.COCO, "item_2")
    assert adm2.status == CandidateAdmissionStatus.ADMITTED

    # 3rd candidate must be rejected with WINDOW_FULL
    adm3 = admit_candidate_for_task(task_id, SourceType.COCO, "item_3")
    assert adm3.status == CandidateAdmissionStatus.WINDOW_FULL

    with isolated_db() as db:
        win = db.get(CandidateWindow, window_id)
        assert win.status == "FULL"


def test_mark_candidate_staged_and_failed(isolated_db):
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=5)
    adm = admit_candidate_for_task(task_id, SourceType.COCO, "item_staged")
    assert adm.status == CandidateAdmissionStatus.ADMITTED

    asset_id = uuid.uuid4()
    ok = mark_candidate_staged(adm.reservation_id, asset_id)
    assert ok is True

    with isolated_db() as db:
        res = db.get(CandidateReservation, adm.reservation_id)
        assert res.status == "STAGED"
        assert res.asset_id == asset_id

    # Marking staged as failed should return False
    assert mark_candidate_failed(adm.reservation_id) is False

    # New reservation marked as failed directly
    adm_fail = admit_candidate_for_task(task_id, SourceType.COCO, "item_failed")
    assert mark_candidate_failed(adm_fail.reservation_id) is True
    with isolated_db() as db:
        res = db.get(CandidateReservation, adm_fail.reservation_id)
        assert res.status == "FAILED"


def test_candidate_window_stats(isolated_db):
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=10)
    adm1 = admit_candidate_for_task(task_id, SourceType.COCO, "s1")
    adm2 = admit_candidate_for_task(task_id, SourceType.COCO, "s2")
    mark_candidate_staged(adm1.reservation_id, uuid.uuid4())
    mark_candidate_failed(adm2.reservation_id)

    stats = get_candidate_window_stats(window_id)
    assert stats["window_id"] == str(window_id)
    assert stats["reserved"] == 2
    assert stats["limit"] == 10
    assert stats["remaining"] == 8
    assert stats["staged"] == 1
    assert stats["failed"] == 1


def test_set_candidate_window_status_guard(isolated_db):
    job_id, batch_id, task_id, window_id = _setup_job_and_batch(isolated_db, candidate_limit=10)
    assert set_candidate_window_status(window_id, "PROCESSING") is True

    with isolated_db() as db:
        win = db.get(CandidateWindow, window_id)
        assert win.status == "PROCESSING"

    # Terminal states directly via set_candidate_window_status must be rejected
    with pytest.raises(ValueError, match="Terminal states require verified NAS"):
        set_candidate_window_status(window_id, "COMPLETED")
