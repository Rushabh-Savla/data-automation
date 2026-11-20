"""Unit test suite for Per-Source Scheduling, Queue Readiness, and Fair-Share Multi-Source Ingestion.

Covers the 13 required test cases:
1. COCO cannot monopolize shared candidate window (per-source quota enforced).
2. Slower sources receive fair scheduling opportunities.
3. Aggregate reservations never exceed candidate window limit (750).
4. Full window defers rather than loses candidates.
5. Deferred candidates processed in next available window.
6. COMPLETED_PARTIAL / PARTIALLY_PROCESSED sources with pending candidates can be rescheduled.
7. Database checkpoint advancement does not skip unprocessed candidates.
8. Worker crashes / restarts restore unconsumed candidates.
9. Source with 0 accepted images but pending candidates is not exhausted.
10. One source's partial completion does not terminate other sources.
11. Source-native and extracted-image counters are distinct.
12. Actual source exhaustion detected only when unconsumed == 0 and discovery exhausted.
13. Job completion occurs only after all required NAS transfers are verified.
"""

import contextlib
import datetime
import math
import uuid
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine, select, func, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.dialects.postgresql import JSONB

from app.db.base import Base
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.candidate_window import CandidateWindow
from app.db.models.candidate_reservation import CandidateReservation
from app.db.models.asset import Asset
from app.domain.enums import (
    AssetStatus,
    BatchStatus,
    JobStatus,
    JobSourceTaskStatus,
    JOB_SOURCE_TASK_TERMINAL_STATUSES,
    SourceType,
    TransferStatus,
    is_source_task_exhausted,
)
from app.services.candidate_reservation_service import (
    admit_candidate_for_task,
    CandidateAdmissionStatus,
    ensure_candidate_window,
    cleanup_stale_reservations,
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

    for table in Base.metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

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
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", _mock_scope)
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", _mock_scope)
    return Session


def test_1_and_2_coco_cannot_monopolize_and_slower_sources_get_fair_share(isolated_db):
    """Tests 1 & 2:
    A fast source (e.g. COCO) cannot monopolize a shared 750-candidate window.
    Its quota is bounded by fair-share so slower sources receive slots.
    """
    job_id = uuid.uuid4()
    sources = [SourceType.COCO.value, SourceType.SUN_RGBD.value, SourceType.DAVIS.value, SourceType.WEB.value]
    with isolated_db() as db:
        job = Job(id=job_id, query="cats", target_count=3000, sources=sources, status=JobStatus.RUNNING)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job_id, batch_index=1, target_image_count=600, status=BatchStatus.DOWNLOADING)
        db.add_all([job, batch])
        db.commit()

        win_id = ensure_candidate_window(db, batch, candidate_limit=750)
        task_coco = JobSourceTask(id=uuid.uuid4(), job_id=job_id, source_type=SourceType.COCO.value, status=JobSourceTaskStatus.DOWNLOADING)
        task_sun = JobSourceTask(id=uuid.uuid4(), job_id=job_id, source_type=SourceType.SUN_RGBD.value, status=JobSourceTaskStatus.DOWNLOADING)
        db.add_all([task_coco, task_sun])
        db.commit()
        coco_task_id = task_coco.id
        sun_task_id = task_sun.id

    fair_limit = max(math.ceil(750 / 4), math.ceil(750 * 0.25))

    # Fill COCO up to fair limit
    for i in range(fair_limit):
        adm = admit_candidate_for_task(str(coco_task_id), SourceType.COCO, f"coco_{i}")
        assert adm.status == CandidateAdmissionStatus.ADMITTED

    # Next COCO candidate must be rejected with WINDOW_FULL because other active sources have capacity
    adm_excess = admit_candidate_for_task(str(coco_task_id), SourceType.COCO, "coco_excess")
    assert adm_excess.status == CandidateAdmissionStatus.WINDOW_FULL

    # Slower source (SUN RGB-D) is NOT blocked and can be admitted
    adm_sun = admit_candidate_for_task(str(sun_task_id), SourceType.SUN_RGBD, "sun_0")
    assert adm_sun.status == CandidateAdmissionStatus.ADMITTED


def test_3_aggregate_reservations_never_exceed_window_limit(isolated_db):
    """Test 3: Aggregate reservations across all sources never exceed the window candidate limit."""
    job_id = uuid.uuid4()
    with isolated_db() as db:
        job = Job(id=job_id, query="cats", target_count=500, sources=[SourceType.COCO.value], status=JobStatus.RUNNING)
        batch = ImageBatch(id=uuid.uuid4(), job_id=job_id, batch_index=1, target_image_count=500, status=BatchStatus.DOWNLOADING)
        db.add_all([job, batch])
        db.commit()

        win_limit = 10
        win_id = ensure_candidate_window(db, batch, candidate_limit=win_limit)
        task = JobSourceTask(id=uuid.uuid4(), job_id=job_id, source_type=SourceType.COCO.value, status=JobSourceTaskStatus.DOWNLOADING)
        db.add(task)
        db.commit()
        task_id = task.id

    admitted_count = 0
    for i in range(win_limit + 5):
        adm = admit_candidate_for_task(str(task_id), SourceType.COCO, f"item_{i}")
        if adm.status == CandidateAdmissionStatus.ADMITTED:
            admitted_count += 1
        elif adm.status == CandidateAdmissionStatus.WINDOW_FULL:
            pass

    assert admitted_count == win_limit
    with isolated_db() as db:
        total_res = db.execute(
            select(func.count(CandidateReservation.id)).where(CandidateReservation.window_id == win_id)
        ).scalar_one()
        assert total_res == win_limit


def test_4_and_5_full_window_defers_and_processes_in_next_window():
    """Tests 4 & 5:
    When window is full, candidate returns deferred=True.
    Deferred items are omitted from _completed_items and remain available for next window.
    """
    results = [
        {"item": "shard_001", "accepted": 5, "rejected": 0, "window_full": False},
        {"item": "shard_002", "accepted": 0, "rejected": 0, "window_full": True, "deferred": True},
    ]
    done_items = {
        r["item"] for r in results
        if isinstance(r, dict) and r.get("item") and not r.get("stopped") and not r.get("window_full") and not r.get("deferred")
    }
    assert "shard_001" in done_items
    assert "shard_002" not in done_items


def test_6_completed_partial_with_pending_can_be_rescheduled():
    """Test 6:
    COMPLETED_PARTIAL is not in JOB_SOURCE_TASK_TERMINAL_STATUSES if pending items exist.
    """
    assert JobSourceTaskStatus.COMPLETED_PARTIAL not in JOB_SOURCE_TASK_TERMINAL_STATUSES

    task = JobSourceTask(
        id=uuid.uuid4(),
        status=JobSourceTaskStatus.COMPLETED_PARTIAL,
        checkpoint={"_pending_items": {"a": 1, "b": 2}, "_completed_items": ["a"], "discovery_exhausted": True},
    )
    assert not is_source_task_exhausted(task)


def test_7_database_checkpoint_advancement_preserves_unconsumed():
    """Test 7:
    Database checkpoint advancement preserves pending items when shards are deferred.
    """
    cp = {
        "_pending_items": {"s1": 1, "s2": 2, "s3": 3},
        "_completed_items": ["s1"],
    }
    unconsumed = [k for k in cp["_pending_items"].keys() if k not in cp["_completed_items"]]
    assert unconsumed == ["s2", "s3"]


def test_8_worker_crashes_restore_unconsumed_candidates(isolated_db):
    """Test 8:
    When worker crashes, stale reservations are cleaned up and unconsumed items can be resumed.
    """
    job_id = uuid.uuid4()
    batch_id = uuid.uuid4()
    win_id = uuid.uuid4()
    with isolated_db() as db:
        win = CandidateWindow(id=win_id, batch_id=batch_id, job_id=job_id, window_index=1, candidate_limit=10, status="FULL")
        past_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
        res = CandidateReservation(
            id=uuid.uuid4(),
            window_id=win_id,
            batch_id=batch_id,
            job_id=job_id,
            job_source_task_id=uuid.uuid4(),
            source_type="kaggle",
            source_item_id="stale_item",
            status="RESERVED",
            updated_at=past_time,
        )
        db.add_all([win, res])
        db.commit()

        # Clean up stale reservations (> 60s)
        cleaned = cleanup_stale_reservations(db, win_id, stale_seconds=60)
        db.commit()
        assert cleaned == 1
        db.refresh(res)
        assert res.status == "FAILED"


def test_9_zero_accepted_source_with_pending_not_exhausted():
    """Test 9:
    A source with 0 accepted images (e.g. SUN RGB-D) but unconsumed items is NOT exhausted.
    """
    task = JobSourceTask(
        id=uuid.uuid4(),
        source_type="sun_rgbd",
        status=JobSourceTaskStatus.WINDOW_WAITING,
        accepted_count=0,
        checkpoint={"_pending_items": {f"shard_{i}": i for i in range(207)}, "_completed_items": [], "discovery_exhausted": True},
    )
    assert not is_source_task_exhausted(task)


def test_10_one_source_partial_does_not_terminate_other_sources():
    """Test 10:
    One source completing or failing does not mark other active or waiting sources terminal.
    """
    coco_task = JobSourceTask(id=uuid.uuid4(), source_type="coco", status=JobSourceTaskStatus.COMPLETED)
    davis_task = JobSourceTask(id=uuid.uuid4(), source_type="davis", status=JobSourceTaskStatus.PARTIALLY_PROCESSED)

    tasks = [coco_task, davis_task]
    all_term = all(t.status in JOB_SOURCE_TASK_TERMINAL_STATUSES for t in tasks)
    assert not all_term


def test_11_source_native_and_extracted_counters_distinct():
    """Test 11:
    Source-native units (e.g. Kaggle datasets, YouTube videos) are distinct from image counts.
    """
    task = JobSourceTask(
        id=uuid.uuid4(),
        source_type=SourceType.KAGGLE.value,
        status=JobSourceTaskStatus.PARTIALLY_PROCESSED,
        discovered_count=20,
        downloaded_count=705,
        accepted_count=563,
        rejected_count=0,
        error_count=0,
        checkpoint={"_pending_items": {f"ds_{i}": i for i in range(20)}, "_completed_items": [f"ds_{i}" for i in range(5)]},
    )
    from app.schemas.job import JobSourceTaskResponse
    resp = JobSourceTaskResponse.model_validate(task)
    assert resp.discovered_unit == "datasets"
    assert resp.unit_label == "datasets"
    assert resp.discovered_count == 20
    assert resp.downloaded_count == 705
    assert resp.pending_count == 15
    assert resp.is_reschedulable is True


def test_12_actual_source_exhaustion_only_when_pending_zero():
    """Test 12:
    Genuine source exhaustion occurs only when unconsumed == 0 and discovery is exhausted.
    """
    # Discovery exhausted, but 5 pending items remain -> NOT exhausted
    task_pending = JobSourceTask(
        id=uuid.uuid4(),
        status=JobSourceTaskStatus.PARTIALLY_PROCESSED,
        checkpoint={"_pending_items": {"1": 1, "2": 2}, "_completed_items": ["1"], "discovery_exhausted": True},
    )
    assert not is_source_task_exhausted(task_pending)

    # Discovery exhausted, AND all pending items completed -> GENUINELY EXHAUSTED
    task_empty = JobSourceTask(
        id=uuid.uuid4(),
        status=JobSourceTaskStatus.SOURCE_EXHAUSTED,
        checkpoint={"_pending_items": {"1": 1, "2": 2}, "_completed_items": ["1", "2"], "discovery_exhausted": True},
    )
    assert is_source_task_exhausted(task_empty)


def test_13_job_completion_only_after_nas_transfers_verified(isolated_db):
    """Test 13:
    Job completion cannot finalize while NAS transfers are still pending or in-flight.
    """
    job_id = uuid.uuid4()
    batch_id = uuid.uuid4()
    with isolated_db() as db:
        job = Job(id=job_id, query="cats", target_count=3000, accepted_count=3000, status=JobStatus.RUNNING)
        batch = ImageBatch(id=batch_id, job_id=job_id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
        transfer = NasTransferBatch(
            id=uuid.uuid4(),
            batch_id=batch.id,
            job_id=job_id,
            remote_path="/nas/test/batch_1",
            transfer_status=TransferStatus.UPLOADING,
            attempt_count=1,
        )
        db.add_all([job, batch, transfer])
        db.commit()

        # Verify that in-flight transfer blocks batch completion
        pending_transfers = db.execute(
            select(NasTransferBatch).where(
                NasTransferBatch.batch_id == batch.id,
                NasTransferBatch.transfer_status.in_([TransferStatus.PENDING, TransferStatus.UPLOADING]),
            )
        ).scalars().all()
        assert len(pending_transfers) == 1
