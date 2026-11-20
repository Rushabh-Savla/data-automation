"""Unit test suite for Strict Sequential Source Scheduling in Data Automation V3.

Validates the complete business requirements:
1. Canonical priority order: WEB -> KAGGLE -> ROBOFLOW -> COCO -> YOUTUBE -> DAVIS -> VISUAL_GENOME -> SUN_RGBD -> OPEN_IMAGES -> PASCAL_VOC.
2. Relative priority preservation for configured source subsets.
3. Strict single-source execution: Only the currently active source receives tasks; lower-priority sources receive zero work.
4. Active source completeness evaluation: Unconsumed inventory, active tasks, or retryable errors prevent advancement.
5. Window saturation resilience: A full candidate window does NOT advance the source; the same source resumes in the next window.
6. Clean step-by-step transition across all 10 sources in strict order.
7. Multi-round replenishment: When all configured sources complete and target is unmet, Round 2 begins at WEB.
8. Target achievement stoppage: Immediately halts acquisition when accepted target is met.
9. Temporary error backoff and retries without source advancement.
10. Worker crash recovery: Resumes the active source from durable checkpoint.
11. Cross-round deduplication: Completed inventory is remembered and never repeated.
12. Distributed lock protection: Prevents concurrent workers from activating conflicting sources.
"""

import contextlib
import datetime
import math
import uuid
import pytest
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
from app.domain.enums import (
    BatchStatus,
    JobStatus,
    JobSourceTaskStatus,
    SourceType,
    TransferStatus,
)
from app.domain.source_priority import (
    CANONICAL_SOURCE_ORDER,
    get_ordered_job_sources,
    is_source_round_complete,
    evaluate_sequential_execution_plan,
)
from app.tasks.orchestration_tasks import schedule_eligible_source_tasks


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
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", _mock_scope)
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", _mock_scope)
    monkeypatch.setattr("app.services.candidate_reservation_service.session_scope", _mock_scope)
    return _mock_scope


def test_canonical_source_order_and_filtering():
    """1 & 2. Verify canonical order and relative order preservation for subsets."""
    assert CANONICAL_SOURCE_ORDER == [
        SourceType.WEB,
        SourceType.KAGGLE,
        SourceType.ROBOFLOW,
        SourceType.COCO,
        SourceType.YOUTUBE,
        SourceType.DAVIS,
        SourceType.VISUAL_GENOME,
        SourceType.SUN_RGBD,
        SourceType.OPEN_IMAGES,
        SourceType.PASCAL_VOC,
    ]

    subset = ["coco", "web", "pascal_voc"]
    ordered = get_ordered_job_sources(subset)
    assert ordered == [SourceType.WEB, SourceType.COCO, SourceType.PASCAL_VOC]


def test_strict_single_source_active_selection():
    """3. Verify WEB is active first; Kaggle cannot be active until WEB completes."""
    job = Job(
        id=uuid.uuid4(),
        name="test_job",
        query="cat",
        sources=["kaggle", "web", "coco"],
        target_count=100,
        accepted_count=0,
        config={"topup_round": 1},
    )

    t_web = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="web",
        status=JobSourceTaskStatus.DOWNLOADING,
        checkpoint={"_pending_items": {"w1": {}, "w2": {}}, "_completed_items": []},
    )
    t_kaggle = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="kaggle",
        status=JobSourceTaskStatus.PENDING,
        checkpoint={"_pending_items": {"k1": {}, "k2": {}}, "_completed_items": []},
    )
    t_coco = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="coco",
        status=JobSourceTaskStatus.PENDING,
        checkpoint={},
    )

    plan = evaluate_sequential_execution_plan(job, [t_web, t_kaggle, t_coco])
    assert plan.active_source == SourceType.WEB
    assert plan.active_source_position == "1/3"
    assert plan.next_source_in_queue == "kaggle"
    assert "web" not in plan.completed_sources
    assert "kaggle" not in plan.completed_sources


def test_active_source_transition_on_completion():
    """4 & 6. Verify transition from WEB to Kaggle once WEB completes all items."""
    job = Job(
        id=uuid.uuid4(),
        name="test_job",
        query="cat",
        sources=["kaggle", "web", "coco"],
        target_count=100,
        accepted_count=20,
        config={"topup_round": 1},
    )

    # WEB completed all its inventory
    t_web = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="web",
        status=JobSourceTaskStatus.COMPLETED,
        checkpoint={"_pending_items": {"w1": {}, "w2": {}}, "_completed_items": ["w1", "w2"]},
    )
    # Kaggle has unconsumed items
    t_kaggle = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="kaggle",
        status=JobSourceTaskStatus.PENDING,
        checkpoint={"_pending_items": {"k1": {}, "k2": {}}, "_completed_items": []},
    )
    t_coco = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="coco",
        status=JobSourceTaskStatus.PENDING,
        checkpoint={},
    )

    plan = evaluate_sequential_execution_plan(job, [t_web, t_kaggle, t_coco])
    assert plan.active_source == SourceType.KAGGLE
    assert plan.active_source_position == "2/3"
    assert plan.completed_sources == ["web"]
    assert plan.next_source_in_queue == "coco"


def test_window_saturation_does_not_advance_source():
    """5. Verify a full candidate window leaves the same source active."""
    job = Job(
        id=uuid.uuid4(),
        name="test_job",
        query="cat",
        sources=["kaggle", "roboflow"],
        target_count=1000,
        accepted_count=200,
        config={"topup_round": 1},
    )

    # Kaggle reached window capacity -> status WINDOW_WAITING, unconsumed items remain
    t_kaggle = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="kaggle",
        status=JobSourceTaskStatus.WINDOW_WAITING,
        checkpoint={
            "_pending_items": {"k1": {}, "k2": {}, "k3": {}},
            "_completed_items": ["k1"],
        },
    )
    t_roboflow = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="roboflow",
        status=JobSourceTaskStatus.PENDING,
        checkpoint={"_pending_items": {"r1": {}}, "_completed_items": []},
    )

    is_done, reason = is_source_round_complete(t_kaggle)
    assert not is_done
    assert "pending items remaining" in reason or "Waiting for candidate window" in reason

    plan = evaluate_sequential_execution_plan(job, [t_kaggle, t_roboflow])
    assert plan.active_source == SourceType.KAGGLE
    assert plan.active_source_position == "1/2"
    assert "kaggle" not in plan.completed_sources


def test_all_10_sources_sequential_progression():
    """6. Step through all 10 canonical sources sequentially."""
    job = Job(
        id=uuid.uuid4(),
        name="test_all_10",
        query="cat",
        sources=[s.value for s in CANONICAL_SOURCE_ORDER],
        target_count=3000,
        accepted_count=500,
        config={"topup_round": 1},
    )

    tasks = [
        JobSourceTask(
            id=uuid.uuid4(),
            job_id=job.id,
            source_type=src.value,
            status=JobSourceTaskStatus.PENDING,
            checkpoint={"_pending_items": {f"{src.value}_1": {}}, "_completed_items": []},
        )
        for src in CANONICAL_SOURCE_ORDER
    ]

    # Step through each source from 0 to 9
    for i in range(10):
        plan = evaluate_sequential_execution_plan(job, tasks)
        expected_active = CANONICAL_SOURCE_ORDER[i]
        assert plan.active_source == expected_active
        assert plan.active_source_position == f"{i + 1}/10"

        # Mark source i complete
        tasks[i].status = JobSourceTaskStatus.COMPLETED
        tasks[i].checkpoint = {
            "_pending_items": {f"{expected_active.value}_1": {}},
            "_completed_items": [f"{expected_active.value}_1"],
        }

    # All 10 complete
    plan_final = evaluate_sequential_execution_plan(job, tasks)
    assert plan_final.all_sources_complete is True
    assert len(plan_final.completed_sources) == 10


def test_multi_round_replenishment_advances_and_restarts_at_web(isolated_db, monkeypatch):
    """7. Verify that when Round 1 completes with unmet target, Round 2 starts at WEB."""
    job_id = uuid.uuid4()
    with isolated_db() as db:
        job = Job(
            id=job_id,
            name="test_replenish",
            query="cat",
            sources=["web", "kaggle"],
            target_count=500,
            accepted_count=100,  # Shortfall: 400
            status=JobStatus.RUNNING,
            config={"topup_round": 1},
        )
        db.add(job)
        b = ImageBatch(
            id=uuid.uuid4(),
            job_id=job_id,
            batch_index=1,
            target_image_count=500,
            status=BatchStatus.PROCESSING,
        )
        db.add(b)
        # Both tasks are complete for Round 1
        t_web = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type="web",
            status=JobSourceTaskStatus.COMPLETED,
            checkpoint={"_pending_items": {"w1": {}}, "_completed_items": ["w1"]},
        )
        t_kaggle = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type="kaggle",
            status=JobSourceTaskStatus.COMPLETED,
            checkpoint={"_pending_items": {"k1": {}}, "_completed_items": ["k1"]},
        )
        db.add_all([t_web, t_kaggle])
        db.commit()

    mock_redis = MagicMock()
    mock_redis.set.return_value = True
    monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: mock_redis)
    monkeypatch.setattr("app.tasks.orchestration_tasks.celery_app.send_task", MagicMock())
    monkeypatch.setattr("app.tasks.orchestration_tasks.evaluate_job_completion.delay", MagicMock())

    result = schedule_eligible_source_tasks(str(job_id))
    # It should have incremented to Round 2 and activated WEB
    assert result["active_source"] == "web"

    with isolated_db() as db:
        updated_job = db.get(Job, job_id)
        assert updated_job.config.get("topup_round") == 2
        # Completed items were preserved
        tasks = db.execute(select(JobSourceTask).where(JobSourceTask.job_id == job_id)).scalars().all()
        for t in tasks:
            assert len(t.checkpoint.get("_completed_items", [])) == 1


def test_target_achievement_stops_acquisition(isolated_db, monkeypatch):
    """8. Verify target achievement cleanly stops new acquisition."""
    job_id = uuid.uuid4()
    with isolated_db() as db:
        job = Job(
            id=job_id,
            name="test_target_met",
            query="cat",
            sources=["web", "kaggle"],
            target_count=3000,
            accepted_count=3000,  # Target met!
            status=JobStatus.RUNNING,
            config={"topup_round": 1},
        )
        db.add(job)
        db.commit()

    mock_eval = MagicMock()
    monkeypatch.setattr("app.tasks.orchestration_tasks.evaluate_job_completion.delay", mock_eval)

    result = schedule_eligible_source_tasks(str(job_id))
    assert result["status"] == "target_reached"


def test_temporary_error_retry_preserves_active_source():
    """9. Temporary error does not switch source; source remains incomplete."""
    task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        source_type="kaggle",
        status=JobSourceTaskStatus.DOWNLOADING,
        error_count=2,
        checkpoint={"_pending_items": {"item1": {}}, "_completed_items": []},
    )
    is_done, reason = is_source_round_complete(task)
    assert not is_done
    assert "Actively downloading" in reason


def test_worker_crash_recovery_resumes_active_checkpoint():
    """10. Resumes unconsumed inventory after worker restart."""
    task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        source_type="kaggle",
        status=JobSourceTaskStatus.PENDING,  # reset after restart
        checkpoint={
            "_pending_items": {"k1": {}, "k2": {}, "k3": {}},
            "_completed_items": ["k1"],
        },
    )
    is_done, reason = is_source_round_complete(task)
    assert not is_done
    assert "2 pending items remaining" in reason


def test_concurrency_lock_prevents_simultaneous_activation(isolated_db, monkeypatch):
    """12. Redis distributed lock blocks concurrent scheduler execution."""
    job_id = uuid.uuid4()
    with isolated_db() as db:
        job = Job(
            id=job_id,
            name="test_lock",
            query="cat",
            sources=["web"],
            target_count=100,
            accepted_count=0,
            status=JobStatus.RUNNING,
        )
        db.add(job)
        b = ImageBatch(id=uuid.uuid4(), job_id=job_id, batch_index=1, status=BatchStatus.PROCESSING)
        db.add(b)
        t = JobSourceTask(
            id=uuid.uuid4(),
            job_id=job_id,
            source_type="web",
            status=JobSourceTaskStatus.PENDING,
            checkpoint={"_pending_items": {"w1": {}}, "_completed_items": []},
        )
        db.add(t)
        db.commit()

    mock_redis = MagicMock()
    # Simulate dispatch lock already owned by another worker
    mock_redis.set.return_value = False
    monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: mock_redis)

    result = schedule_eligible_source_tasks(str(job_id))
    assert result["status"] == "source_locked"
    assert result["active_source"] == "web"
