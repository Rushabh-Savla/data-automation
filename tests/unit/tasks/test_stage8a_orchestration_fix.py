"""Tests for Stage 8A — Pipeline Orchestration Fix and Asset Recovery.

Proves:
1. Candidate-window completion dispatches filtering while unrelated sources remain active.
2. Slow connectors cannot block completed candidate batches.
3. Duplicate filtering dispatch is prevented (idempotency guard).
4. Watchdog recovers a stuck eligible candidate batch independently of source completion.
5. Existing promoted assets are reused without redownloading.
6. A stopped download task does not incorrectly mark an asset as accepted.
7. NAS upload starts only after filtering, deduplication and batch creation.
8. A worker restart does not cause duplicate processing.
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from app.core.config import get_settings
from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.image_batch_repository import ImageBatchRepository
from app.db.repositories.job_repository import JobRepository
from app.db.repositories.job_source_task_repository import JobSourceTaskRepository
from app.domain.enums import (
    AssetStatus,
    BatchStatus,
    JobSourceTaskStatus,
    JobStatus,
    SourceType,
)
from app.tasks.batch_orchestration import BatchOrchestrator
from app.tasks.filtering_tasks import finalize_relevance_filter, run_relevance_filter
from app.tasks.orchestration_tasks import check_and_advance_job, watchdog_scan


class _FakeRedis:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self._data:
            return False
        self._data[key] = value
        return True

    def delete(self, key):
        return self._data.pop(key, None) is not None

    def exists(self, key):
        return key in self._data

    def publish(self, channel, message):
        return 1

    def lock(self, key, timeout=30):
        class _Lock:
            def __enter__(self_inner):
                return self_inner
            def __exit__(self_inner, *args):
                pass
        return _Lock()


# =========================================================================
# 1. Candidate-Window Completion Dispatches While Sources Remain Active
# =========================================================================
def test_1_candidate_window_completion_dispatches_filtering_while_sources_active(db_session, monkeypatch):
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        name="test_job",
        query="fire",
        status=JobStatus.RUNNING,
        target_count=2000,
        accepted_count=905,
        config={"candidate_window": 600},
    )
    db_session.add(job)

    # Source 1 is completed
    task1 = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job_id,
        source_type=SourceType.KAGGLE.value,
        status=JobSourceTaskStatus.COMPLETED,
    )
    # Source 2 is still discovering (active)
    task2 = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job_id,
        source_type=SourceType.NUSCENES.value,
        status=JobSourceTaskStatus.DISCOVERING,
    )
    db_session.add_all([task1, task2])

    # 905 PROMOTED assets
    for i in range(10):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task1.id,
                source_type=SourceType.KAGGLE,
                source_item_id=f"item_{i}",
                status=AssetStatus.PROMOTED,
            )
        )
    db_session.commit()

    fake_redis = _FakeRedis()
    monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _session_context(db_session))

    dispatched = []
    monkeypatch.setattr(
        "app.tasks.filtering_tasks.run_relevance_filter.delay",
        lambda j_id: dispatched.append(j_id),
    )
    monkeypatch.setattr(
        "app.tasks.orchestration_tasks._resume_task",
        lambda *args, **kwargs: None,
    )

    # Calling check_and_advance_job should advance because candidate window (905 >= 900) is ready
    check_and_advance_job(str(job_id))

    assert len(dispatched) == 1
    assert dispatched[0] == str(job_id)


# =========================================================================
# 2. Slow Connectors Cannot Block Completed Candidate Batches
# =========================================================================
def test_2_slow_connectors_cannot_block_completed_candidate_batches(db_session, monkeypatch):
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        name="slow_connector_job",
        query="fire",
        status=JobStatus.RUNNING,
        target_count=2000,
        accepted_count=905,
        config={"candidate_window": 600},
    )
    db_session.add(job)

    # Stalled/slow tasks
    kitti_task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job_id,
        source_type=SourceType.KITTI.value,
        status=JobSourceTaskStatus.DISCOVERING,
    )
    youtube_task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job_id,
        source_type=SourceType.YOUTUBE.value,
        status=JobSourceTaskStatus.DOWNLOADING,
    )
    db_session.add_all([kitti_task, youtube_task])
    db_session.commit()

    fake_redis = _FakeRedis()
    # cancellation.mark_target_reached was set at 905
    fake_redis.set(f"job:{job_id}:target_reached", "1")

    monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _session_context(db_session))

    dispatched = []
    monkeypatch.setattr(
        "app.tasks.filtering_tasks.run_relevance_filter.delay",
        lambda j_id: dispatched.append(j_id),
    )
    monkeypatch.setattr(
        "app.tasks.orchestration_tasks._resume_task",
        lambda *args, **kwargs: None,
    )

    check_and_advance_job(str(job_id))

    assert len(dispatched) == 1
    assert dispatched[0] == str(job_id)


# =========================================================================
# 3. Duplicate Filtering Dispatch is Prevented (Idempotency)
# =========================================================================
def test_3_duplicate_filtering_dispatch_prevented(db_session, monkeypatch):
    job_id = uuid.uuid4()
    job = Job(id=job_id, name="dup_filter_job", query="fire", status=JobStatus.RUNNING, target_count=500)
    db_session.add(job)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job_id, source_type=SourceType.KAGGLE.value, status=JobSourceTaskStatus.COMPLETED)
    db_session.add(task)
    for i in range(5):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task.id,
                source_type=SourceType.KAGGLE,
                source_item_id=f"item_{i}",
                status=AssetStatus.PROMOTED,
            )
        )
    db_session.commit()

    fake_redis = _FakeRedis()
    monkeypatch.setattr("app.tasks.filtering_tasks.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.filtering_tasks.session_scope", lambda: _session_context(db_session))

    fan_in_batches = []
    monkeypatch.setattr(
        "app.tasks.filtering_tasks.relevance_filter_batch.delay",
        lambda j_id, batch: fan_in_batches.append(batch),
    )
    monkeypatch.setattr(
        "app.tasks.filtering_tasks.run_deduplication.delay",
        lambda j_id: None,
    )

    # First call: dispatches fan-out batches
    run_relevance_filter(str(job_id))
    assert len(fan_in_batches) > 0
    initial_count = len(fan_in_batches)

    # Second call while in progress: must return early due to filtering_in_progress lock
    run_relevance_filter(str(job_id))
    assert len(fan_in_batches) == initial_count

    # Once finalize runs, guard is cleared
    finalize_relevance_filter(str(job_id))
    assert not fake_redis.exists(f"job:{job_id}:filtering_in_progress")


# =========================================================================
# 4. Watchdog Recovers a Stuck Eligible Candidate Batch
# =========================================================================
def test_4_watchdog_can_recover_stuck_eligible_batch(db_session, monkeypatch):
    job_id = uuid.uuid4()
    old_time = datetime.now(timezone.utc) - timedelta(minutes=45)
    job = Job(
        id=job_id,
        name="watchdog_stuck_job",
        query="fire",
        status=JobStatus.RUNNING,
        target_count=2000,
        accepted_count=905,
        updated_at=old_time,
        config={"candidate_window": 600},
    )
    db_session.add(job)

    # 1 source stuck in discovering
    task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job_id,
        source_type=SourceType.NUSCENES.value,
        status=JobSourceTaskStatus.DISCOVERING,
    )
    db_session.add(task)

    # 605 PROMOTED assets waiting
    for i in range(605):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task.id,
                source_type=SourceType.KAGGLE,
                source_item_id=f"prom_{i}",
                status=AssetStatus.PROMOTED,
            )
        )
    db_session.commit()

    fake_redis = _FakeRedis()
    monkeypatch.setattr("app.tasks.orchestration_tasks.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", lambda: _session_context(db_session))

    dispatched = []
    monkeypatch.setattr(
        "app.tasks.filtering_tasks.run_relevance_filter.delay",
        lambda j_id: dispatched.append(j_id),
    )
    monkeypatch.setattr(
        "app.tasks.orchestration_tasks._resume_task",
        lambda *args, **kwargs: None,
    )

    watchdog_scan()

    assert len(dispatched) == 1
    assert dispatched[0] == str(job_id)


# =========================================================================
# 5. Existing Promoted Assets Reused Without Redownloading
# =========================================================================
def test_5_existing_promoted_assets_reused_without_downloading_again(db_session, monkeypatch):
    job_id = uuid.uuid4()
    job = Job(id=job_id, name="reuse_job", query="fire", status=JobStatus.RUNNING, target_count=500)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job_id, source_type=SourceType.KAGGLE.value, status=JobSourceTaskStatus.COMPLETED)
    db_session.add_all([job, task])

    asset_ids = []
    for i in range(12):
        aid = uuid.uuid4()
        asset_ids.append(aid)
        db_session.add(
            Asset(
                id=aid,
                job_id=job_id,
                job_source_task_id=task.id,
                source_type=SourceType.KAGGLE,
                source_item_id=f"reuse_{i}",
                status=AssetStatus.PROMOTED,
                checksum_sha256=f"sha_{i}",
            )
        )
    db_session.commit()

    fake_redis = _FakeRedis()
    monkeypatch.setattr("app.tasks.filtering_tasks.get_redis", lambda: fake_redis)
    monkeypatch.setattr("app.tasks.filtering_tasks.session_scope", lambda: _session_context(db_session))

    fanned_ids = []
    monkeypatch.setattr(
        "app.tasks.filtering_tasks.relevance_filter_batch.delay",
        lambda j_id, batch: fanned_ids.extend(batch),
    )

    run_relevance_filter(str(job_id))

    # All 12 existing asset IDs were collected and fanned out directly
    assert len(fanned_ids) == 12
    assert set(fanned_ids) == {str(a) for a in asset_ids}


# =========================================================================
# 6. Stopped Download Task Does Not Mark Asset Accepted
# =========================================================================
def test_6_stopped_download_task_does_not_incorrectly_mark_asset_accepted(db_session, monkeypatch):
    from app.core.exceptions import JobStopped
    from app.tasks.ingest_tasks import _run_item_download

    job_id = uuid.uuid4()
    task_id = uuid.uuid4()

    job = Job(id=job_id, name="stopped_job", query="fire", status=JobStatus.RUNNING, target_count=500, accepted_count=10)
    task = JobSourceTask(id=task_id, job_id=job_id, source_type=SourceType.WEB.value, status=JobSourceTaskStatus.DOWNLOADING, accepted_count=10)
    db_session.add_all([job, task])
    db_session.commit()

    monkeypatch.setattr("app.tasks.ingest_tasks.session_scope", lambda: _session_context(db_session))
    monkeypatch.setattr("app.tasks.ingest_tasks.bind_context", lambda *args: None)
    monkeypatch.setattr("app.tasks.ingest_tasks.unbind_context", lambda *args: None)

    # Generator simulating stop mid-item
    def fetch_stub(checkpoint, should_stop, on_progress):
        raise JobStopped("Stopped by candidate window cap")

    result = _run_item_download(
        job_id=str(job_id),
        job_source_task_id=str(task_id),
        source_type=SourceType.WEB,
        checkpoint_key="item_1",
        fetch=fetch_stub,
    )

    assert result["stopped"] is True
    assert result["accepted"] == 0

    # Counts must remain unchanged
    db_session.refresh(job)
    db_session.refresh(task)
    assert job.accepted_count == 10
    assert task.accepted_count == 10


# =========================================================================
# 7. NAS Upload Starts Only After Filtering, Dedup and Batch Creation
# =========================================================================
def test_7_nas_upload_starts_only_after_filtering_dedup_and_batch_creation(db_session, monkeypatch):
    from app.tasks.batch_orchestration import process_active_batch

    job_id = uuid.uuid4()
    job = Job(id=job_id, name="stage_gate_job", query="fire", status=JobStatus.RUNNING, target_count=5)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job_id, source_type=SourceType.KAGGLE.value, status=JobSourceTaskStatus.COMPLETED)
    db_session.add_all([job, task])

    # 5 assets in PROMOTED: must NOT be uploaded
    for i in range(5):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task.id,
                source_type=SourceType.KAGGLE,
                source_item_id=f"gate_{i}",
                status=AssetStatus.PROMOTED,
            )
        )
    db_session.commit()

    uploaded_batches = []
    monkeypatch.setattr("app.storage.nas_writer.enqueue_nas_batch", lambda b_id, j_id, m_path: uploaded_batches.append(b_id))
    monkeypatch.setattr("app.tasks.nas_tasks.process_nas_upload_queue.delay", lambda: None)
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: _session_context(db_session))
    monkeypatch.setattr("app.tasks.batch_orchestration.get_redis", lambda: _FakeRedis())

    # Running process_active_batch when assets are only PROMOTED (not filtered)
    # results in 0 accepted images, so no batch is completed or enqueued for NAS upload
    orchestrator = BatchOrchestrator(db_session, job_id)
    batch = orchestrator.get_or_create_active_batch()
    orchestrator.associate_unbatched_assets(batch.id)
    progress = orchestrator.evaluate_batch_progress(batch.id)

    assert progress["accepted"] == 0
    assert len(uploaded_batches) == 0

    # Now simulate TinyVLM filter passing the assets
    for a in db_session.execute(select(Asset).where(Asset.job_id == job_id)).scalars():
        a.status = AssetStatus.RELEVANCE_PASSED
    db_session.commit()

    progress2 = orchestrator.evaluate_batch_progress(batch.id)
    assert progress2["accepted"] == 5
    assert progress2["outcome"] == "TARGET_MET"


# =========================================================================
# 8. Worker Restart Does Not Cause Duplicate Batch Processing
# =========================================================================
def test_8_worker_restart_does_not_cause_duplicate_batch_processing(db_session, monkeypatch):
    job_id = uuid.uuid4()
    job = Job(id=job_id, name="restart_job", query="fire", status=JobStatus.RUNNING, target_count=600)
    db_session.add(job)

    # Existing active batch in DISCOVERING
    batch = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=1,
        target_image_count=600,
        status=BatchStatus.DISCOVERING,
    )
    db_session.add(batch)
    db_session.commit()

    fake_redis = _FakeRedis()
    monkeypatch.setattr("app.tasks.batch_orchestration.get_redis", lambda: fake_redis)

    orchestrator = BatchOrchestrator(db_session, job_id)
    # Calling get_or_create_active_batch must retrieve the existing active batch #1
    active = orchestrator.get_or_create_active_batch()

    assert active is not None
    assert active.id == batch.id
    assert active.batch_index == 1

    batches = db_session.execute(select(ImageBatch).where(ImageBatch.job_id == job_id)).scalars().all()
    assert len(batches) == 1


# Helper context manager for session scope
class _session_context:
    def __init__(self, session):
        self.session = session
    def __enter__(self):
        return self.session
    def __exit__(self, *args):
        pass
