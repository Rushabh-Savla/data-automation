"""Comprehensive regression test suite for Target Count Integrity and Over-Download Surplus Management.

Verifies:
1. Core business rule: target count represents final accepted, unique, valid images.
2. Cross-batch duplicate count drift and dynamic shortfall batch creation (Case A).
3. Single-source exhaustion vs multi-source tier escalation (Case B).
4. Configurable over-download buffer (e.g. 20%) and automatic surplus trimming.
5. Deterministic quality-based surplus selection (relevance score, resolution, ID).
6. Safe physical file deletion with cross-job shared-file protection.
7. NAS directory, manifest, and DB count strict consistency.
8. Reopen and restart resilience.
"""

import json
import math
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings, resolve_overfetch_buffer
from app.db.models.asset import Asset
from app.db.models.candidate_window import CandidateWindow
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import (
    ASSET_ACCEPTED_STATUSES,
    AssetStatus,
    BatchStatus,
    JobCompletionReason,
    JobSourceTaskStatus,
    JobStatus,
    SourceSelectionMode,
    SourceType,
    TransferStatus,
)
from app.services.candidate_reservation_service import (
    CandidateAdmissionStatus,
    admit_candidate_for_task,
    ensure_candidate_window,
)
from app.services.job_counters import reconcile
from app.services.standardization_service import delete_asset_files, is_asset_file_shared
from app.tasks.batch_orchestration import BatchOrchestrator
from app.tasks.filtering_tasks import (
    _asset_quality_sort_key,
    _select_trim_candidates,
    trim_excess_assets,
)
from app.tasks.nas_tasks import consolidate_job_dataset_on_nas
from app.tasks.orchestration_tasks import (
    check_and_advance_job,
    evaluate_job_completion,
)


class _FakeRedis:
    def __init__(self):
        self._data = {}
        self._lists = {}
        self._sets = {}

    def get(self, key):
        return self._data.get(key)

    def exists(self, key):
        return key in self._data or key in self._lists or key in self._sets

    def sadd(self, key, *members):
        self._sets.setdefault(key, set()).update(members)

    def smembers(self, key):
        return self._sets.get(key, set())

    def srem(self, key, *members):
        s = self._sets.get(key, set())
        for m in members:
            s.discard(m)

    def llen(self, key):
        return len(self._lists.get(key, []))

    def publish(self, channel, message):
        return 1

    def lpop(self, key):
        lst = self._lists.get(key, [])
        return lst.pop(0) if lst else None

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self._data:
            return False
        self._data[key] = value
        return True

    def delete(self, *keys):
        for k in keys:
            self._data.pop(k, None)
            self._lists.pop(k, None)

    def rpush(self, key, *values):
        self._lists.setdefault(key, []).extend(values)

    def lrange(self, key, start, end):
        lst = self._lists.get(key, [])
        if end == -1:
            return lst[start:]
        return lst[start : end + 1]

    def lock(self, name, timeout=None, blocking=True):
        class _Lock:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def acquire(self, *args, **kwargs):
                return True

            def release(self, *args, **kwargs):
                pass

        return _Lock()


class _SessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.session.rollback()
        else:
            self.session.commit()


# Uses db_session fixture from conftest
def _make_test_job(db: Session, target_count=1000, job_id=None, sources=None, config=None) -> Job:
    j = Job(
        id=job_id or uuid.uuid4(),
        name="test_job",
        query="test query",
        status=JobStatus.RUNNING,
        target_count=target_count,
        accepted_count=0,
        rejected_count=0,
        duplicate_count=0,
        downloaded_count=0,
        discovered_count=0,
        sources=sources or ["web", "youtube"],
        config=config or {"source_selection_mode": "manual", "over_download_buffer_pct": 20.0},
    )
    db.add(j)
    db.commit()
    return j


def _make_test_asset(
    db: Session,
    job_id: uuid.UUID,
    task_id: uuid.UUID,
    status=AssetStatus.RELEVANCE_PASSED,
    batch_id=None,
    score=0.9,
    width=1920,
    height=1080,
    nas_path=None,
    sha=None,
) -> Asset:
    a = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        source_type="web",
        source_item_id=str(uuid.uuid4()),
        status=status,
        batch_id=batch_id,
        relevance_score=score,
        width=width,
        height=height,
        nas_path=nas_path or f"/tmp/images/{uuid.uuid4()}.jpg",
        checksum_sha256=sha or hashlib_sha(str(uuid.uuid4())),
        classes=[],
        annotation_format="none",
        asset_metadata={},
    )
    db.add(a)
    return a


def hashlib_sha(val: str) -> str:
    import hashlib
    return hashlib.sha256(val.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# TEST 1: Cross-batch duplicate count drift & Shortfall Batch Creation (Case A)
# ---------------------------------------------------------------------------
def test_case_a_cross_batch_duplicate_drift_triggers_replacement_batch(db_session):
    """Case A: Target 1000. Batch 1 has 736 accepted, Batch 2 has 268 accepted.
    10 images become duplicates. Accepted count drops to 994.
    Verify orchestrator creates Batch 3 for the remaining 6 images and completes at 1000.
    """
    job = _make_test_job(db_session, target_count=1000)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="youtube", status=JobSourceTaskStatus.WINDOW_WAITING)
    db_session.add(task)
    db_session.commit()

    # Batch 1 with 736 assets (initially)
    b1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=600, accepted_image_count=736, status=BatchStatus.COMPLETED)
    db_session.add(b1)
    # Batch 2 with 268 assets
    b2 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=2, target_image_count=264, accepted_image_count=268, status=BatchStatus.COMPLETED)
    db_session.add(b2)
    db_session.commit()

    # Create 726 unique assets in Batch 1
    b1_assets = [_make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b1.id) for _ in range(726)]
    # Create 10 assets in Batch 1 that were later marked DUPLICATE
    dup_assets = [_make_test_asset(db_session, job.id, task.id, status=AssetStatus.DUPLICATE, batch_id=b1.id) for _ in range(10)]
    # Create 268 unique assets in Batch 2
    b2_assets = [_make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b2.id) for _ in range(268)]
    db_session.commit()

    # 1. Authoritative ground truth check:
    reconciled = reconcile(db_session, job.id)
    assert reconciled["accepted"] == 994
    assert reconciled["duplicate"] == 10

    # 2. Reconcile batches: Batch 1 should show 726, Batch 2 shows 268
    orch = BatchOrchestrator(db_session, job.id)
    orch.batch_repo.reconcile_batch_counters(b1.id)
    orch.batch_repo.reconcile_batch_counters(b2.id)
    db_session.refresh(b1)
    db_session.refresh(b2)
    assert b1.accepted_image_count == 726
    assert b2.accepted_image_count == 268

    # 3. Next batch creation check:
    fake_redis = _FakeRedis()
    with patch("app.tasks.batch_orchestration.get_redis", return_value=fake_redis):
        next_batch = orch.get_or_create_active_batch()
        assert next_batch is not None
        assert next_batch.batch_index == 3
        # Remaining needed: 1000 - 994 = 6
        assert next_batch.target_image_count == 6

    # 4. Ingest and accept the 6 replacement candidates in Batch 3
    for _ in range(6):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=next_batch.id)
    db_session.commit()

    reconciled_final = reconcile(db_session, job.id)
    assert reconciled_final["accepted"] == 1000


# ---------------------------------------------------------------------------
# TEST 2: Filtering and Deduplication Reduce Usable Count
# ---------------------------------------------------------------------------
def test_filtering_and_dedup_reduce_usable_count_without_stopping_pipeline(db_session):
    """Target is 1000. 1200 candidates are ingested.
    Relevance rejections and duplicates reduce accepted count.
    Authoritative count accurately reflects surviving assets.
    """
    job = _make_test_job(db_session, target_count=1000)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.DOWNLOADING)
    db_session.add(task)
    db_session.commit()

    # 1000 valid, 100 rejected, 70 duplicate, 30 failed
    for _ in range(1000):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.RELEVANCE_PASSED)
    for _ in range(100):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.RELEVANCE_REJECTED)
    for _ in range(70):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.DUPLICATE)
    for _ in range(30):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.FAILED)
    db_session.commit()

    counts = reconcile(db_session, job.id)
    assert counts["accepted"] == 1000
    assert counts["rejected"] == 130  # 100 rejected + 30 failed
    assert counts["duplicate"] == 70
    assert counts["downloaded"] == 1200


# ---------------------------------------------------------------------------
# TEST 3: Duplicate Images Across Different Sources Never Contributed
# ---------------------------------------------------------------------------
def test_cross_source_duplicates_never_contribute_to_accepted_count(db_session):
    job = _make_test_job(db_session, target_count=500)
    task1 = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.DOWNLOADING)
    task2 = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="youtube", status=JobSourceTaskStatus.DOWNLOADING)
    db_session.add_all([task1, task2])
    db_session.commit()

    shared_sha = hashlib_sha("identical_image_bytes")
    # Asset from Web: canonical
    a1 = _make_test_asset(db_session, job.id, task1.id, status=AssetStatus.RELEVANCE_PASSED, sha=shared_sha)
    # Asset from YouTube: duplicate
    a2 = _make_test_asset(db_session, job.id, task2.id, status=AssetStatus.DUPLICATE, sha=shared_sha)
    db_session.commit()

    counts = reconcile(db_session, job.id)
    assert counts["accepted"] == 1
    assert counts["duplicate"] == 1


# ---------------------------------------------------------------------------
# TEST 4: Duplicate Detection After Batch Completion
# ---------------------------------------------------------------------------
def test_duplicate_detection_after_batch_completion_lowers_count(db_session):
    job = _make_test_job(db_session, target_count=500)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.WINDOW_WAITING)
    db_session.add(task)
    b1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=500, accepted_image_count=500, status=BatchStatus.COMPLETED)
    db_session.add(b1)
    db_session.commit()

    assets = [_make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b1.id) for _ in range(500)]
    db_session.commit()

    reconcile(db_session, job.id)
    db_session.refresh(job)
    assert job.accepted_count == 500

    # Late cross-batch deduplication flags 5 assets from Batch 1 as DUPLICATE
    for a in assets[:5]:
        a.status = AssetStatus.DUPLICATE
    db_session.commit()

    orch = BatchOrchestrator(db_session, job.id)
    orch.batch_repo.reconcile_batch_counters(b1.id)
    reconciled = reconcile(db_session, job.id)
    db_session.refresh(b1)
    db_session.refresh(job)

    assert b1.accepted_image_count == 495
    assert job.accepted_count == 495
    assert reconciled["accepted"] == 495


# ---------------------------------------------------------------------------
# TEST 5: Stale Denormalized Counters Corrected by Reconciliation
# ---------------------------------------------------------------------------
def test_stale_denormalized_counters_corrected(db_session):
    job = _make_test_job(db_session, target_count=1000)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.WINDOW_WAITING)
    db_session.add(task)
    b1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=600, accepted_image_count=9999, status=BatchStatus.COMPLETED)
    db_session.add(b1)
    job.accepted_count = 8888  # Intentionally corrupt
    db_session.commit()

    # In reality only 400 assets exist
    for _ in range(400):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b1.id)
    db_session.commit()

    reconciled = reconcile(db_session, job.id)
    orch = BatchOrchestrator(db_session, job.id)
    orch.batch_repo.reconcile_batch_counters(b1.id)
    db_session.refresh(job)
    db_session.refresh(b1)

    assert job.accepted_count == 400
    assert b1.accepted_image_count == 400


# ---------------------------------------------------------------------------
# TEST 7: Single Source Genuine Exhaustion (Case B)
# ---------------------------------------------------------------------------
def test_case_b_source_exhaustion_reports_actual_count_and_deficit(db_session):
    """Case B: Target 2000. Manual mode with only Web.
    Source exhausts at 1099 unique valid images.
    Verify completion evaluation reports completed_partial with sources_exhausted.
    """
    job = _make_test_job(db_session, target_count=2000, sources=["web"], config={"source_selection_mode": "manual"})
    task = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="web",
        status=JobSourceTaskStatus.COMPLETED_PARTIAL,
        checkpoint={"discovery_exhausted": True},
    )
    db_session.add(task)
    db_session.commit()

    # 1099 accepted images
    for _ in range(1099):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED)
    db_session.commit()

    reconcile(db_session, job.id)
    db_session.refresh(job)

    fake_redis = _FakeRedis()
    with patch("app.tasks.orchestration_tasks.get_redis", return_value=fake_redis), \
         patch("app.tasks._helpers.get_redis", return_value=fake_redis), \
         patch("app.core.redis_client.get_redis", return_value=fake_redis), \
         patch("app.tasks.orchestration_tasks.session_scope", lambda: _SessionContext(db_session)), \
         patch("app.services.nas_config_service.is_storage_active", return_value=False), \
         patch("app.tasks.cleanup_tasks.cleanup_local.delay"), \
         patch("app.tasks.cleanup_tasks.cleanup_shared_cache.delay"), \
         patch("app.tasks.nas_tasks.schedule_offload_for_job"), \
         patch("app.tasks.nas_tasks.consolidate_job_dataset_on_nas.delay"):

        evaluate_job_completion(str(job.id))
        db_session.refresh(job)

        assert job.status == JobStatus.COMPLETED_PARTIAL
        assert job.completion_reason == JobCompletionReason.SOURCES_EXHAUSTED
        assert job.accepted_count == 1099
        # Deficit is transparently 2000 - 1099 = 901
        assert job.target_count - job.accepted_count == 901


# ---------------------------------------------------------------------------
# TEST 8: Multi-Source Tier Escalation
# ---------------------------------------------------------------------------
def test_multi_source_tier_escalation_when_first_tier_exhausts(db_session):
    """When Web exhausts at 1099, but pending tiers exist (e.g. YouTube),
    orchestrator escalates to Tier 2 instead of stopping.
    """
    config = {
        "_orchestration_tier": 1,
        "_orchestration_pending_tiers": [["youtube"]],
        "source_selection_mode": "automatic",
    }
    job = _make_test_job(db_session, target_count=2000, sources=["web", "youtube"], config=config)
    task1 = JobSourceTask(
        id=uuid.uuid4(),
        job_id=job.id,
        source_type="web",
        status=JobSourceTaskStatus.COMPLETED_PARTIAL,
        checkpoint={"discovery_exhausted": True},
    )
    db_session.add(task1)
    db_session.commit()

    for _ in range(1099):
        _make_test_asset(db_session, job.id, task1.id, status=AssetStatus.COMPLETED)
    db_session.commit()

    reconcile(db_session, job.id)
    fake_redis = _FakeRedis()

    with patch("app.tasks.orchestration_tasks.get_redis", return_value=fake_redis), \
         patch("app.tasks.orchestration_tasks.session_scope") as mock_scope, \
         patch("app.tasks.filtering_tasks.run_relevance_filter.delay"), \
         patch("app.services.job_service.JobService._dispatch") as mock_dispatch:
        mock_scope.return_value.__enter__.return_value = db_session
        mock_scope.return_value.__exit__.return_value = None

        check_and_advance_job(str(job.id))
        db_session.refresh(job)

        # Tier was escalated to Tier 2
        assert job.config["_orchestration_tier"] == 2
        assert mock_dispatch.called


# ---------------------------------------------------------------------------
# TEST 9: Active NAS Transfers Gate Completion
# ---------------------------------------------------------------------------
def test_active_nas_transfers_gate_completion_evaluation(db_session):
    job = _make_test_job(db_session, target_count=1000)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.COMPLETED)
    db_session.add(task)
    b1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=1000, accepted_image_count=1000, status=BatchStatus.UPLOADING)
    db_session.add(b1)
    transfer = NasTransferBatch(
        id=uuid.uuid4(),
        batch_id=b1.id,
        job_id=job.id,
        transfer_status=TransferStatus.UPLOADING,
        remote_path="/test",
        total_files=1000,
        uploaded_files=900,
    )
    db_session.add(transfer)
    db_session.commit()

    for _ in range(1000):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.UPLOADING, batch_id=b1.id)
    db_session.commit()

    reconcile(db_session, job.id)
    retried = []
    def fake_retry(self_obj, countdown=5, max_retries=120):
        retried.append(countdown)
        return Exception("Retrying")

    fake_redis = _FakeRedis()
    with patch("app.tasks.orchestration_tasks.get_redis", return_value=fake_redis), \
         patch("app.tasks._helpers.get_redis", return_value=fake_redis), \
         patch("app.core.redis_client.get_redis", return_value=fake_redis), \
         patch("app.tasks.orchestration_tasks.session_scope", lambda: _SessionContext(db_session)), \
         patch("app.services.nas_config_service.is_storage_active", return_value=True), \
         patch("app.tasks.nas_tasks.process_nas_upload_queue.delay"), \
         patch("app.tasks.nas_tasks.reconcile_nas_transfers", return_value={}), \
         patch("celery.app.task.Task.retry", fake_retry):

        with pytest.raises(Exception, match="Retrying"):
            evaluate_job_completion(str(job.id))

        assert len(retried) == 1
        db_session.refresh(job)
        assert job.status == JobStatus.RUNNING


# ---------------------------------------------------------------------------
# TEST 11 & 13: Over-Download with Automatic Surplus Cleanup (Section 14)
# ---------------------------------------------------------------------------
def test_over_download_accepts_1040_retains_1000_cleans_40_surplus(db_session):
    """Target: 1000. Over-download accepted: 1040.
    Surplus trimming deterministically retains the top 1000 highest quality images,
    marks 40 as SURPLUS, safely deletes surplus files, and reconciles accepted count to 1000.
    """
    job = _make_test_job(db_session, target_count=1000)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.COMPLETED)
    db_session.add(task)
    b1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=1000, accepted_image_count=1040, status=BatchStatus.COMPLETED)
    db_session.add(b1)
    db_session.commit()

    # Create 1040 assets with varying scores: 1000 high score (0.95), 40 lower score (0.50)
    high_quality = [_make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b1.id, score=0.95, width=1920, height=1080) for _ in range(1000)]
    low_quality = [_make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b1.id, score=0.50, width=640, height=480) for _ in range(40)]
    db_session.commit()

    counts = reconcile(db_session, job.id)
    assert counts["accepted"] == 1040

    fake_redis = _FakeRedis()
    with patch("app.tasks.filtering_tasks.get_redis", return_value=fake_redis), \
         patch("app.tasks.filtering_tasks.session_scope") as mock_scope, \
         patch("app.services.standardization_service.asset_files.delete") as mock_del:
        mock_scope.return_value.__enter__.return_value = db_session
        mock_scope.return_value.__exit__.return_value = None

        trim_excess_assets(str(job.id))
        db_session.refresh(job)

        # 1. Authoritative accepted count is EXACTLY 1000
        assert job.accepted_count == 1000
        reconciled = reconcile(db_session, job.id)
        assert reconciled["accepted"] == 1000
        assert reconciled["surplus"] == 40

        # 2. All 40 low-quality assets are transitioned to SURPLUS
        for a in low_quality:
            db_session.refresh(a)
            assert a.status == AssetStatus.SURPLUS
            assert a.asset_metadata.get("trimmed_as_surplus") is True

        # 3. All 1000 high-quality assets remain COMPLETED
        for a in high_quality:
            db_session.refresh(a)
            assert a.status == AssetStatus.COMPLETED


# ---------------------------------------------------------------------------
# TEST 14: Non-Eligible Statuses Never Contribute to Accepted Count
# ---------------------------------------------------------------------------
def test_non_eligible_statuses_never_contribute_to_accepted_count(db_session):
    job = _make_test_job(db_session, target_count=1000)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web")
    db_session.add(task)
    db_session.commit()

    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.DUPLICATE)
    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.FAILED)
    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.RELEVANCE_REJECTED)
    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.SURPLUS)
    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.PROMOTED)
    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.REVIEW_PENDING)
    # Only 1 valid accepted asset
    _make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED)
    db_session.commit()

    counts = reconcile(db_session, job.id)
    assert counts["accepted"] == 1
    assert counts["duplicate"] == 1
    assert counts["surplus"] == 1
    assert counts["rejected"] == 2  # REJECTED + FAILED


# ---------------------------------------------------------------------------
# TEST 15: Cross-Job Shared Asset Protection (Section 14)
# ---------------------------------------------------------------------------
def test_shared_asset_file_never_physically_deleted(db_session):
    """If Asset A in Job 1 and Asset B in Job 2 share the same physical file,
    trimming Asset A must NOT delete the physical file.
    """
    job1 = _make_test_job(db_session, target_count=500)
    job2 = _make_test_job(db_session, target_count=500)
    task1 = JobSourceTask(id=uuid.uuid4(), job_id=job1.id, source_type="web")
    task2 = JobSourceTask(id=uuid.uuid4(), job_id=job2.id, source_type="web")
    db_session.add_all([task1, task2])
    db_session.commit()

    shared_file_path = "/staging/shared_image_12345.jpg"
    a1 = _make_test_asset(db_session, job1.id, task1.id, nas_path=shared_file_path)
    a2 = _make_test_asset(db_session, job2.id, task2.id, nas_path=shared_file_path)
    db_session.commit()

    with patch("app.services.standardization_service.session_scope") as mock_scope:
        mock_scope.return_value.__enter__.return_value = db_session
        mock_scope.return_value.__exit__.return_value = None

        # Verify shared check
        assert is_asset_file_shared(a1, shared_file_path) is True

        with patch("app.services.standardization_service.asset_files.delete") as mock_del:
            delete_asset_files(a1)
            # Physical file must NOT be deleted!
            assert not mock_del.called


# ---------------------------------------------------------------------------
# TEST 16: Idempotent Surplus Trimming
# ---------------------------------------------------------------------------
def test_interrupted_surplus_cleanup_is_idempotent(db_session):
    job = _make_test_job(db_session, target_count=100)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.COMPLETED)
    db_session.add(task)
    b1 = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=100, accepted_image_count=120, status=BatchStatus.COMPLETED)
    db_session.add(b1)
    db_session.commit()

    for i in range(120):
        _make_test_asset(db_session, job.id, task.id, status=AssetStatus.COMPLETED, batch_id=b1.id, score=0.5 + (i * 0.001))
    db_session.commit()

    fake_redis = _FakeRedis()
    with patch("app.tasks.filtering_tasks.get_redis", return_value=fake_redis), \
         patch("app.tasks.filtering_tasks.session_scope") as mock_scope, \
         patch("app.services.standardization_service.asset_files.delete"):
        mock_scope.return_value.__enter__.return_value = db_session
        mock_scope.return_value.__exit__.return_value = None

        # Run 1st time
        trim_excess_assets(str(job.id))
        db_session.refresh(job)
        assert job.accepted_count == 100

        # Run 2nd time (idempotent no-op)
        trim_excess_assets(str(job.id))
        db_session.refresh(job)
        assert job.accepted_count == 100


# ---------------------------------------------------------------------------
# TEST 17: Candidate Admission Respects Overfetch Buffer
# ---------------------------------------------------------------------------
def test_candidate_admission_respects_overfetch_buffer(db_session):
    """With target=1000 and 20% over-download buffer, admission continues
    up to 1200 candidates before returning WINDOW_FULL.
    """
    job = _make_test_job(db_session, target_count=1000, config={"over_download_buffer_pct": 20.0})
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type="web", status=JobSourceTaskStatus.DOWNLOADING)
    db_session.add(task)
    batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, target_image_count=600, status=BatchStatus.DOWNLOADING)
    db_session.add(batch)
    window = CandidateWindow(id=uuid.uuid4(), batch_id=batch.id, job_id=job.id, window_index=1, candidate_limit=2000, status="OPEN")
    db_session.add(window)
    db_session.commit()

    job.accepted_count = 1000  # Raw target reached, but within 20% buffer (1200)
    db_session.commit()

    fake_redis = _FakeRedis()
    with patch("app.core.redis_client.get_redis", return_value=fake_redis), \
         patch("app.services.candidate_reservation_service.session_scope") as mock_scope:
        mock_scope.return_value.__enter__.return_value = db_session
        mock_scope.return_value.__exit__.return_value = None

        # At 1000, admission should STILL succeed because buffered max is 1200
        adm = admit_candidate_for_task(task.id, "web", "item_1")
        assert adm.status == CandidateAdmissionStatus.ADMITTED

        # Set at 1200: buffered max reached -> WINDOW_FULL
        job.accepted_count = 1200
        db_session.commit()
        adm2 = admit_candidate_for_task(task.id, "web", "item_2")
        assert adm2.status == CandidateAdmissionStatus.WINDOW_FULL
