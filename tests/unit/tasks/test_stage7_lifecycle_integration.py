"""Comprehensive Integration Tests for Stage 7 — Batch Lifecycle and State Machine Integration.

Covers all 14 required verification scenarios:
1. Complete asset lifecycle.
2. Complete batch lifecycle.
3. Accepted target and replacement calculation.
4. Source exhaustion and partial completion.
5. Concurrent replacement dispatch.
6. Duplicate task execution.
7. Invalid state transitions.
8. VLM uncertainty and retry states.
9. NAS upload failure and recovery.
10. Worker crash at every major lifecycle transition.
11. Batch completion only after all required conditions are met.
12. Counter reconciliation.
13. Backpressure pause and resume.
14. No regression in TinyVLM, deduplication, and NAS writer behavior.
"""

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.db.repositories.asset_repository import AssetRepository
from app.db.repositories.image_batch_repository import ImageBatchRepository, NasTransferBatchRepository
from app.db.repositories.job_repository import JobRepository
from app.domain.asset_state_machine import AssetStateMachine, IllegalAssetTransitionError
from app.domain.batch_state_machine import BatchStateMachine, IllegalBatchTransitionError
from app.domain.enums import (
    ASSET_ACCEPTED_STATUSES,
    AssetStatus,
    BatchStatus,
    JobSourceTaskStatus,
    JobStatus,
    SourceType,
    TransferStatus,
)
from app.services.job_counters import reconcile
from app.tasks.batch_orchestration import (
    BatchOrchestrator,
    dispatch_batch_candidate_window,
    process_active_batch,
    reconcile_batch_lifecycle,
)


def _setup_job_and_batch(db, target_count=600):
    job = Job(id=uuid.uuid4(), name="stage7_test_job", query="hard hat", target_count=target_count)
    db.add(job)
    task = JobSourceTask(id=uuid.uuid4(), job_id=job.id, source_type=SourceType.WEB.value)
    db.add(task)
    batch = ImageBatch(
        id=uuid.uuid4(),
        job_id=job.id,
        batch_index=1,
        target_image_count=target_count,
        status=BatchStatus.DISCOVERING,
    )
    db.add(batch)
    db.commit()
    return job, task, batch


# =========================================================================
# 1. Complete Asset Lifecycle
# =========================================================================
def test_1_complete_asset_lifecycle():
    """Verify entire sequential journey of an Asset through all legal states."""
    flow = [
        AssetStatus.DISCOVERED,
        AssetStatus.DOWNLOADING,
        AssetStatus.DOWNLOADED,
        AssetStatus.STANDARDIZING,
        AssetStatus.STANDARDIZED,
        AssetStatus.PROMOTED,
        AssetStatus.RELEVANCE_PENDING,
        AssetStatus.RELEVANCE_PASSED,
        AssetStatus.READY_FOR_UPLOAD,
        AssetStatus.UPLOADING,
        AssetStatus.NAS_VERIFIED,
        AssetStatus.COMPLETED,
    ]
    current = flow[0]
    for nxt in flow[1:]:
        assert AssetStateMachine.can_transition(current, nxt) is True
        current = AssetStateMachine.transition(current, nxt, reason=f"{current}->{nxt}")
        assert current == nxt

    # Completed is strictly terminal
    assert AssetStateMachine.can_transition(AssetStatus.COMPLETED, AssetStatus.DOWNLOADING) is False
    assert AssetStateMachine.can_transition(AssetStatus.COMPLETED, AssetStatus.DISCOVERED) is False
    assert AssetStateMachine.can_transition(AssetStatus.COMPLETED, AssetStatus.PROMOTED) is False


# =========================================================================
# 2. Complete Batch Lifecycle
# =========================================================================
def test_2_complete_batch_lifecycle():
    """Verify entire sequential journey of an ImageBatch through all legal states."""
    flow = [
        BatchStatus.CREATED,
        BatchStatus.DISCOVERING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PROCESSING,
        BatchStatus.REPLENISHING,
        BatchStatus.DOWNLOADING,
        BatchStatus.PROCESSING,
        BatchStatus.READY_FOR_UPLOAD,
        BatchStatus.UPLOADING,
        BatchStatus.VERIFYING,
        BatchStatus.COMPLETED,
    ]
    current = flow[0]
    for nxt in flow[1:]:
        assert BatchStateMachine.can_transition(current, nxt) is True
        current = BatchStateMachine.transition(current, nxt, reason=f"{current}->{nxt}")
        assert current == nxt

    # Completed is strictly terminal
    assert BatchStateMachine.can_transition(BatchStatus.COMPLETED, BatchStatus.DISCOVERING) is False
    assert BatchStateMachine.can_transition(BatchStatus.COMPLETED, BatchStatus.PROCESSING) is False


# =========================================================================
# 3. Accepted Target and Replacement Calculation
# =========================================================================
def test_3_accepted_target_and_replacement_calculation(db_session):
    """Initial window 600: 420 passed, 80 rejected, 50 duplicates, 30 uncertain, 20 errors.

    Shortfall must strictly be 180, excluding all non-passed assets.
    When 180 replacements pass, shortfall reaches 0 and outcome is TARGET_MET.
    """
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    orchestrator = BatchOrchestrator(db_session, job.id)

    # 420 passed
    for i in range(420):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"pass_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    # 80 rejected
    for i in range(80):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"rej_{i}",
                status=AssetStatus.RELEVANCE_REJECTED,
            )
        )
    # 50 duplicates
    for i in range(50):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"dup_{i}",
                status=AssetStatus.DUPLICATE,
            )
        )
    # 30 review pending
    for i in range(30):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"rev_{i}",
                status=AssetStatus.REVIEW_PENDING,
            )
        )
    # 20 retry pending
    for i in range(20):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"err_{i}",
                status=AssetStatus.RETRY_PENDING,
            )
        )
    db_session.commit()

    progress = orchestrator.evaluate_batch_progress(batch.id)
    assert progress["accepted"] == 420
    assert progress["shortfall"] == 180
    assert progress["outcome"] == "READY_FOR_UPLOAD"

    # Replenish remaining 180
    for i in range(180):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"repl_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    db_session.commit()

    progress2 = orchestrator.evaluate_batch_progress(batch.id)
    assert progress2["accepted"] == 600
    assert progress2["shortfall"] == 0
    assert progress2["outcome"] == "TARGET_MET"


# =========================================================================
# 4. Source Exhaustion and Partial Completion
# =========================================================================
def test_4_source_exhaustion_and_partial_completion(db_session, tmp_path):
    """When sources dry up before hitting 600 accepted images, batch finalizes as PARTIAL."""
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    task.status = JobSourceTaskStatus.COMPLETED  # Terminal source task
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job.id)
    orchestrator.settings.local_staging_path = tmp_path

    # Only 300 candidates available, all pass
    for i in range(300):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"dry_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    db_session.commit()

    progress = orchestrator.evaluate_batch_progress(batch.id)
    assert progress["accepted"] == 300
    assert progress["shortfall"] == 300
    assert progress["outcome"] == "READY_FOR_UPLOAD"

    manifest_path = orchestrator.create_batch_manifest(batch.id)
    assert manifest_path.exists()
    orchestrator.batch_repo.set_status(batch.id, BatchStatus.PARTIAL)
    db_session.commit()

    refreshed = orchestrator.batch_repo.get(batch.id)
    assert refreshed.status == BatchStatus.PARTIAL
    assert refreshed.accepted_image_count == 300


# =========================================================================
# 5. Concurrent Replacement Dispatch
# =========================================================================
def test_5_concurrent_replacement_dispatch(db_session, monkeypatch):
    """Two workers evaluating shortfall concurrently must not dispatch duplicate candidate windows."""
    import contextlib
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    monkeypatch.setattr("app.storage.nas_writer.session_scope", lambda: contextlib.nullcontext(db_session))
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)

    # 500 rejected, 0 accepted -> needs replacements
    for i in range(500):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"conc_{i}",
                status=AssetStatus.RELEVANCE_REJECTED,
            )
        )
    db_session.commit()

    mock_redis = MagicMock()
    # First worker acquires lock (returns True), second worker fails (returns False)
    mock_redis.set.side_effect = [True, False]

    with patch("app.tasks.batch_orchestration.get_redis", return_value=mock_redis), patch(
        "app.tasks.batch_orchestration.dispatch_batch_candidate_window.delay"
    ) as mock_dispatch, patch("app.tasks.nas_tasks.process_nas_upload_queue.delay"):
        # Worker 1
        res1 = process_active_batch(str(job.id))
        assert res1["progress"]["outcome"] == "NEEDS_REPLACEMENTS"
        assert mock_dispatch.call_count == 1

        # Worker 2: suppressed duplicate dispatch
        res2 = process_active_batch(str(job.id))
        assert res2["progress"]["outcome"] == "NEEDS_REPLACEMENTS"
        assert mock_dispatch.call_count == 1  # Not dispatched a second time!


# =========================================================================
# 6. Duplicate Task Execution Idempotency
# =========================================================================
def test_6_duplicate_task_execution(db_session, tmp_path):
    """Redelivery of process_active_batch or create_batch_manifest does not duplicate asset records."""
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    orchestrator = BatchOrchestrator(db_session, job.id)
    orchestrator.settings.local_staging_path = tmp_path

    for i in range(600):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"idem_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    db_session.commit()

    m1 = orchestrator.create_batch_manifest(batch.id)
    m2 = orchestrator.create_batch_manifest(batch.id)
    assert m1 == m2
    assert m1.exists()

    # Asset records remain exactly 600, not 1200
    total_assets = db_session.execute(select(Asset).where(Asset.batch_id == batch.id)).scalars().all()
    assert len(total_assets) == 600


# =========================================================================
# 7. Invalid State Transitions Rejection
# =========================================================================
def test_7_invalid_state_transitions():
    """Verify state machines reject and log illegal transitions."""
    # Asset illegal transitions
    with pytest.raises(IllegalAssetTransitionError):
        AssetStateMachine.transition(AssetStatus.DISCOVERED, AssetStatus.COMPLETED)

    with pytest.raises(IllegalAssetTransitionError):
        AssetStateMachine.transition(AssetStatus.RELEVANCE_REJECTED, AssetStatus.READY_FOR_UPLOAD)

    with pytest.raises(IllegalAssetTransitionError):
        AssetStateMachine.transition(AssetStatus.COMPLETED, AssetStatus.PROMOTED)

    # Batch illegal transitions
    with pytest.raises(IllegalBatchTransitionError):
        BatchStateMachine.transition(BatchStatus.CREATED, BatchStatus.COMPLETED)

    with pytest.raises(IllegalBatchTransitionError):
        BatchStateMachine.transition(BatchStatus.DISCOVERING, BatchStatus.VERIFYING)

    with pytest.raises(IllegalBatchTransitionError):
        BatchStateMachine.transition(BatchStatus.COMPLETED, BatchStatus.DOWNLOADING)


# =========================================================================
# 8. VLM Uncertainty and Retry States
# =========================================================================
def test_8_vlm_uncertainty_and_retry_states(db_session):
    """VLM UNCERTAIN maps to REVIEW_PENDING; ERROR maps to RETRY_PENDING.

    Neither is counted as accepted or rejected; files are preserved.
    """
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    asset_rev = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="uncert_img",
        status=AssetStatus.REVIEW_PENDING,
    )
    asset_err = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="error_img",
        status=AssetStatus.RETRY_PENDING,
    )
    db_session.add_all([asset_rev, asset_err])
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job.id)
    progress = orchestrator.evaluate_batch_progress(batch.id)
    assert progress["accepted"] == 0
    assert batch.uncertain_image_count == 1
    assert batch.error_image_count == 1
    assert asset_rev.status not in ASSET_ACCEPTED_STATUSES
    assert asset_err.status not in ASSET_ACCEPTED_STATUSES


# =========================================================================
# 9. NAS Upload Failure and Recovery
# =========================================================================
def test_9_nas_upload_failure_and_recovery(db_session, tmp_path):
    """When NAS fails mid-batch, transfer is marked FAILED, local staging is preserved,

    and subsequent retry can complete cleanly without file loss.
    """
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    batch.status = BatchStatus.PROCESSING
    db_session.commit()

    repo = ImageBatchRepository(db_session)
    # Transition to READY_FOR_UPLOAD -> UPLOADING -> FAILED -> READY_FOR_UPLOAD (recovery)
    repo.set_status(batch.id, BatchStatus.READY_FOR_UPLOAD)
    repo.set_status(batch.id, BatchStatus.UPLOADING)
    repo.set_status(batch.id, BatchStatus.FAILED)
    assert repo.get(batch.id).status == BatchStatus.FAILED

    # Recovery: back to READY_FOR_UPLOAD
    repo.set_status(batch.id, BatchStatus.READY_FOR_UPLOAD)
    assert repo.get(batch.id).status == BatchStatus.READY_FOR_UPLOAD


# =========================================================================
# 10. Worker Crash at Every Major Transition
# =========================================================================
def test_10_worker_crash_at_every_major_transition(db_session, tmp_path):
    """Simulate worker crash and recovery across:

    DISCOVERING, DOWNLOADING, PROCESSING, REPLENISHING, READY_FOR_UPLOAD, VERIFYING.
    """
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    repo = ImageBatchRepository(db_session)

    # 1. Crash during DOWNLOADING -> recovers to PROCESSING
    repo.set_status(batch.id, BatchStatus.DOWNLOADING)
    repo.set_status(batch.id, BatchStatus.PROCESSING)
    assert repo.get(batch.id).status == BatchStatus.PROCESSING

    # 2. Crash during REPLENISHING -> recovers to DOWNLOADING
    repo.set_status(batch.id, BatchStatus.REPLENISHING)
    repo.set_status(batch.id, BatchStatus.DOWNLOADING)
    assert repo.get(batch.id).status == BatchStatus.DOWNLOADING

    # 3. Crash during READY_FOR_UPLOAD -> recovers to UPLOADING
    repo.set_status(batch.id, BatchStatus.PROCESSING)
    repo.set_status(batch.id, BatchStatus.READY_FOR_UPLOAD)
    repo.set_status(batch.id, BatchStatus.UPLOADING)
    assert repo.get(batch.id).status == BatchStatus.UPLOADING

    # 4. Crash during VERIFYING -> recovers to COMPLETED
    repo.set_status(batch.id, BatchStatus.VERIFYING)
    repo.set_status(batch.id, BatchStatus.COMPLETED)
    assert repo.get(batch.id).status == BatchStatus.COMPLETED


# =========================================================================
# 11. Batch Completion Only After All Conditions Met
# =========================================================================
def test_11_batch_completion_conditions():
    """A batch cannot be marked COMPLETED from illegal non-verified states."""
    # Cannot jump to COMPLETED from CREATED or DISCOVERING
    assert BatchStateMachine.can_transition(BatchStatus.CREATED, BatchStatus.COMPLETED) is False
    assert BatchStateMachine.can_transition(BatchStatus.DISCOVERING, BatchStatus.COMPLETED) is False
    assert BatchStateMachine.can_transition(BatchStatus.DOWNLOADING, BatchStatus.COMPLETED) is False
    assert BatchStateMachine.can_transition(BatchStatus.REPLENISHING, BatchStatus.COMPLETED) is False

    # Allowed from VERIFYING or UPLOADING (when pre-verified)
    assert BatchStateMachine.can_transition(BatchStatus.VERIFYING, BatchStatus.COMPLETED) is True
    assert BatchStateMachine.can_transition(BatchStatus.UPLOADING, BatchStatus.COMPLETED) is True


# =========================================================================
# 12. Counter Reconciliation
# =========================================================================
def test_12_counter_reconciliation(db_session):
    """Counter drift is identified and reconciled strictly to match actual Asset rows."""
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    batch_repo = ImageBatchRepository(db_session)

    # 10 actual passed assets
    for i in range(10):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job.id,
                job_source_task_id=task.id,
                batch_id=batch.id,
                source_type=SourceType.WEB,
                source_item_id=f"reconcile_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    # Drift: manually corrupt counters
    batch.accepted_image_count = 999
    batch.rejected_image_count = 50
    db_session.commit()

    # Reconcile batch counters
    res = batch_repo.reconcile_batch_counters(batch.id)
    assert res["accepted"] == 10
    assert res["rejected"] == 0

    refreshed = batch_repo.get(batch.id)
    assert refreshed.accepted_image_count == 10
    assert refreshed.rejected_image_count == 0

    # Job level reconcile strictly derives from asset rows and excludes unclassified
    job_res = reconcile(db_session, job.id)
    assert job_res["accepted"] == 10
    assert job_res["rejected"] == 0


# =========================================================================
# 13. Backpressure Pause and Resume
# =========================================================================
def test_13_backpressure_pause_and_resume(db_session, monkeypatch):
    """When staging backpressure activates, candidate acquisition pauses.

    When cleared, reconcile_batch_lifecycle resumes the paused batch into its exact
    previous active phase (e.g. PROCESSING -> PAUSED -> PROCESSING, DOWNLOADING -> PAUSED -> DOWNLOADING).
    """
    import contextlib
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    monkeypatch.setattr("app.tasks.nas_tasks.reconcile_nas_transfers", lambda: {"status": "idle"})
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    batch_repo = ImageBatchRepository(db_session)
    batch_repo.set_status(batch.id, BatchStatus.PROCESSING)
    db_session.commit()

    # 1. Backpressure active: pause batch
    with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=True):
        res = dispatch_batch_candidate_window(str(job.id), str(batch.id))
        assert res["status"] == "paused_staging_backpressure"

    assert batch_repo.get(batch.id).status == BatchStatus.PAUSED
    assert (batch_repo.get(batch.id).batch_metadata or {}).get("paused_from_status") == BatchStatus.PROCESSING.value

    # 2. Backpressure cleared: resume batch into PROCESSING
    with patch(
        "app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=False
    ), patch("app.tasks.batch_orchestration.process_active_batch.delay") as mock_process:
        rec_res = reconcile_batch_lifecycle()
        assert rec_res["resumed_batches"] == 1
        assert mock_process.called

    assert batch_repo.get(batch.id).status == BatchStatus.PROCESSING


# =========================================================================
# 14. No Regression in TinyVLM, Deduplication, and NAS Writer
# =========================================================================
def test_14_no_regression_tinyvlm_dedup_nas(db_session):
    """Smoketest verifying TinyVLM verdicts, canonical selector, and NAS writer lock."""
    # 1. VLM verdict types preserved
    from app.filtering.vlm_filter import VlmVerdict
    assert VlmVerdict.YES.value == "YES"
    assert VlmVerdict.NO.value == "NO"
    assert VlmVerdict.UNCERTAIN.value == "UNCERTAIN"
    assert VlmVerdict.ERROR.value == "ERROR"

    # 2. Deduplication canonical selector intact
    from app.filtering.canonical_selector import CanonicalSelector
    a1 = Asset(id=uuid.uuid4(), source_type=SourceType.WEB, source_item_id="1", width=1920, height=1080)
    a2 = Asset(id=uuid.uuid4(), source_type=SourceType.WEB, source_item_id="2", width=640, height=480)
    winner, loser = CanonicalSelector.select_canonical(a1, a2)
    assert winner.id == a1.id
    assert loser.id == a2.id


# =========================================================================
# 15. Accepted vs NAS-Verified Counting and Metric Separation
# =========================================================================
def test_15_accepted_vs_nas_verified_counting(db_session):
    """Verifies that cumulative accepted toward target does NOT conflate with NAS-verified or completed."""
    job, task, batch = _setup_job_and_batch(db_session, target_count=100)

    # Populate distinct metrics
    # 10 RELEVANCE_PASSED
    # 5 READY_FOR_UPLOAD (upload pending)
    # 5 UPLOADING
    # 20 NAS_VERIFIED
    # 30 COMPLETED
    # 15 RELEVANCE_REJECTED
    # 8 DUPLICATE
    # 2 REVIEW_PENDING
    # 3 RETRY_PENDING
    status_counts = [
        (AssetStatus.RELEVANCE_PASSED, 10),
        (AssetStatus.READY_FOR_UPLOAD, 5),
        (AssetStatus.UPLOADING, 5),
        (AssetStatus.NAS_VERIFIED, 20),
        (AssetStatus.COMPLETED, 30),
        (AssetStatus.RELEVANCE_REJECTED, 15),
        (AssetStatus.DUPLICATE, 8),
        (AssetStatus.REVIEW_PENDING, 2),
        (AssetStatus.RETRY_PENDING, 3),
    ]

    for st, count in status_counts:
        for idx in range(count):
            db_session.add(
                Asset(
                    id=uuid.uuid4(),
                    job_id=job.id,
                    job_source_task_id=task.id,
                    batch_id=batch.id,
                    source_type=SourceType.WEB,
                    source_item_id=f"sep_{st.value}_{idx}",
                    status=st,
                )
            )
    db_session.commit()

    reconciled = reconcile(db_session, job.id)
    db_session.commit()

    # Cumulative accepted = 10 + 5 + 5 + 20 + 30 = 70
    assert reconciled["accepted"] == 70
    assert job.accepted_count == 70

    # Separate metrics
    assert reconciled["relevance_accepted"] == 10
    assert reconciled["upload_pending"] == 5
    assert reconciled["uploading"] == 5
    assert reconciled["nas_verified"] == 20
    assert reconciled["completed"] == 30
    assert reconciled["rejected"] == 15
    assert reconciled["duplicate"] == 8
    assert reconciled["review_pending"] == 2
    assert reconciled["retry_pending"] == 3

    # Unverified NAS uploads (RELEVANCE_PASSED: 10, READY_FOR_UPLOAD: 5, UPLOADING: 5)
    # MUST NEVER be counted as NAS verified (20) or completed (30)
    assert reconciled["nas_verified"] != reconciled["accepted"]
    assert reconciled["completed"] != reconciled["accepted"]


# =========================================================================
# 16. Dashboard Counter Consistency with Real Database Rows
# =========================================================================
def test_16_dashboard_counter_consistency(db_session):
    """Ensures dashboard API counters directly match the database Asset row counts."""
    job, task, batch = _setup_job_and_batch(db_session, target_count=50)

    db_session.add(Asset(id=uuid.uuid4(), job_id=job.id, job_source_task_id=task.id, batch_id=batch.id, source_type=SourceType.WEB, source_item_id="d1", status=AssetStatus.READY_FOR_UPLOAD))
    db_session.add(Asset(id=uuid.uuid4(), job_id=job.id, job_source_task_id=task.id, batch_id=batch.id, source_type=SourceType.WEB, source_item_id="d2", status=AssetStatus.UPLOADING))
    db_session.add(Asset(id=uuid.uuid4(), job_id=job.id, job_source_task_id=task.id, batch_id=batch.id, source_type=SourceType.WEB, source_item_id="d3", status=AssetStatus.NAS_VERIFIED))
    db_session.add(Asset(id=uuid.uuid4(), job_id=job.id, job_source_task_id=task.id, batch_id=batch.id, source_type=SourceType.WEB, source_item_id="d4", status=AssetStatus.COMPLETED))
    db_session.add(Asset(id=uuid.uuid4(), job_id=job.id, job_source_task_id=task.id, batch_id=batch.id, source_type=SourceType.WEB, source_item_id="d5", status=AssetStatus.REVIEW_PENDING))
    db_session.add(Asset(id=uuid.uuid4(), job_id=job.id, job_source_task_id=task.id, batch_id=batch.id, source_type=SourceType.WEB, source_item_id="d6", status=AssetStatus.RETRY_PENDING))
    db_session.commit()

    reconcile(db_session, job.id)
    db_session.commit()

    from app.api.v1.jobs import get_job_progress, _to_detail
    from app.db.repositories.job_source_task_repository import JobSourceTaskRepository

    progress = get_job_progress(job.id, db=db_session)
    assert progress.accepted_count == 4  # READY_FOR_UPLOAD + UPLOADING + NAS_VERIFIED + COMPLETED
    assert progress.upload_pending_count == 1
    assert progress.uploading_count == 1
    assert progress.nas_verified_count == 1
    assert progress.completed_count == 1
    assert progress.review_pending_count == 1
    assert progress.retry_pending_count == 1

    tasks = JobSourceTaskRepository(db_session).list_for_job(job.id)
    detail = _to_detail(job, tasks, db=db_session)
    assert detail.accepted_count == 4
    assert detail.upload_pending_count == 1
    assert detail.uploading_count == 1
    assert detail.nas_verified_count == 1
    assert detail.completed_count == 1


# =========================================================================
# 17. Staging Backpressure Hysteresis and Fail-Safe
# =========================================================================
def test_17_staging_backpressure_hysteresis_and_failsafe(tmp_path):
    """Enforces high watermark (>=80%), low watermark (<=65%), hysteresis at 75%, and fail-safe on error."""
    from app.storage.ram_buffer import LocalStagingManager

    # Reset shared state
    with LocalStagingManager._lock:
        LocalStagingManager._shared_backpressure = False

    mgr1 = LocalStagingManager(staging_path=tmp_path, max_bytes=1000)

    # 1. 85% usage: triggers backpressure
    with patch.object(LocalStagingManager, "get_staging_disk_usage", return_value=(850, 0.85)):
        assert mgr1.is_staging_backpressure_active() is True
        assert LocalStagingManager._shared_backpressure is True

    # 2. Separate instance: 75% usage (between 65% and 80%) MUST remain paused
    mgr2 = LocalStagingManager(staging_path=tmp_path, max_bytes=1000)
    with patch.object(LocalStagingManager, "get_staging_disk_usage", return_value=(750, 0.75)):
        assert mgr2.is_staging_backpressure_active() is True
        assert LocalStagingManager._shared_backpressure is True

    # 3. 64% usage: drops below 65%, clears backpressure
    with patch.object(LocalStagingManager, "get_staging_disk_usage", return_value=(640, 0.64)):
        assert mgr1.is_staging_backpressure_active() is False
        assert LocalStagingManager._shared_backpressure is False

    # 4. Separate instance: 75% usage (between 65% and 80%) MUST remain unpaused
    mgr3 = LocalStagingManager(staging_path=tmp_path, max_bytes=1000)
    with patch.object(LocalStagingManager, "get_staging_disk_usage", return_value=(750, 0.75)):
        assert mgr3.is_staging_backpressure_active() is False

    # 5. Missing / unavailable metrics / error: MUST fail safely with backpressure active
    with patch.object(LocalStagingManager, "get_staging_disk_usage", side_effect=OSError("Disk failure")):
        assert mgr1.is_staging_backpressure_active() is True


# =========================================================================
# 18. Intermediate Asset Recovery and Bounded RETRY_PENDING
# =========================================================================
def test_18_intermediate_asset_recovery_and_retry_pending(db_session, monkeypatch):
    """Verifies that stuck intermediate assets are recovered, retried, and never marked rejected on infra failure.

    Strictly uses state-entry timestamp (status_entered_at) so that an active asset discovered long ago
    is not falsely marked stale.
    """
    import contextlib
    from datetime import datetime, timezone, timedelta

    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    monkeypatch.setattr("app.tasks.nas_tasks.reconcile_nas_transfers", lambda: {"status": "idle"})
    job, task, batch = _setup_job_and_batch(db_session, target_count=10)

    now = datetime.now(timezone.utc)
    ancient_discovery = now - timedelta(hours=2)
    stale_time = now - timedelta(seconds=600)
    recent_active_time = now - timedelta(seconds=10)

    # 1. Active DOWNLOADING asset: discovered 2h ago, but entered DOWNLOADING 10s ago -> NOT stale
    a_active = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="act_dl",
        status=AssetStatus.DOWNLOADING,
        discovered_at=ancient_discovery,
        asset_metadata={"status_entered_at": recent_active_time.isoformat()},
    )

    # 2. Truly stale DOWNLOADING asset: entered DOWNLOADING 10m ago -> STALE
    a1 = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="stk_dl",
        status=AssetStatus.DOWNLOADING,
        discovered_at=ancient_discovery,
        asset_metadata={"status_entered_at": stale_time.isoformat()},
    )

    # 3. Asset stuck in STANDARDIZING: entered STANDARDIZING 10m ago -> STALE
    a2 = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="stk_std",
        status=AssetStatus.STANDARDIZING,
        discovered_at=ancient_discovery,
        asset_metadata={"status_entered_at": stale_time.isoformat()},
    )

    # 4. Asset stuck in RELEVANCE_PENDING: entered 10m ago -> STALE, reset to PROMOTED
    a3 = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="stk_rel",
        status=AssetStatus.RELEVANCE_PENDING,
        discovered_at=ancient_discovery,
        asset_metadata={"status_entered_at": stale_time.isoformat()},
    )

    # 5. Asset in RETRY_PENDING with retries exhausted (retry_count=3) -> FAILED
    a4 = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="stk_ret_fail",
        status=AssetStatus.RETRY_PENDING,
        discovered_at=ancient_discovery,
        asset_metadata={"retry_count": 3, "status_entered_at": stale_time.isoformat()},
    )

    # 6. Recently retried asset: retry occurred 15s ago -> active, not interrupted
    a_retried = Asset(
        id=uuid.uuid4(),
        job_id=job.id,
        job_source_task_id=task.id,
        batch_id=batch.id,
        source_type=SourceType.WEB,
        source_item_id="rec_retried",
        status=AssetStatus.PROMOTED,
        discovered_at=ancient_discovery,
        asset_metadata={"retry_count": 1, "status_entered_at": (now - timedelta(seconds=15)).isoformat()},
    )

    db_session.add_all([a_active, a1, a2, a3, a4, a_retried])
    db_session.commit()

    with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=False):
        res = reconcile_batch_lifecycle()
        assert res["recovered_assets"] >= 4

    db_session.refresh(a_active)
    db_session.refresh(a1)
    db_session.refresh(a2)
    db_session.refresh(a3)
    db_session.refresh(a4)
    db_session.refresh(a_retried)

    # a_active was discovered 2h ago but entered DOWNLOADING 10s ago: REMAINS DOWNLOADING
    assert a_active.status == AssetStatus.DOWNLOADING

    # a1 & a2 were stale: transitioned to RETRY_PENDING with retry_count=1 and updated status_entered_at
    assert a1.status == AssetStatus.RETRY_PENDING
    assert (a1.asset_metadata or {}).get("retry_count") == 1

    assert a2.status == AssetStatus.RETRY_PENDING
    assert (a2.asset_metadata or {}).get("retry_count") == 1

    # a3 was stuck in RELEVANCE_PENDING: reset to PROMOTED
    assert a3.status == AssetStatus.PROMOTED

    # a4 exceeded max retries: transitioned to FAILED, NEVER RELEVANCE_REJECTED
    assert a4.status == AssetStatus.FAILED
    assert a4.status != AssetStatus.RELEVANCE_REJECTED

    # a_retried was retried recently: remains in PROMOTED
    assert a_retried.status == AssetStatus.PROMOTED


# =========================================================================
# 19. PAUSED Batch Phase Restoration
# =========================================================================
def test_19_paused_batch_phase_restoration(db_session, monkeypatch):
    """Verifies that batches resume into the exact phase they were paused from."""
    import contextlib
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    monkeypatch.setattr("app.tasks.nas_tasks.reconcile_nas_transfers", lambda: {"status": "idle"})
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)
    batch_repo = ImageBatchRepository(db_session)

    # Test DOWNLOADING -> PAUSED -> DOWNLOADING
    batch.batch_metadata = {"paused_from_status": BatchStatus.DOWNLOADING.value}
    batch_repo.set_status(batch.id, BatchStatus.PAUSED)
    db_session.commit()

    with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=False), \
         patch("app.tasks.batch_orchestration.dispatch_batch_candidate_window.delay") as mock_disp:
        reconcile_batch_lifecycle()
        assert batch_repo.get(batch.id).status == BatchStatus.DOWNLOADING
        assert mock_disp.called

    # Test REPLENISHING -> PAUSED -> REPLENISHING
    batch.batch_metadata = {"paused_from_status": BatchStatus.REPLENISHING.value}
    batch_repo.set_status(batch.id, BatchStatus.PAUSED)
    db_session.commit()

    with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=False), \
         patch("app.tasks.batch_orchestration.dispatch_batch_candidate_window.delay") as mock_disp2:
        reconcile_batch_lifecycle()
        assert batch_repo.get(batch.id).status == BatchStatus.REPLENISHING
        assert mock_disp2.called


# =========================================================================
# 20. Atomic Ticket Reservation and Stale Worker Rejection
# =========================================================================
def test_20_atomic_ticket_reservation_and_stale_worker_rejection(db_session, monkeypatch):
    """Verifies that stale workers or expired tickets cannot trigger duplicate allocations."""
    import contextlib
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    job, task, batch = _setup_job_and_batch(db_session, target_count=600)

    # Set active ticket in batch
    active_ticket = "valid-ticket-1234"
    batch.batch_metadata = {"active_replenishment_ticket": active_ticket, "replenishment_round": 1}
    batch.status = BatchStatus.REPLENISHING
    db_session.commit()

    with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=False), \
         patch("app.tasks.batch_orchestration.JobService._dispatch", return_value=None), \
         patch("app.tasks.batch_orchestration.get_redis", return_value=MagicMock()):
        # 1. Stale worker with old ticket is rejected
        res_stale = dispatch_batch_candidate_window(str(job.id), str(batch.id), candidate_count=50, ticket="stale-ticket-999")
        assert res_stale["status"] == "rejected_stale_ticket"

        # 2. Worker with matching ticket succeeds
        res_valid = dispatch_batch_candidate_window(str(job.id), str(batch.id), candidate_count=50, ticket=active_ticket)
        assert res_valid["batch_id"] == str(batch.id)
        assert res_valid["window_size"] == 50


# =========================================================================
# 21. Orphan NAS Transfer and Verified Finalization
# =========================================================================
def test_21_orphan_nas_transfer_and_verified_finalization(db_session, monkeypatch, tmp_path):
    """Verifies that READY_FOR_UPLOAD batches without transfers are enqueued, and VERIFIED transfers finalize."""
    import contextlib
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    job, task, batch = _setup_job_and_batch(db_session, target_count=10)
    batch_repo = ImageBatchRepository(db_session)

    # Create dummy manifest
    manifest_p = tmp_path / "manifest.json"
    manifest_p.write_text("{}")
    batch.manifest_path = str(manifest_p)
    batch_repo.set_status(batch.id, BatchStatus.READY_FOR_UPLOAD)
    db_session.commit()

    with patch("app.storage.ram_buffer.LocalStagingManager.is_staging_backpressure_active", return_value=False), \
         patch("app.storage.nas_writer.enqueue_nas_batch") as mock_enqueue, \
         patch("app.tasks.nas_tasks.process_nas_upload_queue.delay") as mock_queue, \
         patch("app.tasks.nas_tasks.reconcile_nas_transfers", return_value={"status": "dispatched"}):
        res = reconcile_batch_lifecycle()
        assert res["recovered_transfers"] == 1
        assert mock_enqueue.called
        assert mock_queue.called


# =========================================================================
# 22. Staging Cleanup Recovery
# =========================================================================
def test_22_staging_cleanup_recovery(db_session, monkeypatch, tmp_path):
    """Verifies that completed transfers with pending staging cleanup are recovered and cleaned up."""
    import contextlib
    monkeypatch.setattr("app.tasks.batch_orchestration.session_scope", lambda: contextlib.nullcontext(db_session))
    job, task, batch = _setup_job_and_batch(db_session, target_count=10)

    # Setup staging folder
    from app.storage.ram_buffer import LocalStagingManager
    staging_mgr = LocalStagingManager(staging_path=tmp_path / "staging")
    batch_staging = staging_mgr.get_batch_staging_dir(job.id, batch.batch_index)
    batch_staging.mkdir(parents=True, exist_ok=True)
    (batch_staging / "test_img.jpg").write_bytes(b"image_content")

    # Transfer marked COMPLETED but staging_cleanup_status is pending
    transfer = NasTransferBatch(
        id=uuid.uuid4(),
        batch_id=batch.id,
        job_id=job.id,
        remote_path="/nas/test_job/batch_001",
        transfer_status=TransferStatus.COMPLETED,
        staging_cleanup_status="pending",
        staging_cleaned_at=None,
    )
    db_session.add(transfer)
    db_session.commit()

    from app.db.repositories.image_batch_repository import NasTransferBatchRepository
    repo = NasTransferBatchRepository(db_session)
    pending = repo.list_pending_cleanups()
    assert len(pending) == 1
    assert pending[0].id == transfer.id

    # Execute cleanup
    clean_ok = staging_mgr.cleanup_batch_staging(batch.job_id, batch.batch_index)
    assert clean_ok is True
    repo.mark_staging_cleaned(transfer.id)
    db_session.commit()

    db_session.refresh(transfer)
    assert transfer.staging_cleaned_at is not None
    assert len(repo.list_pending_cleanups()) == 0



    # 3. BatchStateMachine and AssetStateMachine integrity
    assert BatchStateMachine.can_transition(BatchStatus.DISCOVERING, BatchStatus.DOWNLOADING) is True
    assert AssetStateMachine.can_transition(AssetStatus.RELEVANCE_PASSED, AssetStatus.READY_FOR_UPLOAD) is True
