"""Comprehensive unit and integration tests for Dynamic Final Batch Sizing in Data Automation V3.

Verifies:
- Standard 600-image batches with dynamically sized final batches for all test targets:
  100, 200, 600, 1000, 1500, 2000, 2500.
- Dynamic 25% overfetch calculation for each batch target.
- Exact manifest creation counts matching batch.target_image_count.
- Incomplete batch protection on dynamic final batches (e.g. 399/400 rejected, 400/400 accepted).
- Idempotency and duplicate prevention during retries.
- Worker crash recovery during dynamic batch lifecycle.
"""

import json
import uuid
import pytest
from pathlib import Path
from sqlalchemy import select

from app.core.config import calculate_initial_candidates, resolve_overfetch_buffer
from app.db.models.asset import Asset
from app.db.models.candidate_window import CandidateWindow
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import AssetStatus, BatchStatus, SourceType
from app.services.candidate_reservation_service import ensure_candidate_window
from app.storage.nas_writer import enqueue_nas_batch
from app.tasks.batch_orchestration import BatchOrchestrator


@pytest.mark.parametrize(
    "job_target, expected_batch_targets",
    [
        (100, [100]),
        (200, [200]),
        (600, [600]),
        (1000, [600, 400]),
        (1500, [600, 600, 300]),
        (2000, [600, 600, 600, 200]),
        (2500, [600, 600, 600, 600, 100]),
    ],
)
def test_dynamic_batch_targets_and_sequence(db_session, job_target, expected_batch_targets):
    """Verifies that jobs with various targets dynamically decompose into standard 600-item
    batches plus a dynamically sized final batch matching remaining needed.
    """
    job_id = uuid.uuid4()
    job = Job(id=job_id, query="dynamic_test", target_count=job_target)
    db_session.add(job)
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    created_batches = []

    for expected_target in expected_batch_targets:
        batch = orchestrator.get_or_create_active_batch()
        assert batch is not None, f"Expected batch with target {expected_target}, got None"
        assert batch.target_image_count == expected_target
        created_batches.append(batch)

        # Simulate batch reaching target and completing upload
        batch.status = BatchStatus.COMPLETED
        batch.accepted_image_count = expected_target
        db_session.commit()

    # Once job target is fully satisfied, no further batches must be created
    final_attempt = orchestrator.get_or_create_active_batch()
    assert final_attempt is None, f"Expected no further batches for target {job_target}, got {final_attempt}"
    assert len(created_batches) == len(expected_batch_targets)


@pytest.mark.parametrize(
    "batch_target, expected_window_limit",
    [
        (100, 125),
        (200, 250),
        (300, 375),
        (400, 500),
        (600, 750),
    ],
)
def test_dynamic_overfetch_window_calculations(db_session, batch_target, expected_window_limit):
    """Verifies that 25% candidate overfetch calculation dynamically scales with batch target."""
    calc = calculate_initial_candidates(batch_target)
    assert calc == expected_window_limit

    job_id = uuid.uuid4()
    job = Job(id=job_id, query="window_test", target_count=batch_target)
    db_session.add(job)
    db_session.commit()

    batch = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=1,
        target_image_count=batch_target,
        status=BatchStatus.DOWNLOADING,
    )
    db_session.add(batch)
    db_session.commit()

    window_id = ensure_candidate_window(db_session, batch)
    window = db_session.get(CandidateWindow, window_id)
    assert window.candidate_limit == expected_window_limit


def test_manifest_creation_exact_count_for_dynamic_final_batch(db_session, tmp_path):
    """Verifies that create_batch_manifest trims excess assets down to the exact dynamic target
    and creates a manifest with exactly target items (e.g. 400).
    """
    job_id = uuid.uuid4()
    task_id = uuid.uuid4()
    job = Job(id=job_id, query="containers", target_count=1000)
    task = JobSourceTask(id=task_id, job_id=job_id, source_type=SourceType.WEB.value)
    db_session.add_all([job, task])
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    orchestrator.settings.local_staging_path = tmp_path

    # Simulate Batch 1 already completed with 600
    b1 = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=1,
        target_image_count=600,
        accepted_image_count=600,
        status=BatchStatus.COMPLETED,
    )
    db_session.add(b1)
    db_session.commit()

    # Batch 2 is the final batch: target must be 400
    b2 = orchestrator.get_or_create_active_batch()
    assert b2.batch_index == 2
    assert b2.target_image_count == 400

    # Add 450 candidate accepted assets to Batch 2 (overfetch resulted in 50 surplus)
    for i in range(450):
        img_file = tmp_path / f"img_{i}.jpg"
        img_file.write_bytes(b"data")
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task_id,
                batch_id=b2.id,
                source_type=SourceType.WEB,
                source_item_id=f"item_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
                relevance_score=0.50 + (i * 0.001),
                nas_path=str(img_file),
            )
        )
    db_session.commit()

    # Manifest creation must trim excess back to exactly 400
    manifest_path = orchestrator.create_batch_manifest(b2.id)
    assert manifest_path is not None
    assert manifest_path.exists()

    data = json.loads(manifest_path.read_text())
    assert data["target_count"] == 400
    assert data["accepted_count"] == 400
    assert len(data["items"]) == 400

    db_session.refresh(b2)
    assert b2.accepted_image_count == 400


def test_nas_upload_guard_rejects_incomplete_final_batch(db_session, tmp_path):
    """Verifies that enqueue_nas_batch rejects an incomplete final batch (e.g. 399/400)
    and only allows it when the full dynamic target (400/400) is reached.
    """
    job_id = uuid.uuid4()
    job = Job(id=job_id, query="nas_guard", target_count=1000)
    b2 = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=2,
        target_image_count=400,
        accepted_image_count=399,
        status=BatchStatus.DOWNLOADING,
    )
    db_session.add_all([job, b2])
    db_session.commit()

    # Create incomplete manifest with 399 items
    manifest_dir = tmp_path / "batch_002"
    manifest_dir.mkdir(parents=True)
    manifest_file = manifest_dir / "manifest.json"
    manifest_data = {
        "batch_id": str(b2.id),
        "target_count": 400,
        "items": [{"asset_id": str(uuid.uuid4()), "local_path": "/fake/p"} for _ in range(399)],
    }
    manifest_file.write_text(json.dumps(manifest_data))

    # NAS queue must REFUSE incomplete dynamic batch
    transfer_id = enqueue_nas_batch(
        batch_id=b2.id,
        job_id=job_id,
        manifest_path=manifest_file,
        remote_dest="ai_data_collection/test",
        db=db_session,
    )
    assert transfer_id is None, "Incomplete dynamic batch (399/400) must be rejected by NAS writer!"

    # Now finalize the batch window and fulfill to 400
    manifest_data["items"].append({"asset_id": str(uuid.uuid4()), "local_path": "/fake/p400"})
    manifest_file.write_text(json.dumps(manifest_data))
    b2.status = BatchStatus.READY_FOR_UPLOAD
    b2.accepted_image_count = 400
    db_session.commit()

    transfer_id_success = enqueue_nas_batch(
        batch_id=b2.id,
        job_id=job_id,
        manifest_path=manifest_file,
        remote_dest="ai_data_collection/test",
        db=db_session,
    )
    assert transfer_id_success is not None, "Complete dynamic batch (400/400) must be accepted for NAS upload!"


def test_idempotent_batch_retries_prevent_duplicate_manifests(db_session, tmp_path):
    """Verifies that calling create_batch_manifest repeatedly is idempotent and does not
    create duplicate records or corrupt counters.
    """
    job_id = uuid.uuid4()
    task_id = uuid.uuid4()
    job = Job(id=job_id, query="idempotency", target_count=200)
    task = JobSourceTask(id=task_id, job_id=job_id, source_type=SourceType.WEB.value)
    b1 = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=1,
        target_image_count=200,
        status=BatchStatus.PROCESSING,
    )
    db_session.add_all([job, task, b1])
    db_session.commit()

    for i in range(200):
        img_file = tmp_path / f"item_{i}.jpg"
        img_file.write_bytes(b"data")
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task_id,
                batch_id=b1.id,
                source_type=SourceType.WEB,
                source_item_id=f"item_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
                nas_path=str(img_file),
            )
        )
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    orchestrator.settings.local_staging_path = tmp_path

    m1 = orchestrator.create_batch_manifest(b1.id)
    m2 = orchestrator.create_batch_manifest(b1.id)

    assert m1 == m2
    db_session.refresh(b1)
    assert b1.accepted_image_count == 200


def test_worker_crash_recovery_preserves_dynamic_active_batch(db_session):
    """Verifies that if a worker crashes while a dynamic batch is in flight (status DOWNLOADING),
    the next invocation recovers the existing batch rather than spawning duplicate batches.
    """
    job_id = uuid.uuid4()
    job = Job(id=job_id, query="crash_recovery", target_count=1500)
    b1 = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=1,
        target_image_count=600,
        accepted_image_count=600,
        status=BatchStatus.COMPLETED,
    )
    b2 = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=2,
        target_image_count=600,
        accepted_image_count=600,
        status=BatchStatus.COMPLETED,
    )
    # Batch 3 is in flight with dynamic target 300 when worker crashed
    b3 = ImageBatch(
        id=uuid.uuid4(),
        job_id=job_id,
        batch_index=3,
        target_image_count=300,
        accepted_image_count=150,
        status=BatchStatus.DOWNLOADING,
    )
    db_session.add_all([job, b1, b2, b3])
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    recovered_batch = orchestrator.get_or_create_active_batch()

    assert recovered_batch.id == b3.id
    assert recovered_batch.batch_index == 3
    assert recovered_batch.target_image_count == 300
    assert recovered_batch.accepted_image_count == 150

    # Ensure no ghost batch #4 was created
    all_batches = db_session.execute(select(ImageBatch).where(ImageBatch.job_id == job_id)).scalars().all()
    assert len(all_batches) == 3
