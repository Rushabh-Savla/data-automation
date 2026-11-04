"""Unit tests for Stage 2: Batch Orchestration, Shortfall Replacement & Cross-Batch Dedup."""

import json
import uuid
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.base import Base
from app.db.models.asset import Asset
from app.db.models.image_batch import ImageBatch
from app.db.models.job import Job
from app.domain.enums import AssetStatus, BatchStatus, SourceType
from app.filtering.cross_batch_dedup import CrossBatchDeduplicator
from app.tasks.batch_orchestration import BatchOrchestrator



def test_batch_creation_and_sizing(db_session):
    job_id = uuid.uuid4()
    job = Job(id=job_id, query="fire", target_count=1400)
    db_session.add(job)
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    # First batch: target should be 600
    b1 = orchestrator.get_or_create_active_batch()
    assert b1.batch_index == 1
    assert b1.target_image_count == 600
    assert b1.status == BatchStatus.DISCOVERING

    # If b1 is still in progress, calling get_or_create_active_batch returns b1
    b_active = orchestrator.get_or_create_active_batch()
    assert b_active.id == b1.id

    # Complete batch 1 with 600 accepted images
    b1.status = BatchStatus.COMPLETED
    b1.accepted_image_count = 600
    db_session.commit()

    # Second batch: target should be 600 (remaining needed: 1400 - 600 = 800)
    b2 = orchestrator.get_or_create_active_batch()
    assert b2.batch_index == 2
    assert b2.target_image_count == 600

    # Complete batch 2 with 600 accepted images
    b2.status = BatchStatus.COMPLETED
    b2.accepted_image_count = 600
    db_session.commit()

    # Final batch: remaining needed is 200 (1400 - 1200 = 200)
    b3 = orchestrator.get_or_create_active_batch()
    assert b3.batch_index == 3
    assert b3.target_image_count == 200


def test_shortfall_calculation_and_replacements(db_session):
    job_id = uuid.uuid4()
    task_id = uuid.uuid4()
    job = Job(id=job_id, query="smoke", target_count=600)
    db_session.add(job)
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    b1 = orchestrator.get_or_create_active_batch()

    # Simulate 600 candidates evaluated: 450 passed, 100 rejected, 50 duplicates
    for i in range(450):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task_id,
                batch_id=b1.id,
                source_type=SourceType.WEB,
                source_item_id=f"pass_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    for i in range(100):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task_id,
                batch_id=b1.id,
                source_type=SourceType.WEB,
                source_item_id=f"rej_{i}",
                status=AssetStatus.RELEVANCE_REJECTED,
            )
        )
    for i in range(50):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task_id,
                batch_id=b1.id,
                source_type=SourceType.WEB,
                source_item_id=f"dup_{i}",
                status=AssetStatus.DUPLICATE,
            )
        )
    db_session.commit()

    progress = orchestrator.evaluate_batch_progress(b1.id)
    assert progress["accepted"] == 450
    assert progress["shortfall"] == 150
    assert progress["outcome"] == "NEEDS_REPLACEMENTS"

    # Now add the 150 replacement assets that pass
    for i in range(150):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job_id,
                job_source_task_id=task_id,
                batch_id=b1.id,
                source_type=SourceType.WEB,
                source_item_id=f"rep_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    db_session.commit()

    progress2 = orchestrator.evaluate_batch_progress(b1.id)
    assert progress2["accepted"] == 600
    assert progress2["shortfall"] == 0
    assert progress2["outcome"] == "TARGET_MET"


def test_cross_batch_deduplication(db_session):
    job_id = uuid.uuid4()
    task_id = uuid.uuid4()
    job = Job(id=job_id, query="car", target_count=1200)
    db_session.add(job)
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    b1 = orchestrator.get_or_create_active_batch()

    # Batch 1 accepted image A
    img_a_id = uuid.uuid4()
    img_a = Asset(
        id=img_a_id,
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b1.id,
        source_type=SourceType.COCO,
        source_item_id="coco_1001",
        checksum_sha256="hash_car_unique_123",
        phash="1111222233334444",
        status=AssetStatus.RELEVANCE_PASSED,
    )
    db_session.add(img_a)
    b1.status = BatchStatus.COMPLETED
    b1.accepted_image_count = 600
    db_session.commit()

    # Batch 2 receives candidate B with exact same SHA256 from different source
    b2 = orchestrator.get_or_create_active_batch()
    img_b = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b2.id,
        source_type=SourceType.ROBOFLOW,
        source_item_id="rf_2002",
        checksum_sha256="hash_car_unique_123",  # Exact duplicate of img_a
        phash="1111222233334444",
        status=AssetStatus.PROMOTED,
    )
    # Batch 2 receives candidate C with near-identical pHash (hamming distance = 0)
    img_c = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b2.id,
        source_type=SourceType.WEB,
        source_item_id="web_3003",
        checksum_sha256="hash_car_unique_456",
        phash="1111222233334444",  # Same pHash
        status=AssetStatus.PROMOTED,
    )
    db_session.add_all([img_b, img_c])
    db_session.commit()

    # Run cross-batch deduplication
    dups_found = orchestrator.run_cross_batch_deduplication(b2.id)
    assert dups_found == 2
    assert img_b.status == AssetStatus.DUPLICATE
    assert img_b.is_duplicate_of == img_a_id
    assert img_c.status == AssetStatus.DUPLICATE
    assert img_c.is_duplicate_of == img_a_id


def test_create_batch_manifest(db_session, tmp_path):
    job_id = uuid.uuid4()
    task_id = uuid.uuid4()
    job = Job(id=job_id, query="drone", target_count=600)
    db_session.add(job)
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    orchestrator.settings.local_staging_path = tmp_path
    b1 = orchestrator.get_or_create_active_batch()

    asset_id = uuid.uuid4()
    asset = Asset(
        id=asset_id,
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b1.id,
        source_type=SourceType.WEB,
        source_item_id="drone_01",
        nas_path="/staging/drone_01.jpg",
        status=AssetStatus.RELEVANCE_PASSED,
    )
    db_session.add(asset)
    db_session.commit()

    manifest_path = orchestrator.create_batch_manifest(b1.id)
    assert manifest_path.exists()
    assert b1.status == BatchStatus.READY_FOR_UPLOAD

    content = json.loads(manifest_path.read_text())
    assert content["batch_id"] == str(b1.id)
    assert content["accepted_count"] == 1
    assert content["items"][0]["asset_id"] == str(asset_id)


def test_concurrent_jobs_have_independent_batch_counters(db_session):
    job1_id = uuid.uuid4()
    job2_id = uuid.uuid4()
    task1_id = uuid.uuid4()
    task2_id = uuid.uuid4()

    job1 = Job(id=job1_id, query="cats", target_count=600)
    job2 = Job(id=job2_id, query="dogs", target_count=600)
    db_session.add_all([job1, job2])
    db_session.commit()

    orch1 = BatchOrchestrator(db_session, job1_id)
    orch2 = BatchOrchestrator(db_session, job2_id)

    b1_j1 = orch1.get_or_create_active_batch()
    b1_j2 = orch2.get_or_create_active_batch()

    assert b1_j1.batch_index == 1
    assert b1_j2.batch_index == 1
    assert b1_j1.job_id == job1_id
    assert b1_j2.job_id == job2_id

    # Populate assets for job 1 only
    for i in range(10):
        db_session.add(
            Asset(
                id=uuid.uuid4(),
                job_id=job1_id,
                job_source_task_id=task1_id,
                batch_id=b1_j1.id,
                source_type=SourceType.WEB,
                source_item_id=f"cat_{i}",
                status=AssetStatus.RELEVANCE_PASSED,
            )
        )
    db_session.commit()

    p1 = orch1.evaluate_batch_progress(b1_j1.id)
    p2 = orch2.evaluate_batch_progress(b1_j2.id)

    # Job 1 accepted count changed, Job 2 remains independent at 0
    assert p1["accepted"] == 10
    assert p2["accepted"] == 0


def test_manifest_strictly_excludes_rejected_and_duplicates(db_session, tmp_path):
    job_id = uuid.uuid4()
    task_id = uuid.uuid4()
    job = Job(id=job_id, query="forest", target_count=600)
    db_session.add(job)
    db_session.commit()

    orchestrator = BatchOrchestrator(db_session, job_id)
    orchestrator.settings.local_staging_path = tmp_path
    b1 = orchestrator.get_or_create_active_batch()

    # 1 Passed, 1 Rejected, 1 Duplicate
    a_pass = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b1.id,
        source_type=SourceType.WEB,
        source_item_id="tree_01",
        nas_path="/staging/tree_01.jpg",
        status=AssetStatus.RELEVANCE_PASSED,
    )
    a_rej = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b1.id,
        source_type=SourceType.WEB,
        source_item_id="desert_01",
        nas_path="/staging/desert_01.jpg",
        status=AssetStatus.RELEVANCE_REJECTED,
    )
    a_dup = Asset(
        id=uuid.uuid4(),
        job_id=job_id,
        job_source_task_id=task_id,
        batch_id=b1.id,
        source_type=SourceType.WEB,
        source_item_id="tree_dup",
        nas_path="/staging/tree_dup.jpg",
        status=AssetStatus.DUPLICATE,
    )
    db_session.add_all([a_pass, a_rej, a_dup])
    db_session.commit()

    manifest_path = orchestrator.create_batch_manifest(b1.id)
    content = json.loads(manifest_path.read_text())

    assert content["accepted_count"] == 1
    assert len(content["items"]) == 1
    assert content["items"][0]["asset_id"] == str(a_pass.id)
    item_ids = [item["asset_id"] for item in content["items"]]
    assert str(a_rej.id) not in item_ids
    assert str(a_dup.id) not in item_ids
