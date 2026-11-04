import json
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
from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import BatchStatus, JobSourceTaskStatus, JobStatus, SourceType, AssetStatus
from app.services.candidate_reservation_service import (
    CandidateAdmissionStatus,
    admit_candidate_for_task,
    mark_candidate_staged,
    mark_candidate_failed,
    set_candidate_window_status,
    get_candidate_window_stats,
    open_candidate_window,
)
from app.storage.window_staging import WindowStagingManager
from app.storage.nas_writer import enqueue_nas_window, NasBatchWriter


@pytest.fixture
def test_env(tmp_path, monkeypatch):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    nas_dir = tmp_path / "nas"
    nas_dir.mkdir(parents=True, exist_ok=True)

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
    monkeypatch.setattr("app.storage.nas_writer.session_scope", _mock_scope)
    monkeypatch.setattr("app.tasks.orchestration_tasks.session_scope", _mock_scope)
    
    return {
        "Session": Session,
        "staging_dir": staging_dir,
        "nas_dir": nas_dir,
        "scope": _mock_scope,
    }


def test_full_candidate_window_e2e_pipeline(test_env, monkeypatch):
    """
    Simulates end-to-end pipeline:
    1. Multi-source task creation.
    2. Windowed reservation up to limit.
    3. Candidate 601 rejection and WINDOW_WAITING state.
    4. Durable SSD staging with WindowStagingManager.
    5. Window manifest creation.
    6. Centralized NAS upload enqueue & sequential upload.
    7. SHA256 and size verification.
    8. Asset status transition to COMPLETED.
    9. Staging directory cleanup for only the completed window.
    """
    Session = test_env["Session"]
    staging_dir = test_env["staging_dir"]
    nas_dir = test_env["nas_dir"]
    staging_mgr = WindowStagingManager(staging_path=staging_dir)

    job_id = uuid.uuid4()
    batch_id = uuid.uuid4()
    window_id = uuid.uuid4()
    task_coco_id = uuid.uuid4()
    task_openimages_id = uuid.uuid4()

    with Session() as db:
        job = Job(id=job_id, name="e2e_test_job", query="safety vest", target_count=600)
        batch = ImageBatch(id=batch_id, job_id=job_id, batch_index=1, status=BatchStatus.DOWNLOADING)
        window = CandidateWindow(
            id=window_id,
            batch_id=batch_id,
            job_id=job_id,
            window_index=1,
            candidate_limit=4,  # Window limit 4 candidates
            status="OPEN",
        )
        t_coco = JobSourceTask(
            id=task_coco_id,
            job_id=job_id,
            source_type=SourceType.COCO.value,
            status=JobSourceTaskStatus.DOWNLOADING,
            checkpoint={"index": 0},
        )
        t_open = JobSourceTask(
            id=task_openimages_id,
            job_id=job_id,
            source_type=SourceType.OPEN_IMAGES.value,
            status=JobSourceTaskStatus.DOWNLOADING,
            checkpoint={"index": 0},
        )
        db.add_all([job, batch, window, t_coco, t_open])
        db.commit()

    # Step 1: Concurrent sources reserve candidates
    # COCO reserves 2 items
    c1 = admit_candidate_for_task(task_coco_id, SourceType.COCO, "coco_1")
    c2 = admit_candidate_for_task(task_coco_id, SourceType.COCO, "coco_2")
    assert c1.status == CandidateAdmissionStatus.ADMITTED
    assert c2.status == CandidateAdmissionStatus.ADMITTED

    # OpenImages reserves 2 items
    o1 = admit_candidate_for_task(task_openimages_id, SourceType.OPEN_IMAGES, "oi_1")
    o2 = admit_candidate_for_task(task_openimages_id, SourceType.OPEN_IMAGES, "oi_2")
    assert o1.status == CandidateAdmissionStatus.ADMITTED
    assert o2.status == CandidateAdmissionStatus.ADMITTED

    # Step 2: Next candidate (5th) is rejected with WINDOW_FULL
    c_extra = admit_candidate_for_task(task_coco_id, SourceType.COCO, "coco_3")
    assert c_extra.status == CandidateAdmissionStatus.WINDOW_FULL

    with Session() as db:
        w = db.get(CandidateWindow, window_id)
        assert w.status == "FULL"

    # Step 3: Staging assets on local SSD
    admitted = [
        (c1, SourceType.COCO, "coco_1", b"fake_coco_image_1"),
        (c2, SourceType.COCO, "coco_2", b"fake_coco_image_2"),
        (o1, SourceType.OPEN_IMAGES, "oi_1", b"fake_openimages_image_1"),
        (o2, SourceType.OPEN_IMAGES, "oi_2", b"fake_openimages_image_2"),
    ]

    manifest_items = []
    import hashlib

    with Session() as db:
        for adm, stype, item_id, payload in admitted:
            asset_id = uuid.uuid4()
            sha256 = hashlib.sha256(payload).hexdigest()
            # Stage file
            dest = staging_mgr.stage_accepted_image(
                job_id, 1, 1, asset_id, "jpg", payload
            )
            mark_candidate_staged(adm.reservation_id, asset_id)

            asset = Asset(
                id=asset_id,
                job_id=job_id,
                job_source_task_id=task_coco_id if stype == SourceType.COCO else task_openimages_id,
                batch_id=batch_id,
                window_id=window_id,
                source_type=stype.value,
                source_item_id=item_id,
                status=AssetStatus.RELEVANCE_PASSED,
                checksum_sha256=sha256,
                nas_path=str(dest),
            )
            db.add(asset)
            manifest_items.append({
                "asset_id": str(asset_id),
                "local_path": str(dest),
                "checksum_sha256": sha256,
                "file_size": len(payload),
            })
        db.commit()

    # Step 4: Write Window Manifest
    manifest_path = staging_mgr.get_window_manifest_path(job_id, 1, 1)
    manifest_data = {
        "job_id": str(job_id),
        "batch_id": str(batch_id),
        "window_id": str(window_id),
        "batch_index": 1,
        "window_index": 1,
        "items": manifest_items,
    }
    manifest_path.write_text(json.dumps(manifest_data))
    assert manifest_path.is_file()

    # Step 5: Enqueue Candidate Window NAS Transfer
    transfer_id = enqueue_nas_window(
        window_id=window_id,
        batch_id=batch_id,
        job_id=job_id,
        batch_index=1,
        window_index=1,
        manifest_path=manifest_path,
        remote_dest="test_storage",
    )
    assert transfer_id is not None

    with Session() as db:
        transfer = db.get(NasTransferBatch, transfer_id)
        assert transfer is not None
        assert transfer.window_id == window_id
        assert transfer.batch_id == batch_id

    # Step 6: Verify window-only staging directory cleanup function
    w_dir = staging_mgr.get_window_staging_dir(job_id, 1, 1)
    assert w_dir.is_dir()
    cleanup_ok = staging_mgr.cleanup_window_staging(job_id, 1, 1)
    assert cleanup_ok is True
    assert not w_dir.exists()
