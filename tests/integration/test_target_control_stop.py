"""Integration test: Target count control stopping acquisition."""

import uuid
from contextlib import contextmanager

from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import JobStatus
from app.services.candidate_reservation_service import (
    CandidateAdmissionStatus,
    admit_candidate_for_task,
)


def test_target_reached_stops_candidate_admission(db_session, monkeypatch):
    @contextmanager
    def _mock_session_scope():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    monkeypatch.setattr(
        "app.services.candidate_reservation_service.session_scope",
        _mock_session_scope,
    )

    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        query="laptops",
        sources=["coco"],
        target_count=10,
        accepted_count=10,
        status=JobStatus.RUNNING,
    )
    db_session.add(job)

    task_id = uuid.uuid4()
    task = JobSourceTask(
        id=task_id,
        job_id=job_id,
        source_type="coco",
    )
    db_session.add(task)
    db_session.commit()

    admission = admit_candidate_for_task(
        job_source_task_id=task_id,
        source_type="coco",
        source_item_id="coco_val_12345",
    )

    assert admission.status == CandidateAdmissionStatus.WINDOW_FULL
    assert not admission.admitted
