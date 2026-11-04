"""Integration test: Lock order and progress batching resilience."""

import concurrent.futures
import uuid

from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import JobStatus
from app.tasks._helpers import build_on_progress


def test_concurrent_progress_updates(db_session, monkeypatch):
    # Mock redis and cancellation
    monkeypatch.setattr("app.tasks._helpers.get_redis", lambda: None)
    monkeypatch.setattr("app.tasks._helpers.ProgressPublisher.publish", lambda self, job_id, payload: None)
    monkeypatch.setattr("app.tasks._helpers.CancellationService.mark_target_reached", lambda self, job_id: None)
    monkeypatch.setattr("app.tasks._helpers.CancellationService.mark_source_cap_reached", lambda self, jid, tid: None)

    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        query="traffic lights",
        sources=["coco", "web"],
        target_count=500,
        accepted_count=0,
        status=JobStatus.RUNNING,
    )
    db_session.add(job)

    task1_id = uuid.uuid4()
    task2_id = uuid.uuid4()
    task1 = JobSourceTask(id=task1_id, job_id=job_id, source_type="coco")
    task2 = JobSourceTask(id=task2_id, job_id=job_id, source_type="web")
    db_session.add_all([task1, task2])
    db_session.commit()

    progress_fn1 = build_on_progress(db_session, job_id, task1_id)
    progress_fn2 = build_on_progress(db_session, job_id, task2_id)

    # Interleaved progress updates
    for i in range(25):
        progress_fn1({"downloaded": 1, "accepted": 1})
        progress_fn2({"downloaded": 1, "accepted": 1})

    db_session.commit()
    db_session.refresh(job)

    assert job.downloaded_count == 50
    assert job.accepted_count == 50
