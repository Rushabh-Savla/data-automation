"""Integration test: Authoritative Source Dispatcher & Selection Modes."""

import uuid
import pytest

from app.db.models.job import Job
from app.db.models.job_source_task import JobSourceTask
from app.domain.enums import JobStatus, SourceSelectionMode, SourceType
from app.services.job_service import JobService
from app.services.source_dispatch_service import (
    PROHIBITED_AUTO_SOURCES,
    SourceDispatchService,
)


def test_manual_source_selection_persisted(db_session):
    service = JobService(db_session)
    job = service.create(
        query="cars",
        sources=[SourceType.COCO, SourceType.WEB],
        name="manual_test",
        target_count=100,
    )
    db_session.commit()

    assert job.sources == ["coco", "web"]
    assert job.source_selection_mode == "manual"

    tasks = db_session.query(JobSourceTask).filter_by(job_id=job.id).all()
    task_sources = {t.source_type for t in tasks}
    assert task_sources == {"coco", "web"}


def test_auto_source_selection_excludes_prohibited(db_session):
    resolved, mode = SourceDispatchService.resolve_sources(
        requested_sources=None,
        selection_mode=SourceSelectionMode.AUTO,
        db=db_session,
    )
    assert mode == SourceSelectionMode.AUTO
    for prohibited in PROHIBITED_AUTO_SOURCES:
        assert prohibited not in resolved
    assert SourceType.NAS_SHARE not in resolved


def test_random_source_selection_picks_subset(db_session):
    resolved, mode = SourceDispatchService.resolve_sources(
        requested_sources=None,
        selection_mode=SourceSelectionMode.RANDOM,
        db=db_session,
    )
    assert mode == SourceSelectionMode.RANDOM
    assert len(resolved) >= 1
    for prohibited in PROHIBITED_AUTO_SOURCES:
        assert prohibited not in resolved


def test_unselected_sources_blocked_on_filter(db_session):
    job = Job(
        id=uuid.uuid4(),
        query="dogs",
        sources=["coco", "web"],
        status=JobStatus.RUNNING,
    )
    db_session.add(job)
    db_session.commit()

    candidates = ["coco", "youtube", "web", "kitti"]
    authorized = SourceDispatchService.filter_authorized_sources(job, candidates)
    assert authorized == ["coco", "web"]
