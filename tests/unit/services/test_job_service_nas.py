from types import SimpleNamespace

from app.domain.enums import SourceType
from app.services import job_service
from app.services.job_service import JobService


class _Repo:
    def __init__(self):
        self.created = []

    def create(self, obj):
        self.created.append(obj)


def _service(monkeypatch):
    monkeypatch.setattr(
        job_service,
        "_SOURCE_DISCOVERY_TASKS",
        {SourceType.WEB: "w", SourceType.COCO: "c", SourceType.NAS_SHARE: "n"},
    )
    svc = JobService.__new__(JobService)
    svc.db = SimpleNamespace(flush=lambda: None)
    svc.job_repo, svc.task_repo = _Repo(), _Repo()
    return svc


def test_auto_mode_never_pulls_in_the_users_nas(monkeypatch):
    svc = _service(monkeypatch)
    job = svc.create(query="cars")
    assert SourceType.NAS_SHARE.value not in job.sources
    assert {SourceType.WEB.value, SourceType.COCO.value} <= set(job.sources)
    assert SourceType.NAS_SHARE.value not in [t.source_type for t in svc.task_repo.created]


def test_naming_the_nas_explicitly_still_works(monkeypatch):
    svc = _service(monkeypatch)
    job = svc.create(query="cars", sources=[SourceType.NAS_SHARE])
    assert job.sources == ["nas_share"]
    assert [t.source_type for t in svc.task_repo.created] == ["nas_share"]


def test_nas_can_be_combined_with_public_sources(monkeypatch):
    svc = _service(monkeypatch)
    job = svc.create(query="cars", sources=[SourceType.NAS_SHARE, SourceType.COCO])
    assert set(job.sources) == {"nas_share", "coco"}
