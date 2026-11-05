import shutil
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.domain.enums import AssetStatus, JobSourceTaskStatus, JobStatus, SourceType
from app.tasks.cleanup_tasks import cleanup_job_storage, cleanup_local, purge_orphan_work_dirs


@pytest.fixture
def tmp_work_root(tmp_path, monkeypatch):
    work_dir = tmp_path / "work_data"
    work_dir.mkdir(parents=True, exist_ok=True)
    from app.core.config import get_settings
    settings = get_settings()
    monkeypatch.setattr(settings, "work_root", work_dir)
    return work_dir


def test_cleanup_local(tmp_work_root):
    task_id = str(uuid.uuid4())
    task_dir = tmp_work_root / task_id
    task_dir.mkdir()
    (task_dir / "test.jpg").write_text("dummy")

    assert task_dir.exists()
    res = cleanup_local(task_id)
    assert res["workdir_removed"] is True
    assert not task_dir.exists()


def test_cleanup_job_storage(tmp_path):
    f1 = tmp_path / "asset1.jpg"
    f2 = tmp_path / "asset2.jpg"
    f1.write_text("data1")
    f2.write_text("data2")

    res = cleanup_job_storage("job-123", [str(f1), str(f2)])
    assert res["deleted_files"] == 2
    assert not f1.exists()
    assert not f2.exists()


def test_purge_orphan_work_dirs_includes_staging(tmp_work_root, monkeypatch):
    staging_dir = tmp_work_root / "staging"
    staging_dir.mkdir()

    stale_job_id = uuid.uuid4()
    job_staging = staging_dir / f"job_{stale_job_id}"
    job_staging.mkdir()
    (job_staging / "manifest.json").write_text("{}")

    mock_db = MagicMock()
    mock_db.execute.return_value.all.return_value = []
    mock_db.get.return_value = None  # Job does not exist

    from contextlib import contextmanager

    @contextmanager
    def mock_scope():
        yield mock_db

    monkeypatch.setattr("app.db.session.session_scope", mock_scope)

    assert job_staging.exists()
    res = purge_orphan_work_dirs()
    assert res["removed"] >= 1
    assert not job_staging.exists()


def test_delete_job_cleans_filesystem(tmp_work_root):
    from app.api.v1.jobs import delete_job

    job_id = uuid.uuid4()
    task_id = uuid.uuid4()

    mock_job = MagicMock()
    mock_job.id = job_id
    mock_job.status = JobStatus.COMPLETED

    mock_task = MagicMock()
    mock_task.id = task_id

    local_file = tmp_work_root / "promoted.jpg"
    local_file.write_text("img")

    mock_asset = MagicMock()
    mock_asset.nas_path = str(local_file)
    mock_asset.annotation_nas_path = None

    task_dir = tmp_work_root / str(task_id)
    task_dir.mkdir()
    (task_dir / "raw.jpg").write_text("raw")

    staging_dir = tmp_work_root / "staging" / f"job_{job_id}"
    staging_dir.mkdir(parents=True)
    (staging_dir / "staged.jpg").write_text("staged")

    assert task_dir.exists()
    assert staging_dir.exists()
    assert local_file.exists()

    mock_db = MagicMock()
    mock_db.get.return_value = mock_job
    mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_asset]

    with patch("app.api.v1.jobs._get_job_or_404", return_value=mock_job), \
         patch("app.db.repositories.job_source_task_repository.JobSourceTaskRepository.list_for_job", return_value=[mock_task]):
        delete_job(job_id=job_id, db=mock_db)

    # Check database deletion was called
    mock_db.delete.assert_called_once_with(mock_job)
    mock_db.commit.assert_called_once()

    # Check filesystem deletion
    assert not task_dir.exists()
    assert not staging_dir.exists()
    assert not local_file.exists()


def test_cleanup_shared_cache(tmp_work_root, monkeypatch):
    from contextlib import contextmanager
    from app.tasks.cleanup_tasks import cleanup_shared_cache

    cache_dir = tmp_work_root / "_shared_cache"
    coco_dir = cache_dir / "coco"
    coco_dir.mkdir(parents=True)
    (coco_dir / "index.sqlite3").write_text("sqlite")
    (cache_dir / "dummy.txt").write_text("test")

    assert (coco_dir / "index.sqlite3").exists()

    mock_db = MagicMock()
    mock_db.execute.return_value.scalar.return_value = 0

    @contextmanager
    def mock_scope():
        yield mock_db

    monkeypatch.setattr("app.db.session.session_scope", mock_scope)

    res = cleanup_shared_cache(force=False)
    assert res["status"] == "cleaned"
    assert res["items_removed"] == 2
    assert not (coco_dir / "index.sqlite3").exists()
    assert not (cache_dir / "dummy.txt").exists()
