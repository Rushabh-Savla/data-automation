"""download_youtube_video's per-video workdir isolation.

A job with several manually entered YouTube URLs fans out one task per URL,
all sharing one job_source_task_id, and Celery runs them concurrently. Before
this fix they all shared one workdir, so yt-dlp's MaxDownloadsReached
fallback (YouTubeConnector.fetch's `video_dir.glob("*")`) could pick up a
sibling task's video file for a different URL — mismatching a frame's pixel
content against its source_url/dataset_identifier metadata."""

import contextlib
import uuid
from types import SimpleNamespace

import pytest

import app.tasks.ingest_tasks as it


class _NoRows:
    def get(self, _id):
        return None


@pytest.fixture
def captured(tmp_path, monkeypatch):
    seen_workdirs = []

    class FakeConnector:
        def __init__(self, **kwargs):
            pass

        def fetch(self, item, workdir, checkpoint, should_stop, on_progress):
            seen_workdirs.append(workdir)
            return iter(())

    def fake_run_item_download(job_id, job_source_task_id, source_type, checkpoint_key, fetch, bound_task=None):
        # `fetch` is a generator function (`yield from connector.fetch(...)`);
        # calling it alone builds the generator without running any of its
        # body, so it must actually be consumed to reach connector.fetch().
        list(fetch({}, lambda: False, lambda delta: None))
        return {"item": checkpoint_key}

    monkeypatch.setattr(it, "get_settings", lambda: SimpleNamespace(work_root=tmp_path))
    monkeypatch.setattr(it, "YouTubeConnector", FakeConnector)
    monkeypatch.setattr(it, "session_scope", lambda: contextlib.nullcontext(SimpleNamespace()))
    monkeypatch.setattr(it, "JobSourceTaskRepository", lambda db: _NoRows())
    monkeypatch.setattr(it, "_run_item_download", fake_run_item_download)
    return seen_workdirs


def _item(url: str, video_id: str) -> dict:
    return {"external_ref": {"url": url, "video_id": video_id}}


class TestPerVideoWorkdirIsolation:
    def test_two_urls_for_the_same_job_source_task_get_different_workdirs(self, captured, tmp_path):
        job_source_task_id = str(uuid.uuid4())
        it.download_youtube_video.run("job1", job_source_task_id, _item("https://youtu.be/AAAA", "AAAA"))
        it.download_youtube_video.run("job1", job_source_task_id, _item("https://youtu.be/BBBB", "BBBB"))

        assert len(captured) == 2
        assert captured[0] != captured[1], "sibling URLs in the same job must not share a workdir"

    def test_workdir_is_nested_under_the_job_source_task_dir(self, captured, tmp_path):
        job_source_task_id = str(uuid.uuid4())
        it.download_youtube_video.run("job1", job_source_task_id, _item("https://youtu.be/AAAA", "AAAA"))

        assert captured[0] == tmp_path / job_source_task_id / "AAAA"
        # Still fully contained under the job_source_task_id tree, so the
        # existing whole-tree cleanup (cleanup_tasks.cleanup_local) still
        # removes it with no change needed there.
        assert captured[0].parent == tmp_path / job_source_task_id

    def test_same_video_id_resumed_reuses_its_own_workdir(self, captured, tmp_path):
        job_source_task_id = str(uuid.uuid4())
        it.download_youtube_video.run("job1", job_source_task_id, _item("https://youtu.be/AAAA", "AAAA"))
        it.download_youtube_video.run("job1", job_source_task_id, _item("https://youtu.be/AAAA", "AAAA"))

        assert captured[0] == captured[1]

    def test_missing_video_id_falls_back_to_a_filesystem_safe_name(self, captured, tmp_path):
        # Defensive backstop only — both real dispatch paths in
        # discovery_tasks.py always set video_id before this task runs.
        job_source_task_id = str(uuid.uuid4())
        it.download_youtube_video.run("job1", job_source_task_id, {"external_ref": {"url": "https://youtu.be/weird?x=1"}})

        workdir = captured[0]
        assert workdir.parent == tmp_path / job_source_task_id
        assert "/" not in workdir.name and "?" not in workdir.name
