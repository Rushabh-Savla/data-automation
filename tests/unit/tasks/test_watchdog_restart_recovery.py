"""list_stalled with a restart cutoff: the query the watchdog uses to resume work
that a container restart orphaned, without waiting out the 15 minute stall timer."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.db.repositories.job_source_task_repository import JobSourceTaskRepository

NOW = datetime(2026, 9, 21, 8, 40, 0, tzinfo=timezone.utc)
BOOT = NOW - timedelta(minutes=5)


class _FakeDb:
    def __init__(self, tasks):
        self._tasks = tasks

    def execute(self, stmt):
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: self._tasks))


def _task(name, heartbeat_ago_s):
    beat = None if heartbeat_ago_s is None else NOW - timedelta(seconds=heartbeat_ago_s)
    return SimpleNamespace(name=name, last_heartbeat_at=beat)


def _names(tasks):
    return sorted(t.name for t in tasks)


class TestListStalledWithRestartCutoff:
    def _repo(self, *tasks):
        return JobSourceTaskRepository(_FakeDb(list(tasks)))

    def test_without_a_cutoff_only_the_ordinary_threshold_applies(self):
        # 10 minutes silent is under the 15 minute threshold: not stalled. Unchanged behaviour.
        repo = self._repo(_task("recent", 600), _task("old", 1200))

        assert _names(repo.list_stalled(900, NOW)) == ["old"]

    def test_work_last_seen_before_the_boot_is_stalled_even_if_under_the_threshold(self):
        # Heartbeat 7 minutes ago = 2 minutes before the boot, killed by the restart.
        repo = self._repo(_task("killed", 7 * 60))

        assert _names(repo.list_stalled(900, NOW, orphan_cutoff=BOOT)) == ["killed"]

    def test_work_that_heartbeated_since_the_boot_is_left_alone(self):
        # 3 minutes ago is after the boot 5 minutes ago: it is running on a worker that survived.
        repo = self._repo(_task("alive", 3 * 60))

        assert repo.list_stalled(900, NOW, orphan_cutoff=BOOT) == []

    def test_a_task_that_never_heartbeated_is_stalled(self):
        assert _names(self._repo(_task("never", None)).list_stalled(900, NOW, orphan_cutoff=BOOT)) == ["never"]

    def test_mixed_batch(self):
        repo = self._repo(
            _task("killed_in_restart", 7 * 60),
            _task("kept_running", 30),
            _task("ancient", 3600),
            _task("resumed_after_boot", 2 * 60),
        )

        result = repo.list_stalled(900, NOW, orphan_cutoff=BOOT)

        assert _names(result) == ["ancient", "killed_in_restart"]

    def test_no_cutoff_means_no_orphan_detection(self):
        # Before the grace period has passed the cutoff is None and nothing new is caught.
        assert self._repo(_task("killed", 7 * 60)).list_stalled(900, NOW, orphan_cutoff=None) == []
