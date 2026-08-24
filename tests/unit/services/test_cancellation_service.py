"""Tests for CancellationService's per-source fair-share cap flag — added
alongside the existing job-wide pause/cancel/target-reached flags so one
source can stop itself without touching should_stop() for every other
source in the same job."""

from app.services.cancellation_service import CancellationService


class _FakeRedis:
    """Minimal in-memory stand-in for the handful of redis-py calls
    CancellationService makes (set/delete/exists) — avoids pulling in a new
    test dependency (fakeredis) for what's a tiny surface."""

    def __init__(self):
        self._store: set[str] = set()

    def set(self, key, value):
        self._store.add(key)

    def delete(self, key):
        self._store.discard(key)

    def exists(self, key):
        return 1 if key in self._store else 0


class TestSourceCapReached:
    def test_defaults_to_not_reached(self):
        svc = CancellationService(_FakeRedis())
        assert svc.is_source_cap_reached("job-1", "task-1") is False

    def test_mark_then_is_reached(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_source_cap_reached("job-1", "task-1")
        assert svc.is_source_cap_reached("job-1", "task-1") is True

    def test_scoped_to_the_specific_job_and_task(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_source_cap_reached("job-1", "task-1")
        assert svc.is_source_cap_reached("job-1", "task-2") is False
        assert svc.is_source_cap_reached("job-2", "task-1") is False

    def test_clear_removes_the_flag(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_source_cap_reached("job-1", "task-1")
        svc.clear_source_cap_reached("job-1", "task-1")
        assert svc.is_source_cap_reached("job-1", "task-1") is False


class TestShouldStopWithSourceTaskId:
    def test_false_when_nothing_is_flagged(self):
        svc = CancellationService(_FakeRedis())
        assert svc.should_stop("job-1", "task-1") is False

    def test_true_when_this_sources_cap_is_reached(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_source_cap_reached("job-1", "task-1")
        assert svc.should_stop("job-1", "task-1") is True

    def test_other_sources_in_the_same_job_are_unaffected(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_source_cap_reached("job-1", "task-1")
        assert svc.should_stop("job-1", "task-2") is False

    def test_job_wide_cancel_still_stops_every_source(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_cancelled("job-1")
        assert svc.should_stop("job-1", "task-1") is True
        assert svc.should_stop("job-1", "task-2") is True

    def test_no_source_task_id_only_checks_job_wide_flags(self):
        svc = CancellationService(_FakeRedis())
        svc.mark_source_cap_reached("job-1", "task-1")
        assert svc.should_stop("job-1") is False
