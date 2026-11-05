"""Tests for CancellationService's per-source fair-share cap flag — added
alongside the existing job-wide pause/cancel/target-reached flags so one
source can stop itself without touching should_stop() for every other
source in the same job."""

from app.services.cancellation_service import CancellationService


class _FakeRedis:
    """Minimal in-memory stand-in for the handful of redis-py calls
    CancellationService makes (set/delete/exists/sets) — avoids pulling in a new
    test dependency (fakeredis) for what's a tiny surface."""

    def __init__(self):
        self._store: set[str] = set()
        self._sets: dict[str, set[str]] = {}

    def set(self, key, value, **kwargs):
        self._store.add(key)

    def delete(self, key):
        self._store.discard(key)
        self._sets.pop(key, None)

    def exists(self, key):
        return 1 if (key in self._store or key in self._sets) else 0

    def sadd(self, key, member):
        self._sets.setdefault(key, set()).add(member)

    def srem(self, key, member):
        if key in self._sets:
            self._sets[key].discard(member)

    def smembers(self, key):
        return set(self._sets.get(key, set()))


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


class TestRevokeActiveTasks:
    def test_revoke_active_tasks_calls_celery_and_clears(self, monkeypatch):
        revoked_ids = []

        class _FakeControl:
            def revoke(self, task_id, terminate=True, signal="SIGTERM"):
                revoked_ids.append((task_id, terminate, signal))

        class _FakeCelery:
            control = _FakeControl()

        monkeypatch.setattr("app.tasks.celery_app.celery_app", _FakeCelery())

        svc = CancellationService(_FakeRedis())
        svc.register_active_task("job-1", "task-a")
        svc.register_active_task("job-1", "task-b")

        assert len(svc.list_active_tasks("job-1")) == 2
        revoked = svc.revoke_active_tasks("job-1")

        assert set(revoked) == {"task-a", "task-b"}
        assert len(revoked_ids) == 2
        assert all(t[1] is True and t[2] == "SIGTERM" for t in revoked_ids)
        assert len(svc.list_active_tasks("job-1")) == 0
