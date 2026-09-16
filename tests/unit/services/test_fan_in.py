"""Tests for FanIn — the counting barrier that replaced the Celery chord in
relevance filtering.

The property under test throughout is the one the chord did not have: the
continuation must be reachable even when some participants fail. A chord's
callback runs only if every member succeeds, which is how a single failing
relevance_filter_batch stranded a job in `running` forever.
"""

from app.services.fan_in import FanIn


class _FakeRedis:
    """Minimal in-memory stand-in for the set/decr/delete calls FanIn makes,
    matching the _FakeRedis style already used in test_cancellation_service."""

    def __init__(self):
        self._store: dict[str, int] = {}

    def set(self, key, value, ex=None):
        self._store[key] = int(value)

    def decr(self, key):
        # redis DECR treats a missing key as 0, so it becomes -1.
        self._store[key] = self._store.get(key, 0) - 1
        return self._store[key]

    def delete(self, key):
        self._store.pop(key, None)


class _ExplodingRedis(_FakeRedis):
    def decr(self, key):
        raise ConnectionError("redis is down")


class TestFanIn:
    def test_last_participant_sees_zero(self):
        fan = FanIn(_FakeRedis())
        fan.start("job-1", "relevance_filter", 3)
        assert fan.finish("job-1", "relevance_filter") == 2
        assert fan.finish("job-1", "relevance_filter") == 1
        assert fan.finish("job-1", "relevance_filter") == 0

    def test_single_participant_is_immediately_last(self):
        fan = FanIn(_FakeRedis())
        fan.start("job-1", "relevance_filter", 1)
        assert fan.finish("job-1", "relevance_filter") == 0

    def test_barriers_are_isolated_per_job(self):
        fan = FanIn(_FakeRedis())
        fan.start("job-1", "relevance_filter", 2)
        fan.start("job-2", "relevance_filter", 2)
        fan.finish("job-1", "relevance_filter")
        # job-2 is untouched by job-1's progress
        assert fan.finish("job-2", "relevance_filter") == 1

    def test_barriers_are_isolated_per_name(self):
        fan = FanIn(_FakeRedis())
        fan.start("job-1", "alpha", 2)
        fan.start("job-1", "beta", 2)
        fan.finish("job-1", "alpha")
        assert fan.finish("job-1", "beta") == 1

    def test_extra_finishes_go_negative_not_positive(self):
        """A redelivered task decrementing twice must not push the counter
        back above zero — callers treat "<= 0" as last-one-out, so a
        negative value must keep meaning "done", never "still waiting"."""
        fan = FanIn(_FakeRedis())
        fan.start("job-1", "relevance_filter", 1)
        assert fan.finish("job-1", "relevance_filter") == 0
        assert fan.finish("job-1", "relevance_filter") <= 0

    def test_finish_on_a_cleared_barrier_still_reports_done(self):
        fan = FanIn(_FakeRedis())
        fan.start("job-1", "relevance_filter", 1)
        fan.clear("job-1", "relevance_filter")
        # Missing key must not read as "many outstanding" and strand the job.
        assert fan.finish("job-1", "relevance_filter") <= 0

    def test_redis_failure_fails_open_to_last_one_out(self):
        """If the barrier itself is unreachable we must claim last-one-out.

        A spurious extra continuation is recoverable (finalize is guarded and
        dedup re-queries state); a *skipped* continuation strands the job,
        which is the entire failure mode this class exists to prevent."""
        fan = FanIn(_ExplodingRedis())
        assert fan.finish("job-1", "relevance_filter") == 0

    def test_clear_is_idempotent(self):
        fan = FanIn(_FakeRedis())
        fan.clear("job-1", "relevance_filter")
        fan.clear("job-1", "relevance_filter")
