"""The worker-boot marker the watchdog reads (app/services/worker_boot.py)."""

from datetime import datetime, timezone

from app.services.worker_boot import last_worker_boot, record_worker_boot


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int | None] = {}

    def set(self, key, value, ex=None, nx=False):
        self.store[key] = value
        self.ttls[key] = ex
        return True

    def get(self, key):
        return self.store.get(key)


NOW = datetime(2026, 9, 21, 8, 27, 0, tzinfo=timezone.utc)


class TestWorkerBoot:
    def test_nothing_recorded_is_none(self):
        assert last_worker_boot(_FakeRedis()) is None

    def test_round_trips_the_boot_time(self):
        redis = _FakeRedis()

        record_worker_boot(redis, NOW)

        assert last_worker_boot(redis) == NOW

    def test_the_latest_boot_wins(self):
        redis = _FakeRedis()
        record_worker_boot(redis, NOW)
        later = datetime(2026, 9, 21, 8, 40, 0, tzinfo=timezone.utc)

        record_worker_boot(redis, later)

        assert last_worker_boot(redis) == later

    def test_the_marker_expires_so_a_dead_deployment_leaves_nothing_behind(self):
        redis = _FakeRedis()

        record_worker_boot(redis, NOW)

        assert all(ttl and ttl > 0 for ttl in redis.ttls.values())

    def test_a_corrupt_value_reads_as_no_boot_rather_than_crashing_the_watchdog(self):
        redis = _FakeRedis()
        redis.store["watchdog:last_boot_at"] = "not-a-number"

        assert last_worker_boot(redis) is None

    def test_defaults_to_now(self):
        redis = _FakeRedis()
        before = datetime.now(timezone.utc)

        record_worker_boot(redis)

        assert before <= last_worker_boot(redis) <= datetime.now(timezone.utc)
