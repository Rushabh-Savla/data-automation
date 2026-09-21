"""A worker records its boot for restart recovery, and can never fail to start over it."""

from app.tasks import celery_app as celery_mod


class TestBootSignal:
    def test_records_the_boot_in_redis(self, monkeypatch):
        recorded = []
        monkeypatch.setattr("app.core.redis_client.get_redis", lambda: "redis-handle")
        monkeypatch.setattr("app.services.worker_boot.record_worker_boot", lambda r: recorded.append(r))

        celery_mod._record_boot_for_restart_recovery()

        assert recorded == ["redis-handle"]

    def test_a_redis_outage_at_startup_does_not_stop_the_worker(self, monkeypatch):
        def boom():
            raise ConnectionError("redis is down")

        monkeypatch.setattr("app.core.redis_client.get_redis", boom)

        celery_mod._record_boot_for_restart_recovery()  # must not raise

    def test_it_is_connected_to_the_worker_ready_signal(self):
        from celery.signals import worker_ready

        receivers = [r() if callable(r) else r for _, r in worker_ready.receivers]

        assert celery_mod._record_boot_for_restart_recovery in receivers
