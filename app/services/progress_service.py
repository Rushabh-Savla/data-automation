"""ProgressPublisher — writes a live progress snapshot to Redis (for REST
polling fallback) and publishes events on a pub/sub channel that the
WebSocket endpoint forwards to connected clients."""

import json

import redis

_SNAPSHOT_KEY = "job:{job_id}:progress"
_CHANNEL = "job:{job_id}:events"
_SNAPSHOT_TTL_S = 7 * 24 * 3600


class ProgressPublisher:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def publish(self, job_id: str, event: dict) -> None:
        """Publish a live event (progress delta, state transition, connector_finished, error)."""
        payload = json.dumps(event, default=str)
        self.redis.publish(_CHANNEL.format(job_id=job_id), payload)
        if event.get("type") == "progress_snapshot":
            self.redis.set(_SNAPSHOT_KEY.format(job_id=job_id), payload, ex=_SNAPSHOT_TTL_S)

    def get_snapshot(self, job_id: str) -> dict | None:
        raw = self.redis.get(_SNAPSHOT_KEY.format(job_id=job_id))
        if raw is None:
            return None
        return json.loads(raw)

    def channel_name(self, job_id: str) -> str:
        return _CHANNEL.format(job_id=job_id)
