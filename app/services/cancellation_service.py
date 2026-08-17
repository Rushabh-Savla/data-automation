"""CancellationService — the mechanism behind pause/cancel actually stopping
in-flight Celery work, not just flipping a DB column.

Every connector's per-item loop calls should_stop(job_id) every N iterations;
when true, the caller persists its checkpoint and raises JobPaused/JobCancelled
(app/core/exceptions.py), which the task wrapper treats as a clean stop, not
a failure. Cancel additionally tracks active Celery task IDs so they can be
revoke(terminate=True)'d — see app/tasks/orchestration_tasks.py.
"""

import redis

from app.core.exceptions import JobCancelled, JobPaused, JobTargetReached

_PAUSE_KEY = "job:{job_id}:paused"
_CANCEL_KEY = "job:{job_id}:cancelled"
_TARGET_REACHED_KEY = "job:{job_id}:target_reached"
_ACTIVE_TASKS_KEY = "job:{job_id}:active_task_ids"


class CancellationService:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def mark_paused(self, job_id: str) -> None:
        self.redis.set(_PAUSE_KEY.format(job_id=job_id), "1")

    def clear_paused(self, job_id: str) -> None:
        self.redis.delete(_PAUSE_KEY.format(job_id=job_id))

    def mark_cancelled(self, job_id: str) -> None:
        self.redis.set(_CANCEL_KEY.format(job_id=job_id), "1")

    def clear_cancelled(self, job_id: str) -> None:
        self.redis.delete(_CANCEL_KEY.format(job_id=job_id))

    def is_paused(self, job_id: str) -> bool:
        return bool(self.redis.exists(_PAUSE_KEY.format(job_id=job_id)))

    def is_cancelled(self, job_id: str) -> bool:
        return bool(self.redis.exists(_CANCEL_KEY.format(job_id=job_id)))

    def mark_target_reached(self, job_id: str) -> None:
        self.redis.set(_TARGET_REACHED_KEY.format(job_id=job_id), "1")

    def clear_target_reached(self, job_id: str) -> None:
        self.redis.delete(_TARGET_REACHED_KEY.format(job_id=job_id))

    def is_target_reached(self, job_id: str) -> bool:
        return bool(self.redis.exists(_TARGET_REACHED_KEY.format(job_id=job_id)))

    def should_stop(self, job_id: str) -> bool:
        return self.is_cancelled(job_id) or self.is_paused(job_id) or self.is_target_reached(job_id)

    def raise_if_stopped(self, job_id: str) -> None:
        if self.is_cancelled(job_id):
            raise JobCancelled(job_id)
        if self.is_paused(job_id):
            raise JobPaused(job_id)
        if self.is_target_reached(job_id):
            raise JobTargetReached(job_id)

    # -- Active-task tracking, used by cancel to revoke in-flight Celery tasks --

    def register_active_task(self, job_id: str, task_id: str) -> None:
        self.redis.sadd(_ACTIVE_TASKS_KEY.format(job_id=job_id), task_id)

    def unregister_active_task(self, job_id: str, task_id: str) -> None:
        self.redis.srem(_ACTIVE_TASKS_KEY.format(job_id=job_id), task_id)

    def list_active_tasks(self, job_id: str) -> list[str]:
        return list(self.redis.smembers(_ACTIVE_TASKS_KEY.format(job_id=job_id)))

    def clear_active_tasks(self, job_id: str) -> None:
        self.redis.delete(_ACTIVE_TASKS_KEY.format(job_id=job_id))
