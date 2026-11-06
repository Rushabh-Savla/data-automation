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
_SOURCE_CAP_REACHED_KEY = "job:{job_id}:source:{source_task_id}:cap_reached"


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

    # -- Per-source fair-share cap: lets one source's own connector loop stop
    # itself (so other sources keep running) without touching the job-wide
    # cancel/pause/target flags above. See app/tasks/_helpers.py::build_on_progress
    # for where this gets set, and the bug it fixes: a single fast source
    # (e.g. one whose data was already cached) reaching the job's whole
    # target_count on its own used to flip should_stop() for *every* source,
    # starving the others of any contribution at all. --

    def mark_source_cap_reached(self, job_id: str, source_task_id: str) -> None:
        self.redis.set(_SOURCE_CAP_REACHED_KEY.format(job_id=job_id, source_task_id=source_task_id), "1")

    def clear_source_cap_reached(self, job_id: str, source_task_id: str) -> None:
        self.redis.delete(_SOURCE_CAP_REACHED_KEY.format(job_id=job_id, source_task_id=source_task_id))

    def is_source_cap_reached(self, job_id: str, source_task_id: str) -> bool:
        return bool(self.redis.exists(_SOURCE_CAP_REACHED_KEY.format(job_id=job_id, source_task_id=source_task_id)))

    def should_stop(self, job_id: str, source_task_id: str | None = None) -> bool:
        if self.is_cancelled(job_id) or self.is_paused(job_id) or self.is_target_reached(job_id):
            return True
        if source_task_id is not None and self.is_source_cap_reached(job_id, source_task_id):
            return True
        return False

    def raise_if_stopped(self, job_id: str, source_task_id: str | None = None) -> None:
        if self.is_cancelled(job_id):
            raise JobCancelled(job_id)
        if self.is_paused(job_id):
            raise JobPaused(job_id)
        if self.is_target_reached(job_id):
            raise JobTargetReached(job_id)
        if source_task_id is not None and self.is_source_cap_reached(job_id, source_task_id):
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

    def revoke_active_tasks(self, job_id: str, exclude_task_ids: set[str] | None = None) -> list[str]:
        """Revokes all actively tracked Celery tasks for this job with SIGTERM, purges any pending
        unconsumed tasks from the broker queues, and clears the active task list."""
        import logging
        from app.tasks.celery_app import celery_app

        exclude = exclude_task_ids or set()
        task_ids = [t for t in self.list_active_tasks(job_id) if t not in exclude]
        for task_id in task_ids:
            try:
                celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to revoke task {task_id}: {e}")
        self.clear_active_tasks(job_id)
        try:
            self.purge_queued_tasks_for_job(job_id)
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to purge queued tasks for job {job_id}: {e}")
        return task_ids

    def purge_queued_tasks_for_job(self, job_id: str, queue_names: list[str] | None = None) -> int:
        """Purge all pending unconsumed tasks for job_id from Celery Redis queue lists.
        Prevents broker queue pollution and worker starvation across job runs."""
        import base64
        import json
        import logging

        target_job_str = str(job_id)
        queues = queue_names or ["download", "discovery", "filtering"]
        total_purged = 0

        for q in queues:
            try:
                length = self.redis.llen(q)
                if not length:
                    continue

                staging_key = f"{q}:staging_purge_{target_job_str[:8]}"
                try:
                    self.redis.rename(q, staging_key)
                except Exception:
                    continue

                staged_len = self.redis.llen(staging_key)
                batch_size = 2000
                purged_in_q = 0

                for i in range(0, staged_len, batch_size):
                    chunk = self.redis.lrange(staging_key, i, i + batch_size - 1)
                    to_keep = []
                    for raw in chunk:
                        drop = False
                        try:
                            data = json.loads(raw)
                            headers = data.get("headers", {})
                            if headers.get("root_id") == target_job_str:
                                drop = True
                            else:
                                raw_body = data.get("body")
                                if raw_body:
                                    decoded_body = base64.b64decode(raw_body).decode("utf-8")
                                    body_json = json.loads(decoded_body)
                                    args = body_json[0] if isinstance(body_json, list) and body_json else []
                                    if args and str(args[0]) == target_job_str:
                                        drop = True
                        except Exception:
                            drop = False

                        if drop:
                            purged_in_q += 1
                        else:
                            to_keep.append(raw)

                    if to_keep:
                        self.redis.rpush(q, *to_keep)

                self.redis.delete(staging_key)
                total_purged += purged_in_q
                if purged_in_q > 0:
                    logging.getLogger(__name__).info(
                        f"Purged {purged_in_q} stale queued tasks for job {target_job_str} from queue '{q}'"
                    )
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"Failed to purge queue '{q}' for job {target_job_str}: {e}"
                )

        return total_purged
