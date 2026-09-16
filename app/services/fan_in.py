"""FanIn — a crash-tolerant replacement for Celery's chord callback.

A chord's callback runs only if *every* member task succeeds; one member
raising turns the whole thing into a ChordError and the callback never runs
at all. For relevance filtering that is a job-killing property: the fan-out
is one task per batch of assets, and a single batch hitting a transient DB
error meant finalize_relevance_filter never fired, so run_deduplication ->
trim_excess_assets -> evaluate_job_completion never fired either, and the
job sat in `running` forever with no path to a terminal state. That is
exactly how job 565f5036 stranded (see app/tasks/filtering_tasks.py).

FanIn inverts the responsibility: the dispatcher records how many batches
it created, and each batch decrements the counter from a `finally:` block --
so the decrement happens whether the batch succeeded, failed, or was killed
by a time limit. Whichever batch drives the counter to zero is the last one
out and calls the continuation itself. No task's failure can prevent the
continuation from running.

This is deliberately not folded into CancellationService: that service is
about boolean control flags (paused/cancelled/target-reached) whose keys are
read by connector loops, whereas this is a counting barrier with different
lifetime and semantics. Sharing a class would conflate the two.
"""

import logging

import redis

logger = logging.getLogger(__name__)

_COUNTER_KEY = "job:{job_id}:fanin:{name}"

# Long enough that a slow fan-out (CPU-only filtering measured up to ~3300s
# for one batch) can't have its counter expire mid-flight, but finite so a
# job that dies between start() and the last finish() can't leak the key
# forever.
_DEFAULT_TTL_S = 24 * 3600


class FanIn:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    @staticmethod
    def _key(job_id: str, name: str) -> str:
        return _COUNTER_KEY.format(job_id=job_id, name=name)

    def start(self, job_id: str, name: str, total: int, ttl_s: int = _DEFAULT_TTL_S) -> None:
        """Arms the barrier for `total` expected finish() calls.

        Must be called *before* the batches are dispatched, otherwise a fast
        batch could finish and decrement a key that doesn't exist yet.
        """
        self.redis.set(self._key(job_id, name), total, ex=ttl_s)

    def finish(self, job_id: str, name: str) -> int:
        """Records one participant as done; returns the number still outstanding.

        A return value of 0 means the caller is the last one out and owns the
        continuation. Negative values are possible if a task is retried after
        already decrementing (Celery redelivery); callers treat "<= 0" as
        last-one-out and guard the continuation itself for idempotency.
        """
        try:
            return int(self.redis.decr(self._key(job_id, name)))
        except Exception:
            logger.exception(f"FanIn.finish failed for job {job_id} / {name}")
            # Fail *open*: claim last-one-out so the continuation still runs.
            # A spurious extra continuation is recoverable (the callers are
            # idempotent); a skipped one strands the job, which is the bug
            # this class exists to prevent.
            return 0

    def clear(self, job_id: str, name: str) -> None:
        self.redis.delete(self._key(job_id, name))
