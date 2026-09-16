"""Recurring job schedules.

A job carrying `schedule_cron` is a TEMPLATE. It never runs itself; it sits in
SCHEDULED and, each time its cron fires, produces a child Job that does the
actual collecting.

Why templates and children rather than one re-running row: every per-run
concept on the Job model is singular -- accepted/rejected/duplicate counters,
started_at, completed_at, completion_reason -- and JobStateMachine has no
transition from a terminal status back to discovering. Making one row mean N
runs would mean rewriting all of that, and would overwrite the record of what
each run actually collected. Separate rows keep every run's history intact and
leave the state machine untouched.

The one-shot `scheduled_start_at` path is unaffected and still works; this is
additive.
"""

import logging
import uuid
from datetime import datetime, timezone

from app.db.models.job import Job

logger = logging.getLogger(__name__)

# Guards against a cron firing faster than a collection run can plausibly
# finish, which would pile up overlapping children until something falls over.
# Five minutes because a run involves discovery, downloads and model inference
# -- even a tiny job does not complete inside a minute, so "* * * * *" is
# always a mistake rather than a preference.
MIN_INTERVAL_S = 300


class InvalidCronError(ValueError):
    """Raised for a cron expression that cannot be parsed or fires too often."""


def validate_cron(expression: str, tz: str = "UTC") -> None:
    """Rejects an unparseable or too-frequent cron expression.

    Validated at creation time rather than at fire time: a bad expression that
    only surfaces inside the Beat sweep would fail silently in a background
    task, and the user would just never see their job run.
    """
    from croniter import croniter

    if not croniter.is_valid(expression):
        raise InvalidCronError(f"'{expression}' is not a valid cron expression")

    base = datetime.now(timezone.utc)
    itr = croniter(expression, base)
    first = itr.get_next(datetime)
    second = itr.get_next(datetime)
    if (second - first).total_seconds() < MIN_INTERVAL_S:
        raise InvalidCronError(
            f"'{expression}' fires more often than once per {MIN_INTERVAL_S}s, "
            "which would overlap collection runs"
        )


def next_fire_time(expression: str, tz: str = "UTC", after: datetime | None = None) -> datetime:
    """The next UTC instant this expression fires after `after` (default: now).

    The schedule is interpreted in `tz` so "every day at 02:00" means 02:00
    where the user is, but the returned value is always UTC because that is
    what the database column and every comparison use.
    """
    from croniter import croniter

    try:
        from zoneinfo import ZoneInfo

        zone = ZoneInfo(tz)
    except Exception:
        logger.warning(f"Unknown timezone {tz!r}, falling back to UTC")
        zone = timezone.utc

    base = (after or datetime.now(timezone.utc)).astimezone(zone)
    return croniter(expression, base).get_next(datetime).astimezone(timezone.utc)


def build_child_job(template: Job, now: datetime | None = None) -> Job:
    """A fresh run of `template`, ready to be inserted and started.

    Copies only what defines the *work* -- query, sources, target, config --
    and deliberately not the scheduling fields or any counter, so the child is
    an ordinary job indistinguishable from a manually created one. Notably
    `_topup` is stripped from the copied config: it is per-run orchestration
    state, and inheriting a previous run's round number would make the new run
    think it was already mid-loop.
    """
    now = now or datetime.now(timezone.utc)
    config = {k: v for k, v in (template.config or {}).items() if k != "_topup"}

    return Job(
        id=uuid.uuid4(),
        name=template.name,
        query=template.query,
        sources=list(template.sources or []),
        target_count=template.target_count,
        config=config,
        parent_job_id=template.id,
        # deadline_at is intentionally not copied: it is an absolute instant,
        # so reusing the template's would be already in the past for every run
        # after the first and would complete each child immediately.
    )
