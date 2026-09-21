"""Recognising work that a restart orphaned.

When the containers restart, every task that was running dies with them and
nothing tells the database. A source task keeps saying DOWNLOADING and a job
keeps saying RUNNING, but no process is doing the work. The watchdog's normal
stall timers (15 minutes for a source, 30 for the filtering chain) do eventually
notice, which is why jobs seemed to sit "paused" after a restart.

A restart leaves better evidence than silence. Each worker records when it
booted, and anything whose last sign of life predates that boot belongs to a
process that no longer exists. After a short grace period, so healthy work on a
worker that was NOT restarted has time to heartbeat again, it can be resumed
straight away.

Pure functions, mirroring app/domain/topup.py and completion.py: times in,
decision out, no database or Redis.
"""

from datetime import datetime, timedelta


def orphan_cutoff(boot_at: datetime | None, now: datetime, grace_s: int) -> datetime | None:
    """The boot time to compare heartbeats against, once it is safe to judge.

    None until `grace_s` has passed since the boot. Judging sooner would call
    healthy tasks on another worker orphaned, because they have not had a
    chance to heartbeat since the restart.
    """
    if boot_at is None:
        return None
    return boot_at if now >= boot_at + timedelta(seconds=grace_s) else None


def is_orphaned(last_seen: datetime | None, cutoff: datetime | None) -> bool:
    """True when the work last showed life before the cutoff (or never did)."""
    if cutoff is None:
        return False
    return last_seen is None or last_seen < cutoff
