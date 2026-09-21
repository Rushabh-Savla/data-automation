"""Tests for recognising work a restart orphaned (app/domain/recovery.py).

After `docker restart`, source tasks kept saying DOWNLOADING and jobs kept saying
RUNNING with nothing behind them, and the watchdog's 15/30 minute stall timers
were the only thing that eventually noticed.
"""

from datetime import datetime, timedelta, timezone

from app.domain.recovery import is_orphaned, orphan_cutoff

BOOT = datetime(2026, 9, 21, 8, 27, 0, tzinfo=timezone.utc)
GRACE = 120


def _at(seconds_after_boot: float) -> datetime:
    return BOOT + timedelta(seconds=seconds_after_boot)


class TestOrphanCutoff:
    def test_no_boot_recorded_means_no_judgement(self):
        assert orphan_cutoff(None, _at(10_000), GRACE) is None

    def test_not_judged_during_the_grace_period(self):
        # Work on a worker that was NOT restarted has not yet had a chance to
        # heartbeat since the boot; calling it orphaned would resume live tasks.
        assert orphan_cutoff(BOOT, _at(GRACE - 1), GRACE) is None

    def test_judged_once_the_grace_period_has_passed(self):
        assert orphan_cutoff(BOOT, _at(GRACE), GRACE) == BOOT
        assert orphan_cutoff(BOOT, _at(GRACE + 600), GRACE) == BOOT


class TestIsOrphaned:
    def test_last_seen_before_the_boot_is_orphaned(self):
        assert is_orphaned(_at(-5), BOOT) is True

    def test_seen_since_the_boot_is_alive(self):
        # e.g. a shard on the worker that kept running, heartbeating after the restart.
        assert is_orphaned(_at(30), BOOT) is False

    def test_seen_exactly_at_the_boot_is_alive(self):
        assert is_orphaned(BOOT, BOOT) is False

    def test_never_seen_is_orphaned(self):
        assert is_orphaned(None, BOOT) is True

    def test_nothing_is_orphaned_without_a_cutoff(self):
        assert is_orphaned(_at(-3600), None) is False
        assert is_orphaned(None, None) is False


class TestTheRestartScenario:
    """The sequence that actually happened, end to end in miniature."""

    def test_work_killed_in_a_restart_is_resumed_after_two_minutes_not_thirty(self):
        last_heartbeat = _at(-40)  # heartbeat 40s before the containers went down
        now = _at(GRACE + 5)  # a couple of minutes after they came back

        cutoff = orphan_cutoff(BOOT, now, GRACE)

        assert is_orphaned(last_heartbeat, cutoff) is True

    def test_work_that_resumed_on_its_own_is_left_alone(self):
        # A redelivered task that heartbeated after the boot must not be resumed twice.
        now = _at(GRACE + 5)

        assert is_orphaned(_at(60), orphan_cutoff(BOOT, now, GRACE)) is False

    def test_a_recovered_task_is_no_longer_orphaned_once_resumed(self):
        # _resume_task re-runs discovery, which heartbeats: the watchdog must not resume it again.
        now = _at(GRACE + 65)
        resumed_heartbeat = _at(GRACE + 5)

        assert is_orphaned(resumed_heartbeat, orphan_cutoff(BOOT, now, GRACE)) is False
