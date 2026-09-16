"""Recurring schedule rules.

Scheduling used to be one-shot: a single scheduled_start_at that fired once
and then meant nothing. These cover the cron layer that replaced it.
"""

from datetime import datetime, timedelta, timezone

import pytest

from app.services.schedule_service import (
    InvalidCronError,
    build_child_job,
    next_fire_time,
    validate_cron,
)


class _Template:
    """Stand-in for a Job template; build_child_job only reads attributes."""

    def __init__(self, **kw):
        self.id = kw.get("id", "template-id")
        self.name = kw.get("name", "nightly weapons")
        self.query = kw.get("query", "weapon in movie scene")
        self.sources = kw.get("sources", ["youtube"])
        self.target_count = kw.get("target_count", 500)
        self.config = kw.get("config", {})
        self.deadline_at = kw.get("deadline_at")


class TestValidateCron:
    @pytest.mark.parametrize("expr", ["0 2 * * *", "*/5 * * * *", "30 3 * * 1", "0 0 1 * *"])
    def test_accepts_reasonable_schedules(self, expr):
        validate_cron(expr)

    @pytest.mark.parametrize("expr", ["garbage", "", "99 99 * * *"])
    def test_rejects_unparseable(self, expr):
        with pytest.raises(InvalidCronError):
            validate_cron(expr)

    @pytest.mark.parametrize("expr", ["* * * * *", "*/2 * * * *"])
    def test_rejects_schedules_that_would_overlap_runs(self, expr):
        """A collection run involves discovery, downloads and inference; it
        cannot finish inside a minute, so firing that often just piles up
        overlapping children."""
        with pytest.raises(InvalidCronError):
            validate_cron(expr)


class TestNextFireTime:
    def test_returns_utc(self):
        assert next_fire_time("0 2 * * *").tzinfo is not None
        assert next_fire_time("0 2 * * *").utcoffset() == timedelta(0)

    def test_is_in_the_future(self):
        now = datetime.now(timezone.utc)
        assert next_fire_time("0 2 * * *", after=now) > now

    def test_respects_the_schedule_timezone(self):
        """'Every day at 02:00' should mean 02:00 where the user is."""
        after = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        utc = next_fire_time("0 2 * * *", "UTC", after=after)
        tokyo = next_fire_time("0 2 * * *", "Asia/Tokyo", after=after)
        assert utc != tokyo

    def test_unknown_timezone_falls_back_instead_of_crashing(self):
        # A bad tz must not take down the Beat sweep for every other template.
        assert next_fire_time("0 2 * * *", "Not/AZone") is not None


class TestBuildChildJob:
    def test_copies_the_work_definition(self):
        template = _Template()
        child = build_child_job(template)
        assert child.query == template.query
        assert child.sources == template.sources
        assert child.target_count == template.target_count

    def test_links_back_to_its_template(self):
        template = _Template()
        assert build_child_job(template).parent_job_id == template.id

    def test_child_is_not_itself_a_template(self):
        # Otherwise every run would start spawning its own runs.
        child = build_child_job(_Template())
        assert getattr(child, "schedule_cron", None) is None

    def test_strips_per_run_topup_state(self):
        """_topup is orchestration state for one run.

        Inheriting a previous run's round number would make a fresh run think
        it was already mid-loop, and its first evaluation would compare against
        a stale accepted count.
        """
        template = _Template(config={"min_resolution": [320, 240], "_topup": {"round": 3}})
        child = build_child_job(template)
        assert "_topup" not in child.config
        assert child.config["min_resolution"] == [320, 240]

    def test_does_not_copy_the_deadline(self):
        """deadline_at is an absolute instant.

        Reusing the template's would already be in the past for every run after
        the first, completing each child immediately.
        """
        template = _Template(deadline_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
        assert build_child_job(template).deadline_at is None

    def test_each_run_is_a_distinct_row(self):
        template = _Template()
        assert build_child_job(template).id != build_child_job(template).id

    def test_starts_with_clean_counters(self):
        child = build_child_job(_Template())
        # Defaults come from the model; what matters is nothing was inherited.
        assert getattr(child, "accepted_count", 0) in (0, None)
