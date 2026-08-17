from datetime import datetime, timedelta, timezone

import pytest

from app.domain.completion import JobCompletionEvaluator, JobSnapshot
from app.domain.enums import JobCompletionReason

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class TestJobCompletionEvaluator:
    def test_target_count_reached_takes_priority(self):
        # Even if the deadline also happens to have passed, count-reached is
        # checked first — it's the more specific/intentional stop condition.
        snapshot = JobSnapshot(
            target_count=10, deadline_at=NOW - timedelta(minutes=1), accepted_count=10, all_sources_terminal=False
        )
        assert JobCompletionEvaluator.should_stop(snapshot, now=NOW) == JobCompletionReason.TARGET_COUNT_REACHED

    def test_target_count_not_yet_reached_keeps_running(self):
        snapshot = JobSnapshot(target_count=10, deadline_at=None, accepted_count=9, all_sources_terminal=False)
        assert JobCompletionEvaluator.should_stop(snapshot, now=NOW) is None

    def test_deadline_reached_without_target_count(self):
        snapshot = JobSnapshot(
            target_count=None, deadline_at=NOW - timedelta(seconds=1), accepted_count=3, all_sources_terminal=False
        )
        assert JobCompletionEvaluator.should_stop(snapshot, now=NOW) == JobCompletionReason.DEADLINE_REACHED

    def test_deadline_not_yet_reached_keeps_running(self):
        snapshot = JobSnapshot(
            target_count=None, deadline_at=NOW + timedelta(hours=1), accepted_count=3, all_sources_terminal=False
        )
        assert JobCompletionEvaluator.should_stop(snapshot, now=NOW) is None

    def test_no_count_no_deadline_runs_until_sources_exhausted(self):
        snapshot = JobSnapshot(target_count=None, deadline_at=None, accepted_count=5, all_sources_terminal=False)
        assert JobCompletionEvaluator.should_stop(snapshot, now=NOW) is None

        exhausted = JobSnapshot(target_count=None, deadline_at=None, accepted_count=5, all_sources_terminal=True)
        assert JobCompletionEvaluator.should_stop(exhausted, now=NOW) == JobCompletionReason.SOURCES_EXHAUSTED

    def test_sources_exhausted_before_target_hit(self):
        # Count/deadline given but the sources ran dry first — still a
        # terminal state, just via the exhaustion path (completed_partial).
        snapshot = JobSnapshot(target_count=1000, deadline_at=None, accepted_count=12, all_sources_terminal=True)
        assert JobCompletionEvaluator.should_stop(snapshot, now=NOW) == JobCompletionReason.SOURCES_EXHAUSTED

    @pytest.mark.parametrize(
        "reason,accepted_count,expected",
        [
            (JobCompletionReason.TARGET_COUNT_REACHED, 10, True),
            (JobCompletionReason.SOURCES_EXHAUSTED, 5, True),
            (JobCompletionReason.SOURCES_EXHAUSTED, 0, False),
            (JobCompletionReason.DEADLINE_REACHED, 100, False),
            (JobCompletionReason.CANCELLED, 5, False),
        ],
    )
    def test_is_success(self, reason, accepted_count, expected):
        assert JobCompletionEvaluator.is_success(reason, accepted_count) is expected
