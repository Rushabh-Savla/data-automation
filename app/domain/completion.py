"""JobCompletionEvaluator — decides whether a Job should stop, and why.

Evaluates the plan's three-way completion model: target_count reached,
deadline reached, or all sources exhausted before either limit was hit
(covers both "no count/no deadline, just run until done" and "count/deadline
given but sources ran dry first" -> completed_partial).
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from app.domain.enums import JobCompletionReason


@dataclass
class JobSnapshot:
    """Minimal view of a Job needed for the completion decision — decoupled
    from the ORM model so this is trivially unit-testable."""

    target_count: int | None
    deadline_at: datetime | None
    accepted_count: int
    all_sources_terminal: bool


class JobCompletionEvaluator:
    @staticmethod
    def should_stop(snapshot: JobSnapshot, now: datetime | None = None) -> JobCompletionReason | None:
        now = now or datetime.now(timezone.utc)

        if snapshot.target_count is not None and snapshot.accepted_count >= snapshot.target_count:
            return JobCompletionReason.TARGET_COUNT_REACHED

        if snapshot.deadline_at is not None and now >= snapshot.deadline_at:
            return JobCompletionReason.DEADLINE_REACHED

        if snapshot.all_sources_terminal:
            # Either no count/deadline was set (run-until-exhausted case), or
            # one was set but sources ran out before it was reached.
            return JobCompletionReason.SOURCES_EXHAUSTED

        return None

    @staticmethod
    def is_success(reason: JobCompletionReason, accepted_count: int, target_count: int | None = None) -> bool:
        """Whether the terminal status should be `completed` vs `completed_partial`/`failed`.

        SOURCES_EXHAUSTED with a target_count set means every source ran dry
        before target_count was reached — that's a genuine shortfall, not a
        success, even though *some* assets were delivered. Without this, a
        job that asked for 100 and delivered 60 once real supply ran out
        was reported as a plain `completed` success, silently masking that
        it under-delivered against what was actually requested."""
        if reason == JobCompletionReason.TARGET_COUNT_REACHED:
            return accepted_count > 0
        if reason == JobCompletionReason.SOURCES_EXHAUSTED:
            if target_count is not None:
                return accepted_count >= target_count > 0
            return accepted_count > 0
        return False
