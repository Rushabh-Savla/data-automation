"""TopupPlanner — decides whether a job should go back for more data.

The problem this solves
-----------------------
Before this existed, a job could not reach its target_count except by luck.
Each source discovered once, downloaded that fixed batch and went terminal;
filtering then ran once and handed straight to completion evaluation, which
can only move a job to a terminal state. If filtering removed more than the
over-fetch buffer had anticipated, the job simply ended short and was
reported `completed_partial`. Nothing ever went back for more.

The single compensation was a fixed multiplier, target_overfetch_buffer =
1.5, applied to the raw target before downloading. On the job that motivated
this work that was badly miscalibrated: 754 assets were downloaded against a
target of 500, and filtering left 107. A 1.5x buffer cannot absorb a 7x
attrition rate, and no fixed constant can, because the attrition rate depends
on the query and the source -- which is only observable after a round has run.

The approach
------------
Measure the yield of the round that just completed and size the next one from
it. A query yielding 14% asks for ~7x its shortfall; a query yielding 90%
asks for ~1.1x. The buffer becomes a first-round prior rather than a
permanent guess.

This module is pure: it takes a snapshot and returns a plan, with no database,
Redis or Celery involvement, mirroring JobCompletionEvaluator in
app/domain/completion.py so the stopping rules can be unit-tested exhaustively.
Actually running a round is orchestration_tasks' job.

Bounding the loop
-----------------
A top-up loop that can run forever is worse than no loop: it burns GPU and
API quota on a query with no real supply. Four independent brakes, any one of
which stops it:

  * the target is met;
  * a hard round cap (topup_max_rounds);
  * every source reports itself exhausted;
  * diminishing returns -- a round that delivered almost nothing means the
    supply is dry regardless of what the sources claim.

The existing deadline sweep in watchdog_scan remains a fifth, independent of
this module entirely.
"""

import math
from dataclasses import dataclass, field

# Yield is clamped before it is used as a divisor. The floor is the important
# one: a round that accepted almost nothing would otherwise imply an enormous
# next round (shortfall / 0.01 = 100x). 0.05 caps the over-fetch at 20x, which
# is generous against the 7x seen in practice while still bounded.
_MIN_OBSERVED_YIELD = 0.05
_MAX_OBSERVED_YIELD = 1.0

# Asking for exactly the computed shortfall lands short every time, since the
# next round is filtered too. A modest safety factor on top absorbs the
# variance without inviting a runaway.
_SAFETY_FACTOR = 1.2

# Below this, another round is not worth its cost: the supply is effectively
# dry even if a connector has not said so.
_MIN_ABSOLUTE_GAIN = 5
_MIN_RELATIVE_GAIN = 0.05


@dataclass
class SourceState:
    """What one source contributed, and whether it has more to give."""

    source_type: str
    accepted: int = 0
    exhausted: bool = False


@dataclass
class TopupSnapshot:
    """Minimal view of a job's progress needed to decide on another round."""

    target_count: int | None
    # Post-filter survivors, NOT job.accepted_count. The distinction matters:
    # accepted_count is incremented optimistically at promotion time and only
    # corrected once filtering runs, so sizing a round off it would re-make
    # the original mistake of reacting to a pre-filter number.
    accepted_post_filter: int
    # Everything filtering considered this round; the denominator of the yield.
    promoted_total: int
    round_number: int
    max_rounds: int
    sources: list[SourceState] = field(default_factory=list)
    # Post-filter assets gained by the round that just finished. None on the
    # first evaluation, where there is no previous round to compare against.
    gained_last_round: int | None = None


@dataclass
class TopupPlan:
    """How much more to fetch, and from where."""

    round_number: int
    shortfall: int
    observed_yield: float
    # source_type -> how many raw assets that source should aim to download.
    quota: dict[str, int]

    @property
    def total_quota(self) -> int:
        return sum(self.quota.values())


class TopupPlanner:
    @staticmethod
    def observed_yield(snapshot: TopupSnapshot) -> float:
        """Fraction of downloaded assets that survived filtering.

        Falls back to 1.0 when nothing has been filtered yet, so the first
        round is sized off the caller's prior rather than off a division by
        zero.
        """
        if snapshot.promoted_total <= 0:
            return _MAX_OBSERVED_YIELD
        raw = snapshot.accepted_post_filter / snapshot.promoted_total
        return min(_MAX_OBSERVED_YIELD, max(_MIN_OBSERVED_YIELD, raw))

    @staticmethod
    def _is_diminishing(snapshot: TopupSnapshot, shortfall: int) -> bool:
        if snapshot.gained_last_round is None:
            return False
        floor = max(_MIN_ABSOLUTE_GAIN, int(_MIN_RELATIVE_GAIN * shortfall))
        return snapshot.gained_last_round < floor

    @classmethod
    def plan(cls, snapshot: TopupSnapshot) -> TopupPlan | None:
        """Returns the next round to run, or None if the job should stop.

        None means "stop and evaluate completion normally" -- the caller then
        falls through to JobCompletionEvaluator, which decides whether that is
        a success or a shortfall.
        """
        # No target means "collect what exists"; there is nothing to top up to.
        if snapshot.target_count is None:
            return None

        shortfall = snapshot.target_count - snapshot.accepted_post_filter
        if shortfall <= 0:
            return None

        if snapshot.round_number >= snapshot.max_rounds:
            return None

        available = [s for s in snapshot.sources if not s.exhausted]
        if not available:
            return None

        if cls._is_diminishing(snapshot, shortfall):
            return None

        observed = cls.observed_yield(snapshot)
        needed = math.ceil(shortfall / observed * _SAFETY_FACTOR)

        # Split evenly across the sources that still have supply. Even rather
        # than proportional-to-past-contribution on purpose: a source that
        # under-delivered last round may simply have been slow, and starving
        # it would make that permanent.
        per_source = math.ceil(needed / len(available))
        quota = {s.source_type: per_source for s in available}

        return TopupPlan(
            round_number=snapshot.round_number + 1,
            shortfall=shortfall,
            observed_yield=observed,
            quota=quota,
        )
