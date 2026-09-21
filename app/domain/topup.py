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
# next round (shortfall / 0.01 = 100x). 0.02 caps the over-fetch at 50x.
#
# It was 0.05 (20x) and that was too tight for a source scanned for images the
# model finds by itself: SUN RGB-D's unlabelled images measured 2.4% (5 of 206),
# so every round asked for half of what it needed and the job spent its five
# rounds converging on ~185 of 200. Below 2% a source is not worth chasing, and
# hard_max on each connector's discovery limit still bounds any single round.
_MIN_OBSERVED_YIELD = 0.02
_MAX_OBSERVED_YIELD = 1.0

# Asking for exactly the computed shortfall lands short every time, since the
# next round is filtered too. A modest safety factor on top absorbs the
# variance without inviting a runaway.
_SAFETY_FACTOR = 1.2

# Below this, another round is not worth its cost: the supply is effectively
# dry even if a connector has not said so.
_MIN_ABSOLUTE_GAIN = 5
_MIN_RELATIVE_GAIN = 0.05

# The last round's own yield is only trusted as the size of the next round when
# it filtered at least this many assets; a handful of images is noise.
_MIN_ROUND_SAMPLE = 20

# Even one missing image asks for this many candidates. Dedup and the relevance
# filter each discard some, so a round sized to the shortfall ("1 short -> fetch
# 3") routinely nets zero, and a job that needed a single image stopped a single
# image short of its target.
_MIN_ROUND_QUOTA = 25

# A query with (almost) no supply in a source: after this many judged images, a yield
# below the threshold means the images are not there. Chasing it -- 50x the shortfall a
# round, with a scan that offers every image the dataset has -- would judge thousands of
# images to conclude what the first few hundred already showed, so the job finishes short
# instead. Two rules: not a single hit in 200 judged (a real concept at 1% or more would
# show one 87% of the time), or 1 in 500 (0.2%); 2 in 500 (0.4%, a rare but real concept)
# keeps going. The first-round size is 3x the target, at least 200, so a small job is
# judged on its first round.
_HOPELESS_ZERO_SAMPLE = 200
_HOPELESS_SAMPLE = 500
_HOPELESS_YIELD = 0.003

# Assets the filter could not decide that are tolerated before planning waits for it.
_MIN_UNDECIDED = 3
_MAX_UNDECIDED_FRACTION = 0.05


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
    # Assets that round put through filtering: the denominator of its own yield.
    promoted_last_round: int | None = None


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


def filtering_incomplete(undecided: int, total: int) -> bool:
    """Whether too much of the job is still unjudged for a yield to mean anything.

    An asset the relevance filter could not decide (its model would not load, the
    GPU was out of memory) stays PROMOTED with its file intact, on purpose: an
    error is not a rejection. But a planner that counts those as "did not pass"
    reads a filter outage as a 2% yield and asks for 50x the shortfall -- on a
    COCO job that was ~60,000 extra downloads, after the GPU briefly filled for
    ten seconds and every batch errored.

    So a round is only planned once nearly everything has been judged. The
    allowance covers the few images that are permanently undecidable (a corrupt
    file) and must not block a job forever.
    """
    return undecided > max(_MIN_UNDECIDED, int(_MAX_UNDECIDED_FRACTION * total))


class TopupPlanner:
    @staticmethod
    def observed_yield(snapshot: TopupSnapshot) -> float:
        """Fraction of downloaded assets that survived filtering.

        Sized from the round that just finished when it was big enough to
        measure, and from the whole job otherwise. The distinction matters when
        a source changes character part-way: a dataset's labelled matches pass
        ~85% of the time, but once those run out and the source is scanned for
        images the model can find on its own, ~5% do. Averaged over the whole
        job the yield still reads ~30%, so the next round asked for a third of
        what it needed and the job ran out of rounds short of its target.

        Falls back to 1.0 when nothing has been filtered yet, so the first
        round is sized off the caller's prior rather than off a division by
        zero.
        """
        if (
            snapshot.promoted_last_round is not None
            and snapshot.gained_last_round is not None
            and snapshot.promoted_last_round >= _MIN_ROUND_SAMPLE
        ):
            raw = snapshot.gained_last_round / snapshot.promoted_last_round
            return min(_MAX_OBSERVED_YIELD, max(_MIN_OBSERVED_YIELD, raw))
        if snapshot.promoted_total <= 0:
            return _MAX_OBSERVED_YIELD
        raw = snapshot.accepted_post_filter / snapshot.promoted_total
        return min(_MAX_OBSERVED_YIELD, max(_MIN_OBSERVED_YIELD, raw))

    @staticmethod
    def _is_hopeless(snapshot: TopupSnapshot) -> bool:
        """Enough images were judged and essentially none passed: the supply is not there."""
        if snapshot.accepted_post_filter <= 0 and snapshot.promoted_total >= _HOPELESS_ZERO_SAMPLE:
            return True
        if snapshot.promoted_total < _HOPELESS_SAMPLE:
            return False
        return snapshot.accepted_post_filter / snapshot.promoted_total < _HOPELESS_YIELD

    @staticmethod
    def _is_diminishing(snapshot: TopupSnapshot, shortfall: int) -> bool:
        if snapshot.gained_last_round is None:
            return False
        # A round that fetched a handful of images proves nothing about supply:
        # gaining 0 of 3 is noise, not a dry source. Only a round big enough to
        # measure, or one that found nothing new at all, can trip the brake.
        if snapshot.promoted_last_round is not None and 0 < snapshot.promoted_last_round < _MIN_ROUND_SAMPLE:
            return False
        floor = max(_MIN_ABSOLUTE_GAIN, int(_MIN_RELATIVE_GAIN * shortfall))
        # A round cannot be expected to gain more than half of what is left. With
        # 3 images to go, requiring a gain of 5 read a round that delivered 3 as
        # "dry" and stopped the job a hair short of its target; any progress
        # counts once the shortfall is that small.
        floor = min(floor, max(1, shortfall // 2))
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

        if cls._is_hopeless(snapshot):
            return None

        if cls._is_diminishing(snapshot, shortfall):
            return None

        observed = cls.observed_yield(snapshot)
        needed = max(_MIN_ROUND_QUOTA, math.ceil(shortfall / observed * _SAFETY_FACTOR))

        # Nothing has passed yet and too little was judged to call the query hopeless. The 2%
        # floor would ask for 50x the shortfall on what is really no information ("guitar" on
        # COCO: round 1 judged 82 and passed none, then asked for ~3000 more). Fetch only enough
        # to reach a verdict; if the next round still finds nothing, _is_hopeless ends the job.
        if snapshot.accepted_post_filter <= 0 and snapshot.promoted_total < _HOPELESS_ZERO_SAMPLE:
            needed = min(needed, max(_MIN_ROUND_QUOTA, _HOPELESS_ZERO_SAMPLE - snapshot.promoted_total))

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
