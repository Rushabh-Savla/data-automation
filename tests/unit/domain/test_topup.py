"""Tests for TopupPlanner — the rules that let a job go back for more data,
and, just as importantly, the rules that stop it.

Mirrors tests/unit/domain/test_completion.py: pure snapshot in, decision out.
"""

from app.domain.topup import (
    _MIN_OBSERVED_YIELD,
    filtering_incomplete,
    SourceState,
    TopupPlanner,
    TopupSnapshot,
)


def _snapshot(**overrides) -> TopupSnapshot:
    base = dict(
        target_count=500,
        accepted_post_filter=100,
        promoted_total=700,
        round_number=1,
        max_rounds=5,
        sources=[SourceState("youtube")],
        gained_last_round=None,
    )
    base.update(overrides)
    return TopupSnapshot(**base)


class TestStopConditions:
    def test_no_target_means_nothing_to_top_up_to(self):
        assert TopupPlanner.plan(_snapshot(target_count=None)) is None

    def test_target_met_stops(self):
        assert TopupPlanner.plan(_snapshot(accepted_post_filter=500)) is None

    def test_target_exceeded_stops(self):
        assert TopupPlanner.plan(_snapshot(accepted_post_filter=600)) is None

    def test_round_cap_stops(self):
        assert TopupPlanner.plan(_snapshot(round_number=5, max_rounds=5)) is None

    def test_all_sources_exhausted_stops(self):
        snapshot = _snapshot(sources=[SourceState("youtube", exhausted=True)])
        assert TopupPlanner.plan(snapshot) is None

    def test_no_sources_at_all_stops(self):
        assert TopupPlanner.plan(_snapshot(sources=[])) is None

    def test_diminishing_returns_stops_even_when_sources_claim_supply(self):
        """A round that delivered almost nothing means the supply is dry.

        Connectors are optimistic about having more pages; this is the brake
        that does not trust them.
        """
        snapshot = _snapshot(gained_last_round=1)
        assert TopupPlanner.plan(snapshot) is None

    def test_a_good_round_does_not_trip_the_diminishing_brake(self):
        assert TopupPlanner.plan(_snapshot(gained_last_round=200)) is not None

    def test_first_evaluation_is_never_diminishing(self):
        # No previous round to compare against.
        assert TopupPlanner.plan(_snapshot(gained_last_round=None)) is not None


class TestSizing:
    def test_low_yield_asks_for_proportionally_more(self):
        """The whole point: a fixed 1.5x buffer cannot absorb 7x attrition.

        Reproduces the real job — 107 survivors from 754 downloaded against a
        target of 500 — and asserts the plan is sized off the measured yield
        rather than a constant.
        """
        snapshot = _snapshot(target_count=500, accepted_post_filter=107, promoted_total=754)
        plan = TopupPlanner.plan(snapshot)
        assert plan is not None
        assert plan.shortfall == 393
        assert 0.14 < plan.observed_yield < 0.15
        # Must exceed what the old fixed buffer would have fetched.
        assert plan.total_quota > 750

    def test_high_yield_asks_for_little_more(self):
        snapshot = _snapshot(target_count=500, accepted_post_filter=450, promoted_total=460)
        plan = TopupPlanner.plan(snapshot)
        assert plan is not None
        assert plan.total_quota < 200

    def test_yield_is_floored_so_a_dire_round_cannot_explode_the_next(self):
        # 1 survivor from 1000 would imply a 1000x next round without the clamp.
        # 1 of 400: below the sample size at which a trickle is judged hopeless (500), so it still
        # gets a follow-up round -- but sized from the 2% floor, not from 0.25%.
        snapshot = _snapshot(target_count=500, accepted_post_filter=1, promoted_total=400)
        plan = TopupPlanner.plan(snapshot)
        assert plan is not None
        assert plan.observed_yield >= _MIN_OBSERVED_YIELD
        assert plan.total_quota <= 500 / _MIN_OBSERVED_YIELD * 2

    def test_yield_is_capped_at_one(self):
        snapshot = _snapshot(accepted_post_filter=100, promoted_total=50)
        assert TopupPlanner.observed_yield(snapshot) == 1.0

    def test_nothing_filtered_yet_assumes_full_yield(self):
        assert TopupPlanner.observed_yield(_snapshot(promoted_total=0)) == 1.0

    def test_quota_splits_across_available_sources_only(self):
        snapshot = _snapshot(
            sources=[
                SourceState("youtube"),
                SourceState("web"),
                SourceState("kaggle", exhausted=True),
            ]
        )
        plan = TopupPlanner.plan(snapshot)
        assert plan is not None
        assert set(plan.quota) == {"youtube", "web"}

    def test_round_number_advances(self):
        plan = TopupPlanner.plan(_snapshot(round_number=2))
        assert plan is not None
        assert plan.round_number == 3


class TestSizingFromTheLastRound:
    """The next round is sized from the round that just finished, not the job average.

    Seen on a SUN RGB-D job for 200 images: its 93 labelled matches passed 84%,
    but once they ran out the source was scanned for images the model finds by
    itself, and only ~5% of those pass. The cumulative yield still read ~30%, so
    each round asked for a third of what it needed and the job ran out of rounds
    short of its target.
    """

    def test_a_measurable_last_round_sets_the_yield(self):
        # Job average is 27/300 = 9%, but the last round of 200 gained only 10: 5%.
        snapshot = _snapshot(
            target_count=200, accepted_post_filter=88, promoted_total=300,
            gained_last_round=10, promoted_last_round=200,
        )

        assert TopupPlanner.observed_yield(snapshot) == 10 / 200

    def test_the_regime_change_asks_for_the_full_amount(self):
        cumulative = _snapshot(target_count=200, accepted_post_filter=88, promoted_total=300)
        marginal = _snapshot(
            target_count=200, accepted_post_filter=88, promoted_total=300,
            gained_last_round=10, promoted_last_round=200,
        )

        old_plan = TopupPlanner.plan(cumulative)
        new_plan = TopupPlanner.plan(marginal)

        assert new_plan.total_quota > old_plan.total_quota * 2  # sized from the 5% round, not the 29% average

    def test_a_tiny_last_round_is_noise_so_the_job_average_is_used(self):
        snapshot = _snapshot(
            accepted_post_filter=100, promoted_total=700,
            gained_last_round=0, promoted_last_round=5,
        )

        assert TopupPlanner.observed_yield(snapshot) == 100 / 700

    def test_no_last_round_data_keeps_the_job_average(self):
        # Jobs that started before this was tracked, and the first evaluation.
        assert TopupPlanner.observed_yield(_snapshot(accepted_post_filter=107, promoted_total=754)) == 107 / 754

    def test_a_round_that_gained_nothing_is_floored_not_divided_by_zero(self):
        snapshot = _snapshot(gained_last_round=0, promoted_last_round=200)

        assert TopupPlanner.observed_yield(snapshot) == _MIN_OBSERVED_YIELD

    def test_a_round_that_lost_assets_to_dedup_is_floored(self):
        snapshot = _snapshot(gained_last_round=-3, promoted_last_round=200)

        assert TopupPlanner.observed_yield(snapshot) == _MIN_OBSERVED_YIELD

    def test_a_great_last_round_is_capped_at_one(self):
        snapshot = _snapshot(gained_last_round=250, promoted_last_round=200)

        assert TopupPlanner.observed_yield(snapshot) == 1.0


class TestScanYieldStillConverges:
    """SUN RGB-D scanned images passed 2.4% (5 of 206). With the floor at 5% each
    round asked for half of what it needed, and a 200-image job ran out of rounds
    near 185. The floor now sits below any yield worth chasing."""

    def test_a_2_4_percent_round_is_sized_from_its_real_yield(self):
        snapshot = _snapshot(
            target_count=200, accepted_post_filter=95, promoted_total=805,
            gained_last_round=12, promoted_last_round=506, round_number=2,
        )

        plan = TopupPlanner.plan(snapshot)

        # shortfall 105 at 12/506 = 2.4% -> ~105/0.0237*1.2 = ~5300, not the ~2500 the old floor gave
        assert plan.observed_yield == 12 / 506
        assert plan.total_quota > 5000

    def test_the_job_reaches_its_target_within_the_round_cap(self):
        # Simulate the loop: labelled matches, then rounds at a steady 2.4% scan yield.
        accepted, promoted = 83, 299
        gained, promoted_last = None, None
        for round_number in range(1, 5):
            plan = TopupPlanner.plan(
                _snapshot(
                    target_count=200, accepted_post_filter=accepted, promoted_total=promoted,
                    gained_last_round=gained, promoted_last_round=promoted_last, round_number=round_number,
                )
            )
            if plan is None:
                break
            fetched = min(plan.total_quota, 10_228 - (promoted - 93))  # the dataset is finite
            gain = round(fetched * 0.024)
            accepted, promoted = accepted + gain, promoted + fetched
            gained, promoted_last = gain, fetched

        assert accepted >= 200

    def test_a_dry_source_still_stops_at_the_floor(self):
        # Nothing accepted at all: capped at 50x the shortfall, not unbounded.
        snapshot = _snapshot(
            target_count=200, accepted_post_filter=100, promoted_total=400,
            gained_last_round=0, promoted_last_round=300, round_number=2,
        )

        assert TopupPlanner.plan(snapshot) is None  # zero gain trips the diminishing-returns brake


class TestSmallShortfallIsNotDiminishing:
    """With a few images to go, any progress is progress.

    A 350-image DAVIS job needed 5 more: near-duplicate frames make each round
    close only part of the gap. Requiring a gain of 5 read a round that delivered
    3 as "dry" and would have stopped the job a hair short of its target.
    """

    def _snap(self, accepted, gained):
        return _snapshot(target_count=350, accepted_post_filter=accepted, promoted_total=700, gained_last_round=gained)

    def test_a_small_gain_towards_a_small_shortfall_keeps_going(self):
        assert TopupPlanner.plan(self._snap(accepted=345, gained=3)) is not None

    def test_the_last_image_is_worth_chasing(self):
        assert TopupPlanner.plan(self._snap(accepted=349, gained=1)) is not None

    def test_a_round_that_gained_nothing_still_stops_however_small_the_shortfall(self):
        assert TopupPlanner.plan(self._snap(accepted=345, gained=0)) is None
        assert TopupPlanner.plan(self._snap(accepted=349, gained=0)) is None

    def test_a_large_shortfall_still_needs_a_meaningful_gain(self):
        # Unchanged: 400 to go, 1 gained is a dry source.
        assert TopupPlanner.plan(_snapshot(gained_last_round=1)) is None

    def test_the_floor_scales_with_a_big_shortfall(self):
        # 400 short -> a round must gain 5% (20); 19 is not enough, 20 is.
        assert TopupPlanner.plan(_snapshot(gained_last_round=19)) is None
        assert TopupPlanner.plan(_snapshot(gained_last_round=20)) is not None


class TestRoundCap:
    def test_the_default_allows_a_dedup_heavy_source_to_converge(self):
        from app.core.config import Settings

        assert Settings().topup_max_rounds >= 8

    def test_the_cap_still_stops_a_loop_that_keeps_making_progress(self):
        assert TopupPlanner.plan(_snapshot(round_number=10, max_rounds=10, gained_last_round=50)) is None


class TestTinyRoundsNeverEndAJobOneShort:
    """A 350-image DAVIS job stopped at 349. One image was missing, so the planner
    asked for 3 candidates; dedup and the filter discarded them, the round gained
    0, and the brake read that as a dry source."""

    def _snap(self, **kw):
        base = dict(target_count=350, accepted_post_filter=349, promoted_total=800, round_number=6, max_rounds=10)
        base.update(kw)
        return _snapshot(**base)

    def test_a_3_image_round_that_gained_nothing_is_not_evidence_of_a_dry_source(self):
        plan = TopupPlanner.plan(self._snap(gained_last_round=0, promoted_last_round=3))

        assert plan is not None

    def test_the_smallest_round_still_fetches_a_meaningful_sample(self):
        plan = TopupPlanner.plan(self._snap(gained_last_round=None, promoted_last_round=None))

        assert plan.total_quota >= 25

    def test_a_shortfall_of_one_is_not_sized_to_three_candidates(self):
        # observed yield 0.48 and 1 missing image used to ask for ceil(1/0.48*1.2) = 3
        plan = TopupPlanner.plan(self._snap(gained_last_round=1, promoted_last_round=40))

        assert plan.total_quota >= 25

    def test_a_round_that_found_nothing_new_at_all_still_stops(self):
        # Zero candidates fetched is a genuinely dry source, however small the shortfall.
        assert TopupPlanner.plan(self._snap(gained_last_round=0, promoted_last_round=0)) is None

    def test_a_measurable_round_that_gained_nothing_still_stops(self):
        # 30 candidates and not one survived: that is evidence.
        assert TopupPlanner.plan(self._snap(gained_last_round=0, promoted_last_round=30)) is None

    def test_tiny_rounds_cannot_loop_forever(self):
        # The round cap remains the backstop for a source that only ever dribbles.
        assert TopupPlanner.plan(self._snap(gained_last_round=0, promoted_last_round=3, round_number=10, max_rounds=10)) is None

    def test_quota_is_split_across_sources_but_never_below_the_minimum_in_total(self):
        plan = TopupPlanner.plan(
            self._snap(sources=[SourceState("davis"), SourceState("kitti")], gained_last_round=1, promoted_last_round=40)
        )

        assert plan.total_quota >= 25
        assert set(plan.quota) == {"davis", "kitti"}


class TestFilterOutageDoesNotBecomeADownloadExplosion:
    """The GPU briefly filled, the relevance model would not load, every batch of
    a 1854-image round errored and the images stayed undecided. The planner read
    that as 0 passes -> a 2% yield -> "short by 1000, fetch ~60000 more"."""

    def test_a_round_that_was_entirely_undecided_is_incomplete(self):
        assert filtering_incomplete(undecided=1854, total=1854) is True

    def test_most_of_a_round_undecided_is_incomplete(self):
        assert filtering_incomplete(undecided=400, total=1000) is True

    def test_a_few_permanently_undecidable_images_do_not_block_the_job(self):
        # A corrupt file can never be judged; it must not hold a job hostage forever.
        assert filtering_incomplete(undecided=1, total=500) is False
        assert filtering_incomplete(undecided=3, total=40) is False

    def test_the_allowance_scales_with_the_job(self):
        assert filtering_incomplete(undecided=40, total=1000) is False  # 4%
        assert filtering_incomplete(undecided=60, total=1000) is True  # 6%

    def test_nothing_undecided_is_complete(self):
        assert filtering_incomplete(undecided=0, total=0) is False
        assert filtering_incomplete(undecided=0, total=1854) is False


class TestHopelessQueriesStopEarly:
    """Any query is accepted now: a dataset that cannot supply one is scanned, and the
    relevance filter finds nothing. The job should finish short after a few hundred
    judgements, not plan a round of ~12,000."""

    def _snap(self, accepted, judged, **kw):
        base = dict(target_count=200, accepted_post_filter=accepted, promoted_total=judged, round_number=1, max_rounds=10)
        base.update(kw)
        return _snapshot(**base)

    def test_nothing_passes_after_a_meaningful_sample_stops_the_job(self):
        assert TopupPlanner.plan(self._snap(accepted=0, judged=600)) is None
        assert TopupPlanner.plan(self._snap(accepted=0, judged=300)) is None  # a target-100 job's whole first round

    def test_a_single_hit_in_hundreds_is_still_hopeless(self):
        assert TopupPlanner.plan(self._snap(accepted=1, judged=600)) is None

    def test_a_rare_but_real_concept_keeps_going(self):
        # 3 of 600 = 0.5%: rare, but there is supply to find.
        assert TopupPlanner.plan(self._snap(accepted=3, judged=600)) is not None

    def test_a_small_sample_proves_nothing(self):
        assert TopupPlanner.plan(self._snap(accepted=0, judged=150)) is not None

    def test_the_sample_boundaries(self):
        # No hits at all: 200 judged is enough.
        assert TopupPlanner.plan(self._snap(accepted=0, judged=199)) is not None
        assert TopupPlanner.plan(self._snap(accepted=0, judged=200)) is None
        # A trickle of hits: needs 500 to call it.
        assert TopupPlanner.plan(self._snap(accepted=1, judged=499)) is not None
        assert TopupPlanner.plan(self._snap(accepted=1, judged=500)) is None

    def test_a_healthy_yield_is_unaffected(self):
        assert TopupPlanner.plan(self._snap(accepted=300, judged=600, target_count=1000)) is not None

    def test_the_hopeless_check_uses_judged_assets_only(self):
        # promoted_total already excludes assets the filter could not decide (see the
        # planner's caller), so an outage cannot make a good query look hopeless.
        assert TopupPlanner.plan(self._snap(accepted=0, judged=0)) is not None


class TestNoEvidenceYetIsNotAnExplosion:
    """"guitar" on COCO: round 1 was capped at ~1.5x the target, judged 82 images and passed
    none. The 2% floor turned that into "short by 50 -> fetch ~3000 more". With no hits and
    too few judged to call it hopeless, fetch just enough to find out."""

    def _snap(self, judged, accepted=0, **kw):
        base = dict(target_count=50, accepted_post_filter=accepted, promoted_total=judged, round_number=1, max_rounds=10)
        base.update(kw)
        return _snapshot(**base)

    def test_a_zero_hit_round_is_followed_by_a_small_evidence_round(self):
        plan = TopupPlanner.plan(self._snap(judged=82))

        assert plan.total_quota == 200 - 82  # exactly enough to reach the 200-image verdict

    def test_the_evidence_round_is_never_tiny(self):
        plan = TopupPlanner.plan(self._snap(judged=190))

        assert plan.total_quota >= 25

    def test_the_round_after_it_ends_the_job_if_still_nothing(self):
        assert TopupPlanner.plan(self._snap(judged=200)) is None

    def test_once_something_passes_the_normal_sizing_returns(self):
        # 1 hit in 82 is a 1.2% yield: worth pursuing at full size, not just an evidence round.
        plan = TopupPlanner.plan(self._snap(judged=82, accepted=1))

        assert plan.total_quota > 200 - 82

    def test_it_never_asks_for_more_than_the_normal_sizing_would(self):
        # A target of 1 with a decent yield already asks for little; the cap must not raise it.
        plan = TopupPlanner.plan(self._snap(judged=10, accepted=0, target_count=1))

        assert plan.total_quota <= 200
