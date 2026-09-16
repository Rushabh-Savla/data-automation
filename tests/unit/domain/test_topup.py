"""Tests for TopupPlanner — the rules that let a job go back for more data,
and, just as importantly, the rules that stop it.

Mirrors tests/unit/domain/test_completion.py: pure snapshot in, decision out.
"""

from app.domain.topup import SourceState, TopupPlanner, TopupSnapshot


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
        snapshot = _snapshot(target_count=500, accepted_post_filter=1, promoted_total=1000)
        plan = TopupPlanner.plan(snapshot)
        assert plan is not None
        assert plan.observed_yield >= 0.05
        assert plan.total_quota <= 500 / 0.05 * 2

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
