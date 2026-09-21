"""topup_limit: a discovery round must offer at least as many new candidates as
the planner asked for, or it cannot deliver the quota."""

from types import SimpleNamespace

from app.domain.enums import SourceType
from app.tasks.discovery_tasks import topup_limit


def _job(quota=None):
    return SimpleNamespace(config={"_topup": {"quota": quota}} if quota is not None else {})


class TestTopupLimit:
    def test_first_round_is_unchanged(self):
        assert topup_limit(600, _job(), SourceType.SUN_RGBD, already_seen=0, hard_max=50_000) == 600

    def test_leaves_room_for_the_quota_on_top_of_what_was_already_fetched(self):
        job = _job({"sun_rgbd": 2700})

        assert topup_limit(1200, job, SourceType.SUN_RGBD, already_seen=300, hard_max=50_000) == 3000

    def test_never_shrinks_below_the_scaled_limit(self):
        job = _job({"sun_rgbd": 10})

        assert topup_limit(1800, job, SourceType.SUN_RGBD, already_seen=300, hard_max=50_000) == 1800

    def test_uses_only_this_sources_quota(self):
        job = _job({"kitti": 9000})

        assert topup_limit(600, job, SourceType.SUN_RGBD, already_seen=0, hard_max=50_000) == 600

    def test_is_bounded_by_hard_max(self):
        job = _job({"sun_rgbd": 99_999})

        assert topup_limit(600, job, SourceType.SUN_RGBD, already_seen=0, hard_max=50_000) == 50_000

    def test_a_job_with_no_topup_state_or_an_empty_quota(self):
        assert topup_limit(600, SimpleNamespace(config=None), SourceType.SUN_RGBD, already_seen=5, hard_max=50_000) == 600
        assert topup_limit(600, _job({}), SourceType.SUN_RGBD, already_seen=5, hard_max=50_000) == 600
