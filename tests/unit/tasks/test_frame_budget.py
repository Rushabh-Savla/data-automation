"""Per-video frame cap.

At fps=1 with no cap, one video contributes a frame per second of runtime: a
real job saw 178 frames from a single video, which exhausted the download
budget across only 7 of 16 discovered videos while producing near-identical
consecutive frames. The cap trades depth in one video for breadth across many,
which is what a training set actually needs.
"""

from app.tasks.ingest_tasks import _default_frames_per_video


class TestDefaultFramesPerVideo:
    def test_unknown_target_gets_a_bounded_default(self):
        # Must never be None/unbounded — that is the original bug.
        assert _default_frames_per_video(None) == 60
        assert _default_frames_per_video(0) == 60

    def test_small_target_keeps_a_usable_floor(self):
        # A tiny cap would starve a small job of any coverage per video.
        assert _default_frames_per_video(40) >= 30

    def test_scales_with_target(self):
        assert _default_frames_per_video(5000) > _default_frames_per_video(500)

    def test_is_capped_so_one_video_cannot_dominate(self):
        assert _default_frames_per_video(1_000_000) <= 300

    def test_no_single_video_supplies_the_whole_target(self):
        for target in (100, 500, 2000):
            assert _default_frames_per_video(target) < target
