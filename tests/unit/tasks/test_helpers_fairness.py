"""Tests for the per-source fair-share cap math (app/tasks/_helpers.py) —
the fix for one fast source (e.g. one whose data was already cached)
single-handedly consuming a job's whole target_count before the other
selected sources get a single item accepted."""

from app.tasks._helpers import per_source_cap


class TestPerSourceCap:
    def test_splits_target_across_sources_with_buffer(self):
        # 8 sources, target 50 -> ceil(50/8 * 2) = 13
        assert per_source_cap(target_count=50, num_sources=8) == 13

    def test_never_goes_below_the_minimum_floor(self):
        # A tiny target spread across many sources would otherwise round to
        # ~1-2 per source, starving every one of them.
        assert per_source_cap(target_count=5, num_sources=10) == 10

    def test_single_source_gets_the_full_buffered_target(self):
        assert per_source_cap(target_count=50, num_sources=1) == 100

    def test_zero_or_negative_source_count_does_not_divide_by_zero(self):
        assert per_source_cap(target_count=50, num_sources=0) == per_source_cap(50, 1)
