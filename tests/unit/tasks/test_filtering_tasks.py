from app.tasks.filtering_tasks import _chunk, _select_trim_candidates


class TestChunk:
    def test_splits_evenly(self):
        assert _chunk([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]

    def test_last_batch_can_be_smaller(self):
        assert _chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]

    def test_batch_size_larger_than_input_returns_one_batch(self):
        assert _chunk([1, 2, 3], 10) == [[1, 2, 3]]

    def test_empty_input_returns_no_batches(self):
        assert _chunk([], 5) == []

    def test_batch_size_of_zero_or_negative_does_not_infinite_loop(self):
        # treated as 1 — the smallest sane batch — rather than dividing by
        # zero or producing an empty/infinite range
        assert _chunk([1, 2, 3], 0) == [[1], [2], [3]]
        assert _chunk([1, 2, 3], -5) == [[1], [2], [3]]


class TestSelectTrimCandidates:
    def test_no_excess_selects_nothing(self):
        assert _select_trim_candidates([("a", 0.9), ("b", 0.5)], excess=0) == []
        assert _select_trim_candidates([("a", 0.9), ("b", 0.5)], excess=-1) == []

    def test_cuts_the_worst_scoring_first(self):
        assets = [("a", 0.9), ("b", 0.2), ("c", 0.6), ("d", 0.4)]
        assert _select_trim_candidates(assets, excess=2) == ["b", "d"]

    def test_none_scores_are_cut_before_any_scored_asset(self):
        assets = [("a", 0.9), ("b", None), ("c", 0.1)]
        # "b" (no score at all) is treated as the least confident match —
        # cut before "c", which at least has a (low) real score.
        assert _select_trim_candidates(assets, excess=1) == ["b"]

    def test_excess_larger_than_input_returns_everything(self):
        assets = [("a", 0.9), ("b", 0.5)]
        assert set(_select_trim_candidates(assets, excess=5)) == {"a", "b"}
