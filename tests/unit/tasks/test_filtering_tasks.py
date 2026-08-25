from app.tasks.filtering_tasks import _chunk


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
