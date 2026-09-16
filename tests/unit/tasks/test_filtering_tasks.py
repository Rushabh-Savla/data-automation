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


class _FakeRedis:
    """set/decr/delete/publish surface used by the fan-out + finalize guard."""

    def __init__(self):
        self.store: dict[str, int | str] = {}

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    def decr(self, key):
        self.store[key] = int(self.store.get(key, 0)) - 1
        return self.store[key]

    def delete(self, key):
        self.store.pop(key, None)

    def publish(self, *args, **kwargs):
        pass


class TestBatchFailureDoesNotStrandTheJob:
    """Regression tests for the outage in job 565f5036.

    Filtering used to be dispatched as `chord(group)(finalize...)`. A chord's
    callback runs only if every member succeeds, so one batch raising an
    OperationalError became a ChordError, finalize never ran, and with it
    run_deduplication / trim_excess_assets / evaluate_job_completion never
    ran either — leaving the job in `running` with no path to a terminal
    state. These tests pin the two properties that replaced that: the batch
    never raises, and the continuation is always reached.
    """

    @staticmethod
    def _run_failing_batch(monkeypatch, fake_redis, finalize_calls, job_id, asset_ids):
        import app.tasks.filtering_tasks as ft

        class _CapturingFinalize:
            def delay(self, jid):
                finalize_calls.append(jid)

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB failure")

        monkeypatch.setattr(ft, "get_redis", lambda: fake_redis)
        monkeypatch.setattr(ft, "finalize_relevance_filter", _CapturingFinalize())
        monkeypatch.setattr(ft, "session_scope", _boom)
        return ft.relevance_filter_batch.run(job_id, asset_ids)

    def test_batch_never_raises_out_of_the_task(self, monkeypatch):
        import app.tasks.filtering_tasks as ft

        fake = _FakeRedis()
        ft.FanIn(fake).start("job-1", ft._FANIN_NAME, 1)
        # Must return a result dict rather than propagating — a raising task
        # is what Celery turned into a ChordError.
        result = self._run_failing_batch(monkeypatch, fake, [], "job-1", ["a"])
        assert result["error"] is not None

    def test_last_failing_batch_still_triggers_finalize(self, monkeypatch):
        import app.tasks.filtering_tasks as ft

        fake = _FakeRedis()
        calls: list[str] = []
        ft.FanIn(fake).start("job-1", ft._FANIN_NAME, 1)
        self._run_failing_batch(monkeypatch, fake, calls, "job-1", ["a"])
        assert calls == ["job-1"]

    def test_finalize_fires_exactly_once_when_every_batch_fails(self, monkeypatch):
        import app.tasks.filtering_tasks as ft

        fake = _FakeRedis()
        calls: list[str] = []
        ft.FanIn(fake).start("job-1", ft._FANIN_NAME, 2)
        self._run_failing_batch(monkeypatch, fake, calls, "job-1", ["a"])
        # First of two: not last one out yet, so no continuation.
        assert calls == []
        self._run_failing_batch(monkeypatch, fake, calls, "job-1", ["b"])
        assert calls == ["job-1"]
