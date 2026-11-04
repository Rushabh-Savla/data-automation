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
    """set/decr/delete/publish surface used by the fan-out + finalize guard.
    Also supports rpush/expire/lrange for the deferred-offload accumulation."""

    def __init__(self):
        self.store: dict[str, int | str] = {}
        self.lists: dict[str, list[str]] = {}

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
        self.lists.pop(key, None)

    def publish(self, *args, **kwargs):
        pass

    def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(values)
        return len(self.lists[key])

    def expire(self, key, seconds):
        pass  # TTL bookkeeping not needed in unit tests

    def lrange(self, key, start, end):
        items = self.lists.get(key, [])
        return [v.encode() if isinstance(v, str) else v for v in items]


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


class TestReactiveTrimAfterTerminalJob:
    """A batch can still write a RELEVANCE_PASSED verdict after its job has
    already gone terminal: trim_excess_assets (this module) only ever runs
    once, from plan_topup_round's final round, sized off whatever was
    already RELEVANCE_PASSED at that moment -- a straggler from the same
    round that was still mid-classification then is invisible to it.
    Regression: a target_count=11 job settled at 14 instead of 11. These
    pin that a late PASS re-triggers trim_excess_assets only when the job
    is already terminal, never while it's still actively running (trimming
    mid-round would destroy the margin the next round is trying to build —
    see run_deduplication's docstring)."""

    @staticmethod
    def _run_batch(monkeypatch, job_status, outcome):
        import app.tasks.filtering_tasks as ft
        from contextlib import contextmanager
        from types import SimpleNamespace

        asset = SimpleNamespace(status=ft.AssetStatus.PROMOTED, nas_path="/nas/x.jpg")
        job = SimpleNamespace(status=job_status, query="cars")

        class _FakeAssetRepo:
            def __init__(self, db):
                pass

            def get(self, asset_id):
                return asset

            def set_status(self, asset_id, status, relevance_score=None, is_duplicate_of=None):
                asset.status = status

        class _FakeJobRepo:
            def __init__(self, db):
                pass

            def get(self, job_id):
                return job

            def increment_counts(self, *args, **kwargs):
                pass

        @contextmanager
        def fake_session_scope():
            yield object()

        trim_calls: list[str] = []
        fake_redis = _FakeRedis()
        monkeypatch.setattr(ft, "session_scope", fake_session_scope)
        monkeypatch.setattr(ft, "AssetRepository", _FakeAssetRepo)
        monkeypatch.setattr(ft, "JobRepository", _FakeJobRepo)
        monkeypatch.setattr(ft, "_classify_batch", lambda query, items: [(items[0][0], outcome, 1.0)])
        monkeypatch.setattr(ft, "get_redis", lambda: fake_redis)
        monkeypatch.setattr(ft, "finalize_relevance_filter", SimpleNamespace(delay=lambda jid: None))
        monkeypatch.setattr(ft, "delete_asset_files", lambda a: None)
        monkeypatch.setattr(ft, "trim_excess_assets", lambda jid: trim_calls.append(jid))

        ft.FanIn(fake_redis).start("11111111-1111-1111-1111-111111111111", ft._FANIN_NAME, 1)
        ft.relevance_filter_batch.run("11111111-1111-1111-1111-111111111111", ["22222222-2222-2222-2222-222222222222"])
        return trim_calls

    def test_trims_when_a_pass_lands_after_the_job_is_already_completed(self, monkeypatch):
        import app.tasks.filtering_tasks as ft
        from app.domain.enums import JobStatus

        calls = self._run_batch(monkeypatch, JobStatus.COMPLETED, ft._ACCEPT)
        assert calls == ["11111111-1111-1111-1111-111111111111"]

    def test_trims_when_a_pass_lands_after_the_job_is_already_completed_partial(self, monkeypatch):
        import app.tasks.filtering_tasks as ft
        from app.domain.enums import JobStatus

        calls = self._run_batch(monkeypatch, JobStatus.COMPLETED_PARTIAL, ft._ACCEPT)
        assert calls == ["11111111-1111-1111-1111-111111111111"]

    def test_does_not_trim_while_the_job_is_still_running(self, monkeypatch):
        import app.tasks.filtering_tasks as ft
        from app.domain.enums import JobStatus

        calls = self._run_batch(monkeypatch, JobStatus.RUNNING, ft._ACCEPT)
        assert calls == []

    def test_does_not_trim_when_the_batch_had_no_pass(self, monkeypatch):
        import app.tasks.filtering_tasks as ft
        from app.domain.enums import JobStatus

        calls = self._run_batch(monkeypatch, JobStatus.COMPLETED, ft._REJECT)
        assert calls == []


class TestEmptyTopupRoundStillEndsTheJob:
    """A top-up round that fetches nothing new must still reach completion.

    Seen on a SUN RGB-D job: every matching image was already collected, so
    round 2 downloaded nothing and run_relevance_filter found no PROMOTED
    assets. It went straight to finalize -- but round 1's finalize guard (5 min
    TTL) was still set, so finalize was skipped as a duplicate and the job sat
    in `running` for the watchdog's 30 minutes before failing.
    """

    @staticmethod
    def _run(monkeypatch, fake_redis, finalize_calls, candidate_ids):
        import contextlib

        import app.tasks.filtering_tasks as ft

        class _CapturingFinalize:
            def delay(self, jid):
                finalize_calls.append(jid)

        class _Job:
            def __init__(self):
                self.config = {}

        class _Jobs:
            def __init__(self, db):
                pass

            def get(self, job_uuid):
                return _Job()

        class _Assets:
            def __init__(self, db):
                pass

            def list_ids_for_job(self, *args, **kwargs):
                return candidate_ids

        monkeypatch.setattr(ft, "get_redis", lambda: fake_redis)
        monkeypatch.setattr(ft, "finalize_relevance_filter", _CapturingFinalize())
        monkeypatch.setattr(ft, "session_scope", lambda: contextlib.nullcontext(object()))
        monkeypatch.setattr(ft, "JobRepository", _Jobs)
        monkeypatch.setattr(ft, "AssetRepository", _Assets)
        ft.run_relevance_filter.run("11111111-1111-1111-1111-111111111111")

    def test_no_new_assets_clears_the_previous_rounds_finalize_guard(self, monkeypatch):
        import app.tasks.filtering_tasks as ft

        job_id = "11111111-1111-1111-1111-111111111111"
        fake = _FakeRedis()
        # Round 1's finalize has just run and left its guard behind.
        fake.set(ft._FINALIZE_GUARD_KEY.format(job_id=job_id), "1", ex=300)
        calls: list[str] = []

        self._run(monkeypatch, fake, calls, candidate_ids=[])

        assert calls == [job_id]  # finalize dispatched ...
        assert ft._FINALIZE_GUARD_KEY.format(job_id=job_id) not in fake.store  # ... and not blocked by the stale guard

    def test_finalize_actually_runs_after_an_empty_round(self, monkeypatch):
        import app.tasks.filtering_tasks as ft

        job_id = "11111111-1111-1111-1111-111111111111"
        fake = _FakeRedis()
        fake.set(ft._FINALIZE_GUARD_KEY.format(job_id=job_id), "1", ex=300)

        self._run(monkeypatch, fake, [], candidate_ids=[])

        # The guard finalize itself takes must now be free to acquire.
        assert fake.set(ft._FINALIZE_GUARD_KEY.format(job_id=job_id), "1", nx=True, ex=300) is True

    def test_a_round_with_assets_still_clears_the_guard_and_arms_the_barrier(self, monkeypatch):
        import app.tasks.filtering_tasks as ft

        job_id = "11111111-1111-1111-1111-111111111111"
        fake = _FakeRedis()
        fake.set(ft._FINALIZE_GUARD_KEY.format(job_id=job_id), "1", ex=300)
        dispatched = []

        class _Batch:
            def delay(self, *args):
                dispatched.append(args)

        monkeypatch.setattr(ft, "relevance_filter_batch", _Batch())
        monkeypatch.setattr(ft, "ProgressPublisher", lambda r: type("P", (), {"publish": lambda *a, **k: None})())
        calls: list[str] = []

        self._run(monkeypatch, fake, calls, candidate_ids=["a", "b"])

        assert ft._FINALIZE_GUARD_KEY.format(job_id=job_id) not in fake.store
        assert calls == []  # finalize is left to the last batch
        assert dispatched  # the batches went out


class TestFinalizeWhenEverythingIsJudged:
    """A restart killed filter batches mid-flight. Celery re-delivered enough of them that
    all 1507 images were judged by 12:17, but the killed batches never decremented the
    FanIn counter, so it never reached zero and the job idled until the watchdog's 30
    minute timer (12:47). Finalize must not depend on the counter alone."""

    @staticmethod
    def _run_batch(monkeypatch, counter_left, all_judged):
        import app.tasks.filtering_tasks as ft

        finalize_calls = []

        class _CapturingFinalize:
            def delay(self, jid):
                finalize_calls.append(jid)

        class _FakeFanIn:
            def __init__(self, redis):
                pass

            def finish(self, job_id, name):
                return counter_left

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated failure inside the batch body")

        monkeypatch.setattr(ft, "get_redis", lambda: _FakeRedis())
        monkeypatch.setattr(ft, "FanIn", _FakeFanIn)
        monkeypatch.setattr(ft, "finalize_relevance_filter", _CapturingFinalize())
        monkeypatch.setattr(ft, "session_scope", _boom)
        monkeypatch.setattr(ft, "_all_assets_judged", lambda job_id: all_judged)
        monkeypatch.setattr(ft, "ProgressPublisher", lambda r: type("P", (), {"publish": lambda *a, **k: None})())
        ft.relevance_filter_batch.run("job-1", ["a"])
        return finalize_calls

    def test_finalizes_when_nothing_is_left_even_though_the_counter_is_not_zero(self, monkeypatch):
        assert self._run_batch(monkeypatch, counter_left=2, all_judged=True) == ["job-1"]

    def test_does_not_finalize_while_assets_still_await_a_verdict(self, monkeypatch):
        assert self._run_batch(monkeypatch, counter_left=2, all_judged=False) == []

    def test_still_finalizes_when_the_counter_reaches_zero(self, monkeypatch):
        assert self._run_batch(monkeypatch, counter_left=0, all_judged=False) == ["job-1"]

    def test_a_counter_gone_negative_by_redelivery_still_finalizes(self, monkeypatch):
        assert self._run_batch(monkeypatch, counter_left=-3, all_judged=False) == ["job-1"]


class TestAllAssetsJudged:
    def _patch(self, monkeypatch, promoted_left=None, raises=False):
        import contextlib

        import app.tasks.filtering_tasks as ft

        class _Assets:
            def __init__(self, db):
                pass

            def count_for_job(self, job_uuid, status=None, **kwargs):
                assert status == ft.AssetStatus.PROMOTED
                return promoted_left

        def scope():
            if raises:
                raise RuntimeError("database down")
            return contextlib.nullcontext(object())

        monkeypatch.setattr(ft, "AssetRepository", _Assets)
        monkeypatch.setattr(ft, "session_scope", scope)
        return ft

    def test_true_when_no_asset_is_still_promoted(self, monkeypatch):
        ft = self._patch(monkeypatch, promoted_left=0)

        assert ft._all_assets_judged("11111111-1111-1111-1111-111111111111") is True

    def test_false_while_some_asset_is_still_promoted(self, monkeypatch):
        ft = self._patch(monkeypatch, promoted_left=3)

        assert ft._all_assets_judged("11111111-1111-1111-1111-111111111111") is False

    def test_a_database_error_says_no_rather_than_raising(self, monkeypatch):
        # It runs in a `finally`, which must always reach the counter's continuation.
        ft = self._patch(monkeypatch, raises=True)

        assert ft._all_assets_judged("11111111-1111-1111-1111-111111111111") is False


class TestFilteringIdleThreshold:
    def test_a_dead_chain_is_re_driven_within_ten_minutes_not_thirty(self):
        from app.core.config import Settings

        assert Settings().filtering_stall_threshold_s <= 600


class TestDeferredNasOffloadAccumulation:
    """relevance_filter_batch must accumulate passed IDs in Redis instead of
    dispatching uploads immediately — the upload fires once from finalize."""

    @staticmethod
    def _run_batch(monkeypatch, fake_redis, job_id, asset_ids, outcome=None):
        import app.tasks.filtering_tasks as ft
        from contextlib import contextmanager
        from types import SimpleNamespace

        if outcome is None:
            outcome = ft._ACCEPT

        asset = SimpleNamespace(status=ft.AssetStatus.PROMOTED, nas_path="/nas/x.jpg")
        job = SimpleNamespace(status=None, query="cars")

        class _FakeAssetRepo:
            def __init__(self, db):
                pass

            def get(self, asset_id):
                return asset

            def set_status(self, asset_id, status, relevance_score=None, is_duplicate_of=None):
                asset.status = status

            def count_for_job(self, *a, **kw):
                return 0

        class _FakeJobRepo:
            def __init__(self, db):
                pass

            def get(self, job_id):
                return job

            def increment_counts(self, *args, **kwargs):
                pass

        @contextmanager
        def fake_scope():
            yield object()

        monkeypatch.setattr(ft, "session_scope", fake_scope)
        monkeypatch.setattr(ft, "AssetRepository", _FakeAssetRepo)
        monkeypatch.setattr(ft, "JobRepository", _FakeJobRepo)
        monkeypatch.setattr(ft, "_classify_batch", lambda q, items: [(i[0], outcome, 1.0) for i in items])
        monkeypatch.setattr(ft, "get_redis", lambda: fake_redis)
        monkeypatch.setattr(ft, "finalize_relevance_filter", SimpleNamespace(delay=lambda jid: None))
        monkeypatch.setattr(ft, "delete_asset_files", lambda a: None)
        monkeypatch.setattr(ft, "trim_excess_assets", lambda jid: None)

        ft.FanIn(fake_redis).start(job_id, ft._FANIN_NAME, 1)
        return ft.relevance_filter_batch.run(job_id, asset_ids)

    def test_passed_ids_accumulate_in_redis_not_dispatched_immediately(self, monkeypatch):
        import uuid
        import app.tasks.nas_tasks as nt

        fake = _FakeRedis()
        job_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        aid1 = str(uuid.uuid4())
        aid2 = str(uuid.uuid4())

        # Ensure no direct dispatch happens
        monkeypatch.setattr(nt.offload_asset_batch, "delay", lambda ids: pytest.fail("must not dispatch mid-pass"))

        self._run_batch(monkeypatch, fake, job_id, [aid1, aid2])

        pending = fake.lists.get(f"job:{job_id}:offload_pending", [])
        assert len(pending) == 2
        decoded = {v.decode() if isinstance(v, bytes) else v for v in pending}
        assert decoded == {aid1, aid2}

    def test_rejected_assets_are_not_accumulated(self, monkeypatch):
        import uuid
        import app.tasks.filtering_tasks as ft

        fake = _FakeRedis()
        job_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

        self._run_batch(monkeypatch, fake, job_id, [str(uuid.uuid4())], outcome=ft._REJECT)

        assert fake.lists.get(f"job:{job_id}:offload_pending", []) == []

    def test_multiple_batches_accumulate_ids_into_the_same_list(self, monkeypatch):
        import uuid
        import app.tasks.filtering_tasks as ft

        fake = _FakeRedis()
        job_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        aid1, aid2, aid3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())

        # First batch passes 2 assets
        self._run_batch(monkeypatch, fake, job_id, [aid1, aid2])
        # FanIn was consumed by first run; arm it again for the second batch
        ft.FanIn(fake).start(job_id, ft._FANIN_NAME, 1)
        self._run_batch(monkeypatch, fake, job_id, [aid3])

        pending = fake.lists.get(f"job:{job_id}:offload_pending", [])
        decoded = {v.decode() if isinstance(v, bytes) else v for v in pending}
        assert decoded == {aid1, aid2, aid3}


class TestFinalizeDoesNotTriggerOffload:
    """NAS offload is now triggered once per job, at the true end of it (from
    plan_topup_round's final round — see test_topup_flow.py), not per
    relevance-filter pass. finalize_relevance_filter must not call it."""

    def test_finalize_does_not_call_schedule_offload_for_job(self, monkeypatch):
        import app.tasks.filtering_tasks as ft
        import app.tasks.nas_tasks as nt
        from contextlib import contextmanager
        from types import SimpleNamespace

        job_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        offload_calls: list[str] = []

        monkeypatch.setattr(nt, "schedule_offload_for_job", lambda jid: offload_calls.append(jid))

        class _FakeAssetRepo:
            def __init__(self, db):
                pass

            def count_for_job(self, *a, status=None, **kw):
                return 0

        class _FakeJobRepo:
            def __init__(self, db):
                pass

        @contextmanager
        def fake_scope():
            yield object()

        fake_redis = _FakeRedis()
        monkeypatch.setattr(ft, "get_redis", lambda: fake_redis)
        monkeypatch.setattr(ft, "session_scope", fake_scope)
        monkeypatch.setattr(ft, "AssetRepository", _FakeAssetRepo)
        monkeypatch.setattr(ft, "JobRepository", _FakeJobRepo)
        monkeypatch.setattr(ft, "reconcile", lambda db, job_uuid: None)

        class _FakePublisher:
            def __init__(self, redis):
                pass

            def publish(self, *a, **kw):
                pass

        monkeypatch.setattr(ft, "ProgressPublisher", _FakePublisher)
        monkeypatch.setattr(ft, "run_deduplication", SimpleNamespace(delay=lambda jid: None))

        ft.finalize_relevance_filter.run(job_id)

        assert offload_calls == [], "finalize_relevance_filter must not trigger per-round offload"

    def test_relevance_filter_batch_records_review_and_retry_pending(self, monkeypatch):
        """Verifies that UNCERTAIN and ERROR outcomes transition assets to REVIEW_PENDING and RETRY_PENDING."""
        import uuid
        from types import SimpleNamespace
        from contextlib import contextmanager
        from app.domain.enums import AssetStatus
        import app.tasks.filtering_tasks as ft

        job_id = str(uuid.uuid4())
        asset_1 = str(uuid.uuid4())
        asset_2 = str(uuid.uuid4())
        asset_3 = str(uuid.uuid4())
        asset_4 = str(uuid.uuid4())

        status_updates = {}

        class _MockAssetRepo:
            def __init__(self, db):
                pass

            def get(self, aid):
                return SimpleNamespace(id=aid, nas_path=f"/fake/{aid}.jpg", status=AssetStatus.PROMOTED)

            def set_status(self, aid, status, relevance_score=None):
                status_updates[aid] = (status, relevance_score)

        class _MockJobRepo:
            def __init__(self, db):
                pass

            def get(self, jid):
                return SimpleNamespace(id=jid, query="car", config={}, status="running")

            def increment_counts(self, *a, **kw):
                pass

        @contextmanager
        def fake_scope():
            yield object()

        fake_redis = _FakeRedis()
        monkeypatch.setattr(ft, "get_redis", lambda: fake_redis)
        monkeypatch.setattr(ft, "session_scope", fake_scope)
        monkeypatch.setattr(ft, "AssetRepository", _MockAssetRepo)
        monkeypatch.setattr(ft, "JobRepository", _MockJobRepo)
        monkeypatch.setattr(ft.asset_files, "local_path_for_read", lambda p: p)

        # Mock classify decisions: 1 accept, 1 reject, 1 review, 1 error
        mock_decisions = [
            (asset_1, ft._ACCEPT, 1.0),
            (asset_2, ft._REJECT, 0.0),
            (asset_3, ft._REVIEW, 0.5),
            (asset_4, ft._ERROR, None),
        ]
        monkeypatch.setattr(ft, "_classify_batch", lambda q, items: mock_decisions)
        monkeypatch.setattr(ft, "delete_asset_files", lambda a: None)
        monkeypatch.setattr(ft, "finalize_relevance_filter", SimpleNamespace(delay=lambda jid: None))

        res = ft.relevance_filter_batch.run(job_id, [asset_1, asset_2, asset_3, asset_4])

        assert res["passed"] == 1
        assert res["rejected"] == 1
        assert res["review_pending"] == 1
        assert res["errored"] == 1

        assert status_updates[uuid.UUID(asset_1)][0] == AssetStatus.RELEVANCE_PASSED
        assert status_updates[uuid.UUID(asset_2)][0] == AssetStatus.RELEVANCE_REJECTED
        assert status_updates[uuid.UUID(asset_3)][0] == AssetStatus.REVIEW_PENDING
        assert status_updates[uuid.UUID(asset_4)][0] == AssetStatus.RETRY_PENDING
